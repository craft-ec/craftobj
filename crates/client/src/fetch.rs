//! Fetch client with connection pooling, parallel piece fetching, streaming, and range requests.
//!
//! Core design: client-side intelligence, dumb storage nodes. The client maintains a capped pool
//! of concurrent connections and pipelines piece requests across them.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use craftec_erasure::{check_independence, CodedPiece};
use craftobj_core::{ContentId, ContentManifest, CraftObjError, Result};
use tracing::{debug, warn};

/// Identifier for a provider (opaque, wraps a peer ID string).
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ProviderId(pub String);

impl std::fmt::Display for ProviderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A fetched piece from a provider.
#[derive(Debug, Clone)]
pub struct FetchedPiece {
    pub segment_index: u32,
    pub coefficients: Vec<u8>,
    pub data: Vec<u8>,
    pub provider: ProviderId,
    pub fetch_duration: Duration,
}

/// Trait for requesting pieces from providers. Implementations handle the actual
/// network transport (libp2p streams, mock channels, etc).
#[async_trait::async_trait]
pub trait PieceRequester: Send + Sync {
    /// Request any piece for the given content/segment from the specified provider.
    /// Returns (coefficients, data) on success.
    async fn request_piece(
        &self,
        provider: &ProviderId,
        content_id: &ContentId,
        segment_index: u32,
    ) -> Result<(Vec<u8>, Vec<u8>)>;
}

/// Per-provider performance statistics.
#[derive(Debug, Clone)]
pub struct ProviderStats {
    pub pieces_fetched: u64,
    pub bytes_fetched: u64,
    pub total_duration: Duration,
    pub failures: u64,
    pub last_failure: Option<Instant>,
    pub last_success: Option<Instant>,
}

impl ProviderStats {
    fn new() -> Self {
        Self {
            pieces_fetched: 0,
            bytes_fetched: 0,
            total_duration: Duration::ZERO,
            failures: 0,
            last_failure: None,
            last_success: None,
        }
    }

    /// Throughput in bytes/sec. Returns 0 if no data fetched.
    pub fn throughput_bps(&self) -> f64 {
        let secs = self.total_duration.as_secs_f64();
        if secs < 0.001 {
            return 0.0;
        }
        self.bytes_fetched as f64 / secs
    }

    /// Average latency per piece in milliseconds.
    pub fn avg_latency_ms(&self) -> f64 {
        if self.pieces_fetched == 0 {
            return f64::MAX;
        }
        self.total_duration.as_secs_f64() * 1000.0 / self.pieces_fetched as f64
    }

    fn record_success(&mut self, bytes: usize, duration: Duration) {
        self.pieces_fetched += 1;
        self.bytes_fetched += bytes as u64;
        self.total_duration += duration;
        self.last_success = Some(Instant::now());
    }

    fn record_failure(&mut self) {
        self.failures += 1;
        self.last_failure = Some(Instant::now());
    }
}

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct FetchConfig {
    /// Maximum concurrent connections (default 30).
    pub max_connections: usize,
    /// Timeout per piece request (default 30s).
    pub piece_timeout: Duration,
    /// Minimum throughput in bytes/sec before a provider is considered slow (default 10KB/s).
    pub min_throughput_bps: f64,
    /// Maximum consecutive failures before dropping a provider (default 3).
    pub max_consecutive_failures: u32,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            max_connections: 30,
            piece_timeout: Duration::from_secs(30),
            min_throughput_bps: 10_000.0,
            max_consecutive_failures: 3,
        }
    }
}

/// Tracks segment completion state during a fetch operation.
struct SegmentState {
    /// k required for this segment.
    k: usize,
    /// Collected independent pieces so far.
    pieces: Vec<CodedPiece>,
    /// Coefficient vectors for independence checking.
    coeff_matrix: Vec<Vec<u8>>,
    /// Current rank (number of independent pieces).
    rank: usize,
    /// Whether this segment is fully complete.
    complete: bool,
}

impl SegmentState {
    fn new(k: usize) -> Self {
        Self {
            k,
            pieces: Vec::with_capacity(k),
            coeff_matrix: Vec::with_capacity(k),
            rank: 0,
            complete: false,
        }
    }

    /// Try to add a piece. Returns true if piece was independent and added.
    fn try_add_piece(&mut self, piece: CodedPiece) -> bool {
        if self.complete {
            return false;
        }

        // Check independence
        let mut test_matrix = self.coeff_matrix.clone();
        test_matrix.push(piece.coefficients.clone());
        let new_rank = check_independence(&test_matrix);

        if new_rank > self.rank {
            self.coeff_matrix.push(piece.coefficients.clone());
            self.pieces.push(piece);
            self.rank = new_rank;
            if self.rank >= self.k {
                self.complete = true;
            }
            true
        } else {
            // Dependent piece — discard
            false
        }
    }
}

/// Connection pool for parallel piece fetching.
pub struct ConnectionPool {
    config: FetchConfig,
    requester: Arc<dyn PieceRequester>,
    /// Active providers with their stats.
    provider_stats: HashMap<ProviderId, ProviderStats>,
    /// Ordered list of active providers (best first).
    active_providers: Vec<ProviderId>,
    /// Backup providers not yet in use.
    backup_providers: VecDeque<ProviderId>,
}

impl ConnectionPool {
    /// Create a new connection pool.
    pub fn new(
        requester: Arc<dyn PieceRequester>,
        providers: Vec<ProviderId>,
        config: FetchConfig,
    ) -> Self {
        Self::with_geo_preference(requester, providers, config, HashMap::new(), None)
    }

    /// Create a new connection pool with geographic preference.
    /// Providers with matching regions are preferred (sorted to front).
    pub fn with_geo_preference(
        requester: Arc<dyn PieceRequester>,
        mut providers: Vec<ProviderId>,
        config: FetchConfig,
        provider_regions: HashMap<ProviderId, String>,
        local_region: Option<String>,
    ) -> Self {
        // Sort providers: matching region first, then others
        if let Some(ref my_region) = local_region {
            providers.sort_by(|a, b| {
                let a_match = provider_regions.get(a).map(|r| r == my_region).unwrap_or(false);
                let b_match = provider_regions.get(b).map(|r| r == my_region).unwrap_or(false);
                b_match.cmp(&a_match) // true (matching) sorts first
            });
        }

        let max = config.max_connections.min(providers.len());
        let (active, backup): (Vec<_>, Vec<_>) = providers
            .into_iter()
            .enumerate()
            .partition(|(i, _)| *i < max);

        let active_providers: Vec<ProviderId> = active.into_iter().map(|(_, p)| p).collect();
        let backup_providers: VecDeque<ProviderId> = backup.into_iter().map(|(_, p)| p).collect();

        let mut provider_stats = HashMap::new();
        for p in &active_providers {
            provider_stats.insert(p.clone(), ProviderStats::new());
        }

        Self {
            config,
            requester,
            provider_stats,
            active_providers,
            backup_providers,
        }
    }

    /// Get stats for a provider.
    pub fn stats(&self, provider: &ProviderId) -> Option<&ProviderStats> {
        self.provider_stats.get(provider)
    }

    /// Get all active providers.
    pub fn active_providers(&self) -> &[ProviderId] {
        &self.active_providers
    }

    /// Replace a slow/failed provider with a backup. Returns the replacement if available.
    pub fn replace_provider(&mut self, provider: &ProviderId) -> Option<ProviderId> {
        if let Some(replacement) = self.backup_providers.pop_front() {
            self.active_providers.retain(|p| p != provider);
            self.provider_stats
                .insert(replacement.clone(), ProviderStats::new());
            self.active_providers.push(replacement.clone());
            debug!("Replaced provider {} with {}", provider, replacement);
            Some(replacement)
        } else {
            // No backup available, just remove the bad one
            self.active_providers.retain(|p| p != provider);
            debug!("Dropped provider {} (no backup available)", provider);
            None
        }
    }

    /// Check if a provider should be dropped based on performance.
    fn should_drop(&self, provider: &ProviderId) -> bool {
        if let Some(stats) = self.provider_stats.get(provider) {
            // Drop if too many consecutive failures
            if stats.failures >= self.config.max_consecutive_failures as u64
                && stats.last_failure > stats.last_success
            {
                return true;
            }
            // Drop if throughput is too low (after at least 3 pieces to be fair)
            if stats.pieces_fetched >= 3 && stats.throughput_bps() < self.config.min_throughput_bps {
                return true;
            }
        }
        false
    }

    /// Fetch all segments for a content, returning collected pieces per segment.
    /// This is the main parallel fetch entry point.
    pub async fn fetch_segments(
        &mut self,
        content_id: &ContentId,
        manifest: &ContentManifest,
        segment_indices: &[u32],
    ) -> Result<BTreeMap<u32, Vec<CodedPiece>>> {
        let mut states: BTreeMap<u32, SegmentState> = BTreeMap::new();
        for &seg_idx in segment_indices {
            let k = manifest.k_for_segment(seg_idx as usize);
            states.insert(seg_idx, SegmentState::new(k));
        }

        // Build work queue: (segment_index) — each entry = one piece needed
        let mut work_queue: VecDeque<u32> = VecDeque::new();
        for &seg_idx in segment_indices {
            let k = manifest.k_for_segment(seg_idx as usize);
            for _ in 0..k {
                work_queue.push_back(seg_idx);
            }
        }

        // Track which segments are complete
        let mut completed_segments: HashSet<u32> = HashSet::new();

        // Use FuturesUnordered for concurrent piece requests
        use futures::stream::{FuturesUnordered, StreamExt};

        type FetchFuture = std::pin::Pin<Box<dyn std::future::Future<Output = std::result::Result<FetchedPiece, (ProviderId, u32, String)>> + Send>>;

        let mut in_flight: FuturesUnordered<FetchFuture> = FuturesUnordered::new();

        // Assign initial work to providers
        let mut provider_idx = 0;
        let max_inflight = self.active_providers.len().min(self.config.max_connections);

        // Helper: spawn a piece request future
        let spawn_request = |requester: &Arc<dyn PieceRequester>,
                             provider: &ProviderId,
                             content_id: &ContentId,
                             segment_index: u32,
                             timeout: Duration|
         -> FetchFuture {
            let requester = requester.clone();
            let provider = provider.clone();
            let cid = *content_id;
            Box::pin(async move {
                let start = Instant::now();
                match tokio::time::timeout(timeout, requester.request_piece(&provider, &cid, segment_index)).await {
                    Ok(Ok((coefficients, data))) => Ok(FetchedPiece {
                        segment_index,
                        coefficients,
                        data,
                        provider: provider.clone(),
                        fetch_duration: start.elapsed(),
                    }),
                    Ok(Err(e)) => Err((provider, segment_index, e.to_string())),
                    Err(_) => Err((provider, segment_index, "timeout".into())),
                }
            })
        };

        // Launch initial batch
        while in_flight.len() < max_inflight {
            if let Some(seg_idx) = self.next_needed_segment(&work_queue, &completed_segments) {
                if provider_idx < self.active_providers.len() {
                    let provider = self.active_providers[provider_idx % self.active_providers.len()].clone();
                    provider_idx += 1;
                    in_flight.push(spawn_request(
                        &self.requester,
                        &provider,
                        content_id,
                        seg_idx,
                        self.config.piece_timeout,
                    ));
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        // Process results and spawn new requests
        while let Some(result) = in_flight.next().await {
            match result {
                Ok(fetched) => {
                    let seg_idx = fetched.segment_index;
                    let provider = fetched.provider.clone();
                    let data_len = fetched.data.len();
                    let duration = fetched.fetch_duration;

                    // Record success
                    if let Some(stats) = self.provider_stats.get_mut(&provider) {
                        stats.record_success(data_len, duration);
                    }

                    // Try to add to segment
                    if let Some(state) = states.get_mut(&seg_idx) {
                        let piece = CodedPiece {
                            data: fetched.data,
                            coefficients: fetched.coefficients,
                        };
                        let added = state.try_add_piece(piece);
                        if state.complete {
                            completed_segments.insert(seg_idx);
                            debug!("Segment {} complete", seg_idx);
                        }
                        if !added {
                            debug!(
                                "Discarded dependent piece for segment {} from {}",
                                seg_idx, provider
                            );
                        }
                    }
                }
                Err((provider, _seg_idx, err)) => {
                    warn!("Piece request failed from {}: {}", provider, err);
                    if let Some(stats) = self.provider_stats.get_mut(&provider) {
                        stats.record_failure();
                    }

                    // Check if provider should be dropped
                    if self.should_drop(&provider) {
                        self.replace_provider(&provider);
                    }
                }
            }

            // Check if all segments are done
            if completed_segments.len() == segment_indices.len() {
                break;
            }

            // Spawn replacement request if there's work left
            if let Some(seg_idx) = self.next_needed_segment(&work_queue, &completed_segments) {
                if !self.active_providers.is_empty() {
                    let provider =
                        self.active_providers[provider_idx % self.active_providers.len()].clone();
                    provider_idx += 1;
                    in_flight.push(spawn_request(
                        &self.requester,
                        &provider,
                        content_id,
                        seg_idx,
                        self.config.piece_timeout,
                    ));
                }
            }
        }

        // Check completeness
        for &seg_idx in segment_indices {
            if !completed_segments.contains(&seg_idx) {
                return Err(CraftObjError::TransferError(format!(
                    "failed to complete segment {} (got {}/{} independent pieces)",
                    seg_idx,
                    states[&seg_idx].rank,
                    states[&seg_idx].k,
                )));
            }
        }

        // Extract pieces
        let result: BTreeMap<u32, Vec<CodedPiece>> = states
            .into_iter()
            .map(|(idx, state)| (idx, state.pieces))
            .collect();

        Ok(result)
    }

    /// Find next segment that needs work.
    fn next_needed_segment(
        &self,
        work_queue: &VecDeque<u32>,
        completed: &HashSet<u32>,
    ) -> Option<u32> {
        work_queue.iter().find(|&&seg| !completed.contains(&seg)).copied()
    }
}

/// Streaming fetch mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingMode {
    /// Decode segments in order (for streaming media).
    Sequential,
    /// Decode whichever segment completes first (for downloads).
    Parallel,
}

/// Callback type for streaming fetch.
pub type SegmentCallback = Box<dyn FnMut(u32, Vec<u8>) -> Result<()> + Send>;

/// Fetch content with streaming — calls the callback as each segment is decoded.
#[allow(clippy::too_many_arguments)]
pub async fn fetch_streaming(
    requester: Arc<dyn PieceRequester>,
    content_id: &ContentId,
    manifest: &ContentManifest,
    providers: Vec<ProviderId>,
    config: FetchConfig,
    mode: StreamingMode,
    mut callback: SegmentCallback,
) -> Result<()> {
    use craftec_erasure::{segmenter, ErasureConfig};

    let erasure_config = ErasureConfig {
        piece_size: manifest.piece_size(),
        segment_size: manifest.segment_size(),
        ..Default::default()
    };

    let all_segments: Vec<u32> = (0..manifest.segment_count() as u32).collect();
    let mut pool = ConnectionPool::new(requester, providers, config);

    match mode {
        StreamingMode::Parallel => {
            // Fetch all segments in parallel, deliver as they complete
            let pieces = pool.fetch_segments(content_id, manifest, &all_segments).await?;

            for seg_idx in 0..manifest.segment_count() as u32 {
                if let Some(seg_pieces) = pieces.get(&seg_idx) {
                    let mut single_seg = BTreeMap::new();
                    single_seg.insert(seg_idx, seg_pieces.clone());

                    let seg_size = if (seg_idx as usize + 1) < manifest.segment_count() {
                        manifest.segment_size()
                    } else {
                        (manifest.total_size as usize) - (seg_idx as usize * manifest.segment_size())
                    };

                    let decoded = segmenter::decode_and_reassemble(
                        &single_seg,
                        1,
                        &erasure_config,
                        seg_size,
                    )
                    .map_err(|e| CraftObjError::ErasureError(e.to_string()))?;

                    callback(seg_idx, decoded)?;
                }
            }
        }
        StreamingMode::Sequential => {
            // Fetch and decode segments one at a time, in order
            for seg_idx in 0..manifest.segment_count() as u32 {
                let pieces = pool.fetch_segments(content_id, manifest, &[seg_idx]).await?;

                if let Some(seg_pieces) = pieces.get(&seg_idx) {
                    let mut single_seg = BTreeMap::new();
                    single_seg.insert(seg_idx, seg_pieces.clone());

                    let seg_size = if (seg_idx as usize + 1) < manifest.segment_count() {
                        manifest.segment_size()
                    } else {
                        (manifest.total_size as usize)
                            - (seg_idx as usize * manifest.segment_size())
                    };

                    let decoded = segmenter::decode_and_reassemble(
                        &single_seg,
                        1,
                        &erasure_config,
                        seg_size,
                    )
                    .map_err(|e| CraftObjError::ErasureError(e.to_string()))?;

                    callback(seg_idx, decoded)?;
                }
            }
        }
    }

    Ok(())
}

/// Calculate the segment range covering a byte range.
pub fn segments_for_range(
    manifest: &ContentManifest,
    byte_offset: u64,
    byte_length: u64,
) -> Vec<u32> {
    if manifest.segment_size() == 0 || manifest.segment_count() == 0 {
        return vec![];
    }
    let seg_size = manifest.segment_size() as u64;
    let start_seg = (byte_offset / seg_size) as u32;
    let end_byte = byte_offset.saturating_add(byte_length).saturating_sub(1);
    let end_seg = (end_byte / seg_size) as u32;
    let max_seg = (manifest.segment_count() as u32).saturating_sub(1);

    (start_seg..=end_seg.min(max_seg)).collect()
}

/// Fetch only the segments covering a byte range, returning the requested bytes.
pub async fn fetch_range(
    requester: Arc<dyn PieceRequester>,
    content_id: &ContentId,
    manifest: &ContentManifest,
    providers: Vec<ProviderId>,
    config: FetchConfig,
    byte_offset: u64,
    byte_length: u64,
) -> Result<Vec<u8>> {
    use craftec_erasure::{segmenter, ErasureConfig};

    let segments = segments_for_range(manifest, byte_offset, byte_length);
    if segments.is_empty() {
        return Ok(vec![]);
    }

    let erasure_config = ErasureConfig {
        piece_size: manifest.piece_size(),
        segment_size: manifest.segment_size(),
        ..Default::default()
    };

    let mut pool = ConnectionPool::new(requester, providers, config);
    let pieces = pool.fetch_segments(content_id, manifest, &segments).await?;

    // Decode each segment and extract the requested byte range
    let seg_size = manifest.segment_size();
    let mut result = Vec::with_capacity(byte_length as usize);

    for &seg_idx in &segments {
        if let Some(seg_pieces) = pieces.get(&seg_idx) {
            let mut single_seg = BTreeMap::new();
            single_seg.insert(seg_idx, seg_pieces.clone());

            let actual_seg_size = if (seg_idx as usize + 1) < manifest.segment_count() {
                seg_size
            } else {
                (manifest.total_size as usize) - (seg_idx as usize * seg_size)
            };

            let decoded = segmenter::decode_and_reassemble(
                &single_seg,
                1,
                &erasure_config,
                actual_seg_size,
            )
            .map_err(|e| CraftObjError::ErasureError(e.to_string()))?;

            // Calculate which bytes of this segment we need
            let seg_start_byte = seg_idx as u64 * seg_size as u64;
            let range_start = byte_offset.saturating_sub(seg_start_byte) as usize;
            let range_end = ((byte_offset + byte_length) - seg_start_byte) as usize;
            let range_end = range_end.min(decoded.len());
            let range_start = range_start.min(range_end);

            result.extend_from_slice(&decoded[range_start..range_end]);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    /// Mock piece requester that serves pieces from an in-memory store.
    struct MockRequester {
        /// (content_id_hex, segment_index) -> Vec<(coefficients, data)>
        pieces: StdMutex<HashMap<(String, u32), Vec<(Vec<u8>, Vec<u8>)>>>,
        /// Track request counts per provider
        request_counts: StdMutex<HashMap<String, u64>>,
        /// Optional delay per provider (for slow provider testing)
        delays: StdMutex<HashMap<String, Duration>>,
        /// Optional failure flag per provider
        fail_providers: StdMutex<HashSet<String>>,
    }

    impl MockRequester {
        fn new() -> Self {
            Self {
                pieces: StdMutex::new(HashMap::new()),
                request_counts: StdMutex::new(HashMap::new()),
                delays: StdMutex::new(HashMap::new()),
                fail_providers: StdMutex::new(HashSet::new()),
            }
        }

        fn add_piece(
            &self,
            content_id: &ContentId,
            segment_index: u32,
            coefficients: Vec<u8>,
            data: Vec<u8>,
        ) {
            let key = (content_id.to_hex(), segment_index);
            let mut pieces = self.pieces.lock().unwrap();
            pieces.entry(key).or_default().push((coefficients, data));
        }

        #[allow(dead_code)] // Test utility for future use
        fn set_delay(&self, provider: &ProviderId, delay: Duration) {
            self.delays
                .lock()
                .unwrap()
                .insert(provider.0.clone(), delay);
        }

        fn set_fail(&self, provider: &ProviderId) {
            self.fail_providers
                .lock()
                .unwrap()
                .insert(provider.0.clone());
        }

        #[allow(dead_code)] // Test utility for future use
        fn request_count(&self, provider: &ProviderId) -> u64 {
            *self
                .request_counts
                .lock()
                .unwrap()
                .get(&provider.0)
                .unwrap_or(&0)
        }
    }

    #[async_trait::async_trait]
    impl PieceRequester for MockRequester {
        async fn request_piece(
            &self,
            provider: &ProviderId,
            content_id: &ContentId,
            segment_index: u32,
        ) -> Result<(Vec<u8>, Vec<u8>)> {
            // Track request count
            {
                let mut counts = self.request_counts.lock().unwrap();
                *counts.entry(provider.0.clone()).or_insert(0) += 1;
            }

            // Check if provider should fail
            if self.fail_providers.lock().unwrap().contains(&provider.0) {
                return Err(CraftObjError::TransferError("provider failed".into()));
            }

            // Apply delay
            let delay = self.delays.lock().unwrap().get(&provider.0).copied();
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }

            // Return a random piece from our store
            let key = (content_id.to_hex(), segment_index);
            let pieces = self.pieces.lock().unwrap();
            if let Some(available) = pieces.get(&key) {
                // Pick a random piece
                let idx = {
                    let count = self.request_counts.lock().unwrap();
                    let n = *count.get(&provider.0).unwrap_or(&0) as usize;
                    n.wrapping_sub(1) % available.len()
                };
                let (coeff, data) = &available[idx];
                Ok((coeff.clone(), data.clone()))
            } else {
                Err(CraftObjError::ContentNotFound(format!(
                    "no pieces for segment {}",
                    segment_index
                )))
            }
        }
    }

    fn make_manifest(total_size: u64, _segment_count: usize) -> ContentManifest {
        ContentManifest {
            content_id: ContentId::from_bytes(&[0u8; 32]),
            total_size,
            creator: String::new(),
            signature: vec![],
            verification: craftec_erasure::ContentVerificationRecord {
                file_size: total_size,
                segment_hashes: vec![],
            },
        }
    }

    /// Create k independent coefficient vectors of length k (identity matrix).
    fn make_independent_coefficients(k: usize) -> Vec<Vec<u8>> {
        (0..k)
            .map(|i| {
                let mut v = vec![0u8; k];
                v[i] = 1;
                v
            })
            .collect()
    }

    #[tokio::test]
    async fn test_connection_pool_creation() {
        let requester = Arc::new(MockRequester::new());
        let providers: Vec<ProviderId> = (0..5)
            .map(|i| ProviderId(format!("provider-{}", i)))
            .collect();

        let config = FetchConfig {
            max_connections: 3,
            ..Default::default()
        };

        let pool = ConnectionPool::new(requester, providers, config);
        assert_eq!(pool.active_providers().len(), 3);
        assert_eq!(pool.backup_providers.len(), 2);
    }

    #[tokio::test]
    async fn test_parallel_fetch_single_segment() {
        let requester = Arc::new(MockRequester::new());
        let manifest = make_manifest(1_000, 1);
        let content_id = ContentId::from_bytes(b"test-parallel");
        let k = manifest.k_for_segment(0); // 10

        // Add k independent pieces
        let coeffs = make_independent_coefficients(k);
        for coeff in &coeffs {
            requester.add_piece(&content_id, 0, coeff.clone(), vec![42u8; 100]);
        }

        let providers: Vec<ProviderId> = (0..3)
            .map(|i| ProviderId(format!("p{}", i)))
            .collect();

        let mut pool = ConnectionPool::new(
            requester.clone(),
            providers,
            FetchConfig::default(),
        );

        let result = pool.fetch_segments(&content_id, &manifest, &[0]).await;
        assert!(result.is_ok());
        let pieces = result.unwrap();
        assert!(pieces.contains_key(&0));
        assert!(pieces[&0].len() >= k);
    }

    #[tokio::test]
    async fn test_parallel_fetch_multiple_segments() {
        let requester = Arc::new(MockRequester::new());
        let manifest = make_manifest(10_485_761, 2);
        let content_id = ContentId::from_bytes(b"test-multi-seg");

        for seg_idx in 0..2u32 {
            let k = manifest.k_for_segment(seg_idx as usize);
            let coeffs = make_independent_coefficients(k);
            for coeff in &coeffs {
                requester.add_piece(&content_id, seg_idx, coeff.clone(), vec![seg_idx as u8; 100]);
            }
        }

        let providers: Vec<ProviderId> = (0..5)
            .map(|i| ProviderId(format!("p{}", i)))
            .collect();

        let mut pool = ConnectionPool::new(requester, providers, FetchConfig::default());

        let result = pool
            .fetch_segments(&content_id, &manifest, &[0, 1])
            .await;
        assert!(result.is_ok());
        let pieces = result.unwrap();
        assert!(pieces.contains_key(&0));
        assert!(pieces.contains_key(&1));
    }

    #[tokio::test]
    async fn test_slow_provider_replacement() {
        let requester = Arc::new(MockRequester::new());
        let manifest = make_manifest(1_000, 1);
        let content_id = ContentId::from_bytes(b"test-slow");
        let k = manifest.k_for_segment(0);

        let coeffs = make_independent_coefficients(k);
        for coeff in &coeffs {
            requester.add_piece(&content_id, 0, coeff.clone(), vec![1u8; 100]);
        }

        let slow_provider = ProviderId("slow".into());
        let fast_provider = ProviderId("fast".into());
        let backup_provider = ProviderId("backup".into());

        // Make the slow provider fail every time
        requester.set_fail(&slow_provider);

        let config = FetchConfig {
            max_connections: 2,
            max_consecutive_failures: 2,
            ..Default::default()
        };

        let mut pool = ConnectionPool::new(
            requester.clone(),
            vec![slow_provider.clone(), fast_provider.clone(), backup_provider.clone()],
            config,
        );

        let result = pool.fetch_segments(&content_id, &manifest, &[0]).await;
        assert!(result.is_ok());

        // The slow provider should have been dropped
        let slow_stats = pool.stats(&slow_provider);
        if let Some(stats) = slow_stats {
            assert!(stats.failures >= 1);
        }
    }

    #[tokio::test]
    async fn test_range_request_segments() {
        let manifest = make_manifest(41_943_041, 5);

        // Request bytes spanning segments 1 and 2
        let segments = segments_for_range(&manifest, 10_485_760, 10_485_761);
        assert_eq!(segments, vec![1, 2]);

        // Request first 500 bytes (segment 0 only)
        let segments = segments_for_range(&manifest, 0, 500);
        assert_eq!(segments, vec![0]);

        // Request last segment
        let segments = segments_for_range(&manifest, 41_943_040, 1);
        assert_eq!(segments, vec![4]);
    }

    #[tokio::test]
    async fn test_independence_checking_during_fetch() {
        let requester = Arc::new(MockRequester::new());
        let manifest = make_manifest(300, 1); // k=3
        let content_id = ContentId::from_bytes(b"test-independence");
        let k = manifest.k_for_segment(0);

        // Add k independent pieces + 1 dependent piece
        let coeffs = make_independent_coefficients(k);
        for coeff in &coeffs {
            requester.add_piece(&content_id, 0, coeff.clone(), vec![1u8; 100]);
        }
        // Add a dependent piece (same as first)
        requester.add_piece(&content_id, 0, coeffs[0].clone(), vec![1u8; 100]);

        let providers = vec![ProviderId("p0".into())];
        let mut pool = ConnectionPool::new(requester, providers, FetchConfig::default());

        let result = pool.fetch_segments(&content_id, &manifest, &[0]).await;
        assert!(result.is_ok());
        let pieces = result.unwrap();
        // Should have exactly k pieces (dependent one discarded)
        assert_eq!(pieces[&0].len(), k);
    }

    #[tokio::test]
    async fn test_range_fetch() {
        let requester = Arc::new(MockRequester::new());
        // 2 segments of 1000 bytes each, k=10 pieces of 100 bytes
        let manifest = make_manifest(10_485_761, 2);
        let content_id = ContentId::from_bytes(b"test-range-fetch");

        for seg_idx in 0..2u32 {
            let k = manifest.k_for_segment(seg_idx as usize);
            let coeffs = make_independent_coefficients(k);
            for coeff in &coeffs {
                requester.add_piece(
                    &content_id,
                    seg_idx,
                    coeff.clone(),
                    vec![seg_idx as u8 + 1; 100],
                );
            }
        }

        let _providers = vec![ProviderId("p0".into()), ProviderId("p1".into())];

        // Only fetch segment 1 (starts at byte 10_485_760)
        let segs = segments_for_range(&manifest, 10_485_760, 500);
        assert_eq!(segs, vec![1]);
    }
}
