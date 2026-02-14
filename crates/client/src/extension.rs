//! Shard Extension
//!
//! Generate new parity shards from existing data shards.
//! Any node holding `k` shards can produce additional parity shards
//! at arbitrary shard indices, increasing availability without duplication.

use craftec_erasure::ErasureCoder;
use datacraft_core::{ChunkManifest, ContentId, DataCraftError, Result};
use datacraft_store::FsStore;
use tracing::{debug, info};

/// Generate a new parity shard at `target_index` for a single chunk.
///
/// Requires `k` data shards (indices 0..k) to be available in the store.
/// The target_index must be >= k (parity territory).
///
/// Returns the generated shard data.
pub fn generate_parity_shard(
    store: &FsStore,
    manifest: &ChunkManifest,
    chunk_index: u32,
    target_index: u8,
) -> Result<Vec<u8>> {
    let k = manifest.k;

    if (target_index as usize) < k {
        return Err(DataCraftError::ErasureError(
            "target_index must be >= k (parity shard territory)".into(),
        ));
    }

    // We need to create a Reed-Solomon coder with enough total shards
    // to cover the target index. RS(k, m) where m = target_index - k + 1.
    let parity_needed = (target_index as usize) - k + 1;
    let coder = ErasureCoder::with_config_params(k, parity_needed)
        .map_err(|e| DataCraftError::ErasureError(e.to_string()))?;

    // Gather the k data shards (indices 0..k)
    let mut data_shards: Vec<Vec<u8>> = Vec::with_capacity(k);
    for i in 0..k {
        let shard = store.get_shard(&manifest.content_id, chunk_index, i as u8)?;
        data_shards.push(shard);
    }

    // Build the full shard array for encoding: k data shards + parity_needed empty parity shards
    let shard_size = data_shards[0].len();
    let mut all_shards: Vec<Vec<u8>> = data_shards;
    for _ in 0..parity_needed {
        all_shards.push(vec![0u8; shard_size]);
    }

    // Encode fills in the parity shards
    coder
        .encode_shards(&mut all_shards)
        .map_err(|e| DataCraftError::ErasureError(e.to_string()))?;

    // The target shard is the last one (index = k + parity_needed - 1 = target_index)
    let result = all_shards.pop().ok_or_else(|| {
        DataCraftError::ErasureError("unexpected empty shard array".into())
    })?;

    debug!(
        "Generated parity shard {} for chunk {} of {}",
        target_index, chunk_index, manifest.content_id
    );

    Ok(result)
}

/// Generate a new parity shard at `target_index` for ALL chunks of a CID,
/// storing each in the local store.
pub fn extend_content(
    store: &FsStore,
    manifest: &ChunkManifest,
    target_index: u8,
) -> Result<()> {
    info!(
        "Extending {} with parity shard index {} ({} chunks)",
        manifest.content_id, target_index, manifest.chunk_count
    );

    for chunk_idx in 0..manifest.chunk_count as u32 {
        let shard_data = generate_parity_shard(store, manifest, chunk_idx, target_index)?;
        store.put_shard(&manifest.content_id, chunk_idx, target_index, &shard_data)?;
    }

    info!(
        "Successfully extended {} with shard index {}",
        manifest.content_id, target_index
    );
    Ok(())
}

/// Generate a parity shard from raw data shards (no store needed).
///
/// `data_shards` must contain exactly `k` shards of equal size.
/// Returns the parity shard data at `target_index`.
pub fn generate_parity_shard_from_data(
    k: usize,
    data_shards: &[Vec<u8>],
    target_index: u8,
) -> Result<Vec<u8>> {
    if data_shards.len() != k {
        return Err(DataCraftError::ErasureError(format!(
            "expected {} data shards, got {}",
            k,
            data_shards.len()
        )));
    }
    if (target_index as usize) < k {
        return Err(DataCraftError::ErasureError(
            "target_index must be >= k".into(),
        ));
    }

    let parity_needed = (target_index as usize) - k + 1;
    let coder = ErasureCoder::with_config_params(k, parity_needed)
        .map_err(|e| DataCraftError::ErasureError(e.to_string()))?;

    let shard_size = data_shards[0].len();
    let mut all_shards: Vec<Vec<u8>> = data_shards.to_vec();
    for _ in 0..parity_needed {
        all_shards.push(vec![0u8; shard_size]);
    }

    coder
        .encode_shards(&mut all_shards)
        .map_err(|e| DataCraftError::ErasureError(e.to_string()))?;

    Ok(all_shards.pop().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use craftec_erasure::ErasureConfig;
    use datacraft_core::default_erasure_config;
    use std::path::PathBuf;

    fn test_dir() -> PathBuf {
        use rand::RngCore;
        let mut rng_bytes = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut rng_bytes);
        let dir = std::env::temp_dir()
            .join("datacraft-extension-test")
            .join(hex::encode(rng_bytes));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_generate_parity_shard_from_data() {
        // Create k=4 data shards
        let k = 4;
        let shard_size = 16;
        let data_shards: Vec<Vec<u8>> = (0..k)
            .map(|i| vec![(i as u8) * 10 + 1; shard_size])
            .collect();

        // Generate parity at index 4 (first parity)
        let parity_4 = generate_parity_shard_from_data(k, &data_shards, 4).unwrap();
        assert_eq!(parity_4.len(), shard_size);

        // Generate parity at index 7
        let parity_7 = generate_parity_shard_from_data(k, &data_shards, 7).unwrap();
        assert_eq!(parity_7.len(), shard_size);

        // Parity shards should be different from each other
        assert_ne!(parity_4, parity_7);

        // Verify: we can reconstruct using k-1 data shards + the new parity
        let coder = ErasureCoder::with_config_params(k, 1).unwrap();
        let mut shards: Vec<Option<Vec<u8>>> = data_shards.iter().map(|s| Some(s.clone())).collect();
        shards.push(Some(parity_4.clone()));
        // Remove one data shard
        shards[0] = None;
        let decoded = coder.decode(&mut shards, shard_size * k).unwrap();
        // First shard_size bytes should match data_shards[0]
        assert_eq!(&decoded[..shard_size], &data_shards[0]);
    }

    #[test]
    fn test_generate_parity_shard_from_data_invalid_k() {
        let shards = vec![vec![1u8; 10]; 3]; // only 3 shards
        let result = generate_parity_shard_from_data(4, &shards, 4);
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_parity_shard_from_data_invalid_index() {
        let shards = vec![vec![1u8; 10]; 4];
        let result = generate_parity_shard_from_data(4, &shards, 2); // index < k
        assert!(result.is_err());
    }

    #[test]
    fn test_extend_content_via_store() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();

        // Simulate a published content with k=4, initial parity=4 (total 8 shards)
        let config = default_erasure_config();
        let k = config.data_shards;
        let content = b"hello datacraft extension test! padding to make it meaningful.";
        let content_id = ContentId::from_bytes(content);

        // Chunk + encode
        let coder = ErasureCoder::with_config(&config).unwrap();
        let shards = coder.encode(content).unwrap();

        // Store all initial shards for chunk 0
        for (i, shard) in shards.iter().enumerate() {
            store.put_shard(&content_id, 0, i as u8, shard).unwrap();
        }

        let manifest = ChunkManifest {
            content_id,
            content_hash: content_id.0,
            k,
            chunk_size: config.chunk_size,
            chunk_count: 1,
            erasure_config: config,
            content_size: content.len() as u64,
        };
        store.put_manifest(&manifest).unwrap();

        // Now extend with shard index 8 (one beyond initial 0..7)
        extend_content(&store, &manifest, 8).unwrap();

        // Verify the new shard was stored
        assert!(store.has_shard(&content_id, 0, 8));
        let new_shard = store.get_shard(&content_id, 0, 8).unwrap();
        assert!(!new_shard.is_empty());

        // Verify reconstruction works with original data shards missing,
        // using the new parity shard + some originals
        // We need k=4 shards total. Use shard 1,2,3,8
        let coder2 = ErasureCoder::with_config_params(k, 5).unwrap(); // k=4, m=5 to cover index 8
        let mut opt_shards: Vec<Option<Vec<u8>>> = Vec::new();
        opt_shards.push(None); // shard 0 missing
        for i in 1..k {
            opt_shards.push(Some(store.get_shard(&content_id, 0, i as u8).unwrap()));
        }
        // parity shards 4..7 missing
        for _ in k..8 {
            opt_shards.push(None);
        }
        opt_shards.push(Some(new_shard)); // shard 8

        let decoded = coder2.decode(&mut opt_shards, content.len()).unwrap();
        assert_eq!(&decoded, content);

        std::fs::remove_dir_all(&dir).ok();
    }
}
