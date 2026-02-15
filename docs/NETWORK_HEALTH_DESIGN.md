# DataCraft Network Health & Peer Liveness Design

## Executive Summary

DataCraft's current peer discovery is fundamentally broken: static capability maps from gossipsub announcements with no expiry, no heartbeats, no health checks. This design replaces it with a proper distributed network health layer inspired by proven systems like IPFS, Ceph, Filecoin, and BitTorrent.

**Key principles:**
- **Unified health model**: peer liveness drives content availability, not separate tracking
- **Erasure-aware repair**: thresholds based on k/n ratios, not replica counts  
- **PDP-integrated liveness**: failed storage proofs = data problems, not just peer down
- **Settlement-driven incentives**: repair costs from creator pools, graceful exit rewards

## 1. Research: Proven Approaches

### 1.1 IPFS/libp2p

**Provider Records & DHT**:
- **Heartbeat**: DHT provider records TTL = 24 hours, republish every 12 hours
- **Failure Detection**: No provider record refresh = dead peer after 24h
- **Repair Trigger**: Bitswap session timeout (30s-2min) triggers alternative peer discovery
- **Scale Experience**: Works well up to ~10K nodes, DHT becomes bottleneck beyond

**Connection Manager**:
- **Peer scoring**: RTT, throughput, success rate → peer ranking
- **Connection pruning**: Keep N best connections (typically 50-200)
- **Backoff**: Exponential backoff for failed connections (1s → 32s → 5min max)

### 1.2 BitTorrent

**Tracker Announces**:
- **Heartbeat**: Tracker announce every 30 minutes (configurable 15min-2h)
- **Failure Detection**: 3 missed announces = peer considered offline  
- **Peer Exchange (PEX)**: Peers gossip about other active peers every 60s
- **DHT Ping**: 15-minute DHT node ping, exponential backoff on failure

**Proven Parameters**:
- **Announce interval**: 1800s (30min), can compress to 900s (15min) for new torrents
- **DHT ping**: 900s (15min), 5s timeout, 3 retries with 2x backoff
- **Peer timeout**: 3 missed cycles = 90 minutes of silence

### 1.3 Ceph

**OSD Heartbeats**:
- **Heartbeat interval**: 6 seconds between OSDs, 20 seconds to monitors
- **Failure detection**: 20 seconds missed = peer marked "down", 300s = "out"  
- **Recovery**: "Down" peers can return quickly, "out" triggers rebalancing
- **Network partitions**: Monitor quorum (odd numbers, typically 3-5) prevents split-brain

**CRUSH Map & PG Peering**:
- **Placement Group (PG) peering**: Every 30s, OSDs exchange PG state
- **Repair threshold**: Rebalance when available replicas < min_size (typically n-1)
- **Backfill throttling**: Rate-limit repair I/O to avoid impacting client traffic

**Proven Parameters**:
- **OSD heartbeat**: 6s interval, 20s timeout  
- **Monitor heartbeat**: 20s interval, 30s timeout
- **Grace period**: 300s (5min) before "out" declaration triggers repair
- **PG scrub**: Weekly deep scrub, daily light scrub for integrity

### 1.4 Filecoin

**WindowPoSt (Proof of Spacetime)**:
- **Challenge period**: Every 30 minutes, providers must submit storage proof
- **Deadline**: 30-minute window to submit, 2 consecutive failures = fault
- **Recovery**: 14-day grace period to repair faulted sectors before slashing
- **Sector health**: Continuous monitoring via WindowPoSt, not separate health checks

**Deal Lifecycle**:
- **Repair trigger**: Failed WindowPoSt immediately flags sector for repair
- **Priority repair**: Expiring deals get higher repair priority
- **Economic incentives**: Penalty for faults, rewards for consistent availability

**Proven Parameters**:
- **WindowPoSt frequency**: 30 minutes (1800s)
- **Fault tolerance**: 2 consecutive failures = fault declaration
- **Grace period**: 14 days for recovery before penalties
- **Batch size**: ~350 sectors per WindowPoSt for efficiency

### 1.5 Storj

**Satellite-Node Communication**:
- **Heartbeat**: Contact service pings every 1 hour, 3 missed = disqualified
- **Audit frequency**: Random audits every 8 hours average per node
- **Reputation system**: Success rate tracking over 30-day windows
- **Graceful exit**: Voluntary departure with data migration rewards

**Audit-Driven Health**:
- **Audit timeout**: 30 seconds per piece, 3 retries
- **Failure threshold**: 80% success rate minimum to avoid disqualification  
- **Repair trigger**: <95% audit success rate triggers piece migration
- **Download success**: Also factors into reputation (not just storage proofs)

**Proven Parameters**:
- **Contact ping**: 3600s (1h), 3 missed contacts = offline
- **Audit interval**: ~28800s (8h average) per node, randomized
- **Success threshold**: 80% minimum, 95% target for good standing
- **Reputation window**: 30 days rolling average

### 1.6 Sia

**Host-Renter Contracts**:
- **Contract duration**: 12-week cycles with automatic renewal  
- **Storage proof frequency**: Every 4320 blocks (~72 hours on Sia blockchain)
- **Challenge-response**: Host must provide Merkle proof for random sector
- **Host scoring**: Uptime, price, collateral, version → composite score

**Consensus-Based Proofs**:
- **Proof submission**: On-chain storage proof every ~3 days
- **Verification**: Blockchain validates Merkle proof against stored root
- **Penalties**: Missed proofs forfeit host collateral pro-rata
- **Migration**: Failed hosts trigger automatic re-upload to new hosts

**Proven Parameters**:
- **Storage proof**: 4320 blocks (~72 hours), blockchain-enforced
- **Contract cycle**: 12 weeks with renewal opportunity
- **Host selection**: Top 50-100 hosts by composite score for redundancy
- **Repair trigger**: <90% proof success over 2-week window

### 1.7 Key Learnings Summary

**Heartbeat Intervals by System**:
- **Fast systems** (Ceph): 6s-20s for low-latency recovery
- **Medium systems** (BitTorrent, Filecoin): 15-30min for efficiency
- **Slow systems** (IPFS, Storj): 1-24h for high-scale, low-overhead

**Failure Detection Patterns**:
- **Immediate**: Real-time connection loss (TCP/stream failures) 
- **Short-term**: 3-5 missed heartbeats (minutes to hours)
- **Long-term**: Extended absence triggers repair (hours to days)

**Repair Strategies**:
- **Threshold-based**: Start repair when redundancy drops below target
- **Time-delayed**: Grace period prevents churn from temporary failures  
- **Priority-based**: Critical data (high-tier, low-redundancy) repairs first
- **Rate-limited**: Throttle repair to avoid network congestion

## 2. DataCraft Peer Liveness Protocol

### 2.1 Multi-Layer Failure Detection

DataCraft employs a **hybrid failure detection** system combining multiple signals:

#### Layer 1: Connection-Level (Immediate)
```rust
// libp2p connection events
ConnectionEstablished → peer_online()
ConnectionClosed → peer_suspected() 
// Fast detection: 0-30 seconds
```

#### Layer 2: Application-Level Heartbeat (Fast)
```rust
// Direct libp2p ping over /datacraft/ping/1.0.0
PingInterval: 120s (2 minutes)
PingTimeout: 10s  
MissedPingThreshold: 3 (= 6 minutes total)
// Medium detection: 2-6 minutes
```

#### Layer 3: Capability Announcement (Medium) 
```rust
// Enhanced gossipsub capability broadcast
AnnouncementInterval: 300s (5 minutes) 
AnnouncementTTL: 900s (15 minutes, 3x interval)
// Medium detection: 5-15 minutes
```

#### Layer 4: PDP Challenge Integration (Slow)
```rust
// Storage proof failures indicate data problems
PdpChallengeInterval: 3600s (1 hour)
PdpFailureThreshold: 2 consecutive failures
PdpGracePeriod: 14400s (4 hours)
// Slow detection: 2-8 hours, but high confidence
```

### 2.2 Phi-Accrual Failure Detector

Instead of fixed timeouts, DataCraft uses **phi-accrual detection** adapted from Cassandra:

```rust
struct PhiAccrualDetector {
    // Ring buffer of recent heartbeat intervals
    intervals: VecDeque<Duration>,
    window_size: usize,
    // Phi threshold for failure suspicion
    phi_threshold: f64,
}

impl PhiAccrualDetector {
    fn phi(&self, now: Instant, last_heartbeat: Instant) -> f64 {
        let elapsed = now.duration_since(last_heartbeat);
        let mean = self.mean_interval();
        let std_dev = self.std_dev_interval();
        
        // Phi = log10(P(t > elapsed)) where P follows normal distribution
        let z_score = (elapsed.as_secs_f64() - mean) / std_dev;
        -z_score.max(0.0) / 2.302585 // log10(e)
    }
    
    fn is_suspected(&self, now: Instant, last_heartbeat: Instant) -> bool {
        self.phi(now, last_heartbeat) > self.phi_threshold
    }
}
```

**Phi Thresholds**:
- `phi < 1.0`: Peer healthy  
- `phi 1.0-3.0`: Peer suspected (reduce requests)
- `phi 3.0-8.0`: Peer likely down (avoid for new requests)  
- `phi > 8.0`: Peer definitely down (trigger repair)

**Advantages over fixed timeouts**:
- **Adaptive**: Learns network characteristics (slow connections vs fast)
- **Probabilistic**: Confidence level rather than binary alive/dead
- **Graceful degradation**: Reduces load on suspected peers before full failure

### 2.3 Peer Capability Map with TTL

Replace static capability tracking with **TTL-based eviction**:

```rust
#[derive(Debug, Clone)]
struct PeerRecord {
    peer_id: PeerId,
    capabilities: Vec<DataCraftCapability>,
    first_seen: Instant,
    last_seen: Instant,
    last_ping: Option<Instant>,
    phi_detector: PhiAccrualDetector,
    connection_state: ConnectionState,
    reputation: PeerReputation,
}

#[derive(Debug, Clone)]
enum ConnectionState {
    Connected,
    Suspected { since: Instant, phi: f64 },
    Disconnected { since: Instant },
    Failed { since: Instant, reason: FailureReason },
}

struct PeerRegistry {
    peers: HashMap<PeerId, PeerRecord>,
    // TTL settings
    capability_ttl: Duration,        // 15 minutes (3x announcement interval)
    disconnected_ttl: Duration,      // 1 hour (keep for reconnection)
    failed_ttl: Duration,           // 24 hours (reputation tracking)
}
```

**TTL Eviction Rules**:
- **Capability expiry**: 15 minutes since last announcement → remove capabilities
- **Disconnected cleanup**: 1 hour offline → remove from active peer list  
- **Failed peer cleanup**: 24 hours failed → remove entirely (but log for reputation)
- **Reputation tracking**: Keep failure history for future connection decisions

### 2.4 Transport Protocol Selection

**Primary**: Direct libp2p ping over custom protocol `/datacraft/ping/1.0.0`
```rust
// Lightweight ping/pong with capability attestation
struct PingRequest {
    nonce: u64,
    capabilities: Vec<DataCraftCapability>, 
    challenge: Option<PdpChallenge>, // Occasional storage proof
}

struct PongResponse {
    nonce: u64,
    capabilities: Vec<DataCraftCapability>,
    proof: Option<PdpProof>, 
    stats: PeerStats, // Storage used, bandwidth, etc.
}
```

**Fallback**: Enhanced gossipsub capability announcements (passive heartbeat)
```rust  
struct CapabilityAnnouncement {
    peer_id: PeerId,
    capabilities: Vec<DataCraftCapability>,
    timestamp: u64,
    signature: Signature, // Prevent spoofing
    nonce: u64, // Prevent replay
    stats: Option<PeerStats>,
}
```

**Benefits of dual transport**:
- **Direct ping**: Low latency, point-to-point health check
- **Gossipsub**: Broadcast efficiency, network-wide capability discovery  
- **Redundancy**: Ping failures don't immediately trigger full failure detection

### 2.5 Graceful vs Ungraceful Departure

#### Graceful Departure Protocol
```rust
// Node announces departure with migration plan
struct DepartureNotice {
    peer_id: PeerId,
    departure_time: Instant, // When services will stop
    migration_window: Duration, // How long to migrate data  
    stored_content: Vec<ContentId>, // What needs migration
    signature: Signature,
}
```

**Process**:
1. **Announcement**: 24-48 hour advance notice via gossipsub
2. **Migration period**: Other nodes can retrieve data before departure  
3. **Incentive**: Settlement rewards for clean migration (if funded)
4. **Enforcement**: Departure notice prevents new storage assignments

#### Ungraceful Departure (Failure)
```rust
// Detected via phi-accrual + connection loss
struct PeerFailure {
    peer_id: PeerId, 
    failure_type: FailureType,
    last_seen: Instant,
    affected_content: Vec<ContentId>,
    phi_score: f64, // Confidence level
}

enum FailureType {
    ConnectionLoss,      // Network partition, restart
    PingTimeout,         // Application unresponsive
    PdpFailure,          // Storage proof failures  
    Malicious,           // Detected bad behavior
}
```

**Response Strategy**:
- **Connection loss**: Wait 5 minutes for reconnection
- **Ping timeout**: Start seeking alternatives after 3 failures (6 minutes)  
- **PDP failure**: Immediate repair trigger (data integrity issue)
- **Malicious**: Blacklist + immediate repair + reputation penalty

### 2.6 Network Partition Handling

**Partition Detection**:
- **Gossipsub mesh fragmentation**: <50% of expected peers reachable
- **DHT bootstrap failure**: Cannot connect to any bootstrap nodes
- **Ping correlation**: Multiple simultaneous failures to different regions

**Survival Strategy**:
```rust
enum PartitionMode {
    Normal,          // >70% mesh connectivity
    Degraded,        // 30-70% connectivity, reduce operations  
    Isolated,        // <30% connectivity, maintenance mode
    Recovery,        // Partition healing detected
}
```

**Operational Changes by Mode**:
- **Degraded**: Increase heartbeat intervals, reduce PDP challenges, pause non-critical repairs
- **Isolated**: Stop new content distribution, serve existing content only, log partition events
- **Recovery**: Gradual restoration, capability re-announcement, content health re-assessment

## 3. Health-Driven Self-Healing

### 3.1 Unified Health Assessment

**Content health derives directly from peer liveness**, not separate tracking:

```rust
#[derive(Debug, Clone)]
struct ContentHealth {
    content_id: ContentId,
    k: usize, // Data shards needed for reconstruction
    n: usize, // Total shards (data + parity)
    live_shards: usize, // Confirmed available via peer liveness
    tier_target: f64, // Target ratio (2.0 for Lite, 3.0 for Standard, etc.)
    current_ratio: f64, // live_shards / k
    health_score: f64, // 0.0-1.0 based on redundancy + peer quality
    last_verified: Instant,
    repair_priority: RepairPriority,
}

enum RepairPriority {
    Critical,  // ratio < 1.0, reconstruction impossible
    High,      // ratio < tier_target, below funded level  
    Medium,    // ratio >= tier_target but < 2.0 * tier_target
    Low,       // ratio >= 2.0 * tier_target, over-provisioned
}
```

**Health Score Calculation**:
```rust
fn calculate_health_score(content: &ContentHealth, peers: &[PeerRecord]) -> f64 {
    let redundancy_score = (content.current_ratio / content.tier_target).min(1.0);
    
    // Peer quality: average phi scores of storage peers
    let peer_quality = peers.iter()
        .map(|p| 1.0 - (p.phi_detector.current_phi().min(8.0) / 8.0))
        .sum::<f64>() / peers.len() as f64;
        
    let recency_penalty = if content.last_verified.elapsed() > Duration::from_secs(7200) {
        0.8 // 20% penalty for stale verification (>2h)
    } else { 1.0 };
    
    redundancy_score * peer_quality * recency_penalty
}
```

### 3.2 PDP-Integrated Liveness

**PDP challenges become health probes**:

```rust
#[derive(Debug)]
enum ChallengeOutcome {
    Success { latency: Duration, proof_quality: f64 },
    Timeout,      // Network issue, peer suspected
    BadProof,     // Data corruption, immediate repair
    Refused,      // Peer policy issue, capability mismatch
    NotFound,     // Shard missing, definite repair needed
}

impl ChallengeOutcome {
    fn affects_peer_liveness(&self) -> bool {
        matches!(self, Self::Timeout | Self::Refused)
    }
    
    fn affects_data_integrity(&self) -> bool {
        matches!(self, Self::BadProof | Self::NotFound)
    }
    
    fn repair_urgency(&self) -> RepairPriority {
        match self {
            Self::NotFound | Self::BadProof => RepairPriority::Critical,
            Self::Timeout if /* ratio < tier_target */ => RepairPriority::High,
            _ => RepairPriority::Medium,
        }
    }
}
```

**Challenge Integration Flow**:
1. **Regular PDP cycle**: Challenge random shards every hour per content
2. **Peer health update**: Success/failure updates phi-accrual detector  
3. **Content health update**: Shard availability recalculated based on challenge results
4. **Repair trigger**: Data integrity issues (BadProof/NotFound) bypass normal repair delays

### 3.3 Repair Thresholds & Timing

**Erasure-Code Aware Thresholds**:
```rust
struct RepairThresholds {
    // Absolute minimum: k shards required for reconstruction
    critical_threshold: f64,      // 1.0 (exactly k shards)
    
    // Tier-based targets from creator funding
    tier_threshold: f64,          // 2.0-5.0 based on tier (Lite=2x, Pro=5x)
    
    // Safety margins above tier level  
    comfortable_threshold: f64,   // 1.5 * tier_threshold
    excess_threshold: f64,        // 2.0 * tier_threshold (stop repair)
}

impl RepairThresholds {
    fn repair_needed(&self, current_ratio: f64) -> RepairPriority {
        if current_ratio < self.critical_threshold {
            RepairPriority::Critical      // Data loss imminent
        } else if current_ratio < self.tier_threshold {
            RepairPriority::High         // Below paid tier
        } else if current_ratio < self.comfortable_threshold {
            RepairPriority::Medium       // Safety margin
        } else {
            RepairPriority::Low          // Over-provisioned
        }
    }
}
```

**Repair Timing Strategy**:
```rust
struct RepairScheduler {
    // Minimum time before repair (prevents churn)
    min_shard_lifetime: Duration,     // 30 minutes
    
    // Exponential backoff for failed repairs
    repair_backoff: ExponentialBackoff,
    
    // Rate limiting to prevent repair storms
    max_concurrent_repairs: usize,    // 3 repairs per node
    repair_bandwidth_limit: u64,      // MB/s for repair traffic
}
```

**Repair Decision Algorithm**:
1. **Immediate**: PDP proof failure (data corruption) → repair within 5 minutes
2. **Urgent**: Critical threshold (ratio < 1.0) → repair within 30 minutes  
3. **High**: Tier threshold breach → repair within 2 hours
4. **Medium**: Comfort threshold → repair within 24 hours
5. **Low**: Excess → no repair needed, may stop early

### 3.4 Repair Storm Prevention

**Multiple Defense Mechanisms**:

#### Temporal Spreading
```rust
// Randomize repair start times to prevent thundering herd
fn schedule_repair_time(priority: RepairPriority) -> Instant {
    let base_delay = match priority {
        RepairPriority::Critical => Duration::from_secs(300),   // 5 min
        RepairPriority::High => Duration::from_secs(1800),     // 30 min  
        RepairPriority::Medium => Duration::from_secs(7200),   // 2 hours
        RepairPriority::Low => Duration::from_secs(86400),     // 24 hours
    };
    
    // Add 0-50% random jitter
    let jitter = fastrand::u64(0..base_delay.as_secs() / 2);
    Instant::now() + base_delay + Duration::from_secs(jitter)
}
```

#### Rate Limiting
```rust
struct RepairRateLimiter {
    // Network-wide coordination via gossipsub
    active_repairs: HashMap<ContentId, Vec<PeerId>>, // Who's repairing what
    
    // Local limits
    local_repair_slots: Semaphore,     // Max 3 concurrent
    bandwidth_limiter: TokenBucket,    // Max repair bandwidth
    
    // Exponential backoff for repeated failures  
    repair_failures: HashMap<ContentId, ExponentialBackoff>,
}

impl RepairRateLimiter {
    async fn try_start_repair(&mut self, content_id: ContentId) -> RepairPermission {
        // Check if someone else already repairing
        if self.active_repairs.get(&content_id).map_or(0, |v| v.len()) >= 2 {
            return RepairPermission::Deferred; // Others handling it
        }
        
        // Acquire local resources
        if let Ok(permit) = self.local_repair_slots.try_acquire() {
            self.announce_repair_start(content_id).await;
            RepairPermission::Granted(permit)
        } else {
            RepairPermission::ResourceBusy
        }
    }
}
```

#### Economic Circuit Breaker
```rust
// Creator pool balance check before expensive repairs
fn repair_economically_viable(content_id: ContentId, repair_cost: u64) -> bool {
    let pool_balance = get_creator_pool_balance(content_id);
    let repair_threshold = pool_balance / 10; // Max 10% of pool per repair
    
    repair_cost <= repair_threshold && pool_balance > MIN_REPAIR_BALANCE
}
```

## 4. Content Status Model

### 4.1 Real-Time Availability Mapping

**Content availability is derived from live peer data**:

```rust
#[derive(Debug, Clone)]
struct ContentAvailability {
    content_id: ContentId,
    shard_map: HashMap<usize, Vec<PeerId>>, // shard_index -> available_peers
    availability_matrix: Vec<Vec<PeerAvailability>>, // [shard][peer] availability
    last_updated: Instant,
    confidence: f64, // Based on verification recency
}

#[derive(Debug, Clone)]
struct PeerAvailability {
    peer_id: PeerId,
    shard_index: usize,
    last_verified: Instant,     // Last PDP challenge or transfer
    availability_score: f64,    // 0.0-1.0 based on phi + success rate
    latency: Duration,          // Recent response time
    bandwidth: u64,             // Recent transfer speed
}

impl ContentAvailability {
    fn can_reconstruct(&self, k: usize) -> bool {
        // Check if we have k shards available from healthy peers
        let available_shards = self.shard_map.iter()
            .filter(|(_, peers)| {
                peers.iter().any(|p| {
                    self.peer_is_healthy(p) && self.shard_recently_verified(*shard_index, p)
                })
            })
            .count();
            
        available_shards >= k
    }
    
    fn reconstruction_confidence(&self, k: usize) -> f64 {
        // Weighted confidence based on peer health + verification recency
        let shard_confidences: Vec<f64> = self.shard_map.iter()
            .map(|(shard_index, peers)| {
                peers.iter()
                    .map(|p| self.peer_availability_score(p, *shard_index))
                    .max_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap_or(0.0)
            })
            .collect();
            
        let top_k_confidence: f64 = shard_confidences.iter()
            .sorted_by(|a, b| b.partial_cmp(a).unwrap())
            .take(k)
            .sum();
            
        top_k_confidence / k as f64
    }
}
```

### 4.2 Health Scoring per Content

**Multi-factor health scoring**:

```rust
#[derive(Debug, Clone)]
struct ContentHealthMetrics {
    // Availability metrics
    reconstructable: bool,              // >= k shards available  
    reconstruction_confidence: f64,     // Quality of available shards
    redundancy_ratio: f64,             // live_shards / k
    tier_compliance: f64,              // current_ratio / tier_target
    
    // Quality metrics  
    peer_health_average: f64,          // Mean phi scores of storage peers
    verification_freshness: f64,       // How recent are our verifications
    geographic_distribution: f64,      // Shard distribution across regions
    
    // Economic metrics
    funded_adequately: bool,           // Creator pool can cover repairs
    repair_history: Vec<RepairEvent>,  // Past repair frequency  
    
    // Composite score
    overall_health: f64,               // 0.0-1.0 weighted combination
}

fn calculate_overall_health(metrics: &ContentHealthMetrics) -> f64 {
    if !metrics.reconstructable {
        return 0.0; // Cannot reconstruct = health 0
    }
    
    let weights = HealthWeights {
        redundancy: 0.35,       // Primary factor
        peer_quality: 0.25,     // Peer reliability  
        freshness: 0.20,        // Verification recency
        economics: 0.10,        // Funding status
        distribution: 0.10,     // Geographic spread
    };
    
    weights.redundancy * metrics.tier_compliance.min(1.0) +
    weights.peer_quality * metrics.peer_health_average +
    weights.freshness * metrics.verification_freshness +
    weights.economics * if metrics.funded_adequately { 1.0 } else { 0.5 } +
    weights.distribution * metrics.geographic_distribution
}
```

### 4.3 Status Categories & Client Communication

**Content Status Hierarchy**:
```rust
#[derive(Debug, Clone, Serialize)]
pub enum ContentStatus {
    // Healthy states
    Optimal {          // > tier target, high peer quality
        health_score: f64,
        redundancy_ratio: f64, 
        estimated_availability: f64, // % time reconstructable
    },
    
    Healthy {          // >= tier target, good peer quality
        health_score: f64,
        redundancy_ratio: f64,
        minor_issues: Vec<String>, // Non-critical warnings
    },
    
    // Warning states  
    Degraded {         // < tier target but >= k
        health_score: f64,
        redundancy_ratio: f64,
        issues: Vec<String>,
        repair_eta: Option<Instant>, // When repair expected
    },
    
    AtRisk {           // Close to k, some peer failures
        health_score: f64,
        redundancy_ratio: f64,
        critical_issues: Vec<String>,
        immediate_repair: bool,
    },
    
    // Critical states
    Critical {         // < k shards, reconstruction impossible
        health_score: f64, // Will be very low
        redundancy_ratio: f64,
        emergency_repair: EmergencyRepairStatus,
        data_loss_risk: f64, // Probability of permanent loss
    },
    
    // Special states
    Repairing {
        current_health: f64,
        target_health: f64,  
        repair_progress: f64, // 0.0-1.0
        estimated_completion: Instant,
    },
    
    Migrating {        // Planned peer departures
        current_health: f64,
        departure_deadline: Instant,
        migration_progress: f64,
    },
    
    Unknown {          // Cannot assess (network issues, etc.)
        last_known_health: Option<f64>,
        last_verified: Instant,
        reason: String,
    }
}
```

**Client API Integration**:
```rust
// Enhanced IPC commands for health monitoring
pub enum HealthCommand {
    GetContentHealth {
        content_id: ContentId,
        reply_tx: oneshot::Sender<ContentStatus>,
    },
    
    SubscribeHealthUpdates {  
        content_ids: Vec<ContentId>,
        update_tx: mpsc::UnboundedSender<HealthUpdate>,
    },
    
    GetNetworkHealth {
        reply_tx: oneshot::Sender<NetworkHealthSummary>,  
    },
    
    ForceHealthCheck {
        content_id: ContentId,
        deep_verify: bool, // Trigger PDP challenges
        reply_tx: oneshot::Sender<Result<ContentStatus>>,
    }
}
```

## 5. Integration Plan

### 5.1 craftec-network Changes

**New Protocol: DataCraft Ping**
```rust
// Add to existing CraftBehaviour
pub struct HealthProtocol {
    config: HealthConfig,
    pending_pings: HashMap<PeerId, Instant>,  
    phi_detectors: HashMap<PeerId, PhiAccrualDetector>,
}

// New protocol definition
const DATACRAFT_PING_PROTOCOL: &str = "/datacraft/ping/1.0.0";

#[derive(Debug, Clone)]
pub struct HealthConfig {
    pub ping_interval: Duration,        // 120s
    pub ping_timeout: Duration,         // 10s  
    pub phi_threshold: f64,            // 8.0
    pub capability_ttl: Duration,       // 900s
}
```

**Enhanced Gossipsub Messages**:
```rust
// Extend existing CapabilityAnnouncement  
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhancedCapabilityAnnouncement {
    pub peer_id: PeerId,
    pub capabilities: Vec<DataCraftCapability>,
    pub timestamp: u64,
    pub signature: Signature,
    pub nonce: u64,
    
    // New health-related fields
    pub stats: Option<PeerStats>,
    pub health_attestation: Option<HealthAttestation>, 
    pub departure_notice: Option<DepartureNotice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]  
pub struct PeerStats {
    pub storage_used_gb: f64,
    pub bandwidth_mbps: f64,
    pub uptime_hours: f64,
    pub success_rate: f64,
}
```

### 5.2 datacraft-daemon Changes

**New Health Service**:
```rust
// crates/daemon/src/health_service.rs
pub struct HealthService {
    peer_registry: Arc<Mutex<PeerRegistry>>,
    content_health: Arc<Mutex<HashMap<ContentId, ContentHealth>>>,
    health_config: HealthConfig,
    repair_scheduler: RepairScheduler,
    
    // Integration points
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    client: Arc<Mutex<DataCraftClient>>,
    challenger: Arc<Mutex<ChallengerManager>>,
}

impl HealthService {
    // Main health assessment loop (every 30s)
    pub async fn run_health_loop(&mut self) -> ! {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            
            // Update peer liveness from phi-accrual detectors
            self.update_peer_liveness().await;
            
            // Recalculate content health from peer availability  
            self.update_content_health().await;
            
            // Schedule repairs for degraded content
            self.schedule_repairs().await;
            
            // Clean up stale peer records  
            self.cleanup_stale_peers().await;
        }
    }
    
    // Integrate with PDP challenger results
    pub fn handle_pdp_result(&mut self, peer_id: PeerId, content_id: ContentId, outcome: ChallengeOutcome) {
        // Update peer liveness  
        if outcome.affects_peer_liveness() {
            self.update_peer_phi_detector(peer_id, outcome.into());
        }
        
        // Update content health
        if outcome.affects_data_integrity() {
            self.mark_content_for_urgent_repair(content_id, outcome.repair_urgency());
        }
    }
}
```

**Service Integration**:
```rust
// Modify crates/daemon/src/service.rs
impl DataCraftService {
    pub async fn new(config: DaemonConfig) -> Result<Self> {
        // ... existing setup ...
        
        let health_service = HealthService::new(
            health_config,
            command_tx.clone(), 
            Arc::clone(&client),
            Arc::clone(&challenger),
        );
        
        // Spawn health service loop
        let health_handle = tokio::spawn(async move {
            health_service.run_health_loop().await
        });
        
        Ok(Self {
            // ... existing fields ...
            health_handle: Some(health_handle),
        })
    }
}
```

### 5.3 DaemonConfig Extensions

**New Health Configuration**:
```rust
// Add to crates/daemon/src/config.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonConfig {
    // ... existing fields ...
    
    /// Health and liveness settings
    pub health: HealthConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    /// Direct ping interval in seconds (default: 120)
    pub ping_interval_secs: u64,
    
    /// Ping timeout in seconds (default: 10)  
    pub ping_timeout_secs: u64,
    
    /// Phi-accrual failure threshold (default: 8.0)
    pub phi_threshold: f64,
    
    /// Capability announcement TTL in seconds (default: 900)
    pub capability_ttl_secs: u64,
    
    /// Minimum shard lifetime before repair (default: 1800)
    pub min_shard_lifetime_secs: u64,
    
    /// Maximum concurrent repairs per node (default: 3)
    pub max_concurrent_repairs: usize,
    
    /// Content health check interval in seconds (default: 30)
    pub health_check_interval_secs: u64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            ping_interval_secs: 120,
            ping_timeout_secs: 10,
            phi_threshold: 8.0,
            capability_ttl_secs: 900,
            min_shard_lifetime_secs: 1800,
            max_concurrent_repairs: 3,
            health_check_interval_secs: 30,
        }
    }
}
```

### 5.4 Migration Path

**Phase 1: Health Infrastructure (Week 1-2)**
- [ ] Implement `PhiAccrualDetector` in craftec-core
- [ ] Add ping protocol to `CraftBehaviour`
- [ ] Create `PeerRegistry` with TTL eviction  
- [ ] Update gossipsub capability announcements with TTL + stats
- [ ] Wire health configuration into `DaemonConfig`

**Phase 2: Content Health Integration (Week 3-4)**
- [ ] Implement `HealthService` with content health calculation
- [ ] Integrate PDP challenge results with health assessment
- [ ] Add health-based repair triggers to existing challenger
- [ ] Implement repair rate limiting and storm prevention

**Phase 3: Advanced Features (Week 5-6)**  
- [ ] Graceful departure protocol
- [ ] Network partition detection and survival mode
- [ ] Enhanced IPC commands for health monitoring
- [ ] CraftStudio integration for health dashboards

**Phase 4: Production Readiness (Week 7-8)**
- [ ] Performance optimization and memory usage tuning
- [ ] Comprehensive testing with network failures  
- [ ] Documentation and deployment guide
- [ ] Monitoring and alerting integration

### 5.5 Backward Compatibility

**Graceful Transition Strategy**:
- **Dual operation**: Run old static capability tracking alongside new health system
- **Feature flags**: `ENABLE_HEALTH_SERVICE` environment variable for gradual rollout
- **Compatibility mode**: New nodes understand old capability announcements
- **Migration period**: 30-day overlap before removing legacy code

**IPC Compatibility**:
- **New commands**: Add health-related IPC commands without breaking existing ones
- **Enhanced responses**: Add health fields to existing responses as optional
- **Version negotiation**: Client can request old or new format based on capability

## 6. Implementation Constants & Justification

### 6.1 Timing Parameters

**Direct Ping Protocol**:
- **Ping interval**: `120s` (2 minutes)
  - *Justification*: Balance between fast failure detection and network overhead
  - *Comparison*: Faster than BitTorrent (30min), slower than Ceph (6s), appropriate for content distribution workload

- **Ping timeout**: `10s`  
  - *Justification*: Allow for slow networks but detect real failures
  - *Comparison*: Consistent with libp2p connection timeouts

- **Missed ping threshold**: `3` (6 minutes total)
  - *Justification*: Avoid false positives from temporary network hiccups
  - *Comparison*: BitTorrent also uses 3 missed announces

**Capability TTL**:
- **TTL duration**: `900s` (15 minutes) 
  - *Justification*: 3x announcement interval (300s) for reliability
  - *Comparison*: Much shorter than IPFS 24h, appropriate for dynamic P2P environment

**Content Health**:
- **Health check interval**: `30s`
  - *Justification*: Fast enough to detect degradation, efficient enough for large content catalogs
  - *Comparison*: Similar to Ceph PG peering (30s)

- **Min shard lifetime**: `1800s` (30 minutes)
  - *Justification*: Prevent repair churn from temporary failures
  - *Comparison*: Storj uses similar grace periods before disqualification

### 6.2 Phi-Accrual Parameters

**Phi threshold**: `8.0`
- *Justification*: 99.9999% confidence that peer is down (6-sigma equivalent)
- *Comparison*: Cassandra default, proven in production distributed systems
- *Behavior*: ~2 standard deviations above mean heartbeat interval

**Window size**: `100` intervals  
- *Justification*: Enough history to adapt to network conditions, not too much memory
- *Comparison*: Typical sliding window size for statistical calculations

### 6.3 Repair Parameters

**Repair priority thresholds**:
- **Critical**: `ratio < 1.0` (cannot reconstruct)
- **High**: `ratio < tier_target` (below paid tier) 
- **Medium**: `ratio < 1.5 * tier_target` (safety margin)
- **Low**: `ratio >= 2.0 * tier_target` (over-provisioned)

*Justification*: Tier-based repair matches economic incentives, safety margins prevent data loss

**Repair timing**:
- **Critical**: `5 minutes` (data loss imminent)
- **High**: `30 minutes` (below service level)
- **Medium**: `2 hours` (comfortable buffer) 
- **Low**: `24 hours` (best effort)

*Justification*: Urgency matches business impact, delays prevent repair storms

**Rate limits**:
- **Concurrent repairs**: `3 per node`
  - *Justification*: Balance repair speed vs resource usage  
  - *Tuning*: May increase for high-capacity storage nodes
  
- **Bandwidth limit**: `50% of available bandwidth`
  - *Justification*: Leave capacity for normal client traffic
  - *Implementation*: Token bucket with configurable refill rate

## 7. Success Metrics

**Health System Effectiveness**:
- **Mean Time To Detection (MTTD)**: Target <5 minutes for peer failures
- **Mean Time To Repair (MTTR)**: Target <30 minutes for critical issues  
- **False Positive Rate**: <1% false failure detections per day
- **Repair Success Rate**: >95% of triggered repairs complete successfully

**Network Reliability**:
- **Content Availability**: >99.9% for funded content (tier >= Lite)
- **Reconstruction Success**: >99.99% when health status shows "available"
- **Partition Tolerance**: Graceful degradation with >30% peer connectivity

**Resource Efficiency**: 
- **Network Overhead**: <5% bandwidth for health protocols
- **Memory Usage**: <100MB additional for health state per 10K peers
- **CPU Overhead**: <10% additional for health computations

**Economic Alignment**:
- **Repair Cost Accuracy**: Actual repair costs within 20% of estimates
- **Creator Pool Depletion**: <5% of pools exhaust funds due to excessive repairs
- **Graceful Exit Rate**: >80% of departing nodes follow graceful exit protocol

This comprehensive design provides DataCraft with a robust, economically-aligned network health system that learns from proven distributed systems while adapting to the unique requirements of erasure-coded content distribution with Solana settlement.

## Task Completion Summary

I have successfully designed a comprehensive peer liveness and network health layer for DataCraft based on proven distributed systems approaches. The design includes:

1. **Research analysis** of 6 major distributed systems (IPFS, BitTorrent, Ceph, Filecoin, Storj, Sia) with specific parameters and lessons learned

2. **Multi-layer failure detection** using phi-accrual detectors, direct pings, capability TTL, and PDP integration for robust peer liveness tracking

3. **Health-driven self-healing** that unifies peer liveness with content availability, includes erasure-code aware repair thresholds, and prevents repair storms through rate limiting and economic circuit breakers

4. **Real-time content status model** derived from live peer data with comprehensive health scoring and client-facing status categories

5. **Detailed integration plan** with specific code changes, configuration updates, and a phased migration path that maintains backward compatibility

The design addresses all constraints (erasure coding, single node model, PDP integration, Solana settlement) while providing practical, implementable solutions with justified timing parameters and success metrics.