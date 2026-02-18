# Piece Tracking Design — Event-Sourced Network State

## Overview

Replaces the current announcement-based system (large gossipsub messages with piece counts) with an event-sourced model. Every piece store/drop is a small signed event on gossipsub. All nodes maintain the same materialized view (PieceMap) of who holds what across the entire network.

## Core Events

Two event types, signed by the originating node:

```rust
PieceStored {
    node: PeerId,
    cid: ContentId,
    segment: u32,
    piece_id: [u8; 32],
    coefficients: Vec<u8>,    // k bytes over GF(2^8)
    seq: u64,                 // per-node monotonic sequence number
    timestamp: u64,           // unix seconds
    signature: Vec<u8>,       // ed25519 signature
}

PieceDropped {
    node: PeerId,
    cid: ContentId,
    segment: u32,
    piece_id: [u8; 32],
    seq: u64,
    timestamp: u64,
    signature: Vec<u8>,
}
```

**Size**: PieceStored ≈ 220 bytes, PieceDropped ≈ 180 bytes. Well within gossipsub 1MB limit.

**Gossipsub topic**: `datacraft/pieces/1.0.0`

## Materialized View (PieceMap)

Every node maintains identical state:

```rust
struct PieceMap {
    // (node, cid, segment, piece_id) → coefficients
    pieces: HashMap<(PeerId, ContentId, u32, [u8; 32]), Vec<u8>>,
    
    // Per-node latest processed seq (for gap detection)
    node_seqs: HashMap<PeerId, u64>,
}
```

- `PieceStored` → insert entry
- `PieceDropped` → remove entry
- Duplicate seq → skip (idempotent)

### Derived Computations (from PieceMap, anytime)

- **Network rank per segment**: collect all coefficient vectors for (cid, segment), run `craftec_erasure::check_independence()`. True rank, not approximated from piece count.
- **Provider count per CID**: count unique nodes holding pieces for that CID.
- **Total pieces per CID/segment**: count entries.
- **Health ratio**: rank / k per segment.
- **Merkle root**: hash of all pieces the local node holds (filter by own PeerId). Replaces separate StorageMerkleTree.
- **Network health**: min health ratio across all segments of a CID.

## Sequence Numbers

Each node maintains a monotonic counter (`seq`). Incremented on every PieceStored or PieceDropped event the node emits.

Purpose:
- **Gap detection**: if you see seq 5 then seq 8 from a node, you missed 6 and 7.
- **Idempotency**: replaying same seq = skip.
- **Sync point**: "give me current state where node X's seq > N".

## Sync Protocol

Every node has the same PieceMap. Sync from any peer — aggregator, neighbor, whoever is closest.

### Cold Start (new node or restart)

1. Node connects to any peer
2. Sends: `SyncRequest { known_seqs: HashMap<PeerId, u64> }` — "here's the latest seq I have per node"
3. Peer responds with current state for any node where seq is higher (or full state if `known_seqs` is empty)
4. Node builds PieceMap from response
5. Subscribes to gossipsub → processes new events in real-time

### Gap Fill (missed gossipsub event)

1. Node sees seq gap from peer X (e.g., had seq 50, received seq 53)
2. Requests missing events from any peer or aggregator
3. Fills gap, PieceMap stays consistent

### No Special Bootstrap Node

Any node can serve sync. The aggregator is NOT special for sync — it's only special for long-term history. This makes the system resilient — no single point of failure for bootstrapping.

## Aggregator

The aggregator keeps the **full event history** (append-only, never pruned):

```rust
struct PieceEventLog {
    events: Vec<PieceEvent>,              // full history
    by_node: HashMap<PeerId, Vec<usize>>, // index for fast node-specific queries
}
```

### Aggregator Responsibilities

- **Long-term storage usage history**: how much each node stored over time
- **Node contribution tracking**: uptime, piece counts over periods
- **Analytics**: content popularity, network growth, churn rates
- **Billing/settlement data**: proof of storage duration

### Not Aggregator Responsibilities

- Sync (any node can do this)
- Health computation (every node does this locally)

## Node Event Buffer

Nodes keep a **24-hour rolling buffer** of events for:
- Recent history / local health trending
- Serving gap-fill requests from peers
- Debugging

Older events are discarded on nodes. Only the aggregator keeps everything.

## What This Replaces

| Current System | New System |
|---|---|
| Capability announcements with `piece_counts` HashMap | PieceStored/PieceDropped events |
| `peer_scorer.piece_counts` | PieceMap materialized view |
| Inventory requests in challenger | Read coefficient vectors from PieceMap |
| `compute_network_health()` aggregating peer_scorer | Compute rank from PieceMap via independence checking |
| Separate `StorageMerkleTree` | Derive from PieceMap (own pieces) |
| Large gossipsub announcements (>1MB for 10GB nodes) | Tiny per-event messages (~200 bytes) |

## Node Online/Offline Status

Node status is derived from existing mechanisms — no new events needed.

### Detection

- **Online**: capability announcement received within `capability_announce_interval × 2`
- **Offline (graceful)**: `GoingOffline` broadcast received
- **Offline (crash/disconnect)**: no capability announcement for timeout period

### PieceMap Integration

```rust
struct PieceMap {
    // Scoped: only segments this node holds pieces in
    pieces: HashMap<(PeerId, ContentId, u32, [u8; 32]), Vec<u8>>,
    node_seqs: HashMap<PeerId, u64>,
    node_online: HashMap<PeerId, bool>,      // derived from announcements
    node_last_seen: HashMap<PeerId, Instant>, // last announcement time
    tracked_segments: HashSet<(ContentId, u32)>,  // segments we hold pieces for
}
// Stored on disk. Loaded per-segment for rank computation.
```

### Health Impact

When computing rank for a segment, only include coefficient vectors from **online** nodes. Offline nodes' pieces exist in PieceMap but are unavailable for reconstruction.

```
effective_rank(cid, segment) = independence_check(
    coefficients from PieceMap WHERE node is online
)
```

### HealthScan on Status Change

Node online/offline changes affect effective rank. HealthScan's periodic scan (default 30s) picks this up naturally — no special event handling needed. Offline node's pieces remain in PieceMap but are excluded from rank computation.

### Capability Announcements (Slimmed Down)

Capability announcements remain as heartbeat but no longer carry `piece_counts` — that data flows via PieceStored/PieceDropped events. Announcements carry:
- Capabilities (storage, aggregator, etc.)
- Region
- Max storage bytes
- Used storage bytes

## PDP Integration

PDP still verifies actual possession — PieceMap represents **claimed state**. A node could emit PieceStored without actually storing. PDP challenges verify the claim.

Flow:
1. Challenger reads PieceMap for the CID → knows claimed rank, providers, coefficient vectors
2. Challenges providers: "prove you have piece X by returning its data"
3. Cross-verifies coefficient vectors from PieceMap against returned data
4. Pass → StorageReceipt. Fail → mark provider, adjust trust.

## Repair & Degradation — Periodic HealthScan

No separate signal topics. No event-driven reactions. **HealthScan** runs periodically (default 30s) and scans all tracked segments.

### Scoped PieceMap

Each node's PieceMap only tracks segments it holds pieces in — not all network content. Stored on disk, not RAM. Scales to any network size:

| Network size | Full PieceMap (unscoped) | Scoped (10 GB node) |
|---|---|---|
| 1 TB | ~880 MB | ~8.6 MB |
| 10 TB | ~8.8 GB | ~8.6 MB |
| 1 PB | ~880 GB | ~8.6 MB |

**Scoping lifecycle:**
1. **First piece for new segment** → `track_segment(cid, segment)`: scoped sync from peers (query coefficient vectors for that segment only), then start processing gossipsub events for it
2. **Gossipsub events** → only process for tracked segments, discard the rest
3. **Drop all pieces for segment** → `untrack_segment(cid, segment)`: delete PieceMap entries

### HealthScan Logic

```
every 30 seconds:
  for each tracked segment (cid, segment):
      rank = compute_rank(cid, segment)  // independence check on online nodes' coefficients
      k = manifest.k_for_segment(segment)
      target = ceil(tier_target * k)
      deficit = target - rank

      if deficit > 0:
          // Deterministic repair — all nodes compute same provider ranking
          providers = sort_by_piece_count_desc(providers_for(cid, segment))
          if my_position_in(providers) < deficit:
              create_orthogonal_piece(cid, segment)  // guaranteed independent
              push_to_non_provider()

      if rank > target AND !has_demand(cid) AND my_pieces(cid, segment) > 2:
          drop_one_piece(cid, segment)
```

### Why Periodic Instead of Event-Driven

- **No feedback loops**: repair creating PieceStored doesn't trigger degradation, and vice versa
- **No event tagging**: don't need to distinguish publish/distribution/repair/scaling/degradation events
- **Less compute**: one scan every 30s vs checking on every gossipsub event
- **Simple**: easy to reason about, test, and debug

### Deterministic Repair (No Delays)

All nodes with the same PieceMap entries for a segment compute the **same provider ranking** (sorted by piece count descending, PeerId tiebreak). The top N providers (N = deficit) each create 1 piece. No coordination signals, no random delays — shared state IS the coordination.

### Orthogonal Piece Creation

With the full coefficient matrix from PieceMap, repairing nodes compute a vector that is **guaranteed linearly independent** from all existing pieces. No random recombination, no ~1/256 dependence chance. Every repair action = +1 rank.

### What This Eliminates

- `RepairSignal`, `RepairAnnouncement`, `RepairMessage`, `REPAIR_TOPIC`
- `DegradationSignal`, `DegradationAnnouncement`, `DegradationMessage`, `DEGRADATION_TOPIC`
- `RepairCoordinator`, `DegradationCoordinator`, `HealthReactor`
- Challenger emitting repair/degradation signals
- Random delay coordination
- Event-driven health checking

### Demand Awareness

DemandSignal stays as a separate gossipsub topic (`datacraft/scaling/1.0.0`). HealthScan checks `DemandSignalTracker.has_recent_signal(cid)` before degrading.

## Burst Handling

Initial distribution of a 35MB file = ~140 PieceStored events ≈ 30KB total. Gossipsub handles this fine — no message count limit, only per-message size limit (1MB). Many nodes distributing simultaneously is still small messages.

## Trust & Verification

- Events are **signed** by the originating node (ed25519)
- Invalid signatures → reject event
- PDP verifies actual possession periodically
- False PieceStored without actual storage → PDP fail → no receipt → no earnings
- False PieceDropped → node loses pieces from PieceMap → lower perceived contribution

## What This Replaces (updated)

| Current System | New System |
|---|---|
| Capability announcements with `piece_counts` | PieceStored/PieceDropped events |
| `peer_scorer.piece_counts` | PieceMap materialized view |
| Inventory requests in challenger | Read coefficient vectors from PieceMap |
| `compute_network_health()` aggregating peer_scorer | Compute rank from PieceMap via independence checking |
| Separate `StorageMerkleTree` | Derive from PieceMap (own pieces) |
| Large gossipsub announcements (>1MB) | Tiny per-event messages (~200 bytes) |
| RepairSignal/Announcement + RepairCoordinator | Periodic HealthScan with deterministic assignment |
| DegradationSignal/Announcement + DegradationCoordinator | Periodic HealthScan |
| Challenger-driven repair/degradation | Every node scans independently |
| Random delay coordination | Deterministic provider ranking from shared PieceMap |
| Full network PieceMap (RAM) | Scoped PieceMap on disk (only owned segments) |

## Migration Path

1. ✅ Implement PieceMap + PieceEvent types + gossipsub topic (Phase 1)
2. ✅ Emit PieceStored/Dropped from existing store/drop code paths (Phase 1)
3. Build HealthScan (periodic repair/degradation scanning owned segments)
4. Make PieceMap scoped (track/untrack segments, disk storage, scoped sync on first piece)
5. Implement orthogonal piece creation in craftec-erasure
6. Update health computation to use PieceMap with independence checking
7. Update handler/challenger to read from PieceMap
8. Remove old systems: RepairCoordinator, DegradationCoordinator, HealthReactor, peer_scorer.piece_counts, inventory requests, announcements piece_counts, StorageMerkleTree
