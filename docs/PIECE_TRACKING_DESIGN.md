# Piece Tracking Design — DHT + Merkle Diff Pulls

## Overview

Replaces the previous event-sourced gossipsub model with a **DHT-primary architecture**. Piece provider records live in the DHT (Kademlia). Each node's PieceMap is populated via **Merkle diff pulls** from providers during periodic HealthScan, not gossipsub events. PieceStored/PieceDropped are **local internal events only** — they update the local PieceMap and DHT records but are not broadcast on gossipsub.

**Key insight**: Push cost scales with network activity (O(N × network_activity)). Pull cost scales with local scope (O(local_scope)). At 1M+ users with frequent uploads, push piece events = ~1.75 MB/s per node (unsustainable for home users). Pull: only query providers for YOUR segments during HealthScan.

## DHT Provider Records

When a node stores a piece, it writes a provider record to the DHT. When it drops a piece, it removes the record. Records have a **24h TTL** for crash recovery — stale records from crashed nodes expire naturally.

```rust
// Written to DHT on store, removed on drop
ProviderRecord {
    cid: ContentId,
    segment: u32,
    peer_id: PeerId,
    // Kademlia handles the rest (TTL, replication)
}
```

## Local Internal Events

PieceStored and PieceDropped are internal events within a node — they trigger local PieceMap updates and DHT record writes/removals, but are **never broadcast on gossipsub**.

```rust
// Local event only — not broadcast
PieceStored {
    node: PeerId,
    cid: ContentId,
    segment: u32,
    piece_id: [u8; 32],
    coefficients: Vec<u8>,    // k bytes over GF(2^8)
    timestamp: u64,
}

// Local event only — not broadcast
PieceDropped {
    node: PeerId,
    cid: ContentId,
    segment: u32,
    piece_id: [u8; 32],
    timestamp: u64,
}
```

## Materialized View (PieceMap)

Each node maintains a scoped PieceMap — tracking only segments it holds pieces in:

```rust
struct PieceMap {
    // (node, cid, segment, piece_id) → coefficients
    pieces: HashMap<(PeerId, ContentId, u32, [u8; 32]), Vec<u8>>,
    node_online: HashMap<PeerId, bool>,
    node_last_seen: HashMap<PeerId, Instant>,
    tracked_segments: HashSet<(ContentId, u32)>,  // segments we hold pieces for
}
// Stored on disk. Loaded per-segment for rank computation.
```

**Population**: PieceMap is populated via Merkle diff pulls from providers during HealthScan (every 5 min). NOT via gossipsub events.

### Derived Computations (from PieceMap, anytime)

- **Network rank per segment**: collect all coefficient vectors for (cid, segment), run `craftec_erasure::check_independence()`. True rank, not approximated from piece count.
- **Provider count per CID**: count unique nodes holding pieces for that CID.
- **Total pieces per CID/segment**: count entries.
- **Health ratio**: rank / k per segment.
- **Merkle root**: hash of all pieces the local node holds (filter by own PeerId). Replaces separate StorageMerkleTree.
- **Network health**: min health ratio across all segments of a CID.

## Storage Merkle Tree & Diff Protocol

Each node maintains a Storage Merkle Tree of pieces it holds. This tree enables efficient incremental sync:

1. **First pull (bootstrapping)**: Requesting node has no root → provider sends full tree
2. **Subsequent pulls**: Requesting node sends last known root → provider computes diff → sends only changed entries
3. **Diff cost**: Proportional to changes since last pull, not total tree size

**Current implementation gap**: The Merkle tree implementation currently supports insert/remove/proof but needs diff support (send root, receive changes since that root). This is the primary implementation task.

## Scoped PieceMap

Each node's PieceMap only tracks segments it holds pieces in — not all network content. Stored on disk, not RAM. Scales to any network size:

| Network size | Full PieceMap (unscoped) | Scoped (10 GB node) |
|---|---|---|
| 1 TB | ~880 MB | ~8.6 MB |
| 10 TB | ~8.8 GB | ~8.6 MB |
| 1 PB | ~880 GB | ~8.6 MB |

**Scoping lifecycle:**
1. **First piece for new segment** → `track_segment(cid, segment)`: query DHT for providers, pull their Storage Merkle Trees for that segment (bootstrapping sync). This scoped sync on first piece may still be needed for bootstrapping, but the primary ongoing mechanism is periodic Merkle diff pulls.
2. **HealthScan (every 5 min)** → pull Merkle diffs from providers for all tracked segments, update PieceMap
3. **Drop all pieces for segment** → `untrack_segment(cid, segment)`: delete PieceMap entries

## Node Online/Offline Status

### Detection

- **Online**: capability announcement received on gossipsub within expected interval (~2 hours, given ~1 hour announcement frequency)
- **Offline (graceful)**: `GoingOffline` broadcast received
- **Offline (crash/disconnect)**: no capability announcement for timeout period
- **Liveness verification**: P2P request timeout via DHT relay path (no separate mechanism needed). DHT nodes naturally act as relays for NAT traversal.

### Health Impact

When computing rank for a segment, only include coefficient vectors from **online** nodes. Offline nodes' pieces exist in PieceMap but are unavailable for reconstruction.

```
effective_rank(cid, segment) = independence_check(
    coefficients from PieceMap WHERE node is online
)
```

### Capability Announcements (Single Gossipsub Topic)

The **only gossipsub topic**: `datacraft/capabilities/1.0.0`. ~170 bytes per message, every ~1 hour. Only storage nodes subscribe. Carries:
- Capabilities (storage, aggregator, relay, etc.)
- Region
- Max storage bytes
- Used storage bytes

Scales to 15M+ nodes at <1 MB/s. Provides node enumeration (DHT can't do "find all storage nodes").

## HealthScan — Periodic Repair & Degradation via Merkle Diff Pulls

**HealthScan** runs periodically (every 5 minutes). Each scan:

1. **Pulls Merkle diffs** from providers for all tracked segments → updates PieceMap
2. **Evaluates health** for each tracked segment
3. **Acts** on repair/degradation as needed

### HealthScan Logic

```
every 5 minutes:
  // Phase 1: Pull Merkle diffs
  for each tracked segment (cid, segment):
      providers = get_providers_from_dht(cid, segment)
      for each provider:
          diff = pull_merkle_diff(provider, last_known_root)
          apply_diff_to_piecemap(diff)

  // Phase 2: Evaluate and act
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

      if rank > target AND !has_local_demand(cid) AND my_pieces(cid, segment) > 2:
          drop_one_piece(cid, segment)
```

### Why Pull-Based Instead of Event-Driven

- **Scales with local scope**: Pull cost = O(local_scope). Push cost = O(N × network_activity). At 1M+ users, pull wins.
- **No feedback loops**: repair doesn't trigger degradation, and vice versa
- **Less bandwidth**: no gossipsub piece events flooding the network
- **NAT is not a differentiator**: same libp2p transport. Only difference is push vs pull bandwidth.
- **Simple**: easy to reason about, test, and debug

### Deterministic Repair (No Delays)

All nodes with the same PieceMap entries for a segment compute the **same provider ranking** (sorted by piece count descending, PeerId tiebreak). The top N providers (N = deficit) each create 1 piece. No coordination signals, no random delays — shared state IS the coordination.

### Orthogonal Piece Creation

With the full coefficient matrix from PieceMap, repairing nodes compute a vector that is **guaranteed linearly independent** from all existing pieces. No random recombination, no ~1/256 dependence chance. Every repair action = +1 rank.

### Demand Awareness

Demand tracking is **local fetch count only** — each node tracks how often it serves pieces for a CID. No global gossipsub demand signal. Degradation checks local demand before scheduling drops. This is sufficient for per-node degradation decisions.

### What This Eliminates

- `RepairSignal`, `RepairAnnouncement`, `RepairMessage`, `REPAIR_TOPIC`
- `DegradationSignal`, `DegradationAnnouncement`, `DegradationMessage`, `DEGRADATION_TOPIC`
- `RepairCoordinator`, `DegradationCoordinator`, `HealthReactor`
- Challenger emitting repair/degradation signals
- Random delay coordination
- Event-driven health checking
- `DemandSignalTracker` (gossipsub-based) — replaced by local fetch count
- `datacraft/pieces/1.0.0` gossipsub topic — piece tracking via DHT + Merkle diffs
- `datacraft/demand/1.0.0` gossipsub topic — demand is local
- `capability_announce_interval_secs` and `reannounce_interval_secs` config references

## PDP Integration

PDP still verifies actual possession — PieceMap represents **claimed state**. A node could write a DHT provider record without actually storing. PDP challenges verify the claim.

Flow:
1. Challenger reads PieceMap for the CID → knows claimed rank, providers, coefficient vectors
2. Challenges providers: "prove you have piece X by returning its data"
3. Cross-verifies coefficient vectors from PieceMap against returned data
4. Pass → StorageReceipt. Fail → mark provider, adjust trust.

## Aggregator Role

The aggregator is **NOT integral** to the network. Any node can build a global view from:
- Gossipsub capability announcements (node enumeration — the single gossipsub topic)
- Merkle diff pulls from providers (piece-level inventory)

Settlement can be done by anyone willing to pull all Merkle trees. The aggregator is a convenience for analytics and long-term history, not a required component.

## Trust & Verification

- DHT provider records are self-reported — PDP verifies actual possession
- PDP verifies actual possession periodically
- False provider record without actual storage → PDP fail → no receipt → no earnings
- Stale records from crashes → 24h TTL expiry + local dead-node cache

## What This Replaces

| Previous System | New System |
|---|---|
| PieceStored/PieceDropped gossipsub events | Local internal events + DHT provider records |
| Gossipsub-populated PieceMap | Merkle diff pull-populated PieceMap |
| `datacraft/pieces/1.0.0` topic | DHT provider records + Merkle diff pulls |
| `datacraft/demand/1.0.0` topic | Local fetch count |
| `DemandSignalTracker` (gossipsub) | Local fetch count tracking |
| Event-sourced sync protocol | Merkle tree diff protocol |
| Multiple gossipsub topics | Single topic: `datacraft/capabilities/1.0.0` |
| Capability announcements with `piece_counts` | Slim capability announcements (~170 bytes) |
| RepairSignal/Announcement + RepairCoordinator | Periodic HealthScan with deterministic assignment |
| DegradationSignal/Announcement + DegradationCoordinator | Periodic HealthScan |
| Challenger-driven repair/degradation | Every node scans independently via Merkle diffs |
| Random delay coordination | Deterministic provider ranking from shared PieceMap |
| Full network PieceMap (RAM) | Scoped PieceMap on disk (only owned segments) |

## Migration Path

1. ✅ Implement PieceMap + PieceEvent types (Phase 1)
2. ✅ Emit PieceStored/Dropped from existing store/drop code paths as local events (Phase 1)
3. Implement Merkle tree diff support (send root, receive changes)
4. Build HealthScan with Merkle diff pulls (every 5 min)
5. Write DHT provider records on piece store/drop
6. Make PieceMap scoped (track/untrack segments, disk storage, bootstrapping sync on first piece)
7. Implement orthogonal piece creation in craftec-erasure
8. Update health computation to use PieceMap with independence checking
9. Update handler/challenger to read from PieceMap
10. Remove old systems: RepairCoordinator, DegradationCoordinator, HealthReactor, peer_scorer.piece_counts, inventory requests, gossipsub piece/demand topics, DemandSignalTracker
