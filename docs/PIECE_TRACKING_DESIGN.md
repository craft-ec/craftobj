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

## PDP Integration

PDP still verifies actual possession — PieceMap represents **claimed state**. A node could emit PieceStored without actually storing. PDP challenges verify the claim.

Flow:
1. Challenger reads PieceMap for the CID → knows claimed rank, providers, coefficient vectors
2. Challenges providers: "prove you have piece X by returning its data"
3. Cross-verifies coefficient vectors from PieceMap against returned data
4. Pass → StorageReceipt. Fail → mark provider, adjust trust.

## Repair & Degradation

Same as current design, but health data comes from PieceMap instead of peer_scorer:

- **Repair**: PieceMap shows rank < tier target → emit RepairSignal
- **Degradation**: PieceMap shows rank > tier target AND no DemandSignal → emit DegradationSignal
- Demand tracking unchanged (separate DemandSignal gossipsub topic)

## Burst Handling

Initial distribution of a 35MB file = ~140 PieceStored events ≈ 30KB total. Gossipsub handles this fine — no message count limit, only per-message size limit (1MB). Many nodes distributing simultaneously is still small messages.

## Trust & Verification

- Events are **signed** by the originating node (ed25519)
- Invalid signatures → reject event
- PDP verifies actual possession periodically
- False PieceStored without actual storage → PDP fail → no receipt → no earnings
- False PieceDropped → node loses pieces from PieceMap → lower perceived contribution

## Migration Path

1. Implement PieceMap + events alongside current system
2. Emit PieceStored/Dropped from existing store/drop paths
3. Build PieceMap consumers (health, challenger, handler)
4. Remove old announcement/inventory code
5. Remove peer_scorer.piece_counts
