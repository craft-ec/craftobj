# DataCraft Network Health & Peer Scoring Design

## Overview

DataCraft uses **client-side peer scoring** to track peer reliability. Nodes respond to requests; nobody assigns them load. "Reducing load on a bad peer" means clients stop asking it, not the node self-removing.

## Design Principles

1. **Event-driven, not polled.** Scores update when interactions happen. No background health loop polling nothing.
2. **DHT record TTL + PDP cycles = liveness signals.** No separate ping protocol.
3. **Simple scoring.** Success/fail/timeout counters with exponential decay. No phi-accrual detector.
4. **Capabilities, not roles.** Nodes announce capabilities via gossipsub; the scorer tracks them alongside reliability.
5. **Free vs paid content economics.** Free content: standard erasure ratio baseline, nodes serve more if storage permits. Paid content: repair to tier target from creator pool.
6. **Challengers handle repair.** The PDP challenger for a CID decides repair urgency during its cycle. No separate repair coordinator.

## Peer Scoring

### PeerScorer (`crates/daemon/src/peer_scorer.rs`)

Replaces the old static `PeerCapabilities` map (`HashMap<PeerId, (Vec<Cap>, u64)>`).

**Per-peer state:**
- `capabilities` — from gossipsub announcements
- `last_announcement` — for TTL-based eviction
- `successes`, `failures`, `timeouts` — decayed interaction counters
- `avg_latency_ms` — exponential moving average

**Score formula:**
```
score = successes / (successes + failures + 2 * timeouts)
```
Range: 0.0–1.0. Unknown peers get 0.5 (benefit of the doubt). Timeouts weighted 2× because they consume more client time than fast failures.

**Decay:** All counters multiplied by 0.95 before each update. Old history fades so recent behavior dominates.

**Eviction:** `evict_stale(ttl)` removes peers whose last gossipsub announcement exceeds the TTL (default: 15 min = 3× the 5-min announcement interval). Called after processing gossipsub messages, not on a timer.

### Scoring Integration Points

- **Gossipsub capability announcements** → `scorer.update_capabilities()`
- **Shard fetch success/failure** → `scorer.record_success()` / `scorer.record_failure()` / `scorer.record_timeout()`
- **PDP challenge results** → same scoring methods
- **Peer selection for requests** → `scorer.rank_peers()` sorts by score descending
- **IPC `peers` command** → returns score and latency per peer

## Liveness

No dedicated liveness protocol. Liveness is inferred from:

1. **Gossipsub capability announcements** — periodic (every 5 min), evicted after TTL (15 min). If a peer stops announcing, it's evicted from the scorer.
2. **DHT provider records** — standard Kademlia TTL. If a peer doesn't re-announce to the DHT, its provider records expire.
3. **PDP challenges** — periodic storage proofs. Failed challenges update the scorer and may trigger repair for paid content.
4. **Interaction outcomes** — every shard request or PDP challenge updates the score. Consistently failing peers get deprioritized.

## Content Health

### Free Content
- Maintains standard erasure ratio (data_shards + parity_shards from manifest) for availability
- If storage permits, nodes obtain and serve additional parity shards
- No automatic repair — if shards are lost, they're lost (volunteer model)
- StorageReceipts still issued for reputation tracking via craftec-identity

### Paid Content (Creator Pool)
- Repair to tier target ratio (Lite=2×, Standard=3×, Pro=5×, Enterprise=10×)
- Repair funded from creator pool balance
- The PDP challenger drives repair: after verifying providers, if health is below tier target, generate new parity shards
- `health.rs::assess_health()` computes whether repair is needed and how many shards

### Challenger Duty Cycle
1. Resolve providers from DHT
2. Check rotation (sorted by online time) — is it our turn?
3. Fetch k-1 shards for verification
4. PDP challenge each provider
5. Assess health (`health.rs`): live shards vs tier target
6. If paid content below tier target → generate healing shards
7. Sign and persist StorageReceipts
8. Broadcast receipts via gossipsub

## Research Reference

The original design doc researched IPFS, BitTorrent, Ceph, Filecoin, Storj, and Sia. Key learnings that informed our simpler approach:

- **IPFS**: DHT provider record TTL (24h) as passive liveness — we adopted this principle
- **BitTorrent**: 3 missed announces = offline — we use TTL eviction similarly
- **Filecoin**: WindowPoSt as integrated storage proof + liveness — our PDP serves the same role
- **Storj**: Success rate tracking over rolling windows — our decayed counters achieve similar effect

What we explicitly did **not** adopt:
- Phi-accrual failure detector (Cassandra-style) — too complex for our needs
- Separate ping protocol — DHT + PDP + gossipsub already provide liveness signals
- Graceful departure protocol — unnecessary complexity; TTL eviction handles departures
- Polled health loop — event-driven scoring is simpler and more efficient
- Repair storm gossipsub coordination — the challenger rotation already prevents duplicate repairs
- Network partition detection modes — overkill for current scale
