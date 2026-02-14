# CLAUDE.md — DataCraft

Read the parent [CLAUDE.md](../CLAUDE.md) first for ecosystem conventions.

## Design Docs (READ THESE FIRST)

- [DataCraft Design](../docs/DATACRAFT_DESIGN.md) — Protocol spec, node model, economics
- [Craftec Ecosystem](../docs/CRAFTEC_ECOSYSTEM.md) — Shared infra, capabilities, cross-craft architecture

**Design docs are the source of truth.** They evolve during conversation with the human. Always read the latest version before starting work — stale assumptions break things.

## Crate Structure

```
crates/
├── core/       ContentId, ChunkManifest, StorageReceipt, wire protocol, constants
├── store/      Content-addressed filesystem storage, PinManager, GC
├── routing/    DHT content provider records, CID resolution
├── transfer/   Piece exchange protocol, shard request/response codec
├── client/     High-level orchestration: publish, fetch, pin, unpin
├── daemon/     Background service: swarm, protocol, IPC handler, commands
└── uniffi/     Mobile bindings (future)
```

## Current State

- **P2P pipeline working**: Publish → DHT announce → DHT resolve → libp2p-stream transfer → fetch complete
- **34 tests passing**, build + clippy clean
- **Economics layer not implemented**: StorageReceipt struct exists but is never generated on transfer. No capability announcement, no pool/subscription logic, no free/paid distinction.

## Key Design Decisions (from recent discussions)

- **Capabilities, not roles**: Nodes declare capabilities (Storage, Relay, Client, Aggregator). Each announced via per-craft gossip topics.
- **Separate DHTs**: DataCraft runs its own Kademlia instance, separate from TunnelCraft. Cross-craft peer discovery via shared gossipsub (cross-pollination).
- **Node model**: Single node, two cost dimensions (storage = per-byte-per-time, transfer = per-byte-per-event).
- **Erasure distribution, NOT replication**: No duplicates. Each storage node holds a unique parity shard. Dynamic shard count grows/shrinks with provider set.
- **Immutable ChunkManifest**: Contains k, chunk_size, erasure_config, content_hash. Does NOT track shard count or locations.
- **Two receipt types**: StorageReceipt (from PDP proof-of-possession) + TransferReceipt (from serving data, signed by requester).
- **PDP peer rotation**: Challenger nominated from providers, sorted by online time. Challenger fetches k-1 shards, verifies all providers, assesses health, heals if needed (generates new parity). Self-healing loop.
- **Storage economics attach to the CID** (funded pool or not). **Transfer economics attach to the requester** (subscriber or free-tier).
- **Two pool buckets**: Storage + Transfer, protocol-wide ratio constant. Cache nodes compete in same storage bucket via PDP.
- **Tier-guaranteed shard ratios**: Lite=2x, Standard=3x, Pro=5x, Enterprise=10x. Tier is minimum not cap.
- **CID shared pool**: Fluid settlement, claim anytime. No epoch boundary.
- **Subscription pool**: Time-bound, same as TunnelCraft.
- **Double-pool earning**: Funded CID + subscribed requester = node earns from both pools.
- **Free tier still issues receipts** for reputation tracking — no settlement, but receipts feed into craftec-identity.

## Reference Implementation

TunnelCraft (`../tunnelcraft/`) solved many of the same patterns first. Study it for:
- Protocol handlers (`crates/network/src/`)
- Swarm event loop (`crates/daemon/src/`)
- Receipt generation and exchange

## Build & Verify

```bash
cargo build && cargo test && cargo clippy
```

Must pass before every commit. No exceptions.
