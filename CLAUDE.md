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
- **Separate DHTs**: DataCraft runs its own Kademlia instance, separate from TunnelCraft. Cross-craft peer discovery via shared gossipsub.
- **Node model**: Single node, two cost dimensions (storage = per-byte-per-time, transfer = per-byte-per-event).
- **Storage economics attach to the CID** (funded pool or not). **Transfer economics attach to the requester** (subscriber or free-tier).
- **Free tier still issues receipts** for reputation tracking — no settlement, but receipts feed into craftec-identity.
- **Three-bucket pool allocation**: Storage / Origin transfer / Cache transfer, with protocol-defined ratios.
- **Relays can cache**: A relay serving cached content earns from the cache transfer bucket.

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
