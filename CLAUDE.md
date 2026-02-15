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
- **119 tests passing**, build + clippy clean
- **Capability announcements wired**: Nodes subscribe to gossipsub topic, broadcast capabilities periodically (every 5 min), track peer capabilities in memory
- **TransferReceipts generated on shard transfers**: Requester signs receipt with ed25519 key after receiving shard data, sends back to server. Server verifies signature and stores in PersistentReceiptStore (append-only binary file with in-memory indices, dedup, CID/node/time queries)
- **Signing module active**: `crates/core/src/signing.rs` provides `sign_transfer_receipt`, `verify_transfer_receipt`, `peer_id_to_ed25519_pubkey`
- **Payment channel persistence**: `ChannelStore` (daemon) manages open channels on disk (one JSON per channel in `~/.datacraft/channels/`). Full voucher validation (sig verify, nonce, cumulative amounts). IPC handlers (`channel.open`, `channel.voucher`, `channel.close`, `channel.list`) wired to ChannelStore.
- **Protocol egress pricing**: Fixed rate `PROTOCOL_EGRESS_PRICE_PER_BYTE` (1 USDC lamport/byte) in `economics.rs`. `EgressPricing` struct with `cost()` and `covers()` helpers. Nodes compete on performance, not price.
- **Not yet implemented**: StorageReceipt generation (requires PDP challenger), settlement on-chain, payment channel on-chain settlement

## Key Design Decisions (from recent discussions)

- **Capabilities, not roles**: Nodes declare capabilities (Storage, Relay, Client, Aggregator). Each announced via per-craft gossip topics.
- **Separate DHTs**: DataCraft runs its own Kademlia instance, separate from TunnelCraft. Cross-craft peer discovery via shared gossipsub (cross-pollination).
- **Node model**: Single node, two cost dimensions (storage = per-byte-per-time, transfer = per-byte-per-event).
- **Erasure distribution, NOT replication**: No duplicates. Each storage node holds a unique parity shard. Dynamic shard count grows/shrinks with provider set.
- **Immutable ChunkManifest**: Contains k, chunk_size, erasure_config, content_hash. Does NOT track shard count or locations.
- **Creator Pool model**: One pool per creator, funds all their CIDs. USDC balance. StorageReceipt (PDP) is the ONLY settlement mechanism for storage distribution. No transfer/egress settlement pool.
- **Payment channels for premium egress**: Users open payment channels directly with storage nodes. Cumulative vouchers (latest supersedes all previous, like Filecoin). Protocol fee on redemption.
- **TransferReceipt is analytics-only**: Still generated for bandwidth monitoring and node reputation via `craftec-identity`, but NOT used for settlement.
- **Free egress has no reward**: Best effort, volunteer. No receipt, no settlement.
- **Protocol fee**: Flat fee in basis points (default 5%) on all on-chain settlements (PDP claims + payment channel redemptions).
- **PDP peer rotation**: Challenger nominated from providers, sorted by online time. Challenger fetches k-1 shards, verifies all providers, assesses health, heals if needed (generates new parity). Self-healing loop.
- **Tier-guaranteed shard ratios**: Lite=2x, Standard=3x, Pro=5x, Enterprise=10x. Tier is minimum not cap.
- **Dual revenue**: Funded CID + paying user = storage node earns from creator pool (PDP) + payment channel (premium egress). Two separate services, two separate payers.
- **Free tier still issues StorageReceipts** for reputation tracking — no settlement, but receipts feed into craftec-identity.

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
