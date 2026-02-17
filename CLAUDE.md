# CLAUDE.md — DataCraft

Read the parent [CLAUDE.md](../CLAUDE.md) first for ecosystem conventions.

## Design Docs (READ THESE FIRST)

- [DataCraft Design](../docs/DATACRAFT_DESIGN.md) — Protocol spec, node model, economics
- [Craftec Ecosystem](../docs/CRAFTEC_ECOSYSTEM.md) — Shared infra, capabilities, cross-craft architecture

**Design docs are the source of truth.** They evolve during conversation with the human. Always read the latest version before starting work — stale assumptions break things.

## Crate Structure

```
crates/
├── core/       ContentId, ContentManifest, StorageReceipt, wire protocol, constants
├── store/      Content-addressed filesystem storage, PinManager, GC
├── routing/    DHT content provider records, CID resolution
├── transfer/   Piece exchange protocol, piece request/response codec
├── client/     High-level orchestration: publish, fetch, pin, unpin
├── daemon/     Background service: swarm, protocol, IPC handler, commands
└── uniffi/     Mobile bindings (future)
```

**⚠️ Naming updates needed**: `ChunkManifest` → `ContentManifest` throughout all crates. `shard_index` → removed (pieces identified by coefficient vectors). `data_shards`/`parity_shards` → `piece_size`/`segment_size`/`initial_parity`.

## Current State

- **P2P pipeline working**: Publish → DHT announce → DHT resolve → persistent libp2p-stream transfer → fetch complete
- **StreamManager active**: Two unidirectional persistent streams per peer (Bitswap-style), background writer task, seq_id ack tracking. Replaced request_response.
- **166 tests passing**, build + clippy clean
- **⚠️ Erasure coding is Reed-Solomon — needs refactoring to RLNC**: All existing erasure code uses `reed-solomon-erasure` crate with `data_shards`/`parity_shards`/`shard_index` model. The design has moved to RLNC (Random Linear Network Coding) with segments (10MB), pieces (100KB), coefficient vectors, and no shard indices. See "RS → RLNC Refactoring" section below.
- **Peer scoring active**: `PeerScorer` in daemon tracks per-peer reliability, latency EMA, and capabilities from gossipsub.
- **StorageReceipts generated on PDP challenges**: ChallengerManager signs receipts after successful PDP challenges, persists to PersistentReceiptStore.
- **Signing module active**: `crates/core/src/signing.rs` provides receipt signing/verification.
- **Content encryption**: ChaCha20-Poly1305 per-content key. Publish with `encrypted=true` → `(CID, content_key)`.
- **Access control + PRE**: ECDH key wrapping, Proxy Re-Encryption for trustless access delegation via DHT.
- **Content removal daemon fully wired**: RemovalNotice, gossipsub propagation, pre-serve check.
- **Settlement module (dry-run)**: `SolanaClient` wraps `craftec-settlement` instruction builders. Real RPC transport available.

### What's been removed from the design (code still exists, needs cleanup)

- **TransferReceipt for settlement** — removed. No transfer/egress receipts for payment. StorageReceipt (PDP) is the sole settlement mechanism.
- **Payment channels** — removed. No user↔storage-node payment channels. Subscriptions replace per-byte egress pricing.
- **Per-byte egress pricing** — removed. `PROTOCOL_EGRESS_PRICE_PER_BYTE` in economics.rs is obsolete.
- **ChunkManifest with RS parameters** — replaced by ContentManifest with RLNC parameters.

## Key Design Decisions (from current design)

- **RLNC, NOT Reed-Solomon**: Random Linear Network Coding over GF(2^8). Pieces identified by coefficient vectors, not shard indices. Any node with 2+ pieces can create new unique pieces via linear combination. No 256-shard limit.
- **Segments + Pieces**: Content split into 10MB segments, each segment into 100KB pieces. k=100 per full segment (k = ceil(segment_bytes/piece_size) for partial segments). Initial parity: 1.2x (20 extra coded pieces).
- **ContentManifest is simple**: `content_hash`, `segment_count`, `erasure_config` (segment_size, piece_size, initial_parity). No shard count, no piece locations, no Merkle roots.
- **"Give me any piece for segment Y"**: Client requests any piece, provider returns random piece + coefficient vector. Client checks linear independence. Cheaper to discard dependent piece (~1/256 probability) than coordinate upfront.
- **Client-side delivery intelligence**: Storage nodes are dumb piece servers. All fetch strategy (connection pooling, provider selection, piece validation) lives in the client.
- **PDP uses coefficient vector cross-checking**: Challenger holds pieces, uses GF(2^8) linear algebra to verify prover's data. No Merkle roots needed. Coefficient vector IS piece identity.
- **Four-function distribution**: Push (initial upload), Distribution (ongoing equalization), Repair (reactive after PDP), Scaling (demand-driven, bounded by tier max).
- **Dual-pool economics**: Global Pool (protocol fees → all nodes by total PDP passes) + Creator Pool (creator allocation → nodes storing their content by PDP passes).
- **Subscriptions, not payment channels**: Creator subscription + platform subscription → revenue to creators. Protocol fee → Global Pool.
- **Priority serving**: Creator subscriber > Platform subscriber > Free user. On-chain subscription proofs, cached by storage nodes.
- **No separate egress revenue**: PDP covers everything. Holding data IS the service.
- **Capabilities, not roles**: Nodes declare capabilities (Storage, Relay, Client, Aggregator).
- **Separate DHTs**: DataCraft runs its own Kademlia instance.
- **Bounded redundancy**: Base (pool-funded) → demand-driven scaling → max (hard ceiling). Tiers: Free(1.5x/3x), Lite(2x/4x), Standard(3x/6x), Pro(5x/10x), Enterprise(10x/20x).

## RS → RLNC Refactoring

The existing codebase is built on Reed-Solomon erasure coding. The design has moved to RLNC. Here's what needs to change:

### craftec-core (shared crate)
- **`crates/erasure/src/lib.rs`**: Core RS implementation using `reed-solomon-erasure` crate. `ErasureCoder` wraps `ReedSolomon` with `data_shards`/`parity_shards`. Must be replaced with RLNC encoder/decoder over GF(2^8).
- **`crates/erasure/src/chunker.rs`**: Chunking logic. Needs update for segment/piece model.
- **`crates/erasure/Cargo.toml`**: Depends on `reed-solomon-erasure`. Replace with RLNC library or custom implementation.
- **`ErasureConfig`**: Currently has `data_shards`/`parity_shards`. Replace with `piece_size`/`segment_size`/`initial_parity`.

### datacraft/crates/core/
- **`src/lib.rs`**: Defines `ChunkManifest` (rename to `ContentManifest`), uses `ErasureConfig` from craftec-erasure. Update struct fields.
- **`src/signing.rs`**: Tests reference `shard_index` in StorageReceipt. Update to use `piece_id` (SHA-256 of coefficient vector).
- **`src/economics.rs`**: References `shard_index` in test receipts, has `PROTOCOL_EGRESS_PRICE_PER_BYTE` (remove).

### datacraft/crates/store/
- **`src/lib.rs`**: Stores shards at `chunks/<cid>/<chunk_index>/<shard_index>`. Change to segment/piece model with coefficient vector identification.

### datacraft/crates/transfer/
- **`src/lib.rs`**: Wire protocol uses `[content_id:32][chunk_index:4][shard_index:1]`. Replace with segment-based "any piece" request model.

### datacraft/crates/routing/
- **`src/lib.rs`**: References `ChunkManifest`. Rename to `ContentManifest`.

### datacraft/crates/client/
- **`src/lib.rs`**: Publish flow uses RS encode. Update to RLNC segment/piece model.
- **`src/extension.rs`**: Fetch reconstruction uses `data_shards` and RS decode. Replace with RLNC Gaussian elimination.
- **`tests/e2e.rs`, `tests/e2e_two_node.rs`**: Reference RS concepts. Update tests.

### datacraft/crates/daemon/ (many files)
- **`src/commands.rs`**: `DataCraftCommand` variants use `ChunkManifest`, `shard_index`. Update.
- **`src/service.rs`**: References `ChunkManifest`, `shard_index` in command handling. Update.
- **`src/protocol.rs`**: Uses `ChunkManifest`, `shard_index`, `TransferReceipt`. Update wire protocol.
- **`src/handler.rs`**: Publish handler uses `data_shards`/`parity_shards`. Update to RLNC.
- **`src/content_tracker.rs`**: Uses `ChunkManifest`, `data_shards + parity_shards` for total shard count. Update.
- **`src/health.rs`**: Health assessment uses `shard_index`, `ChunkManifest`. Replace with rank-based health from coefficient vectors.
- **`src/pdp.rs`**: PDP challenge uses `shard_index`. Replace with coefficient vector cross-checking.
- **`src/challenger.rs`**: Queries `max_shard_index`, challenges by shard index. Replace with coefficient vector model.
- **`src/eviction.rs`**: References `ChunkManifest`, `default_erasure_config()`. Update.
- **`src/reannounce.rs`**: Uses `data_shards + parity_shards` for shard iteration. Update.
- **`src/receipt_store.rs`**: Dedup hash includes `shard_index`. Replace with `piece_id`.
- **`src/settlement.rs`**: Test receipts use `shard_index`. Update.

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
