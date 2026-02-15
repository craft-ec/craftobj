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
- **166 tests passing**, build + clippy clean
- **Capability announcements wired**: Nodes subscribe to gossipsub topic, broadcast capabilities periodically (every 5 min), track peer capabilities in memory
- **TransferReceipts generated on shard transfers**: Requester signs receipt with ed25519 key after receiving shard data, sends back to server. Server verifies signature and stores in PersistentReceiptStore (append-only binary file with in-memory indices, dedup, CID/node/time queries)
- **Signing module active**: `crates/core/src/signing.rs` provides `sign_transfer_receipt`, `verify_transfer_receipt`, `peer_id_to_ed25519_pubkey`
- **Payment channel persistence**: `ChannelStore` (daemon) manages open channels on disk (one JSON per channel in `~/.datacraft/channels/`). Full voucher validation (sig verify, nonce, cumulative amounts). IPC handlers (`channel.open`, `channel.voucher`, `channel.close`, `channel.list`) wired to ChannelStore.
- **Protocol egress pricing**: Fixed rate `PROTOCOL_EGRESS_PRICE_PER_BYTE` (1 USDC lamport/byte) in `economics.rs`. `EgressPricing` struct with `cost()` and `covers()` helpers. Nodes compete on performance, not price.
- **Content encryption**: ChaCha20-Poly1305 per-content key, nonce prepended. Publish with `encrypted=true` → `(CID, content_key)`. Reconstruct with key → plaintext, without → ciphertext.
- **Access control (access.rs)**: `AccessEntry`/`AccessList` types. ECDH(ephemeral, recipient_x25519) + ChaCha20 key wrapping. Signed by creator. Bincode serialization for DHT.
- **Proxy Re-Encryption (pre.rs)**: Client-side PRE using x25519 ECDH. `encrypt_content_key` (to creator), `generate_re_key` (creator→recipient), `re_encrypt_with_content_key`, `decrypt_re_encrypted`. Ed25519→x25519 via SHA-512 clamping (secret) + Edwards→Montgomery (public).
- **Client PRE API**: `publish_with_pre()`, `grant_access()` (returns ReKeyEntry + ReEncryptedKey for DHT), `reconstruct_with_pre()` (recipient decrypts via PRE).
- **DHT access metadata**: AccessList and ReKeyEntry stored/retrieved via DHT (`/datacraft/access/<cid>`, `/datacraft/rekey/<cid>/<did>`). ContentRouter methods: `put_access_list`, `get_access_list`, `put_re_key`, `get_re_key`, `remove_re_key`. Bincode serialization. Tombstone pattern for revocation.
- **Access IPC handlers**: `access.grant` (generates ReKeyEntry + ReEncryptedKey, stores in DHT), `access.revoke` (tombstones re-key), `access.revoke_rotate` (revoke + rotate content key + re-encrypt content + re-grant remaining users), `access.list` (fetches AccessList from DHT, returns authorized DIDs). Full async DHT round-trip via protocol event flow.
- **Content revocation with key rotation**: `revoke_and_rotate()` in client generates new content key, re-encrypts content (new CID), re-grants remaining users via PRE. `access.revoke_rotate` IPC handler orchestrates full flow: tombstone old re-key → rotate → re-grant → store in DHT → announce new CID.
- **StorageReceipt generation wired into PDP challenger**: ChallengerManager signs receipts with ed25519 after successful PDP challenges, persists to PersistentReceiptStore. `receipt.storage.list` IPC handler with pagination/filters. Challenger runs periodically in daemon event loop.
- **Settlement module wired**: `SolanaClient` in `crates/daemon/src/settlement.rs` wraps `craftec-settlement` instruction builders. Methods: `create_creator_pool`, `fund_pool`, `claim_pdp`, `open_payment_channel`, `close_payment_channel`, `force_close_channel`. IPC handlers: `settlement.create_pool`, `settlement.fund_pool`, `settlement.claim`, `settlement.open_channel`, `settlement.close_channel`. Currently dry-run mode (LoggingTransport) — swap to real `solana-client` RPC when program is deployed. Initialized in `DataCraftService` with ed25519 signing key from libp2p keypair.
- **Not yet implemented**: actual Solana RPC submission (needs `solana-client`/`solana-sdk` deps + deployed program), challenger→claim_pdp integration in challenger loop, payment channel close→settlement integration

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
