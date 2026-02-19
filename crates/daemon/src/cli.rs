//! CraftObj CLI — connects to a running daemon via IPC

use clap::{Parser, Subcommand};
use craftec_ipc::IpcClient;
use craftobj_daemon::service;

#[derive(Parser)]
#[command(name = "craftobj-cli", about = "CraftObj CLI — talks to a running daemon via IPC")]
struct Cli {
    /// Path to the daemon IPC socket
    #[arg(short, long, global = true)]
    socket: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    // ── Data ──────────────────────────────────────────────────────────────
    /// Show daemon status
    Status,
    /// List stored content CIDs
    List,
    /// Publish a file to the network
    Publish {
        /// Path to the file
        path: String,
        /// Encrypt before publishing
        #[arg(long)]
        encrypted: bool,
    },
    /// Fetch content by CID
    Fetch {
        /// Content ID (hex)
        cid: String,
        /// Output file path
        output: String,
        /// Decryption key (hex)
        #[arg(long)]
        key: Option<String>,
    },
    /// Pin content locally
    Pin {
        /// Content ID (hex)
        cid: String,
    },
    /// Unpin content
    Unpin {
        /// Content ID (hex)
        cid: String,
    },
    /// Extend content (regenerate pieces)
    Extend {
        /// Content ID (hex)
        cid: String,
    },

    // ── Health ────────────────────────────────────────────────────────────
    /// Content health info for a CID
    #[command(name = "content.health")]
    ContentHealth {
        /// Content ID (hex)
        cid: String,
    },
    /// Content health history
    #[command(name = "content.health-history")]
    ContentHealthHistory {
        /// Content ID (hex)
        cid: String,
        /// Unix timestamp (ms) cutoff
        #[arg(long)]
        since: Option<u64>,
    },
    /// Detailed list of all stored content
    #[command(name = "content.list-detailed")]
    ContentListDetailed,
    /// List segments for a CID
    #[command(name = "content.segments")]
    ContentSegments {
        /// Content ID (hex)
        cid: String,
    },
    /// Network-wide health summary
    #[command(name = "network.health")]
    NetworkHealth,
    /// Node statistics
    #[command(name = "node.stats")]
    NodeStats,

    // ── Network ──────────────────────────────────────────────────────────
    /// List known peers
    Peers,
    /// List connected peers (with addresses)
    #[command(name = "connected-peers")]
    ConnectedPeers,
    /// Node capabilities
    #[command(name = "node.capabilities")]
    NodeCapabilities,
    /// Network storage summary
    #[command(name = "network.storage")]
    NetworkStorage,
    /// Find providers for a CID
    #[command(name = "data.providers")]
    DataProviders {
        /// Content ID (hex)
        cid: String,
    },
    /// Remove content from network (creator only)
    #[command(name = "data.remove")]
    DataRemove {
        /// Content ID (hex)
        cid: String,
        /// Creator secret key (hex)
        creator_secret: String,
        /// Removal reason
        #[arg(long)]
        reason: Option<String>,
    },
    /// Delete content from local store only
    #[command(name = "data.delete-local")]
    DataDeleteLocal {
        /// Content ID (hex)
        cid: String,
    },

    // ── Access ────────────────────────────────────────────────────────────
    /// List authorized DIDs for a CID
    #[command(name = "access.list")]
    AccessList {
        /// Content ID (hex)
        cid: String,
    },
    /// Grant access via PRE re-encryption
    #[command(name = "access.grant")]
    AccessGrant {
        /// Content ID (hex)
        cid: String,
        /// Creator secret key (hex)
        creator_secret: String,
        /// Recipient public key (hex)
        recipient_pubkey: String,
        /// Content encryption key (hex)
        content_key: String,
    },
    /// Revoke access for a recipient
    #[command(name = "access.revoke")]
    AccessRevoke {
        /// Content ID (hex)
        cid: String,
        /// Recipient public key (hex)
        recipient_pubkey: String,
    },
    /// Revoke access and rotate content key
    #[command(name = "access.revoke-rotate")]
    AccessRevokeRotate {
        /// Content ID (hex)
        cid: String,
        /// Creator secret key (hex)
        creator_secret: String,
        /// Recipient to revoke (hex)
        recipient_pubkey: String,
        /// Current content key (hex)
        content_key: String,
        /// Remaining authorized pubkeys (hex, comma-separated)
        #[arg(long, value_delimiter = ',')]
        authorized: Vec<String>,
    },

    // ── Channels ─────────────────────────────────────────────────────────
    /// Open a payment channel
    #[command(name = "channel.open")]
    ChannelOpen {
        /// Sender pubkey (hex)
        sender: String,
        /// Receiver pubkey (hex)
        receiver: String,
        /// Amount to lock
        amount: u64,
    },
    /// Submit a voucher on a channel
    #[command(name = "channel.voucher")]
    ChannelVoucher {
        /// Channel ID (hex)
        channel_id: String,
        /// Cumulative amount
        amount: u64,
        /// Voucher nonce
        nonce: u64,
        /// Voucher signature (hex, optional)
        #[arg(long)]
        signature: Option<String>,
    },
    /// Close a payment channel
    #[command(name = "channel.close")]
    ChannelClose {
        /// Channel ID (hex)
        channel_id: String,
    },
    /// List payment channels
    #[command(name = "channel.list")]
    ChannelList {
        /// Filter by peer pubkey (hex)
        #[arg(long)]
        peer: Option<String>,
    },

    // ── Settlement ───────────────────────────────────────────────────────
    /// Create a creator settlement pool
    #[command(name = "settlement.create-pool")]
    SettlementCreatePool {
        /// Creator pubkey (hex)
        creator: String,
        /// Pool tier (default: 2)
        #[arg(long, default_value = "2")]
        tier: u8,
    },
    /// Fund a settlement pool
    #[command(name = "settlement.fund-pool")]
    SettlementFundPool {
        /// Creator pubkey (hex)
        creator: String,
        /// Amount to fund
        amount: u64,
    },
    /// Claim PDP rewards from a pool
    #[command(name = "settlement.claim")]
    SettlementClaim {
        /// Pool address (hex)
        pool: String,
        /// Operator/storage node pubkey (hex)
        operator: String,
        /// Weight for the claim
        weight: u64,
        /// Merkle proof hashes (hex, comma-separated)
        #[arg(long, value_delimiter = ',')]
        merkle_proof: Vec<String>,
        /// Leaf index in merkle tree
        #[arg(long, default_value = "0")]
        leaf_index: u32,
    },
    /// Open a settlement payment channel
    #[command(name = "settlement.open-channel")]
    SettlementOpenChannel {
        /// Payee pubkey (hex)
        payee: String,
        /// Amount to deposit
        amount: u64,
    },
    /// Close a settlement payment channel
    #[command(name = "settlement.close-channel")]
    SettlementCloseChannel {
        /// User pubkey (hex)
        user: String,
        /// Node pubkey (hex)
        node: String,
        /// Final amount
        amount: u64,
        /// Final nonce
        nonce: u64,
        /// Voucher signature (hex)
        #[arg(long)]
        voucher_signature: Option<String>,
    },

    // ── Receipts ─────────────────────────────────────────────────────────
    /// Get total receipt count
    #[command(name = "receipts.count")]
    ReceiptsCount,
    /// Query receipts
    #[command(name = "receipts.query")]
    ReceiptsQuery {
        /// Filter by CID (hex)
        #[arg(long)]
        cid: Option<String>,
        /// Filter by node pubkey (hex)
        #[arg(long)]
        node: Option<String>,
        /// Time range start (unix ms)
        #[arg(long)]
        from: Option<u64>,
        /// Time range end (unix ms)
        #[arg(long)]
        to: Option<u64>,
    },
    /// List storage receipts
    #[command(name = "receipt.storage.list")]
    ReceiptStorageList {
        #[arg(long, default_value = "100")]
        limit: u64,
        #[arg(long, default_value = "0")]
        offset: u64,
        /// Filter by CID (hex)
        #[arg(long)]
        cid: Option<String>,
        /// Filter by node pubkey (hex)
        #[arg(long)]
        node: Option<String>,
    },

    // ── Config ───────────────────────────────────────────────────────────
    /// Get daemon configuration
    #[command(name = "get-config")]
    GetConfig,
    /// Set daemon configuration (JSON string or key=value fields)
    #[command(name = "set-config")]
    SetConfig {
        /// JSON config string
        config: String,
    },

    // ── Lifecycle ────────────────────────────────────────────────────────
    /// Shut down the daemon
    Shutdown,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let socket = cli.socket.unwrap_or_else(service::default_socket_path);
    let client = IpcClient::new(&socket);

    let result = match cli.command {
        // Data
        Commands::Status => client.send_request("status", None).await,
        Commands::Peers => client.send_request("peers", None).await,
        Commands::List => client.send_request("list", None).await,
        Commands::Publish { path, encrypted } => {
            client.send_request("publish", Some(serde_json::json!({ "path": path, "encrypted": encrypted }))).await
        }
        Commands::Fetch { cid, output, key } => {
            let mut p = serde_json::json!({ "cid": cid, "output": output });
            if let Some(k) = key { p["key"] = serde_json::Value::String(k); }
            client.send_request("fetch", Some(p)).await
        }
        Commands::Pin { cid } => {
            client.send_request("pin", Some(serde_json::json!({ "cid": cid }))).await
        }
        Commands::Unpin { cid } => {
            client.send_request("unpin", Some(serde_json::json!({ "cid": cid }))).await
        }
        Commands::Extend { cid } => {
            client.send_request("extend", Some(serde_json::json!({ "cid": cid }))).await
        }

        // Health
        Commands::ContentHealth { cid } => {
            client.send_request("content.health", Some(serde_json::json!({ "cid": cid }))).await
        }
        Commands::ContentHealthHistory { cid, since } => {
            let mut p = serde_json::json!({ "cid": cid });
            if let Some(s) = since { p["since"] = serde_json::json!(s); }
            client.send_request("content.health_history", Some(p)).await
        }
        Commands::ContentListDetailed => client.send_request("content.list_detailed", None).await,
        Commands::ContentSegments { cid } => {
            client.send_request("content.segments", Some(serde_json::json!({ "cid": cid }))).await
        }
        Commands::NetworkHealth => client.send_request("network.health", None).await,
        Commands::NodeStats => client.send_request("node.stats", None).await,

        // Network
        Commands::ConnectedPeers => client.send_request("connected_peers", None).await,
        Commands::NodeCapabilities => client.send_request("node.capabilities", None).await,
        Commands::NetworkStorage => client.send_request("network.storage", None).await,
        Commands::DataProviders { cid } => {
            client.send_request("data.providers", Some(serde_json::json!({ "cid": cid }))).await
        }
        Commands::DataRemove { cid, creator_secret, reason } => {
            let mut p = serde_json::json!({ "cid": cid, "creator_secret": creator_secret });
            if let Some(r) = reason { p["reason"] = serde_json::Value::String(r); }
            client.send_request("data.remove", Some(p)).await
        }
        Commands::DataDeleteLocal { cid } => {
            client.send_request("data.delete_local", Some(serde_json::json!({ "cid": cid }))).await
        }

        // Access
        Commands::AccessList { cid } => {
            client.send_request("access.list", Some(serde_json::json!({ "cid": cid }))).await
        }
        Commands::AccessGrant { cid, creator_secret, recipient_pubkey, content_key } => {
            client.send_request("access.grant", Some(serde_json::json!({
                "cid": cid,
                "creator_secret": creator_secret,
                "recipient_pubkey": recipient_pubkey,
                "content_key": content_key,
            }))).await
        }
        Commands::AccessRevoke { cid, recipient_pubkey } => {
            client.send_request("access.revoke", Some(serde_json::json!({
                "cid": cid,
                "recipient_pubkey": recipient_pubkey,
            }))).await
        }
        Commands::AccessRevokeRotate { cid, creator_secret, recipient_pubkey, content_key, authorized } => {
            client.send_request("access.revoke_rotate", Some(serde_json::json!({
                "cid": cid,
                "creator_secret": creator_secret,
                "recipient_pubkey": recipient_pubkey,
                "content_key": content_key,
                "authorized": authorized,
            }))).await
        }

        // Channels
        Commands::ChannelOpen { sender, receiver, amount } => {
            client.send_request("channel.open", Some(serde_json::json!({
                "sender": sender,
                "receiver": receiver,
                "amount": amount,
            }))).await
        }
        Commands::ChannelVoucher { channel_id, amount, nonce, signature } => {
            let mut p = serde_json::json!({
                "channel_id": channel_id,
                "amount": amount,
                "nonce": nonce,
            });
            if let Some(s) = signature { p["signature"] = serde_json::Value::String(s); }
            client.send_request("channel.voucher", Some(p)).await
        }
        Commands::ChannelClose { channel_id } => {
            client.send_request("channel.close", Some(serde_json::json!({ "channel_id": channel_id }))).await
        }
        Commands::ChannelList { peer } => {
            let p = peer.map(|p| serde_json::json!({ "peer": p }));
            client.send_request("channel.list", p).await
        }

        // Settlement
        Commands::SettlementCreatePool { creator, tier } => {
            client.send_request("settlement.create_pool", Some(serde_json::json!({
                "creator": creator,
                "tier": tier,
            }))).await
        }
        Commands::SettlementFundPool { creator, amount } => {
            client.send_request("settlement.fund_pool", Some(serde_json::json!({
                "creator": creator,
                "amount": amount,
            }))).await
        }
        Commands::SettlementClaim { pool, operator, weight, merkle_proof, leaf_index } => {
            client.send_request("settlement.claim", Some(serde_json::json!({
                "pool": pool,
                "operator": operator,
                "weight": weight,
                "merkle_proof": merkle_proof,
                "leaf_index": leaf_index,
            }))).await
        }
        Commands::SettlementOpenChannel { payee, amount } => {
            client.send_request("settlement.open_channel", Some(serde_json::json!({
                "payee": payee,
                "amount": amount,
            }))).await
        }
        Commands::SettlementCloseChannel { user, node, amount, nonce, voucher_signature } => {
            let mut p = serde_json::json!({
                "user": user,
                "node": node,
                "amount": amount,
                "nonce": nonce,
            });
            if let Some(s) = voucher_signature { p["voucher_signature"] = serde_json::Value::String(s); }
            client.send_request("settlement.close_channel", Some(p)).await
        }

        // Receipts
        Commands::ReceiptsCount => client.send_request("receipts.count", None).await,
        Commands::ReceiptsQuery { cid, node, from, to } => {
            let mut p = serde_json::json!({});
            if let Some(c) = cid { p["cid"] = serde_json::Value::String(c); }
            if let Some(n) = node { p["node"] = serde_json::Value::String(n); }
            if let Some(f) = from { p["from"] = serde_json::json!(f); }
            if let Some(t) = to { p["to"] = serde_json::json!(t); }
            client.send_request("receipts.query", Some(p)).await
        }
        Commands::ReceiptStorageList { limit, offset, cid, node } => {
            let mut p = serde_json::json!({ "limit": limit, "offset": offset });
            if let Some(c) = cid { p["cid"] = serde_json::Value::String(c); }
            if let Some(n) = node { p["node"] = serde_json::Value::String(n); }
            client.send_request("receipt.storage.list", Some(p)).await
        }

        // Config
        Commands::GetConfig => client.send_request("get-config", None).await,
        Commands::SetConfig { config } => {
            client.send_request("set-config", Some(serde_json::json!({ "config": config }))).await
        }

        // Lifecycle
        Commands::Shutdown => client.send_request("shutdown", None).await,
    };

    match result {
        Ok(value) => {
            println!("{}", serde_json::to_string_pretty(&value).unwrap());
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}
