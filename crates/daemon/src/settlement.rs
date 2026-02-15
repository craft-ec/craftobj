//! Solana settlement client for on-chain transactions.
//!
//! Wraps `craftec-settlement` instruction builders with RPC submission.
//! Uses a generic RPC transport so the daemon can run without heavy
//! `solana-client` deps initially — real Solana RPC, mock for tests.

use craftec_settlement::instruction;
use craftec_settlement::pda;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;
use tracing::{debug, info};

/// Settlement program ID (replace with actual deployed program ID).
const PROGRAM_ID: [u8; 32] = pda::PROGRAM_ID;

/// Default RPC URL for devnet.
pub const DEFAULT_RPC_URL: &str = "https://api.devnet.solana.com";

/// Default channel expiry (7 days in seconds).
const DEFAULT_CHANNEL_EXPIRY_SECS: i64 = 7 * 24 * 60 * 60;

// ── Errors ──────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum SettlementError {
    #[error("RPC error: {0}")]
    Rpc(String),
    #[error("signing error: {0}")]
    Signing(String),
    #[error("invalid parameter: {0}")]
    InvalidParam(String),
    #[error("transaction failed: {0}")]
    TransactionFailed(String),
    #[error("not configured: settlement client not initialized")]
    NotConfigured,
}

// ── Transaction types ───────────────────────────────────────────────────

/// Serialized transaction ready for RPC submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedTransaction {
    /// Instruction data (discriminator + payload from craftec-settlement).
    pub instruction_data: Vec<u8>,
    /// Account addresses involved (from account list helpers).
    pub accounts: Vec<TransactionAccount>,
    /// Human-readable description for logging.
    pub description: String,
}

/// Account metadata for a prepared transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionAccount {
    pub pubkey: [u8; 32],
    pub is_signer: bool,
    pub is_writable: bool,
}

impl From<instruction::AccountMeta> for TransactionAccount {
    fn from(meta: instruction::AccountMeta) -> Self {
        Self {
            pubkey: meta.pubkey,
            is_signer: meta.is_signer,
            is_writable: meta.is_writable,
        }
    }
}

/// Result of a submitted transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionResult {
    /// Transaction signature (base58-encoded when using real Solana).
    pub signature: String,
    /// Whether the transaction was confirmed.
    pub confirmed: bool,
}

impl fmt::Display for TransactionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tx:{} confirmed:{}", self.signature, self.confirmed)
    }
}

// ── RPC Transport trait ─────────────────────────────────────────────────

/// Logging-only transport that records transactions without submitting.
///
/// Used for development and testing before a Solana program is deployed.
#[derive(Debug)]
pub struct LoggingTransport;

impl LoggingTransport {
    async fn submit_impl(
        &self,
        tx: &PreparedTransaction,
    ) -> Result<TransactionResult, SettlementError> {
        info!(
            "Settlement TX (dry-run): {} | {} accounts | {} bytes instruction data",
            tx.description,
            tx.accounts.len(),
            tx.instruction_data.len()
        );
        // Generate a deterministic fake signature from instruction data
        let sig = {
            use sha2::{Digest, Sha256};
            let hash = Sha256::digest(&tx.instruction_data);
            hex::encode(&hash[..16])
        };
        Ok(TransactionResult {
            signature: format!("dry-run:{}", sig),
            confirmed: true,
        })
    }

    #[allow(dead_code)]
    async fn is_ready_impl(&self) -> bool {
        true
    }
}

// ── SolanaClient ────────────────────────────────────────────────────────

/// Configuration for the settlement client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementConfig {
    /// Solana RPC URL.
    pub rpc_url: String,
    /// Program ID (32 bytes). Uses default if not set.
    pub program_id: Option<[u8; 32]>,
    /// Signing keypair (ed25519 secret key bytes).
    /// In production this would come from a secure keystore.
    #[serde(skip)]
    pub signing_key: Option<[u8; 32]>,
    /// Whether to use dry-run mode (log only, don't submit).
    pub dry_run: bool,
}

impl Default for SettlementConfig {
    fn default() -> Self {
        Self {
            rpc_url: DEFAULT_RPC_URL.to_string(),
            program_id: None,
            signing_key: None,
            dry_run: true,
        }
    }
}

/// Solana settlement client.
///
/// Wraps `craftec-settlement` instruction builders with transaction
/// preparation and RPC submission.
pub struct SolanaClient {
    #[allow(dead_code)]
    config: SettlementConfig,
    program_id: [u8; 32],
    /// Our node's public key (derived from signing key).
    local_pubkey: [u8; 32],
    /// ed25519 signing key for transactions.
    signing_key: Option<ed25519_dalek::SigningKey>,
    /// Transport for submitting transactions (logging or real RPC).
    transport: Box<dyn Send + Sync + std::any::Any>,
    /// Whether we're in dry-run mode.
    #[allow(dead_code)]
    dry_run: bool,
}

impl SolanaClient {
    /// Create a new settlement client.
    pub fn new(config: SettlementConfig) -> Result<Self, SettlementError> {
        let program_id = config.program_id.unwrap_or(PROGRAM_ID);
        let dry_run = config.dry_run;

        let (signing_key, local_pubkey) = if let Some(secret) = &config.signing_key {
            let sk = ed25519_dalek::SigningKey::from_bytes(secret);
            let pk = ed25519_dalek::VerifyingKey::from(&sk);
            (Some(sk), pk.to_bytes())
        } else {
            (None, [0u8; 32])
        };

        let transport: Box<dyn Send + Sync + std::any::Any> = Box::new(LoggingTransport);

        info!(
            "Settlement client initialized: rpc={} program={} dry_run={}",
            config.rpc_url,
            hex::encode(&program_id[..8]),
            dry_run
        );

        Ok(Self {
            config,
            program_id,
            local_pubkey,
            signing_key,
            transport,
            dry_run,
        })
    }

    /// Set the ed25519 signing key (e.g., from the node's libp2p keypair).
    pub fn set_signing_key(&mut self, key: ed25519_dalek::SigningKey) {
        let pk = ed25519_dalek::VerifyingKey::from(&key);
        self.local_pubkey = pk.to_bytes();
        self.signing_key = Some(key);
    }

    /// Get our local public key.
    pub fn local_pubkey(&self) -> &[u8; 32] {
        &self.local_pubkey
    }

    /// Get the program ID.
    pub fn program_id(&self) -> &[u8; 32] {
        &self.program_id
    }

    // ── Transaction submission helper ───────────────────────────────────

    async fn submit(&self, tx: PreparedTransaction) -> Result<TransactionResult, SettlementError> {
        // Currently always uses LoggingTransport via dry-run
        let transport = self
            .transport
            .downcast_ref::<LoggingTransport>()
            .ok_or_else(|| SettlementError::Rpc("transport not available".into()))?;
        transport.submit_impl(&tx).await
    }

    // ── Creator Pool operations ─────────────────────────────────────────

    /// Create a creator pool for the given creator.
    pub async fn create_creator_pool(
        &self,
        creator: &[u8; 32],
        tier: u8,
    ) -> Result<TransactionResult, SettlementError> {
        let ix_data = instruction::create_creator_pool(*creator, tier);
        let pool_pda = pda::creator_pool_pda(&self.program_id, creator);

        let tx = PreparedTransaction {
            instruction_data: ix_data,
            accounts: vec![
                TransactionAccount {
                    pubkey: *creator,
                    is_signer: true,
                    is_writable: true,
                },
                TransactionAccount {
                    pubkey: pool_pda,
                    is_signer: false,
                    is_writable: true,
                },
            ],
            description: format!(
                "CreateCreatorPool creator={} tier={}",
                hex::encode(&creator[..8]),
                tier
            ),
        };

        debug!("Preparing CreateCreatorPool for {}", hex::encode(&creator[..8]));
        self.submit(tx).await
    }

    /// Fund (deposit USDC into) a creator pool.
    pub async fn fund_pool(
        &self,
        creator: &[u8; 32],
        amount: u64,
    ) -> Result<TransactionResult, SettlementError> {
        if amount == 0 {
            return Err(SettlementError::InvalidParam("amount must be > 0".into()));
        }

        let ix_data = instruction::fund_creator_pool(*creator, amount);
        let pool_pda = pda::creator_pool_pda(&self.program_id, creator);

        let tx = PreparedTransaction {
            instruction_data: ix_data,
            accounts: vec![
                TransactionAccount {
                    pubkey: self.local_pubkey,
                    is_signer: true,
                    is_writable: true,
                },
                TransactionAccount {
                    pubkey: pool_pda,
                    is_signer: false,
                    is_writable: true,
                },
            ],
            description: format!(
                "FundCreatorPool creator={} amount={}",
                hex::encode(&creator[..8]),
                amount
            ),
        };

        debug!(
            "Preparing FundCreatorPool for {} amount={}",
            hex::encode(&creator[..8]),
            amount
        );
        self.submit(tx).await
    }

    /// Submit a PDP claim to earn a storage reward from a creator pool.
    pub async fn claim_pdp(
        &self,
        pool: &[u8; 32],
        receipt: &datacraft_core::StorageReceipt,
        operator_weight: u64,
        merkle_proof: Vec<[u8; 32]>,
        leaf_index: u32,
    ) -> Result<TransactionResult, SettlementError> {
        let operator = receipt.storage_node;
        let ix_data = instruction::claim_payout(
            *pool,
            operator,
            operator_weight,
            merkle_proof,
            leaf_index,
        );

        let accounts: Vec<TransactionAccount> =
            instruction::claim_payout_accounts(&self.program_id, pool, &operator)
                .into_iter()
                .map(TransactionAccount::from)
                .collect();

        let tx = PreparedTransaction {
            instruction_data: ix_data,
            accounts,
            description: format!(
                "ClaimPDP pool={} operator={} weight={}",
                hex::encode(&pool[..8]),
                hex::encode(&operator[..8]),
                operator_weight
            ),
        };

        debug!(
            "Preparing ClaimPDP for pool={} operator={}",
            hex::encode(&pool[..8]),
            hex::encode(&operator[..8])
        );
        self.submit(tx).await
    }

    // ── Payment Channel operations ──────────────────────────────────────

    /// Open a payment channel, locking USDC.
    pub async fn open_payment_channel(
        &self,
        payee: &[u8; 32],
        amount: u64,
    ) -> Result<TransactionResult, SettlementError> {
        if amount == 0 {
            return Err(SettlementError::InvalidParam("amount must be > 0".into()));
        }

        let expires_at = {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            now + DEFAULT_CHANNEL_EXPIRY_SECS
        };

        let ix_data =
            instruction::open_payment_channel(self.local_pubkey, *payee, amount, expires_at);
        let channel_pda =
            pda::payment_channel_pda(&self.program_id, &self.local_pubkey, payee);

        let tx = PreparedTransaction {
            instruction_data: ix_data,
            accounts: vec![
                TransactionAccount {
                    pubkey: self.local_pubkey,
                    is_signer: true,
                    is_writable: true,
                },
                TransactionAccount {
                    pubkey: *payee,
                    is_signer: false,
                    is_writable: false,
                },
                TransactionAccount {
                    pubkey: channel_pda,
                    is_signer: false,
                    is_writable: true,
                },
            ],
            description: format!(
                "OpenPaymentChannel payee={} amount={} expires_at={}",
                hex::encode(&payee[..8]),
                amount,
                expires_at
            ),
        };

        debug!(
            "Preparing OpenPaymentChannel payee={} amount={}",
            hex::encode(&payee[..8]),
            amount
        );
        self.submit(tx).await
    }

    /// Cooperative close of a payment channel with the final voucher.
    pub async fn close_payment_channel(
        &self,
        channel_pda: &[u8; 32],
        user: &[u8; 32],
        node: &[u8; 32],
        voucher_amount: u64,
        voucher_nonce: u64,
        voucher_signature: Vec<u8>,
    ) -> Result<TransactionResult, SettlementError> {
        let ix_data = instruction::settle_payment_channel(
            *user,
            *node,
            voucher_amount,
            voucher_nonce,
            voucher_signature,
        );

        let accounts: Vec<TransactionAccount> =
            instruction::settle_payment_channel_accounts(&self.program_id, user, node)
                .into_iter()
                .map(TransactionAccount::from)
                .collect();

        let tx = PreparedTransaction {
            instruction_data: ix_data,
            accounts,
            description: format!(
                "ClosePaymentChannel channel={} amount={} nonce={}",
                hex::encode(&channel_pda[..8]),
                voucher_amount,
                voucher_nonce
            ),
        };

        debug!(
            "Preparing ClosePaymentChannel channel={}",
            hex::encode(&channel_pda[..8])
        );
        self.submit(tx).await
    }

    /// Force-close a payment channel (unilateral close by sender after timeout).
    pub async fn force_close_channel(
        &self,
        user: &[u8; 32],
        node: &[u8; 32],
    ) -> Result<TransactionResult, SettlementError> {
        let ix_data = instruction::close_payment_channel(*user, *node);
        let channel_pda =
            pda::payment_channel_pda(&self.program_id, user, node);

        let tx = PreparedTransaction {
            instruction_data: ix_data,
            accounts: vec![
                TransactionAccount {
                    pubkey: channel_pda,
                    is_signer: false,
                    is_writable: true,
                },
                TransactionAccount {
                    pubkey: *user,
                    is_signer: true,
                    is_writable: true,
                },
                TransactionAccount {
                    pubkey: *node,
                    is_signer: false,
                    is_writable: true,
                },
            ],
            description: format!(
                "ForceCloseChannel user={} node={}",
                hex::encode(&user[..8]),
                hex::encode(&node[..8])
            ),
        };

        debug!(
            "Preparing ForceCloseChannel user={} node={}",
            hex::encode(&user[..8]),
            hex::encode(&node[..8])
        );
        self.submit(tx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> SettlementConfig {
        let secret = [42u8; 32];
        SettlementConfig {
            rpc_url: "http://localhost:8899".into(),
            program_id: Some([1u8; 32]),
            signing_key: Some(secret),
            dry_run: true,
        }
    }

    #[tokio::test]
    async fn test_create_creator_pool() {
        let client = SolanaClient::new(test_config()).unwrap();
        let creator = [2u8; 32];
        let result = client.create_creator_pool(&creator, 2).await.unwrap();
        assert!(result.signature.starts_with("dry-run:"));
        assert!(result.confirmed);
    }

    #[tokio::test]
    async fn test_fund_pool() {
        let client = SolanaClient::new(test_config()).unwrap();
        let creator = [2u8; 32];
        let result = client.fund_pool(&creator, 1_000_000).await.unwrap();
        assert!(result.confirmed);
    }

    #[tokio::test]
    async fn test_fund_pool_zero_amount() {
        let client = SolanaClient::new(test_config()).unwrap();
        let creator = [2u8; 32];
        let result = client.fund_pool(&creator, 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_open_payment_channel() {
        let client = SolanaClient::new(test_config()).unwrap();
        let payee = [3u8; 32];
        let result = client.open_payment_channel(&payee, 50_000_000).await.unwrap();
        assert!(result.confirmed);
    }

    #[tokio::test]
    async fn test_open_payment_channel_zero() {
        let client = SolanaClient::new(test_config()).unwrap();
        let payee = [3u8; 32];
        let result = client.open_payment_channel(&payee, 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_close_payment_channel() {
        let client = SolanaClient::new(test_config()).unwrap();
        let channel = [4u8; 32];
        let user = [2u8; 32];
        let node = [3u8; 32];
        let result = client
            .close_payment_channel(&channel, &user, &node, 25_000_000, 5, vec![0u8; 64])
            .await
            .unwrap();
        assert!(result.confirmed);
    }

    #[tokio::test]
    async fn test_force_close_channel() {
        let client = SolanaClient::new(test_config()).unwrap();
        let user = [2u8; 32];
        let node = [3u8; 32];
        let result = client.force_close_channel(&user, &node).await.unwrap();
        assert!(result.confirmed);
    }

    #[tokio::test]
    async fn test_claim_pdp() {
        let client = SolanaClient::new(test_config()).unwrap();
        let pool = [5u8; 32];
        let receipt = datacraft_core::StorageReceipt {
            content_id: datacraft_core::ContentId::from_bytes(&[6u8; 32]),
            storage_node: [7u8; 32],
            challenger: [8u8; 32],
            shard_index: 0,
            timestamp: 1700000000,
            nonce: [9u8; 32],
            proof_hash: [10u8; 32],
            signature: vec![0u8; 64],
        };
        let result = client
            .claim_pdp(&pool, &receipt, 100, vec![[1u8; 32]], 0)
            .await
            .unwrap();
        assert!(result.confirmed);
    }
}
