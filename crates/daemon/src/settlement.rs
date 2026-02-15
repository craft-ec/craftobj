//! Solana settlement client for on-chain transactions.
//!
//! Wraps `craftec-settlement` instruction builders with RPC submission.
//! Supports two transports:
//! - `RpcTransport`: real Solana RPC via `solana-client`
//! - `LoggingTransport`: dry-run for development/testing

use craftec_settlement::instruction;
use craftec_settlement::pda;
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::instruction::{AccountMeta as SolAccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use std::fmt;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info, warn};

/// Settlement program ID — deployed devnet program.
const PROGRAM_ID: [u8; 32] = pda::PROGRAM_ID;

/// Default RPC URL for devnet.
pub const DEFAULT_RPC_URL: &str = "https://api.devnet.solana.com";

/// Default channel expiry (7 days in seconds).
const DEFAULT_CHANNEL_EXPIRY_SECS: i64 = 7 * 24 * 60 * 60;

/// Devnet USDC mint address.
const USDC_MINT_DEVNET: &str = "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU";

/// Mainnet USDC mint address.
const USDC_MINT_MAINNET: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

/// Compute unit limit for settlement transactions.
const COMPUTE_UNIT_LIMIT: u32 = 200_000;

/// Max retries for RPC submission on timeout.
const MAX_RPC_RETRIES: u32 = 3;

/// Environment variable for RPC URL override.
pub const RPC_URL_ENV: &str = "DATACRAFT_SOLANA_RPC_URL";

/// Environment variable for USDC mint override.
pub const USDC_MINT_ENV: &str = "DATACRAFT_USDC_MINT";

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

// ── Transport trait ─────────────────────────────────────────────────────

/// Abstract transport for submitting settlement transactions.
#[async_trait::async_trait]
trait SettlementTransport: Send + Sync {
    async fn submit(
        &self,
        tx: &PreparedTransaction,
        program_id: &[u8; 32],
        signer: Option<&Keypair>,
    ) -> Result<TransactionResult, SettlementError>;

    fn is_dry_run(&self) -> bool;
}

// ── LoggingTransport ────────────────────────────────────────────────────

/// Logging-only transport that records transactions without submitting.
///
/// Used for development and testing before a Solana program is deployed.
#[derive(Debug)]
pub struct LoggingTransport;

#[async_trait::async_trait]
impl SettlementTransport for LoggingTransport {
    async fn submit(
        &self,
        tx: &PreparedTransaction,
        _program_id: &[u8; 32],
        _signer: Option<&Keypair>,
    ) -> Result<TransactionResult, SettlementError> {
        info!(
            "Settlement TX (dry-run): {} | {} accounts | {} bytes instruction data",
            tx.description,
            tx.accounts.len(),
            tx.instruction_data.len()
        );
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

    fn is_dry_run(&self) -> bool {
        true
    }
}

// ── RpcTransport ────────────────────────────────────────────────────────

/// Real Solana RPC transport that builds, signs, and submits transactions.
pub struct RpcTransport {
    rpc: Arc<RpcClient>,
    usdc_mint: Pubkey,
}

impl RpcTransport {
    /// Create a new RPC transport.
    pub fn new(rpc_url: &str, usdc_mint: Pubkey) -> Self {
        let rpc = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));
        Self { rpc, usdc_mint }
    }

    /// Build a Solana `Transaction` from a `PreparedTransaction`.
    fn build_transaction(
        &self,
        tx: &PreparedTransaction,
        program_id: &Pubkey,
        signer: &Keypair,
        blockhash: solana_sdk::hash::Hash,
    ) -> Transaction {
        // Convert our account metas to Solana account metas
        let accounts: Vec<SolAccountMeta> = tx
            .accounts
            .iter()
            .map(|a| {
                let pubkey = Pubkey::new_from_array(a.pubkey);
                if a.is_signer && a.is_writable {
                    SolAccountMeta::new(pubkey, true)
                } else if a.is_signer {
                    SolAccountMeta::new_readonly(pubkey, true)
                } else if a.is_writable {
                    SolAccountMeta::new(pubkey, false)
                } else {
                    SolAccountMeta::new_readonly(pubkey, false)
                }
            })
            .collect();

        let settlement_ix = Instruction {
            program_id: *program_id,
            accounts,
            data: tx.instruction_data.clone(),
        };

        // Add compute budget instruction
        let compute_ix = ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNIT_LIMIT);

        Transaction::new_signed_with_payer(
            &[compute_ix, settlement_ix],
            Some(&signer.pubkey()),
            &[signer],
            blockhash,
        )
    }
}

impl fmt::Debug for RpcTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcTransport")
            .field("usdc_mint", &self.usdc_mint)
            .finish()
    }
}

#[async_trait::async_trait]
impl SettlementTransport for RpcTransport {
    async fn submit(
        &self,
        tx: &PreparedTransaction,
        program_id: &[u8; 32],
        signer: Option<&Keypair>,
    ) -> Result<TransactionResult, SettlementError> {
        let signer = signer.ok_or(SettlementError::Signing(
            "no signing keypair configured".into(),
        ))?;

        let program_pubkey = Pubkey::new_from_array(*program_id);

        info!(
            "Settlement TX (RPC): {} | {} accounts",
            tx.description,
            tx.accounts.len()
        );

        let mut last_err = None;
        for attempt in 0..MAX_RPC_RETRIES {
            // Get fresh blockhash for each attempt
            let blockhash = self
                .rpc
                .get_latest_blockhash()
                .await
                .map_err(|e| SettlementError::Rpc(format!("get blockhash: {}", e)))?;

            let solana_tx = self.build_transaction(tx, &program_pubkey, signer, blockhash);

            match self.rpc.send_and_confirm_transaction(&solana_tx).await {
                Ok(sig) => {
                    info!("Settlement TX confirmed: {} (attempt {})", sig, attempt + 1);
                    return Ok(TransactionResult {
                        signature: sig.to_string(),
                        confirmed: true,
                    });
                }
                Err(e) => {
                    let err_str = e.to_string();
                    warn!(
                        "Settlement TX attempt {}/{} failed: {}",
                        attempt + 1,
                        MAX_RPC_RETRIES,
                        err_str
                    );
                    // Only retry on timeout-like errors
                    if err_str.contains("timeout")
                        || err_str.contains("Timeout")
                        || err_str.contains("BlockhashNotFound")
                    {
                        last_err = Some(err_str);
                        continue;
                    }
                    return Err(SettlementError::TransactionFailed(err_str));
                }
            }
        }

        Err(SettlementError::TransactionFailed(format!(
            "max retries exceeded: {}",
            last_err.unwrap_or_default()
        )))
    }

    fn is_dry_run(&self) -> bool {
        false
    }
}

// ── SolanaClient ────────────────────────────────────────────────────────

/// Configuration for the settlement client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementConfig {
    /// Solana RPC URL. If empty or "dry-run", uses LoggingTransport.
    pub rpc_url: String,
    /// Program ID (32 bytes). Uses default if not set.
    pub program_id: Option<[u8; 32]>,
    /// Signing keypair (ed25519 secret key bytes).
    /// In production this would come from a secure keystore.
    #[serde(skip)]
    pub signing_key: Option<[u8; 32]>,
    /// Whether to use dry-run mode (log only, don't submit).
    pub dry_run: bool,
    /// USDC mint address override (base58). Uses devnet default if not set.
    pub usdc_mint: Option<String>,
}

impl Default for SettlementConfig {
    fn default() -> Self {
        Self {
            rpc_url: DEFAULT_RPC_URL.to_string(),
            program_id: None,
            signing_key: None,
            dry_run: true,
            usdc_mint: None,
        }
    }
}

impl SettlementConfig {
    /// Create config from environment, falling back to defaults.
    ///
    /// - `DATACRAFT_SOLANA_RPC_URL` → sets RPC URL and disables dry-run
    /// - `DATACRAFT_USDC_MINT` → overrides USDC mint address
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(rpc_url) = std::env::var(RPC_URL_ENV) {
            if !rpc_url.is_empty() {
                config.rpc_url = rpc_url;
                config.dry_run = false;
            }
        }

        if let Ok(mint) = std::env::var(USDC_MINT_ENV) {
            if !mint.is_empty() {
                config.usdc_mint = Some(mint);
            }
        }

        config
    }

    /// Resolve the USDC mint pubkey.
    fn resolve_usdc_mint(&self) -> Pubkey {
        if let Some(ref mint_str) = self.usdc_mint {
            mint_str
                .parse::<Pubkey>()
                .unwrap_or_else(|_| USDC_MINT_DEVNET.parse().unwrap())
        } else if self.rpc_url.contains("mainnet") {
            USDC_MINT_MAINNET.parse().unwrap()
        } else {
            USDC_MINT_DEVNET.parse().unwrap()
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
    /// Solana keypair for signing transactions (derived from ed25519 key).
    solana_keypair: Option<Keypair>,
    /// ed25519 signing key for transactions.
    signing_key: Option<ed25519_dalek::SigningKey>,
    /// Transport for submitting transactions.
    transport: Box<dyn SettlementTransport>,
}

impl SolanaClient {
    /// Create a new settlement client.
    ///
    /// Transport selection:
    /// - `dry_run = true` → `LoggingTransport`
    /// - `dry_run = false` with valid RPC URL → `RpcTransport`
    pub fn new(config: SettlementConfig) -> Result<Self, SettlementError> {
        let program_id = config.program_id.unwrap_or(PROGRAM_ID);

        let (signing_key, local_pubkey, solana_keypair) =
            if let Some(secret) = &config.signing_key {
                let sk = ed25519_dalek::SigningKey::from_bytes(secret);
                let pk = ed25519_dalek::VerifyingKey::from(&sk);
                // Build Solana keypair from the same 32-byte secret
                let combined: Vec<u8> =
                    [secret.as_slice(), pk.as_bytes().as_slice()].concat();
                let solana_kp = Keypair::try_from(combined.as_slice())
                    .map_err(|e| SettlementError::Signing(format!("keypair: {}", e)))?;
                (Some(sk), pk.to_bytes(), Some(solana_kp))
            } else {
                (None, [0u8; 32], None)
            };

        let transport: Box<dyn SettlementTransport> = if config.dry_run {
            info!("Settlement transport: LoggingTransport (dry-run)");
            Box::new(LoggingTransport)
        } else {
            let usdc_mint = config.resolve_usdc_mint();
            info!(
                "Settlement transport: RpcTransport rpc={} usdc_mint={}",
                config.rpc_url, usdc_mint
            );
            Box::new(RpcTransport::new(&config.rpc_url, usdc_mint))
        };

        info!(
            "Settlement client initialized: program={} dry_run={}",
            hex::encode(&program_id[..8]),
            config.dry_run
        );

        Ok(Self {
            config,
            program_id,
            local_pubkey,
            solana_keypair,
            signing_key,
            transport,
        })
    }

    /// Set the ed25519 signing key (e.g., from the node's libp2p keypair).
    pub fn set_signing_key(&mut self, key: ed25519_dalek::SigningKey) {
        let pk = ed25519_dalek::VerifyingKey::from(&key);
        self.local_pubkey = pk.to_bytes();

        // Also update Solana keypair
        let secret_bytes = key.to_bytes();
        let combined: Vec<u8> = [secret_bytes.as_slice(), pk.as_bytes().as_slice()].concat();
        if let Ok(kp) = Keypair::try_from(combined.as_slice()) {
            self.solana_keypair = Some(kp);
        }

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

    /// Whether this client is in dry-run mode.
    pub fn is_dry_run(&self) -> bool {
        self.transport.is_dry_run()
    }

    // ── Transaction submission helper ───────────────────────────────────

    async fn submit(&self, tx: PreparedTransaction) -> Result<TransactionResult, SettlementError> {
        self.transport
            .submit(&tx, &self.program_id, self.solana_keypair.as_ref())
            .await
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

        debug!(
            "Preparing CreateCreatorPool for {}",
            hex::encode(&creator[..8])
        );
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
            usdc_mint: None,
        }
    }

    #[tokio::test]
    async fn test_create_creator_pool() {
        let client = SolanaClient::new(test_config()).unwrap();
        assert!(client.is_dry_run());
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
        let result = client
            .open_payment_channel(&payee, 50_000_000)
            .await
            .unwrap();
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

    #[test]
    fn test_config_from_env_default() {
        // Without env vars set, should be dry-run
        let config = SettlementConfig::default();
        assert!(config.dry_run);
    }

    #[test]
    fn test_usdc_mint_resolution_devnet() {
        let config = SettlementConfig {
            rpc_url: "https://api.devnet.solana.com".into(),
            usdc_mint: None,
            ..Default::default()
        };
        let mint = config.resolve_usdc_mint();
        assert_eq!(mint, USDC_MINT_DEVNET.parse::<Pubkey>().unwrap());
    }

    #[test]
    fn test_usdc_mint_resolution_mainnet() {
        let config = SettlementConfig {
            rpc_url: "https://api.mainnet-beta.solana.com".into(),
            usdc_mint: None,
            ..Default::default()
        };
        let mint = config.resolve_usdc_mint();
        assert_eq!(mint, USDC_MINT_MAINNET.parse::<Pubkey>().unwrap());
    }

    #[test]
    fn test_usdc_mint_resolution_override() {
        let custom = "11111111111111111111111111111111".to_string();
        let config = SettlementConfig {
            usdc_mint: Some(custom.clone()),
            ..Default::default()
        };
        let mint = config.resolve_usdc_mint();
        assert_eq!(mint, custom.parse::<Pubkey>().unwrap());
    }

    #[test]
    fn test_rpc_transport_not_dry_run() {
        let config = SettlementConfig {
            dry_run: false,
            signing_key: Some([42u8; 32]),
            ..Default::default()
        };
        let client = SolanaClient::new(config).unwrap();
        assert!(!client.is_dry_run());
    }
}
