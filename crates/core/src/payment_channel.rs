//! Payment channels for premium egress (user ↔ storage node).
//!
//! Users open payment channels directly with storage nodes for prioritized,
//! faster serving. Pure pay-per-use, per-byte. Vouchers are cumulative —
//! the latest voucher supersedes all previous (like Filecoin payment channels).
//!
//! Protocol fee is applied when the channel is redeemed on-chain.

use ed25519_dalek::{Signature, Signer, Verifier, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

/// Unique identifier for a payment channel.
pub type ChannelId = [u8; 32];

/// Payment channel state (user ↔ storage node).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentChannel {
    /// Unique channel identifier.
    pub channel_id: ChannelId,
    /// Sender (user/payer) public key.
    pub sender: [u8; 32],
    /// Receiver (storage node) public key.
    pub receiver: [u8; 32],
    /// Total USDC lamports locked in the channel.
    pub locked_amount: u64,
    /// Current nonce (increments with each voucher).
    pub nonce: u64,
    /// Cumulative amount spent (from latest voucher).
    pub spent: u64,
}

impl PaymentChannel {
    /// Create a new payment channel.
    pub fn new(channel_id: ChannelId, sender: [u8; 32], receiver: [u8; 32], locked_amount: u64) -> Self {
        Self {
            channel_id,
            sender,
            receiver,
            locked_amount,
            nonce: 0,
            spent: 0,
        }
    }

    /// Remaining balance in the channel.
    pub fn remaining(&self) -> u64 {
        self.locked_amount.saturating_sub(self.spent)
    }

    /// Apply a verified voucher to the channel state.
    /// Returns an error if the voucher is invalid for this channel.
    pub fn apply_voucher(&mut self, voucher: &PaymentVoucher) -> Result<(), PaymentChannelError> {
        if voucher.channel_id != self.channel_id {
            return Err(PaymentChannelError::ChannelMismatch);
        }
        if voucher.nonce <= self.nonce && self.nonce > 0 {
            return Err(PaymentChannelError::StaleNonce {
                voucher_nonce: voucher.nonce,
                current_nonce: self.nonce,
            });
        }
        if voucher.cumulative_amount > self.locked_amount {
            return Err(PaymentChannelError::Overspend {
                amount: voucher.cumulative_amount,
                locked: self.locked_amount,
            });
        }
        if voucher.cumulative_amount < self.spent {
            return Err(PaymentChannelError::DecreasingAmount {
                new_amount: voucher.cumulative_amount,
                current_spent: self.spent,
            });
        }

        self.nonce = voucher.nonce;
        self.spent = voucher.cumulative_amount;
        Ok(())
    }
}

/// Incremental signed voucher for a payment channel.
///
/// Vouchers are **cumulative** — the `cumulative_amount` field represents
/// the total amount spent across all vouchers. The latest voucher supersedes
/// all previous ones. Only the latest voucher needs to be redeemed on-chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentVoucher {
    /// Channel this voucher belongs to.
    pub channel_id: ChannelId,
    /// Cumulative amount (total spent so far, not incremental).
    pub cumulative_amount: u64,
    /// Monotonically increasing nonce.
    pub nonce: u64,
    /// Sender's ed25519 signature over the signable data.
    pub signature: Vec<u8>,
}

impl PaymentVoucher {
    /// Data to be signed by the sender.
    pub fn signable_data(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(48);
        data.extend_from_slice(&self.channel_id);
        data.extend_from_slice(&self.cumulative_amount.to_le_bytes());
        data.extend_from_slice(&self.nonce.to_le_bytes());
        data
    }
}

/// Sign a payment voucher with the sender's signing key.
pub fn sign_voucher(voucher: &mut PaymentVoucher, keypair: &SigningKey) {
    let data = voucher.signable_data();
    let sig: Signature = keypair.sign(&data);
    voucher.signature = sig.to_bytes().to_vec();
}

/// Verify a payment voucher's signature against the sender's public key.
pub fn verify_voucher(voucher: &PaymentVoucher, pubkey: &VerifyingKey) -> bool {
    if voucher.signature.len() != 64 {
        return false;
    }
    let mut sig_bytes = [0u8; 64];
    sig_bytes.copy_from_slice(&voucher.signature);
    let sig = Signature::from_bytes(&sig_bytes);
    let data = voucher.signable_data();
    pubkey.verify(&data, &sig).is_ok()
}

/// Payment channel errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaymentChannelError {
    /// Voucher is for a different channel.
    ChannelMismatch,
    /// Voucher nonce is not newer than current.
    StaleNonce { voucher_nonce: u64, current_nonce: u64 },
    /// Voucher amount exceeds locked funds.
    Overspend { amount: u64, locked: u64 },
    /// Voucher amount is less than already-spent (cumulative amounts must be non-decreasing).
    DecreasingAmount { new_amount: u64, current_spent: u64 },
    /// Invalid signature.
    InvalidSignature,
}

impl std::fmt::Display for PaymentChannelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChannelMismatch => write!(f, "voucher channel_id does not match"),
            Self::StaleNonce { voucher_nonce, current_nonce } =>
                write!(f, "stale nonce: voucher={}, current={}", voucher_nonce, current_nonce),
            Self::Overspend { amount, locked } =>
                write!(f, "overspend: amount={} exceeds locked={}", amount, locked),
            Self::DecreasingAmount { new_amount, current_spent } =>
                write!(f, "decreasing amount: new={} < current={}", new_amount, current_spent),
            Self::InvalidSignature => write!(f, "invalid voucher signature"),
        }
    }
}

impl std::error::Error for PaymentChannelError {}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    fn make_channel() -> PaymentChannel {
        PaymentChannel::new([1u8; 32], [2u8; 32], [3u8; 32], 1_000_000)
    }

    #[test]
    fn test_channel_new() {
        let ch = make_channel();
        assert_eq!(ch.remaining(), 1_000_000);
        assert_eq!(ch.nonce, 0);
        assert_eq!(ch.spent, 0);
    }

    #[test]
    fn test_sign_verify_voucher() {
        let keypair = SigningKey::generate(&mut OsRng);
        let pubkey = keypair.verifying_key();

        let mut voucher = PaymentVoucher {
            channel_id: [1u8; 32],
            cumulative_amount: 50_000,
            nonce: 1,
            signature: vec![],
        };

        sign_voucher(&mut voucher, &keypair);
        assert_eq!(voucher.signature.len(), 64);
        assert!(verify_voucher(&voucher, &pubkey));
    }

    #[test]
    fn test_verify_voucher_wrong_key() {
        let keypair1 = SigningKey::generate(&mut OsRng);
        let keypair2 = SigningKey::generate(&mut OsRng);

        let mut voucher = PaymentVoucher {
            channel_id: [1u8; 32],
            cumulative_amount: 50_000,
            nonce: 1,
            signature: vec![],
        };

        sign_voucher(&mut voucher, &keypair1);
        assert!(!verify_voucher(&voucher, &keypair2.verifying_key()));
    }

    #[test]
    fn test_verify_voucher_tampered() {
        let keypair = SigningKey::generate(&mut OsRng);
        let pubkey = keypair.verifying_key();

        let mut voucher = PaymentVoucher {
            channel_id: [1u8; 32],
            cumulative_amount: 50_000,
            nonce: 1,
            signature: vec![],
        };

        sign_voucher(&mut voucher, &keypair);
        voucher.cumulative_amount = 999_999;
        assert!(!verify_voucher(&voucher, &pubkey));
    }

    #[test]
    fn test_verify_voucher_empty_sig() {
        let keypair = SigningKey::generate(&mut OsRng);
        let voucher = PaymentVoucher {
            channel_id: [1u8; 32],
            cumulative_amount: 50_000,
            nonce: 1,
            signature: vec![],
        };
        assert!(!verify_voucher(&voucher, &keypair.verifying_key()));
    }

    #[test]
    fn test_apply_voucher_success() {
        let mut ch = make_channel();
        let voucher = PaymentVoucher {
            channel_id: ch.channel_id,
            cumulative_amount: 100_000,
            nonce: 1,
            signature: vec![0; 64], // signature validation is separate
        };
        assert!(ch.apply_voucher(&voucher).is_ok());
        assert_eq!(ch.spent, 100_000);
        assert_eq!(ch.nonce, 1);
        assert_eq!(ch.remaining(), 900_000);
    }

    #[test]
    fn test_apply_voucher_cumulative() {
        let mut ch = make_channel();

        // First voucher: spend 100k
        let v1 = PaymentVoucher {
            channel_id: ch.channel_id,
            cumulative_amount: 100_000,
            nonce: 1,
            signature: vec![0; 64],
        };
        ch.apply_voucher(&v1).unwrap();

        // Second voucher: cumulative 300k (spent 200k more)
        let v2 = PaymentVoucher {
            channel_id: ch.channel_id,
            cumulative_amount: 300_000,
            nonce: 2,
            signature: vec![0; 64],
        };
        ch.apply_voucher(&v2).unwrap();
        assert_eq!(ch.spent, 300_000);
        assert_eq!(ch.remaining(), 700_000);
    }

    #[test]
    fn test_apply_voucher_overspend() {
        let mut ch = make_channel();
        let voucher = PaymentVoucher {
            channel_id: ch.channel_id,
            cumulative_amount: 2_000_000, // > locked 1M
            nonce: 1,
            signature: vec![0; 64],
        };
        assert_eq!(
            ch.apply_voucher(&voucher),
            Err(PaymentChannelError::Overspend { amount: 2_000_000, locked: 1_000_000 })
        );
    }

    #[test]
    fn test_apply_voucher_stale_nonce() {
        let mut ch = make_channel();
        let v1 = PaymentVoucher {
            channel_id: ch.channel_id,
            cumulative_amount: 100_000,
            nonce: 5,
            signature: vec![0; 64],
        };
        ch.apply_voucher(&v1).unwrap();

        let v2 = PaymentVoucher {
            channel_id: ch.channel_id,
            cumulative_amount: 200_000,
            nonce: 3, // stale
            signature: vec![0; 64],
        };
        assert_eq!(
            ch.apply_voucher(&v2),
            Err(PaymentChannelError::StaleNonce { voucher_nonce: 3, current_nonce: 5 })
        );
    }

    #[test]
    fn test_apply_voucher_decreasing_amount() {
        let mut ch = make_channel();
        let v1 = PaymentVoucher {
            channel_id: ch.channel_id,
            cumulative_amount: 500_000,
            nonce: 1,
            signature: vec![0; 64],
        };
        ch.apply_voucher(&v1).unwrap();

        let v2 = PaymentVoucher {
            channel_id: ch.channel_id,
            cumulative_amount: 300_000, // less than current spent
            nonce: 2,
            signature: vec![0; 64],
        };
        assert_eq!(
            ch.apply_voucher(&v2),
            Err(PaymentChannelError::DecreasingAmount { new_amount: 300_000, current_spent: 500_000 })
        );
    }

    #[test]
    fn test_apply_voucher_channel_mismatch() {
        let mut ch = make_channel();
        let voucher = PaymentVoucher {
            channel_id: [99u8; 32], // wrong channel
            cumulative_amount: 100_000,
            nonce: 1,
            signature: vec![0; 64],
        };
        assert_eq!(ch.apply_voucher(&voucher), Err(PaymentChannelError::ChannelMismatch));
    }

    #[test]
    fn test_voucher_signable_data_deterministic() {
        let v = PaymentVoucher {
            channel_id: [7u8; 32],
            cumulative_amount: 12345,
            nonce: 42,
            signature: vec![0xAA, 0xBB], // should not appear in signable data
        };
        let data1 = v.signable_data();
        let data2 = v.signable_data();
        assert_eq!(data1, data2);
        // 32 + 8 + 8 = 48
        assert_eq!(data1.len(), 48);
        // signature bytes must not leak into signable data
        assert!(!data1.contains(&0xAA));
    }

    #[test]
    fn test_channel_serde_roundtrip() {
        let ch = make_channel();
        let json = serde_json::to_string(&ch).unwrap();
        let parsed: PaymentChannel = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.channel_id, ch.channel_id);
        assert_eq!(parsed.locked_amount, ch.locked_amount);
    }

    #[test]
    fn test_voucher_serde_roundtrip() {
        let v = PaymentVoucher {
            channel_id: [5u8; 32],
            cumulative_amount: 999,
            nonce: 7,
            signature: vec![1, 2, 3],
        };
        let json = serde_json::to_string(&v).unwrap();
        let parsed: PaymentVoucher = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.cumulative_amount, 999);
        assert_eq!(parsed.nonce, 7);
    }
}
