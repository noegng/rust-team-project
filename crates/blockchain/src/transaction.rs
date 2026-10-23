use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("could not serialize transaction data: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("invalid sender public key")]
    InvalidSender,
    #[error("invalid recipient public key")]
    InvalidRecipient,
    #[error("invalid transaction signature")]
    InvalidSignature,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Transaction {
    pub from: Vec<u8>,
    pub to: Vec<u8>,
    pub amount: u64,
    pub nonce: u64,
    pub timestamp: u64,
    pub signature: Vec<u8>,
}

impl Transaction {
    /// Creates a signed transaction. Timestamp is supplied in Unix seconds.
    /// The sender is derived from the signing key to prevent key mismatches.
    pub fn new(
        to: &VerifyingKey,
        amount: u64,
        nonce: u64,
        timestamp: u64,
        signing_key: &SigningKey,
    ) -> Result<Self, TransactionError> {
        let mut transaction = Self {
            from: signing_key.verifying_key().to_bytes().to_vec(),
            to: to.to_bytes().to_vec(),
            amount,
            nonce,
            timestamp,
            signature: Vec::new(),
        };
        transaction.signature = signing_key
            .sign(&transaction.signing_payload()?)
            .to_bytes()
            .to_vec();
        Ok(transaction)
    }

    /// Validates key encodings and the sender's signature.
    /// Balances and previously accepted nonces must be checked by the chain.
    pub fn verify(&self) -> Result<(), TransactionError> {
        let recipient: [u8; 32] = self
            .to
            .as_slice()
            .try_into()
            .map_err(|_| TransactionError::InvalidRecipient)?;
        VerifyingKey::from_bytes(&recipient).map_err(|_| TransactionError::InvalidRecipient)?;
        self.verify_signature()
    }

    pub fn verify_signature(&self) -> Result<(), TransactionError> {
        let sender: [u8; 32] = self
            .from
            .as_slice()
            .try_into()
            .map_err(|_| TransactionError::InvalidSender)?;
        let key = VerifyingKey::from_bytes(&sender).map_err(|_| TransactionError::InvalidSender)?;
        let signature = Signature::from_slice(&self.signature)
            .map_err(|_| TransactionError::InvalidSignature)?;
        key.verify_strict(&self.signing_payload()?, &signature)
            .map_err(|_| TransactionError::InvalidSignature)
    }

    /// SHA-256 of the complete signed transaction, encoded as lowercase hex.
    /// Used for gossip deduplication; nonce checks remain necessary against replay.
    pub fn calculate_hash(&self) -> Result<String, TransactionError> {
        Ok(Sha256::digest(serde_json::to_vec(self)?)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect())
    }

    // Fixed field order; the signature itself is excluded from the signed data.
    fn signing_payload(&self) -> Result<Vec<u8>, TransactionError> {
        Ok(serde_json::to_vec(&(
            "authora:transaction:v1",
            &self.from,
            &self.to,
            self.amount,
            self.nonce,
            self.timestamp,
        ))?)
    }
}
