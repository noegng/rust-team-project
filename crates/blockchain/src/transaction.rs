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
    /// Creates a signed transaction. Timestamp is supplied in Unix milliseconds
    /// (only used to measure latencies).
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

#[cfg(test)]
mod tests {
    use super::*;

    fn transaction() -> Transaction {
        let sender = SigningKey::from_bytes(&[7; 32]);
        let recipient = SigningKey::from_bytes(&[8; 32]).verifying_key();
        Transaction::new(&recipient, 10, 3, 100, &sender).unwrap()
    }

    #[test]
    fn new_transaction_is_signed_by_sender() {
        let tx = transaction();
        assert_eq!(
            tx.from,
            SigningKey::from_bytes(&[7; 32]).verifying_key().to_bytes()
        );
        assert_eq!(
            tx.to,
            SigningKey::from_bytes(&[8; 32]).verifying_key().to_bytes()
        );
        assert_eq!((tx.amount, tx.nonce, tx.timestamp), (10, 3, 100));
        assert_eq!(tx.signature.len(), 64);
        tx.verify().unwrap();
    }

    #[test]
    fn every_payload_field_is_authenticated() {
        let original = transaction();
        let mut variants = vec![original.clone(); 5];
        variants[0].from = SigningKey::from_bytes(&[9; 32])
            .verifying_key()
            .to_bytes()
            .to_vec();
        variants[1].to = SigningKey::from_bytes(&[9; 32])
            .verifying_key()
            .to_bytes()
            .to_vec();
        variants[2].amount += 1;
        variants[3].nonce += 1;
        variants[4].timestamp += 1;
        for tx in variants {
            assert!(matches!(
                tx.verify(),
                Err(TransactionError::InvalidSignature)
            ));
            assert_ne!(
                tx.calculate_hash().unwrap(),
                original.calculate_hash().unwrap()
            );
        }
    }

    #[test]
    fn malformed_key_lengths_are_rejected() {
        for length in [0, 31, 33] {
            let mut tx = transaction();
            tx.from = vec![0; length];
            assert!(matches!(tx.verify(), Err(TransactionError::InvalidSender)));
            let mut tx = transaction();
            tx.to = vec![0; length];
            assert!(matches!(
                tx.verify(),
                Err(TransactionError::InvalidRecipient)
            ));
        }
    }

    #[test]
    fn malformed_and_forged_signatures_are_rejected() {
        for signature in [vec![], vec![0; 63], vec![0; 64], vec![0; 65]] {
            let mut tx = transaction();
            tx.signature = signature;
            assert!(matches!(
                tx.verify(),
                Err(TransactionError::InvalidSignature)
            ));
        }
        let mut tx = transaction();
        tx.signature = SigningKey::from_bytes(&[9; 32])
            .sign(&tx.signing_payload().unwrap())
            .to_bytes()
            .to_vec();
        assert!(matches!(
            tx.verify(),
            Err(TransactionError::InvalidSignature)
        ));
    }

    #[test]
    fn signed_transaction_survives_json_round_trip() {
        let original = transaction();
        let json = serde_json::to_vec(&original).unwrap();
        let decoded: Transaction = serde_json::from_slice(&json).unwrap();
        decoded.verify().unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), json);
        assert_eq!(
            decoded.calculate_hash().unwrap(),
            original.calculate_hash().unwrap()
        );
    }

    #[test]
    fn hash_is_deterministic_hex_and_commits_to_signature() {
        let mut tx = transaction();
        let hash = tx.calculate_hash().unwrap();
        assert_eq!(hash, transaction().calculate_hash().unwrap());
        assert_eq!(hash.len(), 64);
        assert!(
            hash.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        );
        tx.signature[0] ^= 1;
        assert_ne!(hash, tx.calculate_hash().unwrap());
    }
}
