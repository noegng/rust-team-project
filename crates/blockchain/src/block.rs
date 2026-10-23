use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::transaction::Transaction;

pub type PublicKey = [u8; 32];

#[derive(Debug, thiserror::Error)]
pub enum BlockError {
    #[error("could not serialize block data: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("invalid block index")]
    InvalidIndex,

    #[error("invalid block timestamp")]
    InvalidTimestamp,

    #[error("previous block hash does not match")]
    InvalidPreviousHash,

    #[error("block hash does not match its contents")]
    InvalidHash,

    #[error("transaction root does not match the transactions")]
    InvalidTransactionRoot,

    #[error("missing validator")]
    MissingValidator,

    #[error("validator is not authorized")]
    UnauthorizedValidator,

    #[error("missing block signature")]
    MissingSignature,

    #[error("invalid block signature")]
    InvalidSignature,

    #[error("invalid transaction: {0}")]
    InvalidTransaction(#[from] crate::transaction::TransactionError),

    #[error("duplicate transaction in block")]
    DuplicateTransaction,

    #[error("block does not match the expected genesis block")]
    InvalidGenesis,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BlockData {
    pub index: u64,
    pub timestamp: u64,
    pub prev_hash: String,
    pub hash: String,
    pub transactions: Vec<Transaction>,
    pub tx_root: String,
}

impl BlockData {
    /// Exact bytes signed by the producer; used by `node` to verify the signature.
    /// Fixed field order and a protocol tag define the encoding.
    pub fn signing_payload(&self, validator: Option<VerifyingKey>) -> Result<Vec<u8>, BlockError> {
        Ok(serde_json::to_vec(&(
            "authora:block:v1",
            self.index,
            &self.prev_hash,
            &self.tx_root,
            self.timestamp,
            validator,
        ))?)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Block {
    pub block_data: BlockData,
    pub validator: Option<VerifyingKey>,
    pub signature: Option<Signature>,
}

impl Block {
    /// Builds and signs a block; validation is the responsibility of `node`.
    pub fn new(
        index: u64,
        prev_hash: String,
        transactions: Vec<Transaction>,
        timestamp: u64,
        signing_key: &SigningKey,
    ) -> Result<Self, BlockError> {
        let block_data = BlockData {
            index,
            prev_hash,
            tx_root: Self::calculate_tx_root(&transactions)?,
            timestamp,
            hash: String::new(),
            transactions,
        };

        let validator = Some(signing_key.verifying_key());
        let mut block = Self {
            block_data,
            validator,
            signature: None,
        };
        block.block_data.hash = block.calculate_hash()?;
        block.signature = Some(signing_key.sign(&block.signing_payload()?));

        Ok(block)
    }

    pub fn calculate_tx_root(transactions: &[Transaction]) -> Result<String, BlockError> {
        Ok(hash_bytes(&serde_json::to_vec(transactions)?))
    }

    pub fn calculate_hash(&self) -> Result<String, BlockError> {
        Ok(hash_bytes(&self.signing_payload()?))
    }

    pub fn signing_payload(&self) -> Result<Vec<u8>, BlockError> {
        self.block_data.signing_payload(self.validator)
    }

    /// Recompute both commitments before accepting a received block.
    pub fn verify_hashes(&self) -> Result<(), BlockError> {
        if Self::calculate_tx_root(&self.block_data.transactions)? != self.block_data.tx_root {
            return Err(BlockError::InvalidTransactionRoot);
        }
        if self.calculate_hash()? != self.block_data.hash {
            return Err(BlockError::InvalidHash);
        }
        Ok(())
    }

    /// Genesis is accepted separately; ordinary blocks require a signer.
    pub fn verify_signature(&self) -> Result<(), BlockError> {
        let validator = self
            .validator
            .as_ref()
            .ok_or(BlockError::MissingValidator)?;
        let signature = self
            .signature
            .as_ref()
            .ok_or(BlockError::MissingSignature)?;
        validator
            .verify_strict(&self.signing_payload()?, signature)
            .map_err(|_| BlockError::InvalidSignature)
    }
}

fn hash_bytes(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block() -> Block {
        let key = SigningKey::from_bytes(&[7; 32]);
        let recipient = SigningKey::from_bytes(&[8; 32]).verifying_key();
        let tx = Transaction::new(&recipient, 10, 0, 100, &key).unwrap();
        Block::new(1, "0".repeat(64), vec![tx], 100, &key).unwrap()
    }

    #[test]
    fn new_block_has_valid_commitments_and_signature() {
        let block = block();
        assert_eq!(block.block_data.index, 1);
        assert_eq!(block.block_data.prev_hash, "0".repeat(64));
        assert_eq!(block.block_data.timestamp, 100);
        assert_eq!(block.block_data.transactions.len(), 1);
        assert_eq!(
            block.validator,
            Some(SigningKey::from_bytes(&[7; 32]).verifying_key())
        );
        assert_eq!(block.block_data.hash.len(), 64);
        block.verify_hashes().unwrap();
        block.verify_signature().unwrap();
    }

    #[test]
    fn empty_blocks_are_valid_and_deterministic() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let a = Block::new(1, "0".repeat(64), vec![], 100, &key).unwrap();
        let b = Block::new(1, "0".repeat(64), vec![], 100, &key).unwrap();
        a.verify_hashes().unwrap();
        a.verify_signature().unwrap();
        assert_eq!(a.block_data.hash, b.block_data.hash);
        assert_eq!(a.signature, b.signature);
        // Known SHA-256 of the empty JSON list, independent of our helper.
        assert_eq!(
            a.block_data.tx_root,
            "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"
        );
    }

    #[test]
    fn modified_transactions_are_rejected() {
        let mut block = block();
        block.block_data.transactions[0].amount += 1;
        assert!(matches!(
            block.verify_hashes(),
            Err(BlockError::InvalidTransactionRoot)
        ));
        // Recomputing commitments does not allow an attacker to forge a signature.
        block.block_data.tx_root =
            Block::calculate_tx_root(&block.block_data.transactions).unwrap();
        block.block_data.hash = block.calculate_hash().unwrap();
        block.verify_hashes().unwrap();
        assert!(matches!(
            block.verify_signature(),
            Err(BlockError::InvalidSignature)
        ));
    }

    #[test]
    fn every_header_field_is_authenticated() {
        let original = block();
        let mut variants = vec![original.clone(); 5];
        variants[0].block_data.index += 1;
        variants[1].block_data.prev_hash = "1".repeat(64);
        variants[2].block_data.timestamp += 1;
        variants[3].block_data.tx_root = "1".repeat(64);
        variants[4].validator = Some(SigningKey::from_bytes(&[9; 32]).verifying_key());
        for mut changed in variants {
            assert!(changed.verify_hashes().is_err());
            changed.block_data.hash = changed.calculate_hash().unwrap();
            assert!(matches!(
                changed.verify_signature(),
                Err(BlockError::InvalidSignature)
            ));
        }
    }

    #[test]
    fn transaction_root_commits_to_order_count_and_signatures() {
        let mut txs = block().block_data.transactions;
        let key = SigningKey::from_bytes(&[7; 32]);
        txs.push(Transaction::new(&key.verifying_key(), 10, 1, 100, &key).unwrap());
        let root = Block::calculate_tx_root(&txs).unwrap();
        txs.reverse();
        assert_ne!(root, Block::calculate_tx_root(&txs).unwrap());
        txs.reverse();
        txs[0].signature[0] ^= 1;
        assert_ne!(root, Block::calculate_tx_root(&txs).unwrap());
        txs[0].signature[0] ^= 1;
        txs.pop();
        assert_ne!(root, Block::calculate_tx_root(&txs).unwrap());
    }

    #[test]
    fn missing_and_forged_signatures_are_rejected() {
        let mut block = block();
        block.signature = None;
        assert!(matches!(
            block.verify_signature(),
            Err(BlockError::MissingSignature)
        ));
        block.signature =
            Some(SigningKey::from_bytes(&[9; 32]).sign(&block.signing_payload().unwrap()));
        assert!(matches!(
            block.verify_signature(),
            Err(BlockError::InvalidSignature)
        ));
        block.validator = None;
        assert!(matches!(
            block.verify_signature(),
            Err(BlockError::MissingValidator)
        ));
    }

    #[test]
    fn stored_hash_tampering_is_rejected() {
        let mut block = block();
        block.block_data.hash = "1".repeat(64);
        assert!(matches!(
            block.verify_hashes(),
            Err(BlockError::InvalidHash)
        ));
    }

    #[test]
    fn signed_block_survives_json_round_trip() {
        let original = block();
        let json = serde_json::to_vec(&original).unwrap();
        let decoded: Block = serde_json::from_slice(&json).unwrap();
        decoded.verify_hashes().unwrap();
        decoded.verify_signature().unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), json);
    }
}
