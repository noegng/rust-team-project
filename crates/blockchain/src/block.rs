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
