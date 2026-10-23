use ed25519_dalek::{Signer, SigningKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::transaction::Transaction;

pub type PublicKey = [u8; 32];

#[derive(Debug, thiserror::Error)]
pub enum BlockError {
    #[error("could not serialize block data: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Block {
    pub index: u64,
    pub prev_hash: String,
    pub tx_root: String,
    pub timestamp: u64,
    pub validator: Vec<u8>,
    pub signature: Vec<u8>,
    pub hash: String,
    pub transactions: Vec<Transaction>,
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
        let mut block = Self {
            index,
            prev_hash,
            tx_root: Self::calculate_tx_root(&transactions)?,
            timestamp,
            validator: signing_key.verifying_key().to_bytes().to_vec(),
            signature: Vec::new(),
            hash: String::new(),
            transactions,
        };
        block.hash = block.calculate_hash()?;
        block.signature = signing_key
            .sign(&block.signing_payload()?)
            .to_bytes()
            .to_vec();
        Ok(block)
    }

    pub fn calculate_tx_root(transactions: &[Transaction]) -> Result<String, BlockError> {
        Ok(hash_bytes(&serde_json::to_vec(transactions)?))
    }

    pub fn calculate_hash(&self) -> Result<String, BlockError> {
        Ok(hash_bytes(&self.signing_payload()?))
    }

    /// Exact bytes signed by the producer; used by `node` to verify the signature.
    /// Fixed field order and a protocol tag define the encoding.
    pub fn signing_payload(&self) -> Result<Vec<u8>, BlockError> {
        Ok(serde_json::to_vec(&(
            "authora:block:v1",
            self.index,
            &self.prev_hash,
            &self.tx_root,
            self.timestamp,
            &self.validator,
        ))?)
    }
}

fn hash_bytes(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
