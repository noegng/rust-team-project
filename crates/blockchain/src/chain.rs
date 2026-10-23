use crate::block::{Block, BlockData, BlockError};

/// Hash hex du bloc genesis précédent (aucun parent).
pub const GENESIS_PREV_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug, thiserror::Error)]
pub enum ChainError {
    #[error("chain is empty (missing genesis block)")]
    EmptyChain,

    #[error("block index overflow")]
    IndexOverflow,

    #[error("invalid block index: expected {expected}, got {got}")]
    InvalidIndex { expected: u64, got: u64 },

    #[error("previous block hash does not match: expected {expected}, got {got}")]
    InvalidPreviousHash { expected: String, got: String },

    #[error("invalid block: {0}")]
    InvalidBlock(#[from] BlockError),
}

pub struct Chain {
    pub blocks: Vec<Block>,
}

impl Chain {
    /// Crée une chaîne contenant uniquement le bloc genesis.
    /// Il est déterministe : les 3 nodes partent du même hash.
    pub fn new() -> Self {
        Self {
            blocks: vec![genesis_block()],
        }
    }

    /// Nombre de blocs (= index du prochain bloc à produire).
    pub fn len(&self) -> u64 {
        self.blocks.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Dernier bloc. `None` seulement si `blocks` a été vidé à la main.
    pub fn last(&self) -> Option<&Block> {
        self.blocks.last()
    }

    /// Vérifie qu'un bloc peut être ajouté à la suite de la chaîne :
    /// index, lien avec le bloc précédent, hash et signature du validateur.
    pub fn validate_successor(&self, block: &Block) -> Result<(), ChainError> {
        let last = self.last().ok_or(ChainError::EmptyChain)?;
        let expected = last
            .block_data
            .index
            .checked_add(1)
            .ok_or(ChainError::IndexOverflow)?;
        if block.block_data.index != expected {
            return Err(ChainError::InvalidIndex {
                expected,
                got: block.block_data.index,
            });
        }
        if block.block_data.prev_hash != last.block_data.hash {
            return Err(ChainError::InvalidPreviousHash {
                expected: last.block_data.hash.clone(),
                got: block.block_data.prev_hash.clone(),
            });
        }
        block.verify_hashes()?;
        block.verify_signature()?;
        Ok(())
    }

    pub fn is_valid_successor(&self, block: &Block) -> bool {
        self.validate_successor(block).is_ok()
    }

    /// Ajoute le bloc s'il est un successeur valide.
    pub fn push(&mut self, block: Block) -> Result<(), ChainError> {
        self.validate_successor(&block)?;
        self.blocks.push(block);
        Ok(())
    }
}

impl Default for Chain {
    fn default() -> Self {
        Self::new()
    }
}

/// Bloc 0, identique sur tous les nodes (pas de validateur ni de signature).
pub fn genesis_block() -> Block {
    let mut block = Block {
        block_data: BlockData {
            index: 0,
            prev_hash: GENESIS_PREV_HASH.to_string(),
            tx_root: Block::calculate_tx_root(&[])
                .expect("empty transaction list can be serialized"),
            timestamp: 0,
            hash: String::new(),
            transactions: Vec::new(),
        },
        validator: None,
        signature: None,
    };
    block.block_data.hash = block
        .calculate_hash()
        .expect("fixed genesis payload can be serialized");
    block
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::Transaction;
    use ed25519_dalek::SigningKey;

    fn last_hash(chain: &Chain) -> String {
        chain.last().unwrap().block_data.hash.clone()
    }

    fn successor(chain: &Chain) -> Block {
        let key = SigningKey::from_bytes(&[7; 32]);
        let tx = Transaction::new(&key.verifying_key(), 10, 0, 1, &key).unwrap();
        Block::new(chain.len(), last_hash(chain), vec![tx], 1, &key).unwrap()
    }

    #[test]
    fn genesis_is_deterministic_and_unsigned() {
        let genesis = genesis_block();
        assert_eq!(genesis.block_data.hash, genesis_block().block_data.hash);
        assert_eq!(genesis.block_data.hash.len(), 64);
        assert!(genesis.validator.is_none());
        assert!(genesis.signature.is_none());
        genesis.verify_hashes().unwrap();
    }

    #[test]
    fn signed_block_survives_serialization_and_can_be_appended() {
        let mut chain = Chain::new();
        let block = successor(&chain);
        let encoded = serde_json::to_vec(&block).unwrap();
        let decoded: Block = serde_json::from_slice(&encoded).unwrap();
        decoded.verify_hashes().unwrap();
        decoded.verify_signature().unwrap();
        chain.push(decoded).unwrap();
        assert_eq!(chain.len(), 2);
    }

    #[test]
    fn transaction_tampering_is_rejected_even_if_commitments_are_recomputed() {
        let chain = Chain::new();
        let mut block = successor(&chain);
        block.block_data.transactions[0].amount += 1;
        assert!(matches!(
            chain.validate_successor(&block),
            Err(ChainError::InvalidBlock(BlockError::InvalidTransactionRoot))
        ));
        block.block_data.tx_root =
            Block::calculate_tx_root(&block.block_data.transactions).unwrap();
        assert!(matches!(
            chain.validate_successor(&block),
            Err(ChainError::InvalidBlock(BlockError::InvalidHash))
        ));
        block.block_data.hash = block.calculate_hash().unwrap();
        assert!(matches!(
            chain.validate_successor(&block),
            Err(ChainError::InvalidBlock(BlockError::InvalidSignature))
        ));
    }

    #[test]
    fn changed_validator_and_missing_signature_are_rejected() {
        let chain = Chain::new();
        let mut block = successor(&chain);
        block.validator = Some(SigningKey::from_bytes(&[8; 32]).verifying_key());
        block.block_data.hash = block.calculate_hash().unwrap();
        assert!(matches!(
            chain.validate_successor(&block),
            Err(ChainError::InvalidBlock(BlockError::InvalidSignature))
        ));
        block.signature = None;
        assert!(matches!(
            chain.validate_successor(&block),
            Err(ChainError::InvalidBlock(BlockError::MissingSignature))
        ));
        block.validator = None;
        block.block_data.hash = block.calculate_hash().unwrap();
        assert!(matches!(
            chain.validate_successor(&block),
            Err(ChainError::InvalidBlock(BlockError::MissingValidator))
        ));
    }

    #[test]
    fn wrong_parent_and_index_are_rejected() {
        let mut chain = Chain::new();
        let key = SigningKey::from_bytes(&[7; 32]);
        let wrong_parent = Block::new(1, "wrong".into(), vec![], 1, &key).unwrap();
        let wrong_index = Block::new(2, last_hash(&chain), vec![], 1, &key).unwrap();
        assert!(matches!(
            chain.push(wrong_parent),
            Err(ChainError::InvalidPreviousHash { .. })
        ));
        assert!(matches!(
            chain.push(wrong_index),
            Err(ChainError::InvalidIndex {
                expected: 1,
                got: 2
            })
        ));
        assert_eq!(chain.len(), 1);
    }

    #[test]
    fn empty_chain_is_reported() {
        let chain = Chain { blocks: Vec::new() };
        assert!(matches!(
            chain.validate_successor(&genesis_block()),
            Err(ChainError::EmptyChain)
        ));
    }
}
