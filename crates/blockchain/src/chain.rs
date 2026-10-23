use crate::block::{Block, BlockData};

/// Hash hex du bloc genesis précédent (aucun parent).
pub const GENESIS_PREV_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

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

    /// Dernier bloc. Toujours présent grâce au genesis.
    pub fn last(&self) -> &Block {
        self.blocks.last().expect("chain always contains genesis")
    }

    /// Vérifie qu'un bloc peut être ajouté à la suite de la chaîne :
    /// index, lien avec le bloc précédent, hash et signature du validateur.
    pub fn is_valid_successor(&self, block: &Block) -> bool {
        let last = self.last();
        last.block_data.index.checked_add(1) == Some(block.block_data.index)
            && block.block_data.prev_hash == last.block_data.hash
            && block.verify_hashes().is_ok()
            && block.verify_signature().is_ok()
    }

    /// Ajoute le bloc s'il est un successeur valide. Retourne `false` sinon.
    pub fn push(&mut self, block: Block) -> bool {
        if !self.is_valid_successor(&block) {
            return false;
        }
        self.blocks.push(block);
        true
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
    use crate::{block::BlockError, transaction::Transaction};
    use ed25519_dalek::SigningKey;

    fn successor(chain: &Chain) -> Block {
        let key = SigningKey::from_bytes(&[7; 32]);
        let tx = Transaction::new(&key.verifying_key(), 10, 0, 1, &key).unwrap();
        Block::new(
            chain.len(),
            chain.last().block_data.hash.clone(),
            vec![tx],
            1,
            &key,
        )
        .unwrap()
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
        assert!(chain.push(decoded));
    }

    #[test]
    fn transaction_tampering_is_rejected_even_if_commitments_are_recomputed() {
        let chain = Chain::new();
        let mut block = successor(&chain);
        block.block_data.transactions[0].amount += 1;
        assert!(matches!(
            block.verify_hashes(),
            Err(BlockError::InvalidTransactionRoot)
        ));
        assert!(!chain.is_valid_successor(&block));
        block.block_data.tx_root =
            Block::calculate_tx_root(&block.block_data.transactions).unwrap();
        assert!(matches!(
            block.verify_hashes(),
            Err(BlockError::InvalidHash)
        ));
        block.block_data.hash = block.calculate_hash().unwrap();
        block.verify_hashes().unwrap();
        assert!(matches!(
            block.verify_signature(),
            Err(BlockError::InvalidSignature)
        ));
        assert!(!chain.is_valid_successor(&block));
    }

    #[test]
    fn changed_validator_and_missing_signature_are_rejected() {
        let chain = Chain::new();
        let mut block = successor(&chain);
        block.validator = Some(SigningKey::from_bytes(&[8; 32]).verifying_key());
        block.block_data.hash = block.calculate_hash().unwrap();
        assert!(matches!(
            block.verify_signature(),
            Err(BlockError::InvalidSignature)
        ));
        assert!(!chain.is_valid_successor(&block));
        block.signature = None;
        assert!(matches!(
            block.verify_signature(),
            Err(BlockError::MissingSignature)
        ));
        block.validator = None;
        assert!(matches!(
            block.verify_signature(),
            Err(BlockError::MissingValidator)
        ));
    }

    #[test]
    fn wrong_parent_and_index_are_rejected() {
        let chain = Chain::new();
        let key = SigningKey::from_bytes(&[7; 32]);
        let wrong_parent = Block::new(1, "wrong".into(), vec![], 1, &key).unwrap();
        let wrong_index =
            Block::new(2, chain.last().block_data.hash.clone(), vec![], 1, &key).unwrap();
        assert!(!chain.is_valid_successor(&wrong_parent));
        assert!(!chain.is_valid_successor(&wrong_index));
    }
}
