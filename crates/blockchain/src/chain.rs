use std::collections::HashMap;

use ed25519_dalek::VerifyingKey;

use crate::block::{Block, BlockData, BlockError};
use crate::transaction::TransactionError;

/// Hash hex du bloc genesis précédent (aucun parent).
pub const GENESIS_PREV_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug, thiserror::Error)]
pub enum ChainError {
    #[error("no validators configured")]
    NoValidators,

    #[error("block index overflow")]
    IndexOverflow,

    #[error("invalid block index: expected {expected}, got {got}")]
    InvalidIndex { expected: u64, got: u64 },

    #[error("previous block hash does not match: expected {expected}, got {got}")]
    InvalidPreviousHash { expected: String, got: String },

    #[error("block timestamp {got} is older than previous block timestamp {previous}")]
    InvalidTimestamp { previous: u64, got: u64 },

    #[error("validator is not authorized")]
    UnauthorizedValidator,

    #[error("validator is not the proposer of block {index}")]
    WrongProposer { index: u64 },

    #[error("invalid block: {0}")]
    InvalidBlock(#[from] BlockError),

    #[error("invalid transaction: {0}")]
    InvalidTransaction(#[from] TransactionError),

    #[error("invalid transaction nonce: expected {expected}, got {got}")]
    InvalidNonce { expected: u64, got: u64 },

    #[error("transaction nonce overflow")]
    NonceOverflow,
}

/// Prochain nonce attendu, par clé publique de l'expéditeur.
type Nonces = HashMap<Vec<u8>, u64>;

pub struct Chain {
    blocks: Vec<Block>,
    validators: Vec<VerifyingKey>,
    nonces: Nonces,
}

impl Chain {
    /// Crée une chaîne contenant uniquement le bloc genesis.
    /// Il est déterministe : les 3 nodes partent du même hash.
    /// `validators` doit être dans le même ordre sur tous les nodes (rotation).
    pub fn new(validators: Vec<VerifyingKey>) -> Result<Self, ChainError> {
        if validators.is_empty() {
            return Err(ChainError::NoValidators);
        }
        Ok(Self {
            blocks: vec![genesis_block()],
            validators,
            nonces: Nonces::new(),
        })
    }

    pub fn blocks(&self) -> &[Block] {
        &self.blocks
    }

    pub fn validators(&self) -> &[VerifyingKey] {
        &self.validators
    }

    /// Nombre de blocs (= index du prochain bloc à produire).
    pub fn len(&self) -> u64 {
        self.blocks.len() as u64
    }

    /// Toujours `false` : la chaîne contient au moins le genesis.
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Dernier bloc. Le genesis est toujours présent (`blocks` est privé).
    pub fn last(&self) -> &Block {
        self.blocks.last().expect("chain always contains genesis")
    }

    /// Validateur attendu pour le bloc `index` (round-robin).
    pub fn proposer(&self, index: u64) -> &VerifyingKey {
        let len = self.validators.len() as u64;
        &self.validators[(index % len) as usize]
    }

    /// Prochain nonce accepté pour l'expéditeur `from` (0 s'il n'a jamais émis).
    pub fn next_nonce(&self, from: &[u8]) -> u64 {
        self.nonces.get(from).copied().unwrap_or(0)
    }

    /// Vérifie qu'un bloc peut être ajouté à la suite de la chaîne :
    /// index, lien, timestamp, hash, signature, proposant et transactions.
    pub fn validate_successor(&self, block: &Block) -> Result<(), ChainError> {
        self.check_successor(block).map(|_| ())
    }

    pub fn is_valid_successor(&self, block: &Block) -> bool {
        self.validate_successor(block).is_ok()
    }

    /// Ajoute le bloc s'il est un successeur valide.
    pub fn push(&mut self, block: Block) -> Result<(), ChainError> {
        let nonces = self.check_successor(&block)?;
        self.nonces.extend(nonces);
        self.blocks.push(block);
        Ok(())
    }

    /// Valide le bloc et retourne les nonces mis à jour par ses transactions.
    fn check_successor(&self, block: &Block) -> Result<Nonces, ChainError> {
        let last = self.last();
        let data = &block.block_data;

        let expected = last
            .block_data
            .index
            .checked_add(1)
            .ok_or(ChainError::IndexOverflow)?;
        if data.index != expected {
            return Err(ChainError::InvalidIndex {
                expected,
                got: data.index,
            });
        }
        if data.prev_hash != last.block_data.hash {
            return Err(ChainError::InvalidPreviousHash {
                expected: last.block_data.hash.clone(),
                got: data.prev_hash.clone(),
            });
        }
        if data.timestamp < last.block_data.timestamp {
            return Err(ChainError::InvalidTimestamp {
                previous: last.block_data.timestamp,
                got: data.timestamp,
            });
        }

        block.verify_hashes()?;
        block.verify_signature()?;
        let validator = block.validator.ok_or(BlockError::MissingValidator)?;
        if !self.validators.contains(&validator) {
            return Err(ChainError::UnauthorizedValidator);
        }
        if &validator != self.proposer(data.index) {
            return Err(ChainError::WrongProposer { index: data.index });
        }

        // Les nonces doivent se suivre par expéditeur : bloque les rejeux
        // et les doublons, dans le bloc comme d'un bloc à l'autre.
        let mut nonces = Nonces::new();
        for tx in &data.transactions {
            tx.verify()?;
            let expected = nonces
                .get(&tx.from)
                .copied()
                .unwrap_or_else(|| self.next_nonce(&tx.from));
            if tx.nonce != expected {
                return Err(ChainError::InvalidNonce {
                    expected,
                    got: tx.nonce,
                });
            }
            let next = expected.checked_add(1).ok_or(ChainError::NonceOverflow)?;
            nonces.insert(tx.from.clone(), next);
        }
        Ok(nonces)
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

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn chain() -> Chain {
        Chain::new(vec![key(7).verifying_key()]).unwrap()
    }

    fn tx(nonce: u64) -> Transaction {
        let sender = key(1);
        Transaction::new(&key(2).verifying_key(), 10, nonce, 1, &sender).unwrap()
    }

    fn next_block(chain: &Chain, txs: Vec<Transaction>, signer: &SigningKey) -> Block {
        Block::new(
            chain.len(),
            chain.last().block_data.hash.clone(),
            txs,
            chain.last().block_data.timestamp + 1,
            signer,
        )
        .unwrap()
    }

    fn successor(chain: &Chain) -> Block {
        next_block(chain, vec![tx(0)], &key(7))
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
    fn chain_requires_validators() {
        assert!(matches!(Chain::new(vec![]), Err(ChainError::NoValidators)));
    }

    #[test]
    fn signed_block_survives_serialization_and_can_be_appended() {
        let mut chain = chain();
        let block = successor(&chain);
        let encoded = serde_json::to_vec(&block).unwrap();
        let decoded: Block = serde_json::from_slice(&encoded).unwrap();
        chain.push(decoded).unwrap();
        assert_eq!(chain.len(), 2);
        assert_eq!(chain.next_nonce(&tx(0).from), 1);
    }

    #[test]
    fn transaction_tampering_is_rejected_even_if_commitments_are_recomputed() {
        let chain = chain();
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
    fn forged_transaction_signed_by_validator_is_rejected() {
        let chain = chain();
        let mut forged = tx(0);
        forged.amount = 1_000_000;
        let block = next_block(&chain, vec![forged], &key(7));
        assert!(matches!(
            chain.validate_successor(&block),
            Err(ChainError::InvalidTransaction(
                TransactionError::InvalidSignature
            ))
        ));
    }

    #[test]
    fn changed_validator_and_missing_signature_are_rejected() {
        let chain = chain();
        let mut block = successor(&chain);
        block.validator = Some(key(8).verifying_key());
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
    fn unknown_validator_is_rejected() {
        let chain = chain();
        let block = next_block(&chain, vec![], &key(9));
        assert!(matches!(
            chain.validate_successor(&block),
            Err(ChainError::UnauthorizedValidator)
        ));
    }

    #[test]
    fn validators_take_turns() {
        let mut chain = Chain::new(vec![key(7).verifying_key(), key(8).verifying_key()]).unwrap();
        // Block 1 → validators[1], block 2 → validators[0].
        let out_of_turn = next_block(&chain, vec![], &key(7));
        assert!(matches!(
            chain.push(out_of_turn),
            Err(ChainError::WrongProposer { index: 1 })
        ));
        chain.push(next_block(&chain, vec![], &key(8))).unwrap();
        chain.push(next_block(&chain, vec![], &key(7))).unwrap();
        assert_eq!(chain.len(), 3);
    }

    #[test]
    fn older_timestamp_is_rejected() {
        let mut chain = chain();
        chain.push(next_block(&chain, vec![], &key(7))).unwrap();
        let block =
            Block::new(2, chain.last().block_data.hash.clone(), vec![], 0, &key(7)).unwrap();
        assert!(matches!(
            chain.push(block),
            Err(ChainError::InvalidTimestamp {
                previous: 1,
                got: 0
            })
        ));
    }

    #[test]
    fn replayed_duplicated_and_out_of_order_nonces_are_rejected() {
        let mut chain = chain();
        chain.push(successor(&chain)).unwrap();

        let replay = next_block(&chain, vec![tx(0)], &key(7));
        assert!(matches!(
            chain.validate_successor(&replay),
            Err(ChainError::InvalidNonce {
                expected: 1,
                got: 0
            })
        ));
        let duplicate = next_block(&chain, vec![tx(1), tx(1)], &key(7));
        assert!(matches!(
            chain.validate_successor(&duplicate),
            Err(ChainError::InvalidNonce {
                expected: 2,
                got: 1
            })
        ));
        let out_of_order = next_block(&chain, vec![tx(2), tx(1)], &key(7));
        assert!(matches!(
            chain.validate_successor(&out_of_order),
            Err(ChainError::InvalidNonce {
                expected: 1,
                got: 2
            })
        ));

        chain
            .push(next_block(&chain, vec![tx(1), tx(2)], &key(7)))
            .unwrap();
        assert_eq!(chain.next_nonce(&tx(0).from), 3);
    }

    #[test]
    fn wrong_parent_and_index_are_rejected() {
        let mut chain = chain();
        let wrong_parent = Block::new(1, "wrong".into(), vec![], 1, &key(7)).unwrap();
        let wrong_index =
            Block::new(2, chain.last().block_data.hash.clone(), vec![], 1, &key(7)).unwrap();
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
}
