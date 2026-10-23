use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use sha2::{Digest, Sha256};

use crate::block::Block;

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
        block.index == last.index + 1
            && block.prev_hash == last.hash
            && block.hash == compute_block_hash(block)
            && verify_block_signature(block)
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
        index: 0,
        prev_hash: GENESIS_PREV_HASH.to_string(),
        tx_root: compute_tx_root(&[]),
        timestamp: 0,
        validator: Vec::new(),
        signature: Vec::new(),
        hash: String::new(),
    };
    block.hash = compute_block_hash(&block);
    block
}

/// Hash hex de l'en-tête du bloc (tout sauf `signature` et `hash`).
/// C'est ce hash que le validateur signe.
pub fn compute_block_hash(block: &Block) -> String {
    let mut hasher = Sha256::new();
    hasher.update(block.index.to_le_bytes());
    hasher.update(block.prev_hash.as_bytes());
    hasher.update(block.tx_root.as_bytes());
    hasher.update(block.timestamp.to_le_bytes());
    hasher.update(&block.validator);
    to_hex(&hasher.finalize())
}

/// Hash hex de la liste des hash de TX incluses dans le bloc.
pub fn compute_tx_root(tx_hashes: &[String]) -> String {
    let mut hasher = Sha256::new();
    for hash in tx_hashes {
        hasher.update(hash.as_bytes());
    }
    to_hex(&hasher.finalize())
}

/// Vérifie que `block.signature` est la signature de `block.hash`
/// par la clé `block.validator`.
pub fn verify_block_signature(block: &Block) -> bool {
    let Ok(key_bytes) = <[u8; 32]>::try_from(block.validator.as_slice()) else {
        return false;
    };
    let Ok(key) = VerifyingKey::from_bytes(&key_bytes) else {
        return false;
    };
    let Ok(signature) = Signature::from_slice(&block.signature) else {
        return false;
    };
    key.verify(block.hash.as_bytes(), &signature).is_ok()
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
