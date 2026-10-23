use std::collections::{HashMap, HashSet};

use crate::transaction::Transaction;

/// Pool des transactions en attente d'inclusion dans un bloc.
#[derive(Default)]
pub struct Mempool {
    pub txs: HashMap<String, Transaction>, // key = tx hash
    pub seen: HashSet<String>,             // déduplication
}

impl Mempool {
    pub fn new() -> Self {
        Self::default()
    }

    /// Ajoute une TX. Retourne `false` si elle a déjà été vue
    /// (évite de la re-gossiper en boucle entre les nodes).
    pub fn add(&mut self, tx: Transaction, hash: String) -> bool {
        if self.seen.contains(&hash) {
            return false;
        }
        self.seen.insert(hash.clone());
        self.txs.insert(hash, tx);
        true
    }

    /// Vide le pool pour construire un bloc. `seen` est conservé :
    /// une TX déjà incluse ne doit pas revenir par gossip.
    pub fn drain(&mut self) -> Vec<Transaction> {
        self.txs.drain().map(|(_, tx)| tx).collect()
    }

    /// Retire les TX incluses dans un bloc reçu d'un autre validateur.
    pub fn remove(&mut self, hashes: &[String]) {
        for hash in hashes {
            self.txs.remove(hash);
            self.seen.insert(hash.clone());
        }
    }

    pub fn contains(&self, hash: &str) -> bool {
        self.seen.contains(hash)
    }

    pub fn len(&self) -> usize {
        self.txs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }
}
