use std::collections::{HashMap, HashSet};

use crate::transaction::{Transaction, TransactionError};

#[derive(Debug, thiserror::Error)]
pub enum MempoolError {
    #[error("transaction already seen: {0}")]
    AlreadySeen(String),

    #[error("invalid transaction: {0}")]
    InvalidTransaction(#[from] TransactionError),
}

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

    /// Vérifie la TX puis l'ajoute. Retourne son hash (à utiliser pour le gossip).
    /// `AlreadySeen` signifie qu'elle ne doit pas être re-gossipée.
    pub fn add(&mut self, tx: Transaction) -> Result<String, MempoolError> {
        let hash = tx.calculate_hash()?;
        if self.seen.contains(&hash) {
            return Err(MempoolError::AlreadySeen(hash));
        }
        tx.verify()?;
        self.seen.insert(hash.clone());
        self.txs.insert(hash.clone(), tx);
        Ok(hash)
    }

    /// Vide le pool pour construire un bloc. `seen` est conservé :
    /// une TX déjà incluse ne doit pas revenir par gossip.
    pub fn drain(&mut self) -> Vec<Transaction> {
        self.txs.drain().map(|(_, tx)| tx).collect()
    }

    /// Retire les TX incluses dans un bloc reçu d'un autre validateur.
    pub fn remove_included(&mut self, txs: &[Transaction]) -> Result<(), MempoolError> {
        for tx in txs {
            let hash = tx.calculate_hash()?;
            self.txs.remove(&hash);
            self.seen.insert(hash);
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn signed_tx() -> Transaction {
        let key = SigningKey::from_bytes(&[7; 32]);
        Transaction::new(&key.verifying_key(), 10, 0, 1, &key).unwrap()
    }

    #[test]
    fn valid_transaction_is_added_once() {
        let mut mempool = Mempool::new();
        let hash = mempool.add(signed_tx()).unwrap();
        assert!(mempool.contains(&hash));
        assert!(matches!(
            mempool.add(signed_tx()),
            Err(MempoolError::AlreadySeen(h)) if h == hash
        ));
        assert_eq!(mempool.len(), 1);
    }

    #[test]
    fn tampered_transaction_is_rejected_and_not_marked_seen() {
        let mut mempool = Mempool::new();
        let mut tx = signed_tx();
        tx.amount += 1;
        let hash = tx.calculate_hash().unwrap();
        assert!(matches!(
            mempool.add(tx),
            Err(MempoolError::InvalidTransaction(
                TransactionError::InvalidSignature
            ))
        ));
        assert!(!mempool.contains(&hash));
        assert!(mempool.is_empty());
    }

    #[test]
    fn drained_and_included_transactions_stay_seen() {
        let mut mempool = Mempool::new();
        mempool.add(signed_tx()).unwrap();
        let drained = mempool.drain();
        assert!(mempool.is_empty());
        assert!(matches!(
            mempool.add(drained[0].clone()),
            Err(MempoolError::AlreadySeen(_))
        ));

        let mut other = Mempool::new();
        other.add(signed_tx()).unwrap();
        other.remove_included(&drained).unwrap();
        assert!(other.is_empty());
    }
}
