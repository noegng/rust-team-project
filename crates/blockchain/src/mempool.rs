use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::chain::Chain;
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

    /// Retire du pool les TX incluables dans le prochain bloc de `chain`,
    /// triées par expéditeur puis par nonce. `seen` est conservé :
    /// une TX déjà incluse ne doit pas revenir par gossip.
    ///
    /// - nonce attendu : la TX est retournée ;
    /// - nonce déjà utilisé (rejeu ou conflit) : la TX est supprimée ;
    /// - nonce trop grand (trou) : la TX reste dans le pool.
    pub fn drain(&mut self, chain: &Chain) -> Vec<Transaction> {
        let mut pending: Vec<_> = self.txs.drain().collect();
        pending.sort_by(|(hash_a, a), (hash_b, b)| {
            (&a.from, a.nonce, hash_a).cmp(&(&b.from, b.nonce, hash_b))
        });

        let mut next_nonces: HashMap<Vec<u8>, u64> = HashMap::new();
        let mut ready = Vec::new();
        for (hash, tx) in pending {
            let next = next_nonces
                .entry(tx.from.clone())
                .or_insert_with(|| chain.next_nonce(&tx.from));
            match tx.nonce.cmp(next) {
                Ordering::Less => {}
                Ordering::Equal => {
                    if let Some(following) = next.checked_add(1) {
                        *next = following;
                        ready.push(tx);
                    }
                }
                Ordering::Greater => {
                    self.txs.insert(hash, tx);
                }
            }
        }
        ready
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
    use crate::block::Block;
    use ed25519_dalek::SigningKey;

    fn validator() -> SigningKey {
        SigningKey::from_bytes(&[9; 32])
    }

    fn chain() -> Chain {
        Chain::new(vec![validator().verifying_key()]).unwrap()
    }

    fn tx_from(seed: u8, amount: u64, nonce: u64) -> Transaction {
        let key = SigningKey::from_bytes(&[seed; 32]);
        Transaction::new(&key.verifying_key(), amount, nonce, 1, &key).unwrap()
    }

    fn signed_tx() -> Transaction {
        tx_from(7, 10, 0)
    }

    fn nonces(txs: &[Transaction]) -> Vec<(u8, u64)> {
        txs.iter().map(|tx| (tx.amount as u8, tx.nonce)).collect()
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
        let drained = mempool.drain(&chain());
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

    #[test]
    fn drain_orders_by_nonce_keeps_gaps_and_drops_stale() {
        let mut chain = chain();
        // L'expéditeur 7 a déjà utilisé le nonce 0 dans un bloc.
        let first = Block::new(
            1,
            chain.last().block_data.hash.clone(),
            vec![tx_from(7, 7, 0)],
            1,
            &validator(),
        )
        .unwrap();
        chain.push(first).unwrap();

        let mut mempool = Mempool::new();
        for tx in [
            tx_from(7, 7, 2),
            tx_from(5, 5, 1),
            tx_from(7, 70, 0), // rejeu d'un nonce déjà inclus
            tx_from(7, 7, 1),
            tx_from(5, 5, 0),
            tx_from(5, 5, 3), // trou : nonce 2 manquant
        ] {
            mempool.add(tx).unwrap();
        }

        let ready = mempool.drain(&chain);
        let mut sorted = ready.clone();
        sorted.sort_by(|a, b| (&a.from, a.nonce).cmp(&(&b.from, b.nonce)));
        assert_eq!(nonces(&ready), nonces(&sorted));
        assert_eq!(ready.len(), 4);
        assert!(!ready.iter().any(|tx| tx.amount == 70));
        assert_eq!(
            nonces(&mempool.txs.values().cloned().collect::<Vec<_>>()),
            [(5, 3)]
        );

        let block = Block::new(
            2,
            chain.last().block_data.hash.clone(),
            ready,
            2,
            &validator(),
        )
        .unwrap();
        chain.push(block).unwrap();
        assert_eq!(chain.next_nonce(&tx_from(5, 0, 0).from), 2);
        assert_eq!(chain.next_nonce(&tx_from(7, 0, 0).from), 3);
    }
}
