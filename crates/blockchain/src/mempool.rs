use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::chain::{Chain, MAX_TXS_PER_BLOCK};
use crate::transaction::{Transaction, TransactionError, VerifiedTransaction};

#[derive(Debug, thiserror::Error)]
pub enum MempoolError {
    #[error("transaction already seen: {0}")]
    AlreadySeen(String),

    #[error("invalid transaction: {0}")]
    InvalidTransaction(#[from] TransactionError),
}

/// Place d'une suite de TX en attente par rapport au prochain bloc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingState {
    /// Prise par le prochain `drain`.
    NextBlock,
    /// Incluable, mais au-delà de `MAX_TXS_PER_BLOCK` : attend un bloc suivant.
    Later,
    /// Un nonce manque avant elle : attend la TX qui comble le trou.
    Gap,
}

/// TX d'un même expéditeur aux nonces consécutifs, dans le même état.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRun {
    pub from: Vec<u8>,
    pub first_nonce: u64,
    pub count: usize,
    pub state: PendingState,
    /// Timestamp (ms Unix, posé par le client) de la plus ancienne TX de la suite.
    pub oldest_timestamp: u64,
}

/// Pool des transactions en attente d'inclusion dans un bloc.
#[derive(Default)]
pub struct Mempool {
    /// Clé : hash de la TX.
    txs: HashMap<String, Transaction>,
    /// Déduplication : TX vérifiées à l'entrée ou incluses dans un bloc accepté.
    seen: HashSet<String>,
    /// Les mêmes TX que `txs`, dans l'ordre (expéditeur, nonce, hash) : `drain` ne lit
    /// que ce qu'il prend, au lieu de trier tout le pool à chaque bloc.
    by_sender: BTreeMap<Vec<u8>, BTreeSet<(u64, String)>>,
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
        self.insert(hash.clone(), tx);
        Ok(hash)
    }

    /// Comme `add` pour une TX déjà vérifiée (ex. en parallèle, hors verrou).
    pub fn add_verified(&mut self, tx: VerifiedTransaction) -> Result<String, MempoolError> {
        let hash = tx.hash().to_string();
        if !self.seen.insert(hash.clone()) {
            return Err(MempoolError::AlreadySeen(hash));
        }
        self.insert(hash.clone(), tx.transaction().clone());
        Ok(hash)
    }

    fn insert(&mut self, hash: String, tx: Transaction) {
        self.by_sender
            .entry(tx.from.clone())
            .or_default()
            .insert((tx.nonce, hash.clone()));
        self.txs.insert(hash, tx);
    }

    /// Retire une TX en attente ; `seen` est conservé.
    fn remove(&mut self, hash: &str) -> Option<Transaction> {
        let tx = self.txs.remove(hash)?;
        if let Some(pending) = self.by_sender.get_mut(&tx.from) {
            pending.remove(&(tx.nonce, hash.to_string()));
            if pending.is_empty() {
                self.by_sender.remove(&tx.from);
            }
        }
        Some(tx)
    }

    /// Retire du pool les TX incluables dans le prochain bloc de `chain`,
    /// triées par expéditeur puis par nonce. `seen` est conservé :
    /// une TX déjà incluse ne doit pas revenir par gossip.
    ///
    /// - nonce attendu : la TX est retournée ;
    /// - nonce déjà utilisé (rejeu ou conflit) : la TX est supprimée ;
    /// - nonce trop grand (trou) : la TX reste dans le pool.
    /// - au plus `MAX_TXS_PER_BLOCK` TX : le reste attend le bloc suivant.
    pub fn drain(&mut self, chain: &Chain) -> Vec<Transaction> {
        self.drain_up_to(chain, MAX_TXS_PER_BLOCK)
    }

    fn drain_up_to(&mut self, chain: &Chain, limit: usize) -> Vec<Transaction> {
        let mut ready = Vec::new();
        let mut stale = Vec::new();
        'senders: for (from, pending) in &self.by_sender {
            let mut next = chain.next_nonce(from);
            for (nonce, hash) in pending {
                if ready.len() == limit {
                    break 'senders;
                }
                match nonce.cmp(&next) {
                    Ordering::Less => stale.push(hash.clone()),
                    Ordering::Equal => match next.checked_add(1) {
                        Some(following) => {
                            next = following;
                            ready.push(hash.clone());
                        }
                        None => stale.push(hash.clone()),
                    },
                    // Trou : les nonces suivants de cet expéditeur attendent aussi.
                    Ordering::Greater => break,
                }
            }
        }
        for hash in &stale {
            self.remove(hash);
        }
        ready
            .iter()
            .map(|hash| self.remove(hash).expect("indexed transactions are pending"))
            .collect()
    }

    /// Vue du pool par suites de nonces consécutifs d'un même expéditeur,
    /// avec la place de chaque suite par rapport au prochain `drain`.
    /// Ne modifie pas le pool. Les TX que `drain` supprimerait (nonce déjà
    /// utilisé) n'apparaissent pas.
    pub fn pending_runs(&self, chain: &Chain) -> Vec<PendingRun> {
        self.pending_runs_up_to(chain, MAX_TXS_PER_BLOCK)
    }

    /// Même parcours que `drain_up_to` : même ordre, même limite.
    fn pending_runs_up_to(&self, chain: &Chain, limit: usize) -> Vec<PendingRun> {
        let mut runs: Vec<PendingRun> = Vec::new();
        let mut included = 0;
        for (from, pending) in &self.by_sender {
            // Prochain nonce contigu, et si l'expéditeur est encore incluable.
            let mut next = chain.next_nonce(from);
            let mut contiguous = true;
            let first_run = runs.len();
            for (nonce, hash) in pending {
                let nonce = *nonce;
                if nonce < next {
                    continue;
                }
                let state = if nonce == next && contiguous {
                    if included < limit {
                        included += 1;
                        PendingState::NextBlock
                    } else {
                        PendingState::Later
                    }
                } else {
                    contiguous = false;
                    PendingState::Gap
                };
                next = nonce.saturating_add(1);
                let timestamp = self.txs[hash].timestamp;
                match runs[first_run..].last_mut() {
                    Some(run)
                        if run.state == state && run.first_nonce + run.count as u64 == nonce =>
                    {
                        run.count += 1;
                        run.oldest_timestamp = run.oldest_timestamp.min(timestamp);
                    }
                    _ => runs.push(PendingRun {
                        from: from.clone(),
                        first_nonce: nonce,
                        count: 1,
                        state,
                        oldest_timestamp: timestamp,
                    }),
                }
            }
        }
        runs
    }

    /// TX en attente de hash `hash`.
    pub fn get(&self, hash: &str) -> Option<&Transaction> {
        self.txs.get(hash)
    }

    /// TX en attente, dans un ordre quelconque.
    pub fn transactions(&self) -> impl Iterator<Item = &Transaction> {
        self.txs.values()
    }

    /// Retire les TX incluses dans un bloc reçu d'un autre validateur.
    pub fn remove_included(&mut self, txs: &[Transaction]) -> Result<(), MempoolError> {
        for tx in txs {
            let hash = tx.calculate_hash()?;
            self.remove(&hash);
            self.seen.insert(hash);
        }
        Ok(())
    }

    /// Comme `remove_included`, avec les hash déjà calculés des TX du bloc.
    pub fn remove_hashes(&mut self, hashes: &[String]) {
        for hash in hashes {
            self.remove(hash);
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
    fn verified_transaction_is_added_once_and_dedups_with_add() {
        let mut mempool = Mempool::new();
        let verified = VerifiedTransaction::new(signed_tx()).unwrap();
        let hash = mempool.add_verified(verified.clone()).unwrap();
        assert!(mempool.contains(&hash));
        assert!(matches!(
            mempool.add_verified(verified),
            Err(MempoolError::AlreadySeen(h)) if h == hash
        ));
        assert!(matches!(
            mempool.add(signed_tx()),
            Err(MempoolError::AlreadySeen(_))
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

    #[test]
    fn sender_index_follows_every_removal() {
        let chain = chain();
        let mut mempool = Mempool::new();
        let txs: Vec<_> = (0..4).map(|nonce| tx_from(7, 7, nonce)).collect();
        for tx in &txs {
            mempool.add(tx.clone()).unwrap();
        }
        mempool.add(tx_from(5, 5, 0)).unwrap();
        // Inclus par un autre validateur : ni rendu par drain, ni laissé dans l'index.
        mempool.remove_included(&txs[..2]).unwrap();
        let hash = txs[2].calculate_hash().unwrap();
        mempool.remove_hashes(std::slice::from_ref(&hash));
        assert!(mempool.get(&hash).is_none());
        assert_eq!(mempool.len(), 2);
        assert_eq!(mempool.by_sender.values().map(BTreeSet::len).sum::<usize>(), 2);

        // Le nonce 3 de l'expéditeur 7 attend les nonces 0 à 2 sur la chaîne.
        assert_eq!(nonces(&mempool.drain(&chain)), [(5, 0)]);
        assert_eq!(mempool.len(), 1);
        mempool.remove_included(&txs[3..]).unwrap();
        assert!(mempool.is_empty());
        assert!(mempool.by_sender.is_empty());
    }

    #[test]
    fn pending_runs_match_what_drain_would_take() {
        let mut chain = chain();
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
            tx_from(5, 5, 0),
            tx_from(5, 5, 1),
            tx_from(5, 5, 2),
            tx_from(5, 5, 4), // trou : nonce 3 manquant
            tx_from(5, 5, 5),
            tx_from(7, 70, 0), // rejeu : ignoré
            tx_from(7, 7, 1),
            tx_from(7, 7, 2),
        ] {
            mempool.add(tx).unwrap();
        }

        let runs = mempool.pending_runs_up_to(&chain, 4);
        let shape: Vec<_> = runs
            .iter()
            .map(|run| {
                (
                    run.from == tx_from(5, 0, 0).from,
                    run.first_nonce,
                    run.count,
                    run.state,
                )
            })
            .collect();
        use PendingState::*;
        // 4 places, prises dans l'ordre de drain (par expéditeur puis nonce).
        let expected = if tx_from(5, 0, 0).from < tx_from(7, 0, 0).from {
            vec![
                (true, 0, 3, NextBlock),
                (true, 4, 2, Gap),
                (false, 1, 1, NextBlock),
                (false, 2, 1, Later),
            ]
        } else {
            vec![
                (false, 1, 2, NextBlock),
                (true, 0, 2, NextBlock),
                (true, 2, 1, Later),
                (true, 4, 2, Gap),
            ]
        };
        assert_eq!(shape, expected);
        assert_eq!(mempool.len(), 8, "la vue ne modifie pas le pool");

        let drained = mempool.drain_up_to(&chain, 4);
        assert_eq!(drained.len(), 4);
    }

    #[test]
    fn drain_stops_at_limit_and_keeps_the_rest_in_nonce_order() {
        let chain = chain();
        let mut mempool = Mempool::new();
        for nonce in 0..5 {
            mempool.add(tx_from(7, 7, nonce)).unwrap();
        }
        let first = mempool.drain_up_to(&chain, 3);
        assert_eq!(nonces(&first), [(7, 0), (7, 1), (7, 2)]);
        assert_eq!(mempool.len(), 2);

        let mut chain = chain;
        let block = Block::new(
            1,
            chain.last().block_data.hash.clone(),
            first,
            1,
            &validator(),
        )
        .unwrap();
        chain.push(block).unwrap();
        assert_eq!(nonces(&mempool.drain_up_to(&chain, 3)), [(7, 3), (7, 4)]);
        assert!(mempool.is_empty());
    }
}
