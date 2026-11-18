use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use blockchain::{block::Block, transaction::Transaction};
use serde::Serialize;

/// Une rafale d'envoi se termine après ce délai sans nouvelle TX ; la suivante repart de zéro.
const BURST_IDLE_MS: u64 = 1_000;

/// Temps de traitement conservés : autant que `GET /blocks` peut en montrer.
pub(crate) const BLOCK_TIMINGS_KEPT: usize = 100;

#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BlockSource {
    Produced,
    Received,
}

/// Coût d'ajout d'un bloc sur ce node, étape par étape (microsecondes).
/// Les quatre étapes se suivent : leur somme est le temps passé sur le bloc.
#[derive(Serialize, Debug, Clone, PartialEq)]
pub(crate) struct BlockTiming {
    pub index: u64,
    pub source: BlockSource,
    /// Ajout à la chaîne locale, ms Unix.
    pub added_at_ms: u64,
    /// Écart avec le bloc précédent ajouté par ce node ; `None` pour le premier.
    pub interval_ms: Option<u64>,
    /// Reçu : hash des TX et vérification de leurs signatures, en parallèle, hors verrous.
    /// Produit : 0.
    pub verify_us: u64,
    /// Attente des verrous `chain` puis `mempool` (tenus par l'admission de TX).
    pub lock_wait_us: u64,
    /// Produit : tri du mempool, construction, signature, ajout.
    /// Reçu : vérification du bloc (hashes, signature, nonces), ajout, nettoyage du mempool.
    pub validate_us: u64,
    /// Sérialisation du bloc et envoi aux peers (verrous encore tenus).
    pub publish_us: u64,
    /// Signatures de TX vérifiées pendant la validation : TX pas encore reçues par gossip.
    pub signatures_checked: usize,
    pub transactions: usize,
}

/// Compteurs du node, partagés via `Arc<Metrics>`.
pub(crate) struct Metrics {
    throughput: Throughput,
    txs_received: AtomicU64,
    propagation_ms: Average,
    txs_committed: AtomicU64,
    finality_ms: Average,
    txs_rejected: AtomicU64,
    sync_resets: AtomicU64,
    /// Derniers blocs ajoutés, du plus ancien au plus récent.
    block_timings: Mutex<VecDeque<BlockTiming>>,
}

/// Vue sérialisable des métriques (ex. réponse de `GET /metrics`).
#[derive(Serialize, Debug, Clone, PartialEq)]
pub(crate) struct MetricsSnapshot {
    pub txs_received: u64,
    /// Débit réel de la rafale en cours ou de la dernière, en TX/s :
    /// entre la première et la dernière TX reçues. `None` si la rafale ne dure pas 1 ms.
    pub tps: Option<f64>,
    pub avg_propagation_ms: Option<u64>,
    pub txs_committed: u64,
    pub avg_finality_ms: Option<u64>,
    pub chain_len: u64,
    pub mempool_len: usize,
    /// TX refusées par `POST /transactions` (doublon, signature invalide...).
    pub txs_rejected: u64,
    /// Connexions peer coupées sur un bloc rejeté (sync incohérente).
    pub sync_resets: u64,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        Self {
            throughput: Throughput::default(),
            txs_received: AtomicU64::new(0),
            propagation_ms: Average::default(),
            txs_committed: AtomicU64::new(0),
            finality_ms: Average::default(),
            txs_rejected: AtomicU64::new(0),
            sync_resets: AtomicU64::new(0),
            block_timings: Mutex::default(),
        }
    }

    /// `interval_ms` est calculé ici, à partir du bloc précédent enregistré.
    pub(crate) fn record_block_timing(&self, mut timing: BlockTiming) {
        let mut timings = self
            .block_timings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        timing.interval_ms = timings
            .back()
            .map(|last| timing.added_at_ms.saturating_sub(last.added_at_ms));
        if timings.len() == BLOCK_TIMINGS_KEPT {
            timings.pop_front();
        }
        timings.push_back(timing);
    }

    pub(crate) fn block_timings(&self) -> Vec<BlockTiming> {
        self.block_timings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .cloned()
            .collect()
    }

    pub(crate) fn record_rejected_tx(&self) {
        self.txs_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Un peer a envoyé un bloc refusé : la connexion est coupée puis rejouée.
    pub(crate) fn record_sync_reset(&self) {
        self.sync_resets.fetch_add(1, Ordering::Relaxed);
    }

    /// TX acceptée dans le mempool (client ou gossip) :
    /// sert au TPS et à la latence de propagation (injection → réception).
    pub(crate) fn record_tx(&self, tx: &Transaction) {
        self.record_tx_at(tx, unix_millis());
    }

    /// Bloc ajouté à la chaîne (produit ou reçu) :
    /// temps de finalité de chaque TX (soumission → inclusion).
    pub(crate) fn record_block(&self, block: &Block) {
        self.record_block_at(block, unix_millis());
    }

    /// `chain_len` et `mempool_len` permettent de comparer A, B et C (lag de sync).
    pub(crate) fn snapshot(&self, chain_len: u64, mempool_len: usize) -> MetricsSnapshot {
        MetricsSnapshot {
            txs_received: self.txs_received.load(Ordering::Relaxed),
            tps: self.throughput.tps(),
            avg_propagation_ms: self.propagation_ms.get(),
            txs_committed: self.txs_committed.load(Ordering::Relaxed),
            avg_finality_ms: self.finality_ms.get(),
            chain_len,
            mempool_len,
            txs_rejected: self.txs_rejected.load(Ordering::Relaxed),
            sync_resets: self.sync_resets.load(Ordering::Relaxed),
        }
    }

    fn record_tx_at(&self, tx: &Transaction, now_ms: u64) {
        self.txs_received.fetch_add(1, Ordering::Relaxed);
        self.throughput.record(now_ms);
        self.propagation_ms.record(age_ms(tx.timestamp, now_ms));
    }

    fn record_block_at(&self, block: &Block, now_ms: u64) {
        for tx in &block.block_data.transactions {
            self.txs_committed.fetch_add(1, Ordering::Relaxed);
            self.finality_ms.record(age_ms(tx.timestamp, now_ms));
        }
    }
}

/// Débit de la dernière rafale de TX reçues.
#[derive(Default)]
struct Throughput(Mutex<Option<Burst>>);

#[derive(Clone, Copy)]
struct Burst {
    first_ms: u64,
    last_ms: u64,
    count: u64,
}

impl Throughput {
    fn record(&self, now_ms: u64) {
        let mut burst = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match burst.as_mut() {
            Some(current) if now_ms <= current.last_ms + BURST_IDLE_MS => {
                // Des réceptions concurrentes peuvent arriver légèrement dans le désordre.
                current.last_ms = current.last_ms.max(now_ms);
                current.count += 1;
            }
            _ => {
                *burst = Some(Burst {
                    first_ms: now_ms,
                    last_ms: now_ms,
                    count: 1,
                })
            }
        }
    }

    /// TX/s entre la première et la dernière réception : `count - 1` intervalles
    /// sur la durée de la rafale. Reste lisible une fois l'envoi terminé.
    fn tps(&self) -> Option<f64> {
        let burst = (*self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()))?;
        let span_ms = burst.last_ms.saturating_sub(burst.first_ms);
        (span_ms > 0).then(|| (burst.count - 1) as f64 * 1_000.0 / span_ms as f64)
    }
}

#[derive(Default)]
struct Average {
    total: AtomicU64,
    count: AtomicU64,
}

impl Average {
    fn record(&self, value: u64) {
        self.total.fetch_add(value, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn get(&self) -> Option<u64> {
        let count = self.count.load(Ordering::Relaxed);
        (count > 0).then(|| self.total.load(Ordering::Relaxed) / count)
    }
}

/// Âge d'un timestamp de TX (millisecondes Unix, posé par le client).
fn age_ms(timestamp_ms: u64, now_ms: u64) -> u64 {
    now_ms.saturating_sub(timestamp_ms)
}

pub(crate) fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn tx(timestamp: u64) -> Transaction {
        let key = SigningKey::from_bytes(&[1; 32]);
        Transaction::new(&key.verifying_key(), 1, 0, timestamp, &key).unwrap()
    }

    #[test]
    fn empty_metrics_have_no_averages() {
        let snapshot = Metrics::new().snapshot(1, 0);
        assert_eq!(snapshot.txs_received, 0);
        assert_eq!(snapshot.avg_propagation_ms, None);
        assert_eq!(snapshot.avg_finality_ms, None);
        assert_eq!(snapshot.chain_len, 1);
    }

    #[test]
    fn records_propagation_and_finality() {
        let metrics = Metrics::new();
        metrics.record_tx_at(&tx(10_000), 10_200);
        metrics.record_tx_at(&tx(10_000), 10_400);

        let key = SigningKey::from_bytes(&[2; 32]);
        let block = Block::new(1, "prev".into(), vec![tx(10_000), tx(11_000)], 12, &key).unwrap();
        metrics.record_block_at(&block, 12_000);

        let snapshot = metrics.snapshot(2, 3);
        assert_eq!(snapshot.txs_received, 2);
        // 1 intervalle en 200 ms.
        assert_eq!(snapshot.tps, Some(5.0));
        assert_eq!(snapshot.avg_propagation_ms, Some(300));
        assert_eq!(snapshot.txs_committed, 2);
        assert_eq!(snapshot.avg_finality_ms, Some(1_500));
        assert_eq!(snapshot.mempool_len, 3);
    }

    #[test]
    fn tps_is_the_real_rate_of_the_last_burst() {
        let metrics = Metrics::new();
        // Envoi court sur un node démarré depuis longtemps : seule la rafale compte.
        for at in [100_000, 100_100, 100_200, 100_300, 100_400] {
            metrics.record_tx_at(&tx(at), at);
        }
        assert_eq!(metrics.snapshot(1, 0).tps, Some(10.0));

        // Après plus de BURST_IDLE_MS de silence, un nouvel envoi remplace la mesure.
        for at in [200_000, 200_010, 200_020] {
            metrics.record_tx_at(&tx(at), at);
        }
        let snapshot = metrics.snapshot(1, 0);
        assert_eq!(snapshot.tps, Some(100.0));
        assert_eq!(snapshot.txs_received, 8);
    }

    #[test]
    fn tps_is_unknown_without_a_measurable_duration() {
        let metrics = Metrics::new();
        assert_eq!(metrics.snapshot(1, 0).tps, None);
        // Un lot reçu dans la même milliseconde n'a pas de durée mesurable.
        for _ in 0..100 {
            metrics.record_tx_at(&tx(5_000), 5_000);
        }
        assert_eq!(metrics.snapshot(1, 0).tps, None);
    }

    #[test]
    fn counts_rejected_transactions_and_sync_resets() {
        let metrics = Metrics::new();
        assert_eq!(metrics.snapshot(1, 0).txs_rejected, 0);
        assert_eq!(metrics.snapshot(1, 0).sync_resets, 0);
        metrics.record_rejected_tx();
        metrics.record_rejected_tx();
        metrics.record_sync_reset();
        let snapshot = metrics.snapshot(1, 0);
        assert_eq!(snapshot.txs_rejected, 2);
        assert_eq!(snapshot.sync_resets, 1);
        assert_eq!(snapshot.txs_received, 0);
    }

    #[test]
    fn block_timings_keep_the_latest_with_intervals() {
        let metrics = Metrics::new();
        let timing = |index, added_at_ms| BlockTiming {
            index,
            source: BlockSource::Received,
            added_at_ms,
            interval_ms: Some(999),
            verify_us: 0,
            lock_wait_us: 1,
            validate_us: 2,
            publish_us: 3,
            signatures_checked: 0,
            transactions: 0,
        };
        for index in 0..BLOCK_TIMINGS_KEPT as u64 + 2 {
            metrics.record_block_timing(timing(index, 1_000 + index * 2_000));
        }
        let timings = metrics.block_timings();
        assert_eq!(timings.len(), BLOCK_TIMINGS_KEPT);
        assert_eq!(timings[0].index, 2);
        assert_eq!(timings.last().unwrap().interval_ms, Some(2_000));

        let first = Metrics::new();
        first.record_block_timing(timing(1, 5_000));
        assert_eq!(first.block_timings()[0].interval_ms, None);
    }

    #[test]
    fn timestamp_in_the_future_counts_as_zero() {
        assert_eq!(age_ms(20_000, 10_000), 0);
    }
}
