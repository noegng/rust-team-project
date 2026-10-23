use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use blockchain::{block::Block, transaction::Transaction};
use serde::Serialize;

/// Compteurs du node, partagés via `Arc<Metrics>` sans verrou.
pub(crate) struct Metrics {
    started: Instant,
    txs_received: AtomicU64,
    propagation_ms: Average,
    txs_committed: AtomicU64,
    finality_ms: Average,
}

/// Vue sérialisable des métriques (ex. réponse de `GET /metrics`).
#[derive(Serialize, Debug, Clone, PartialEq)]
pub(crate) struct MetricsSnapshot {
    pub txs_received: u64,
    pub tps: f64,
    pub avg_propagation_ms: Option<u64>,
    pub txs_committed: u64,
    pub avg_finality_ms: Option<u64>,
    pub chain_len: u64,
    pub mempool_len: usize,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        Self {
            started: Instant::now(),
            txs_received: AtomicU64::new(0),
            propagation_ms: Average::default(),
            txs_committed: AtomicU64::new(0),
            finality_ms: Average::default(),
        }
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
        let txs_received = self.txs_received.load(Ordering::Relaxed);
        let elapsed = self.started.elapsed().as_secs_f64();
        MetricsSnapshot {
            txs_received,
            tps: if elapsed > 0.0 {
                txs_received as f64 / elapsed
            } else {
                0.0
            },
            avg_propagation_ms: self.propagation_ms.get(),
            txs_committed: self.txs_committed.load(Ordering::Relaxed),
            avg_finality_ms: self.finality_ms.get(),
            chain_len,
            mempool_len,
        }
    }

    fn record_tx_at(&self, tx: &Transaction, now_ms: u64) {
        self.txs_received.fetch_add(1, Ordering::Relaxed);
        self.propagation_ms.record(age_ms(tx.timestamp, now_ms));
    }

    fn record_block_at(&self, block: &Block, now_ms: u64) {
        for tx in &block.block_data.transactions {
            self.txs_committed.fetch_add(1, Ordering::Relaxed);
            self.finality_ms.record(age_ms(tx.timestamp, now_ms));
        }
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

fn unix_millis() -> u64 {
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
        assert!(snapshot.tps > 0.0);
        assert_eq!(snapshot.avg_propagation_ms, Some(300));
        assert_eq!(snapshot.txs_committed, 2);
        assert_eq!(snapshot.avg_finality_ms, Some(1_500));
        assert_eq!(snapshot.mempool_len, 3);
    }

    #[test]
    fn timestamp_in_the_future_counts_as_zero() {
        assert_eq!(age_ms(20_000, 10_000), 0);
    }
}
