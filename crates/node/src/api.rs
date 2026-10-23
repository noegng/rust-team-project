use std::sync::Arc;

use axum::{Json, extract::State};
use blockchain::transaction::Transaction;
use serde::Serialize;

use crate::{NodeMessage, NodeState, metrics::MetricsSnapshot};

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum TransactionResult {
    Accepted { hash: String },
    Rejected { error: String },
}

/// Results follow the same order as the submitted transactions.
/// Accepted transactions are gossiped to the peers.
pub(crate) async fn submit_transactions(
    State(state): State<Arc<NodeState>>,
    Json(transactions): Json<Vec<Transaction>>,
) -> Json<Vec<TransactionResult>> {
    let mut accepted = Vec::new();
    let results = {
        let mut mempool = state.mempool.lock().await;
        transactions
            .into_iter()
            .map(|tx| match mempool.add(tx.clone()) {
                Ok(hash) => {
                    accepted.push(tx);
                    TransactionResult::Accepted { hash }
                }
                Err(error) => TransactionResult::Rejected {
                    error: error.to_string(),
                },
            })
            .collect()
    };
    for tx in accepted {
        state.metrics.record_tx(&tx);
        state.broadcast(&NodeMessage::NewTx(tx));
    }
    Json(results)
}

/// `GET /metrics`: compared across nodes by the client after a benchmark.
pub(crate) async fn metrics(State(state): State<Arc<NodeState>>) -> Json<MetricsSnapshot> {
    let chain_len = state.chain.lock().await.len();
    let mempool_len = state.mempool.lock().await.len();
    Json(state.metrics.snapshot(chain_len, mempool_len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    #[tokio::test]
    async fn batch_accepts_valid_and_rejects_duplicate_and_tampered_transactions() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let validators = vec![key.verifying_key()];
        let tx = Transaction::new(&key.verifying_key(), 10, 0, 1, &key).unwrap();
        let hash = tx.calculate_hash().unwrap();
        let mut invalid = tx.clone();
        invalid.amount += 1;
        let state = Arc::new(NodeState::new(key, validators).unwrap());
        let mut outgoing = state.peers.subscribe();

        let Json(results) = submit_transactions(
            State(Arc::clone(&state)),
            Json(vec![tx.clone(), tx.clone(), invalid]),
        )
        .await;

        assert_eq!(results.len(), 3);
        assert!(
            matches!(&results[0], TransactionResult::Accepted { hash: accepted } if accepted == &hash)
        );
        assert!(matches!(&results[1], TransactionResult::Rejected { .. }));
        assert!(matches!(&results[2], TransactionResult::Rejected { .. }));
        assert_eq!(state.mempool.lock().await.len(), 1);
        assert_eq!(
            serde_json::to_value(&results[0]).unwrap(),
            serde_json::json!({"status": "accepted", "hash": hash}),
        );

        // Seule la TX acceptée est diffusée aux peers.
        let gossiped = outgoing.try_recv().unwrap();
        assert_eq!(
            gossiped,
            serde_json::to_string(&NodeMessage::NewTx(tx)).unwrap()
        );
        assert!(outgoing.try_recv().is_err());

        let Json(snapshot) = metrics(State(state)).await;
        assert_eq!(snapshot.txs_received, 1);
        assert_eq!((snapshot.chain_len, snapshot.mempool_len), (1, 1));
    }
}
