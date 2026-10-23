use std::sync::Arc;

use axum::{Json, extract::State};
use blockchain::transaction::Transaction;
use serde::Serialize;

use crate::NodeState;

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum TransactionResult {
    Accepted { hash: String },
    Rejected { error: String },
}

/// Results follow the same order as the submitted transactions.
pub(crate) async fn submit_transactions(
    State(state): State<Arc<NodeState>>,
    Json(transactions): Json<Vec<Transaction>>,
) -> Json<Vec<TransactionResult>> {
    let mut mempool = state.mempool.lock().await;
    Json(
        transactions
            .into_iter()
            .map(|tx| match mempool.add(tx) {
                Ok(hash) => TransactionResult::Accepted { hash },
                Err(error) => TransactionResult::Rejected {
                    error: error.to_string(),
                },
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockchain::{chain::Chain, mempool::Mempool};
    use ed25519_dalek::SigningKey;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn batch_accepts_valid_and_rejects_duplicate_and_tampered_transactions() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let validators = vec![key.verifying_key()];
        let tx = Transaction::new(&key.verifying_key(), 10, 0, 1, &key).unwrap();
        let hash = tx.calculate_hash().unwrap();
        let mut invalid = tx.clone();
        invalid.amount += 1;
        let state = Arc::new(NodeState {
            chain: Mutex::new(Chain::new(validators.clone()).unwrap()),
            mempool: Mutex::new(Mempool::new()),
            signing_key: key,
            authorized_validators: validators,
        });

        let Json(results) = submit_transactions(
            State(Arc::clone(&state)),
            Json(vec![tx.clone(), tx, invalid]),
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
    }
}
