use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use blockchain::{block::Block, transaction::Transaction};
use serde::{Deserialize, Serialize};

use crate::NodeState;
use crate::metrics::MetricsSnapshot;

#[derive(Serialize)]
pub(crate) struct ApiError {
    error: &'static str,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

fn api_error(status: StatusCode, error: &'static str) -> (StatusCode, Json<ApiError>) {
    (status, Json(ApiError { error }))
}

pub(crate) async fn get_block(
    State(state): State<Arc<NodeState>>,
    Path(index): Path<u64>,
) -> ApiResult<Block> {
    let chain = state.chain.lock().await;
    usize::try_from(index)
        .ok()
        .and_then(|index| chain.blocks().get(index))
        .cloned()
        .map(Json)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "block not found"))
}

/// Bloc sans ses TX : assez léger pour être interrogé en boucle par le dashboard.
#[derive(Serialize)]
pub(crate) struct BlockSummary {
    index: u64,
    hash: String,
    /// Clé publique hex du validateur ; `None` pour le genesis.
    validator: Option<String>,
    transactions: usize,
    timestamp: u64,
}

#[derive(Deserialize)]
pub(crate) struct RecentBlocksQuery {
    limit: Option<usize>,
}

const RECENT_BLOCKS_DEFAULT: usize = 20;
const RECENT_BLOCKS_MAX: usize = 100;

/// Derniers blocs, du plus récent au plus ancien.
pub(crate) async fn get_recent_blocks(
    State(state): State<Arc<NodeState>>,
    Query(query): Query<RecentBlocksQuery>,
) -> Json<Vec<BlockSummary>> {
    let limit = query
        .limit
        .unwrap_or(RECENT_BLOCKS_DEFAULT)
        .min(RECENT_BLOCKS_MAX);
    let chain = state.chain.lock().await;
    Json(
        chain
            .blocks()
            .iter()
            .rev()
            .take(limit)
            .map(|block| BlockSummary {
                index: block.block_data.index,
                hash: block.block_data.hash.clone(),
                validator: block.validator.map(|key| hex::encode(key.to_bytes())),
                transactions: block.block_data.transactions.len(),
                timestamp: block.block_data.timestamp,
            })
            .collect(),
    )
}

#[derive(Serialize)]
pub(crate) struct Peer {
    node_id: String,
    urls: Vec<String>,
}

/// Peers découverts par mDNS (sans ce node), pour que le dashboard les trouve seul.
pub(crate) async fn get_peers(State(state): State<Arc<NodeState>>) -> Json<Vec<Peer>> {
    Json(
        state
            .peers()
            .into_iter()
            .map(|(node_id, urls)| Peer { node_id, urls })
            .collect(),
    )
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum TransactionStatus {
    Pending {
        hash: String,
        transaction: Transaction,
    },
    Included {
        hash: String,
        block_index: u64,
        block_hash: String,
        transaction: Transaction,
    },
}

pub(crate) async fn get_transaction(
    State(state): State<Arc<NodeState>>,
    Path(hash): Path<String>,
) -> ApiResult<TransactionStatus> {
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "transaction hash must contain 64 hexadecimal characters",
        ));
    }
    let hash = hash.to_ascii_lowercase();
    // Keep both locks until lookup completes: inclusion cannot race this snapshot.
    let chain = state.chain.lock().await;
    let mempool = state.mempool.lock().await;
    for block in chain.blocks() {
        for tx in &block.block_data.transactions {
            let candidate = tx.calculate_hash().map_err(|_| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "could not hash stored transaction",
                )
            })?;
            if candidate == hash {
                return Ok(Json(TransactionStatus::Included {
                    hash,
                    block_index: block.block_data.index,
                    block_hash: block.block_data.hash.clone(),
                    transaction: tx.clone(),
                }));
            }
        }
    }
    match mempool.txs.get(&hash) {
        Some(tx) => Ok(Json(TransactionStatus::Pending {
            hash,
            transaction: tx.clone(),
        })),
        None => Err(api_error(StatusCode::NOT_FOUND, "transaction not found")),
    }
}

#[derive(Serialize)]
pub(crate) struct NodeMetrics {
    #[serde(flatten)]
    pub metrics: MetricsSnapshot,
    /// Genesis is at height 0; chain_len includes genesis.
    pub height: u64,
    pub last_block_hash: String,
    /// Clé publique hex de ce node : relie les blocs à leur producteur.
    pub public_key: String,
}

/// Snapshot under the same lock order as block production and reception.
pub(crate) async fn get_metrics(State(state): State<Arc<NodeState>>) -> Json<NodeMetrics> {
    let chain = state.chain.lock().await;
    let mempool = state.mempool.lock().await;
    Json(NodeMetrics {
        metrics: state.metrics.snapshot(chain.len(), mempool.len()),
        height: chain.last().block_data.index,
        last_block_hash: chain.last().block_data.hash.clone(),
        public_key: hex::encode(state.signing_key.verifying_key().to_bytes()),
    })
}

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
    let mut results = Vec::with_capacity(transactions.len());
    for tx in transactions {
        results.push(match state.submit_transaction(tx).await {
            Ok(hash) => TransactionResult::Accepted { hash },
            Err(error) => {
                state.metrics.record_rejected_tx();
                TransactionResult::Rejected {
                    error: error.to_string(),
                }
            }
        });
    }
    Json(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    #[tokio::test]
    async fn http_lookup_tracks_transaction_from_pending_to_its_block() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let tx = Transaction::new(&key.verifying_key(), 10, 0, 1, &key).unwrap();
        let state = Arc::new(NodeState::new(key.clone(), vec![key.verifying_key()]).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = crate::app(Arc::clone(&state));
        let mut servers = tokio::task::JoinSet::new();
        servers.spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        // Genesis is available even before the first ordinary block.
        let genesis: Block = client
            .get(format!("{base}/blocks/0"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(genesis.block_data.index, 0);
        assert!(genesis.block_data.transactions.is_empty());
        genesis.verify_hashes().unwrap();

        let submitted: serde_json::Value = client
            .post(format!("{base}/transactions"))
            .json(std::slice::from_ref(&tx))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let hash = submitted[0]["hash"].as_str().unwrap();
        let pending: serde_json::Value = client
            .get(format!("{base}/transactions/{hash}"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(pending["status"], "pending");
        assert_eq!(pending["hash"], hash);
        assert_eq!(pending["transaction"], serde_json::to_value(&tx).unwrap());
        assert!(pending.get("block_index").is_none());

        state.produce_next_block(2).await.unwrap();
        let included: serde_json::Value = client
            .get(format!("{base}/transactions/{hash}"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(included["status"], "included");
        assert_eq!(included["hash"], hash);
        assert_eq!(included["block_index"], 1);
        assert_eq!(included["transaction"], pending["transaction"]);
        let block: Block = client
            .get(format!("{base}/blocks/1"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(included["block_hash"], block.block_data.hash);
        assert_eq!(block.block_data.transactions.len(), 1);
        assert_eq!(
            block.block_data.transactions[0].calculate_hash().unwrap(),
            hash
        );
        block.verify_hashes().unwrap();
        block.verify_signature().unwrap();

        // Hex lookup is case-insensitive and returns the canonical lowercase hash.
        let uppercase: serde_json::Value = client
            .get(format!("{base}/transactions/{}", hash.to_uppercase()))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(uppercase, included);
        for path in [
            "blocks/2".to_string(),
            format!("blocks/{}", u64::MAX),
            format!("transactions/{}", "0".repeat(64)),
        ] {
            let response = client.get(format!("{base}/{path}")).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            let body: serde_json::Value = response.json().await.unwrap();
            assert!(body["error"].is_string());
        }
        for path in [
            "blocks/invalid".to_string(),
            "blocks/-1".to_string(),
            "transactions/short".to_string(),
            format!("transactions/{}", "z".repeat(64)),
        ] {
            assert_eq!(
                client
                    .get(format!("{base}/{path}"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::BAD_REQUEST
            );
        }
    }

    #[tokio::test]
    async fn http_metrics_follow_admission_and_commit_without_counting_duplicates() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let tx = Transaction::new(&key.verifying_key(), 10, 0, 1, &key).unwrap();
        let state = Arc::new(NodeState::new(key.clone(), vec![key.verifying_key()]).unwrap());
        let genesis_hash = state.chain.lock().await.last().block_data.hash.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = crate::app(Arc::clone(&state));
        let mut servers = tokio::task::JoinSet::new();
        servers.spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let initial: serde_json::Value = client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(initial["txs_received"], 0);
        assert_eq!(initial["txs_committed"], 0);
        assert_eq!(initial["txs_rejected"], 0);
        assert_eq!(initial["mempool_len"], 0);
        assert_eq!(initial["chain_len"], 1);
        assert_eq!(initial["height"], 0);
        assert_eq!(initial["last_block_hash"], genesis_hash);
        assert!(initial["tps"].is_null());
        assert!(initial["avg_propagation_ms"].is_null());
        assert!(initial["avg_finality_ms"].is_null());

        let mut forged = tx.clone();
        forged.amount += 1;
        let results: serde_json::Value = client
            .post(format!("{base}/transactions"))
            .json(&[tx.clone(), tx, forged])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(results[0]["status"], "accepted");
        assert_eq!(results[1]["status"], "rejected");
        assert_eq!(results[2]["status"], "rejected");

        let pending: serde_json::Value = client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(pending["txs_received"], 1);
        assert_eq!(pending["txs_committed"], 0);
        assert_eq!(pending["txs_rejected"], 2);
        assert_eq!(pending["sync_resets"], 0);
        assert_eq!(pending["mempool_len"], 1);
        // Une seule TX : pas de durée, donc pas de débit mesurable.
        assert!(pending["tps"].is_null());
        assert!(pending["avg_propagation_ms"].is_u64());

        state.produce_next_block(2).await.unwrap();
        let expected_hash = state.chain.lock().await.last().block_data.hash.clone();
        for _ in 0..2 {
            let committed: serde_json::Value = client
                .get(format!("{base}/metrics"))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(committed["txs_received"], 1);
            assert_eq!(committed["txs_committed"], 1);
            assert_eq!(committed["mempool_len"], 0);
            assert_eq!(committed["chain_len"], 2);
            assert_eq!(committed["height"], 1);
            assert_eq!(committed["last_block_hash"], expected_hash);
            assert!(committed["avg_finality_ms"].is_u64());
        }
    }

    #[tokio::test]
    async fn dashboard_routes_serve_page_block_summaries_peers_and_cors() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let tx = Transaction::new(&key.verifying_key(), 10, 0, 1, &key).unwrap();
        let state = Arc::new(NodeState::new(key.clone(), vec![key.verifying_key()]).unwrap());
        state.submit_transaction(tx).await.unwrap();
        state.produce_next_block(2).await.unwrap();
        state.produce_next_block(3).await.unwrap();
        state.record_peer("b".into(), vec!["http://10.0.0.2:3000".into()]);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = crate::app(Arc::clone(&state));
        let mut servers = tokio::task::JoinSet::new();
        servers.spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let get = |path: &str| client.get(format!("{base}{path}")).send();

        let page = get("/").await.unwrap().error_for_status().unwrap();
        assert!(
            page.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/html")
        );
        assert!(
            page.text()
                .await
                .unwrap()
                .contains("<title>Authora Dashboard</title>")
        );

        // Newest first, without the transactions themselves.
        let blocks: serde_json::Value = get("/blocks?limit=2").await.unwrap().json().await.unwrap();
        let public_key = hex::encode(key.verifying_key().to_bytes());
        assert_eq!(blocks.as_array().unwrap().len(), 2);
        assert_eq!(blocks[0]["index"], 2);
        assert_eq!(blocks[0]["transactions"], 0);
        assert_eq!(blocks[1]["index"], 1);
        assert_eq!(blocks[1]["transactions"], 1);
        assert_eq!(blocks[1]["validator"], public_key);
        assert!(blocks[1].get("block_data").is_none());
        let all: serde_json::Value = get("/blocks").await.unwrap().json().await.unwrap();
        assert_eq!(all.as_array().unwrap().len(), 3);
        assert!(all[2]["validator"].is_null());

        let peers: serde_json::Value = get("/peers").await.unwrap().json().await.unwrap();
        assert_eq!(
            peers,
            serde_json::json!([{"node_id": "b", "urls": ["http://10.0.0.2:3000"]}])
        );

        // A dashboard served by another node may read the metrics cross-origin.
        let metrics = client
            .get(format!("{base}/metrics"))
            .header("origin", "http://10.0.0.2:3000")
            .send()
            .await
            .unwrap();
        assert_eq!(metrics.headers()["access-control-allow-origin"], "*");
        let metrics: serde_json::Value = metrics.json().await.unwrap();
        assert_eq!(metrics["public_key"], public_key);
    }

    #[tokio::test]
    async fn batch_accepts_valid_and_rejects_duplicate_and_tampered_transactions() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let validators = vec![key.verifying_key()];
        let tx = Transaction::new(&key.verifying_key(), 10, 0, 1, &key).unwrap();
        let hash = tx.calculate_hash().unwrap();
        let mut invalid = tx.clone();
        invalid.amount += 1;
        let state = Arc::new(NodeState::new(key, validators).unwrap());

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
        assert_eq!(state.metrics.snapshot(1, 1).txs_rejected, 2);
        assert_eq!(
            serde_json::to_value(&results[0]).unwrap(),
            serde_json::json!({"status": "accepted", "hash": hash}),
        );
    }
}
