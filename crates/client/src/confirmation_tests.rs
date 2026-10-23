use super::*;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use blockchain::{chain::genesis_block, transaction::Transaction};
use ed25519_dalek::SigningKey;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::task::JoinSet;

enum Mode {
    ReadyAfter(usize),
    Unavailable,
    Hang,
}

struct MockState {
    blocks: Vec<Block>,
    mode: Mode,
    polls: AtomicUsize,
    block_reads: AtomicUsize,
}

struct MockNode {
    url: String,
    state: Arc<MockState>,
    _server: JoinSet<()>,
}

async fn start(blocks: Vec<Block>, mode: Mode) -> MockNode {
    let state = Arc::new(MockState {
        blocks,
        mode,
        polls: AtomicUsize::new(0),
        block_reads: AtomicUsize::new(0),
    });
    let app = Router::new()
        .route(
            "/metrics",
            get(|State(state): State<Arc<MockState>>| async move {
                let poll = state.polls.fetch_add(1, Ordering::Relaxed);
                let height = match state.mode {
                    Mode::ReadyAfter(delay) if poll >= delay => state.blocks.len(),
                    Mode::Unavailable => {
                        return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({})));
                    }
                    Mode::Hang => {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        0
                    }
                    _ => 0,
                };
                // Global counters deliberately do not describe this benchmark.
                (
                    StatusCode::OK,
                    Json(serde_json::json!({"height": height, "txs_committed": 10000})),
                )
            }),
        )
        .route(
            "/blocks/{index}",
            get(
                |State(state): State<Arc<MockState>>, Path(index): Path<usize>| async move {
                    state.block_reads.fetch_add(1, Ordering::Relaxed);
                    match index
                        .checked_sub(1)
                        .and_then(|index| state.blocks.get(index))
                    {
                        Some(block) => (StatusCode::OK, Json(serde_json::to_value(block).unwrap())),
                        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({}))),
                    }
                },
            ),
        )
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let mut server = JoinSet::new();
    server.spawn(async move { axum::serve(listener, app).await.unwrap() });
    MockNode {
        url,
        state,
        _server: server,
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
}

fn tx(nonce: u64) -> Transaction {
    let key = SigningKey::from_bytes(&[7; 32]);
    Transaction::new(&key.verifying_key(), 1, nonce, 1, &key).unwrap()
}

fn blocks(batches: Vec<Vec<Transaction>>) -> Vec<Block> {
    let key = SigningKey::from_bytes(&[1; 32]);
    let mut previous = genesis_block().block_data.hash;
    batches
        .into_iter()
        .enumerate()
        .map(|(index, txs)| {
            let block = Block::new(
                index as u64 + 1,
                previous.clone(),
                txs,
                index as u64 + 1,
                &key,
            )
            .unwrap();
            previous = block.block_data.hash.clone();
            block
        })
        .collect()
}

#[tokio::test]
async fn waits_for_all_three_nodes_and_reads_each_block_once() {
    let history = blocks(vec![vec![tx(0)], vec![tx(1), tx(2)]]);
    let mut nodes = Vec::new();
    for delay in [0, 1, 2] {
        nodes.push(start(history.clone(), Mode::ReadyAfter(delay)).await);
    }
    let urls = nodes
        .iter()
        .map(|node| format!("{}/", node.url))
        .collect::<Vec<_>>();
    let hashes = [
        tx(1).calculate_hash().unwrap(),
        tx(2).calculate_hash().unwrap(),
    ]
    .into_iter()
    .collect();
    let report = wait_for_confirmations(
        &client(),
        &urls,
        &hashes,
        Duration::from_secs(2),
        Duration::from_millis(5),
    )
    .await
    .unwrap();
    assert!(report.complete());
    assert_eq!(report.confirmed, 2);
    assert!(
        report
            .nodes
            .iter()
            .all(|(_, confirmed, error)| *confirmed == 2 && error.is_none())
    );
    for node in nodes {
        assert_eq!(node.state.block_reads.load(Ordering::Relaxed), 2);
    }
}

#[tokio::test]
async fn matching_inclusion_is_required_even_if_both_nodes_include_the_hash() {
    let history = blocks(vec![vec![tx(0)]]);
    let mut fork = history.clone();
    fork[0] = Block::new(
        1,
        genesis_block().block_data.hash,
        vec![tx(0)],
        99,
        &SigningKey::from_bytes(&[1; 32]),
    )
    .unwrap();
    let a = start(history, Mode::ReadyAfter(0)).await;
    let b = start(fork, Mode::ReadyAfter(0)).await;
    let hashes = [tx(0).calculate_hash().unwrap()].into_iter().collect();
    let result = wait_for_confirmations(
        &client(),
        &[a.url.clone(), b.url.clone()],
        &hashes,
        Duration::from_secs(2),
        Duration::from_millis(5),
    )
    .await;
    assert!(matches!(result, Err(ClientError::InvalidConfirmation(_))));
}

#[tokio::test]
async fn deadline_reports_partial_confirmation_and_unavailable_node() {
    let a = start(blocks(vec![vec![tx(0)]]), Mode::ReadyAfter(0)).await;
    let b = start(Vec::new(), Mode::Unavailable).await;
    let hashes = [tx(0).calculate_hash().unwrap()].into_iter().collect();
    let report = wait_for_confirmations(
        &client(),
        &[a.url.clone(), b.url.clone()],
        &hashes,
        Duration::from_millis(150),
        Duration::from_millis(5),
    )
    .await
    .unwrap();
    assert!(!report.complete());
    assert_eq!(report.confirmed, 0);
    assert_eq!(report.nodes[0].1, 1);
    assert_eq!(report.nodes[1].1, 0);
    assert!(report.nodes[1].2.is_some());
}

#[tokio::test]
async fn global_deadline_also_cancels_an_unresponsive_request() {
    let node = start(Vec::new(), Mode::Hang).await;
    let hashes = [tx(0).calculate_hash().unwrap()].into_iter().collect();
    let report = wait_for_confirmations(
        &client(),
        &[node.url.clone()],
        &hashes,
        Duration::from_millis(80),
        Duration::from_millis(5),
    )
    .await
    .unwrap();
    assert!(!report.complete());
    assert_eq!(report.confirmed, 0);
    assert!(report.elapsed < Duration::from_secs(1));
}

#[tokio::test]
async fn tampered_block_cannot_confirm_a_transaction() {
    let mut history = blocks(vec![vec![tx(0)]]);
    history[0].block_data.transactions[0].amount += 1;
    let node = start(history, Mode::ReadyAfter(0)).await;
    let hashes = [tx(0).calculate_hash().unwrap()].into_iter().collect();
    let result = wait_for_confirmations(
        &client(),
        &[node.url.clone()],
        &hashes,
        Duration::from_secs(2),
        Duration::from_millis(5),
    )
    .await;
    assert!(matches!(result, Err(ClientError::InvalidConfirmation(_))));
}

#[tokio::test]
async fn zero_accepted_transactions_do_not_poll_nodes() {
    let node = start(Vec::new(), Mode::Hang).await;
    let report = wait_for_confirmations(
        &client(),
        &[node.url.clone()],
        &HashSet::new(),
        Duration::from_secs(1),
        Duration::from_millis(5),
    )
    .await
    .unwrap();
    assert!(report.complete());
    assert_eq!(report.total, 0);
    assert_eq!(node.state.polls.load(Ordering::Relaxed), 0);
}
