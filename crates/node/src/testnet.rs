//! In-process network for integration tests and benchmarks (feature `testnet`).
//!
//! Every node runs in its own Tokio task on a loopback port: HTTP API and
//! WebSocket server, block production and outbound peer connections. A load
//! generator ([`send_load`]) plays the client and goes through the public HTTP API.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use blockchain::{block::Block, transaction::Transaction};
use ed25519_dalek::{SigningKey, VerifyingKey};
use futures_util::{SinkExt, StreamExt};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_tungstenite::tungstenite::Message;

use crate::{NodeMessage, NodeState, app, discovery, validator::Validator};

/// Delay before an outbound peer connection is retried, e.g. after a sync reset.
const RECONNECT_DELAY: Duration = Duration::from_millis(100);
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// A running network. Dropping it stops every node task.
pub struct Testnet {
    nodes: Vec<TestNode>,
    validators: Vec<VerifyingKey>,
    tasks: JoinSet<()>,
}

struct TestNode {
    address: SocketAddr,
    state: Arc<NodeState>,
}

/// One node's view, read under the same lock order as block production.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeStats {
    pub chain_len: u64,
    /// TXs included in the local chain.
    pub committed_txs: u64,
    pub mempool_len: usize,
    pub txs_received: u64,
    pub txs_rejected: u64,
    pub sync_resets: u64,
    pub avg_propagation_ms: Option<u64>,
    pub avg_finality_ms: Option<u64>,
}

impl Testnet {
    /// Starts `size` validators (keys seeded `1..=size`), fully meshed:
    /// node `i` dials every node `j > i`, like mDNS discovery does.
    pub async fn spawn(size: u8, block_time: Duration) -> anyhow::Result<Self> {
        anyhow::ensure!(size > 0, "a testnet needs at least one validator");
        anyhow::ensure!(!block_time.is_zero(), "block time must be positive");
        let keys: Vec<_> = (1..=size).map(validator_key).collect();
        let validators: Vec<_> = keys.iter().map(SigningKey::verifying_key).collect();
        let mut network = Self {
            nodes: Vec::new(),
            validators,
            tasks: JoinSet::new(),
        };
        let mut listeners = Vec::new();
        for key in keys {
            let (listener, node) = network.bind(key).await?;
            network.nodes.push(node);
            listeners.push(listener);
        }
        for (index, listener) in listeners.into_iter().enumerate() {
            let peers = network.nodes[index + 1..]
                .iter()
                .map(|node| ws_url(node.address))
                .collect();
            let state = Arc::clone(&network.nodes[index].state);
            network
                .tasks
                .spawn(run_node(listener, state, peers, Some(block_time)));
        }
        Ok(network)
    }

    /// Adds a node that only replays and relays (no block production),
    /// connected to `peer`. Returns its index.
    pub async fn spawn_follower(&mut self, peer: usize) -> anyhow::Result<usize> {
        let (listener, node) = self.bind(validator_key(1)).await?;
        let state = Arc::clone(&node.state);
        let peers = vec![ws_url(self.nodes[peer].address)];
        self.tasks.spawn(run_node(listener, state, peers, None));
        self.nodes.push(node);
        Ok(self.nodes.len() - 1)
    }

    async fn bind(&self, key: SigningKey) -> anyhow::Result<(TcpListener, TestNode)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let node = TestNode {
            address: listener.local_addr()?,
            state: Arc::new(NodeState::new(key, self.validators.clone())?),
        };
        Ok((listener, node))
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Base URL of a node's HTTP API, e.g. `http://127.0.0.1:41234`.
    pub fn http_url(&self, node: usize) -> String {
        format!("http://{}", self.nodes[node].address)
    }

    pub async fn stats(&self, node: usize) -> NodeStats {
        let state = &self.nodes[node].state;
        let chain = state.chain.lock().await;
        let mempool = state.mempool.lock().await;
        let metrics = state.metrics.snapshot(chain.len(), mempool.len());
        NodeStats {
            chain_len: chain.len(),
            committed_txs: chain
                .blocks()
                .iter()
                .map(|block| block.block_data.transactions.len() as u64)
                .sum(),
            mempool_len: mempool.len(),
            txs_received: metrics.txs_received,
            txs_rejected: metrics.txs_rejected,
            sync_resets: metrics.sync_resets,
            avg_propagation_ms: metrics.avg_propagation_ms,
            avg_finality_ms: metrics.avg_finality_ms,
        }
    }

    /// Hash of block `index`, if the node has it.
    async fn block_hash(&self, node: usize, index: u64) -> Option<String> {
        let chain = self.nodes[node].state.chain.lock().await;
        let block = chain.blocks().get(usize::try_from(index).ok()?)?;
        Some(block.block_data.hash.clone())
    }

    /// Waits until every node has exactly `committed_txs` TXs in its chain, an
    /// empty mempool and the same history up to the shortest chain.
    /// Returns how long it took.
    pub async fn wait_until_synced(
        &self,
        committed_txs: u64,
        timeout: Duration,
    ) -> anyhow::Result<Duration> {
        wait_for(timeout, "network sync", async || {
            let mut min_len = u64::MAX;
            for node in 0..self.len() {
                let stats = self.stats(node).await;
                if stats.committed_txs != committed_txs || stats.mempool_len != 0 {
                    return false;
                }
                min_len = min_len.min(stats.chain_len);
            }
            let reference = self.block_hash(0, min_len - 1).await;
            for node in 1..self.len() {
                if self.block_hash(node, min_len - 1).await != reference {
                    return false;
                }
            }
            true
        })
        .await
    }

    /// Waits until `node` has at least `chain_len` blocks.
    pub async fn wait_for_height(
        &self,
        node: usize,
        chain_len: u64,
        timeout: Duration,
    ) -> anyhow::Result<Duration> {
        wait_for(timeout, "chain height", async || {
            self.stats(node).await.chain_len >= chain_len
        })
        .await
    }

    /// Acts as a rogue peer: sends `node` a block signed by an unauthorized
    /// key on top of its tip, then waits for the node to hang up (sync reset).
    /// Returns the time between sending the block and the disconnect.
    pub async fn send_bad_block(&self, node: usize) -> anyhow::Result<Duration> {
        let (index, prev_hash, timestamp) = {
            let chain = self.nodes[node].state.chain.lock().await;
            let tip = &chain.last().block_data;
            (tip.index + 1, tip.hash.clone(), tip.timestamp)
        };
        let rogue = SigningKey::from_bytes(&[0xEE; 32]);
        let block = Block::new(index, prev_hash, Vec::new(), timestamp, &rogue)?;
        let message = serde_json::to_string(&NodeMessage::NewBlock(Box::new(block)))?;
        let (mut socket, _) =
            tokio_tungstenite::connect_async(ws_url(self.nodes[node].address)).await?;
        let started = Instant::now();
        socket.send(Message::Text(message.into())).await?;
        // The node replays its history to us, then must drop the connection.
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(Ok(message)) = socket.next().await {
                if message.is_close() {
                    break;
                }
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("node kept the connection after a bad block"))?;
        Ok(started.elapsed())
    }
}

async fn run_node(
    listener: TcpListener,
    state: Arc<NodeState>,
    peers: Vec<String>,
    block_time: Option<Duration>,
) {
    // Dropped (and aborted) together with the node task.
    let mut tasks = JoinSet::new();
    for url in peers {
        tasks.spawn(discovery::connect_peer(
            vec![url],
            Arc::clone(&state),
            RECONNECT_DELAY,
        ));
    }
    if let Some(block_time) = block_time {
        tasks.spawn(
            Validator {
                state: Arc::clone(&state),
                block_time,
            }
            .run(),
        );
    }
    let app = app(state).into_make_service_with_connect_info::<SocketAddr>();
    tasks.spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::error!(%error, "testnet node server stopped");
        }
    });
    while tasks.join_next().await.is_some() {}
}

async fn wait_for(
    timeout: Duration,
    what: &str,
    mut ready: impl AsyncFnMut() -> bool,
) -> anyhow::Result<Duration> {
    let started = Instant::now();
    tokio::time::timeout(timeout, async {
        while !ready().await {
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out after {timeout:?} waiting for {what}"))?;
    Ok(started.elapsed())
}

fn ws_url(address: SocketAddr) -> String {
    format!("ws://{address}/ws")
}

fn validator_key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

/// Deterministic, distinct sender key for load generator `id`.
pub fn sender_key(id: u64) -> SigningKey {
    let mut secret = [0xAB; 32];
    secret[..8].copy_from_slice(&id.to_le_bytes());
    SigningKey::from_bytes(&secret)
}

/// What the load generator sends: `count` valid TXs with nonces `0..count`,
/// in requests of `batch_size`. With `forged_every = Some(k)`, every k-th TX
/// is followed by a tampered copy that the node must reject.
pub struct Load {
    pub sender: SigningKey,
    pub count: u64,
    pub batch_size: u64,
    pub forged_every: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct LoadReport {
    pub submitted: u64,
    pub accepted: u64,
    pub rejected: u64,
    /// TXs lost with a failed request (connection error, timeout, HTTP error).
    pub failed: u64,
    pub elapsed: Duration,
}

impl LoadReport {
    /// Share of submitted TXs that were not accepted.
    pub fn error_rate(&self) -> f64 {
        if self.submitted == 0 {
            return 0.0;
        }
        (self.rejected + self.failed) as f64 / self.submitted as f64
    }
}

/// Sends `load` to `POST {url}/transactions` and counts the per-TX results.
pub async fn send_load(url: &str, load: Load) -> LoadReport {
    let started = Instant::now();
    let mut report = LoadReport::default();
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("static reqwest configuration");
    let recipient = validator_key(200).verifying_key();
    let batch_size = load.batch_size.max(1);
    let mut nonce = 0;
    while nonce < load.count {
        let end = (nonce + batch_size).min(load.count);
        let mut batch = Vec::new();
        for nonce in nonce..end {
            // Timestamp set at send time: nodes measure propagation and finality from it.
            let tx = Transaction::new(&recipient, 1, nonce, unix_millis(), &load.sender)
                .expect("signing a transaction cannot fail");
            if load.forged_every.is_some_and(|k| k > 0 && (nonce + 1) % k == 0) {
                let mut forged = tx.clone();
                forged.amount += 1;
                batch.push(tx);
                batch.push(forged);
            } else {
                batch.push(tx);
            }
        }
        nonce = end;
        report.submitted += batch.len() as u64;
        let response = client
            .post(format!("{url}/transactions"))
            .json(&batch)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status);
        let results = match response {
            Ok(response) => response.json::<Vec<serde_json::Value>>().await.ok(),
            Err(_) => None,
        };
        match results {
            Some(results) if results.len() == batch.len() => {
                for result in results {
                    if result["status"] == "accepted" {
                        report.accepted += 1;
                    } else {
                        report.rejected += 1;
                    }
                }
            }
            _ => report.failed += batch.len() as u64,
        }
    }
    report.elapsed = started.elapsed();
    report
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BLOCK_TIME: Duration = Duration::from_millis(50);
    const TIMEOUT: Duration = Duration::from_secs(10);

    fn load(id: u64, count: u64) -> Load {
        Load {
            sender: sender_key(id),
            count,
            batch_size: 50,
            forged_every: None,
        }
    }

    #[test]
    fn error_rate_counts_rejected_and_failed_transactions() {
        assert_eq!(LoadReport::default().error_rate(), 0.0);
        let report = LoadReport {
            submitted: 10,
            accepted: 7,
            rejected: 2,
            failed: 1,
            elapsed: Duration::ZERO,
        };
        assert!((report.error_rate() - 0.3).abs() < f64::EPSILON);
    }

    #[test]
    fn sender_keys_are_distinct_and_stable() {
        assert_eq!(sender_key(1).to_bytes(), sender_key(1).to_bytes());
        assert_ne!(sender_key(1).to_bytes(), sender_key(2).to_bytes());
    }

    #[tokio::test]
    async fn unreachable_node_counts_every_transaction_as_failed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let report = send_load(&url, load(1, 5)).await;
        assert_eq!(report.submitted, 5);
        assert_eq!(report.failed, 5);
        assert_eq!(report.error_rate(), 1.0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn three_node_tasks_sync_load_sent_by_a_fourth_task() {
        let network = Testnet::spawn(3, BLOCK_TIME).await.unwrap();
        let url = network.http_url(0);
        let report = tokio::spawn(async move { send_load(&url, load(1, 200)).await })
            .await
            .unwrap();
        assert_eq!(report.submitted, 200);
        assert_eq!(report.accepted, 200);
        assert_eq!(report.error_rate(), 0.0);

        network.wait_until_synced(200, TIMEOUT).await.unwrap();
        for node in 0..network.len() {
            let stats = network.stats(node).await;
            assert_eq!(stats.committed_txs, 200);
            assert_eq!(stats.txs_received, 200, "node {node} must get every TX");
            assert_eq!(stats.txs_rejected, 0);
            assert_eq!(stats.sync_resets, 0);
            assert!(stats.avg_propagation_ms.is_some());
            assert!(stats.avg_finality_ms.is_some());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn forged_transactions_raise_error_rate_without_blocking_sync() {
        let network = Testnet::spawn(3, BLOCK_TIME).await.unwrap();
        let report = send_load(
            &network.http_url(0),
            Load {
                forged_every: Some(10),
                ..load(2, 100)
            },
        )
        .await;
        assert_eq!(report.submitted, 110);
        assert_eq!(report.accepted, 100);
        assert_eq!(report.rejected, 10);
        assert_eq!(report.failed, 0);
        assert!((report.error_rate() - 10.0 / 110.0).abs() < 1e-9);

        network.wait_until_synced(100, TIMEOUT).await.unwrap();
        assert_eq!(network.stats(0).await.txs_rejected, 10);
        // Forged TXs never leave the node that refused them.
        assert_eq!(network.stats(1).await.txs_received, 100);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bad_blocks_reset_the_peer_and_network_keeps_syncing() {
        let network = Testnet::spawn(3, BLOCK_TIME).await.unwrap();
        for _ in 0..2 {
            network.send_bad_block(1).await.unwrap();
        }
        assert_eq!(network.stats(1).await.sync_resets, 2);
        assert_eq!(network.stats(0).await.sync_resets, 0);
        assert_eq!(network.stats(2).await.sync_resets, 0);

        let report = send_load(&network.http_url(1), load(3, 20)).await;
        assert_eq!(report.accepted, 20);
        network.wait_until_synced(20, TIMEOUT).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn follower_replays_existing_chain_from_one_peer() {
        let mut network = Testnet::spawn(3, BLOCK_TIME).await.unwrap();
        send_load(&network.http_url(2), load(4, 50)).await;
        network.wait_until_synced(50, TIMEOUT).await.unwrap();
        let height = network.stats(2).await.chain_len;

        let follower = network.spawn_follower(2).await.unwrap();
        network
            .wait_for_height(follower, height, TIMEOUT)
            .await
            .unwrap();
        assert_eq!(network.stats(follower).await.committed_txs, 50);
        network.wait_until_synced(50, TIMEOUT).await.unwrap();
        assert_eq!(network.stats(follower).await.sync_resets, 0);
    }
}
