use std::{fmt::Display, sync::Arc, time::Duration};

use axum::extract::ws::Message as AxumMessage;
use blockchain::{mempool::MempoolError, transaction::Transaction};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::sync::broadcast::error::RecvError;
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;

use crate::{NodeMessage, NodeState};

/// Pending TXs per message when replaying the mempool to a new peer.
/// About 210 bytes of JSON per TX: 100 TX ≈ 21 KB, which a slow mobile uplink
/// still sends within `SEND_TIMEOUT` (1000 TX ≈ 210 KB did not).
const REPLAY_BATCH: usize = 100;

/// A send slower than this drops the connection. Dead links are already caught
/// by `IDLE_TIMEOUT`; a short value here only made slow uplinks reconnect and
/// replay the whole history again, adding even more upload.
const SEND_TIMEOUT: Duration = Duration::from_secs(15);

/// A connection can die silently (NAT or VPN path change): the side with nothing
/// to send would then wait forever and never reconnect. Each side pings when idle
/// and drops a peer that stays silent for `IDLE_TIMEOUT`. The margin covers the
/// time spent validating a large block, during which this task sends nothing.
const PING_INTERVAL: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(2)
};
const IDLE_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_millis(500)
} else {
    Duration::from_secs(15)
};

/// Both inbound (Axum) and outbound (Tungstenite) sockets use the same protocol.
pub(crate) trait PeerMessage {
    fn text(value: String) -> Self;
    fn as_text(&self) -> Option<&str>;
    fn is_close(&self) -> bool;
}

macro_rules! peer_message {
    ($message:ty) => {
        impl PeerMessage for $message {
            fn text(value: String) -> Self {
                Self::Text(value.into())
            }

            fn as_text(&self) -> Option<&str> {
                match self {
                    Self::Text(text) => Some(text.as_str()),
                    _ => None,
                }
            }

            fn is_close(&self) -> bool {
                matches!(self, Self::Close(_))
            }
        }
    };
}

peer_message!(AxumMessage);
peer_message!(TungsteniteMessage);

/// Why a peer connection ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PeerExit {
    Disconnected,
    /// The address led back to this node (e.g. an address several machines share).
    Ourselves,
}

pub(crate) async fn handle_peer<S, M, E>(socket: S, state: Arc<NodeState>)
where
    S: Stream<Item = Result<M, E>> + Sink<M, Error = E> + Unpin,
    M: PeerMessage,
    E: Display,
{
    run_peer(socket, state).await;
}

pub(crate) async fn run_peer<S, M, E>(socket: S, state: Arc<NodeState>) -> PeerExit
where
    S: Stream<Item = Result<M, E>> + Sink<M, Error = E> + Unpin,
    M: PeerMessage,
    E: Display,
{
    let me = hex::encode(state.signing_key.verifying_key().to_bytes());
    // Subscribe before taking the snapshot, so concurrent admissions are not missed.
    let mut outbound = state.gossip.subscribe();
    // Use the same lock order as block production. Replay history before pending TXs.
    let (blocks, pending): (Vec<_>, Vec<_>) = {
        let chain = state.chain.lock().await;
        let mempool = state.mempool.lock().await;
        (
            chain.blocks().iter().skip(1).cloned().collect(),
            mempool.transactions().cloned().collect(),
        )
    };
    tracing::info!(
        blocks = blocks.len(),
        pending_txs = pending.len(),
        "replaying history to peer"
    );
    let (mut writer, mut reader) = socket.split();
    // Until the peer's `HistoryEnd`, its chain may be ahead of ours: no block production.
    let mut replay = state.sync.begin_replay();
    // Read while sending the snapshot, including when both peers replay at once.
    let send = async {
        send_text(
            &mut writer,
            serde_json::to_string(&NodeMessage::Hello(me.clone()))?,
        )
        .await?;
        for block in blocks {
            let text = serde_json::to_string(&NodeMessage::NewBlock(Box::new(block)))?;
            send_text(&mut writer, text).await?;
        }
        send_text(
            &mut writer,
            serde_json::to_string(&NodeMessage::HistoryEnd)?,
        )
        .await?;
        for txs in pending.chunks(REPLAY_BATCH) {
            let text = serde_json::to_string(&NodeMessage::NewTxs(txs.to_vec()))?;
            send_text(&mut writer, text).await?;
        }
        tracing::debug!("history sent to peer");
        let ping = serde_json::to_string(&NodeMessage::Ping)?;
        let mut heartbeat = tokio::time::interval(PING_INTERVAL);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                message = outbound.recv() => match message {
                    Ok(text) => send_text(&mut writer, text).await?,
                    Err(RecvError::Lagged(skipped)) => {
                        // Skipped messages may include a block: without it the peer
                        // stalls the rotation. Reconnecting replays the whole history.
                        state.metrics.record_sync_reset();
                        break Err(anyhow::anyhow!(
                            "peer gossip lagged by {skipped} messages; reconnect to replay history"
                        ));
                    }
                    Err(RecvError::Closed) => break Ok::<(), anyhow::Error>(()),
                },
                _ = heartbeat.tick() => send_text(&mut writer, ping.clone()).await?,
            }
        }
    };
    let receive = async {
        loop {
            let message = match tokio::time::timeout(IDLE_TIMEOUT, reader.next()).await {
                Err(_) => {
                    tracing::warn!(
                        "peer silent for {}s; disconnecting",
                        IDLE_TIMEOUT.as_secs_f32()
                    );
                    break PeerExit::Disconnected;
                }
                Ok(None) => break PeerExit::Disconnected,
                Ok(Some(Ok(message))) => message,
                Ok(Some(Err(error))) => {
                    tracing::debug!(%error, "peer read failed");
                    break PeerExit::Disconnected;
                }
            };
            if message.is_close() {
                break PeerExit::Disconnected;
            }
            let Some(text) = message.as_text() else {
                continue;
            };
            match serde_json::from_str::<NodeMessage>(text) {
                Ok(NodeMessage::Hello(key)) if key == me => break PeerExit::Ourselves,
                Ok(NodeMessage::Hello(_)) => {}
                Ok(NodeMessage::NewTx(tx)) => submit_peer_transactions(&state, vec![tx]).await,
                Ok(NodeMessage::NewTxs(txs)) => submit_peer_transactions(&state, txs).await,
                Ok(NodeMessage::Ping) => {}
                Ok(NodeMessage::HistoryEnd) => {
                    tracing::info!("peer history received");
                    replay.finish();
                }
                Ok(NodeMessage::NewBlock(block)) => {
                    if let Err(error) = state.accept_peer_block(*block).await {
                        state.metrics.record_sync_reset();
                        tracing::warn!(%error, "peer block rejected; reconnect to replay history");
                        break PeerExit::Disconnected;
                    }
                }
                Err(error) => tracing::warn!(%error, "invalid peer message"),
            }
        }
    };
    tokio::select! {
        result = send => {
            if let Err(error) = result {
                tracing::warn!(%error, "peer send failed; disconnecting");
            }
            PeerExit::Disconnected
        }
        exit = receive => exit,
    }
}

/// Duplicates are normal (every peer relays); anything else is logged.
async fn submit_peer_transactions(state: &NodeState, txs: Vec<Transaction>) {
    for result in state.submit_transactions(txs).await {
        match result {
            Ok(_) | Err(MempoolError::AlreadySeen(_)) => {}
            Err(error) => tracing::warn!(%error, "invalid peer transaction"),
        }
    }
}

async fn send_text<S, M, E>(writer: &mut S, text: String) -> anyhow::Result<()>
where
    S: Sink<M, Error = E> + Unpin,
    M: PeerMessage,
    E: Display,
{
    // The size tells a stuck link (any size) from a path MTU black hole
    // (small pings pass, large blocks or TX batches time out).
    let bytes = text.len();
    let started = std::time::Instant::now();
    tokio::time::timeout(SEND_TIMEOUT, writer.send(M::text(text)))
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "sending {bytes} bytes timed out after {}s",
                SEND_TIMEOUT.as_secs()
            )
        })?
        .map_err(|error| anyhow::anyhow!("sending {bytes} bytes failed: {error}"))?;
    let elapsed = started.elapsed();
    if elapsed > Duration::from_secs(1) {
        tracing::warn!(
            bytes,
            elapsed_ms = elapsed.as_millis() as u64,
            "slow peer send"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockchain::transaction::Transaction;
    use ed25519_dalek::SigningKey;
    use tokio::{sync::broadcast::error::TryRecvError, task::JoinSet};

    fn state(seed: u8) -> Arc<NodeState> {
        let key = SigningKey::from_bytes(&[seed; 32]);
        let validators = vec![key.verifying_key()];
        Arc::new(NodeState::new(key, validators).unwrap())
    }

    fn transaction(nonce: u64) -> Transaction {
        let key = SigningKey::from_bytes(&[9; 32]);
        Transaction::new(&key.verifying_key(), 1, nonce, 1, &key).unwrap()
    }

    #[tokio::test]
    async fn only_new_valid_transactions_are_broadcast() {
        let state = state(1);
        let mut events = state.gossip.subscribe();
        let tx = transaction(0);
        let hash = state.submit_transaction(tx.clone()).await.unwrap();
        let NodeMessage::NewTxs(received) =
            serde_json::from_str(&events.try_recv().unwrap()).unwrap()
        else {
            panic!("expected a transaction batch");
        };
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].calculate_hash().unwrap(), hash);
        assert!(matches!(
            state.submit_transaction(tx.clone()).await,
            Err(MempoolError::AlreadySeen(_))
        ));
        let mut forged = tx;
        forged.amount += 1;
        assert!(state.submit_transaction(forged).await.is_err());
        assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));
        assert_eq!(state.mempool.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn a_batch_is_relayed_as_one_message_of_its_new_valid_transactions() {
        let state = state(1);
        state.submit_transaction(transaction(0)).await.unwrap();
        let mut events = state.gossip.subscribe();
        let mut forged = transaction(2);
        forged.amount += 1;
        let batch = vec![
            transaction(0),
            transaction(1),
            forged,
            transaction(1),
            transaction(3),
        ];
        let results = state.submit_transactions(batch).await;
        assert!(matches!(results[0], Err(MempoolError::AlreadySeen(_))));
        assert!(results[1].is_ok());
        assert!(matches!(
            results[2],
            Err(MempoolError::InvalidTransaction(_))
        ));
        assert!(matches!(results[3], Err(MempoolError::AlreadySeen(_))));
        assert!(results[4].is_ok());

        let NodeMessage::NewTxs(relayed) =
            serde_json::from_str(&events.try_recv().unwrap()).unwrap()
        else {
            panic!("expected a transaction batch");
        };
        let nonces: Vec<_> = relayed.iter().map(|tx| tx.nonce).collect();
        assert_eq!(nonces, [1, 3]);
        assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));
        assert_eq!(state.mempool.lock().await.len(), 3);
    }

    async fn wait_for_pool(states: &[Arc<NodeState>], expected: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut ready = true;
                for state in states {
                    ready &= state.mempool.lock().await.len() == expected;
                }
                if ready {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("transactions should propagate to every node");
    }

    #[tokio::test]
    async fn three_nodes_replay_pending_and_relay_http_transactions_in_both_directions() {
        let states = [state(1), state(2), state(3)];
        let mut tasks = JoinSet::new(); // Aborts servers and connections when dropped.
        let mut addresses = Vec::new();
        for state in &states {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.push(listener.local_addr().unwrap());
            let app = crate::app(Arc::clone(state));
            tasks.spawn(async move {
                axum::serve(
                    listener,
                    app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                )
                .await
                .unwrap();
            });
        }

        // Submit before any peer is connected: replay must recover it.
        let first = transaction(0);
        let first_hash = states[0].submit_transaction(first).await.unwrap();
        // A -- B -- C: there is deliberately no direct A/C connection.
        for index in 0..2 {
            let (socket, _) =
                tokio_tungstenite::connect_async(format!("ws://{}/ws", addresses[index + 1]))
                    .await
                    .unwrap();
            tasks.spawn(handle_peer(socket, Arc::clone(&states[index])));
        }
        wait_for_pool(&states, 1).await;

        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let transactions: Vec<_> = (1..=10).map(transaction).collect();
        let results: serde_json::Value = client
            .post(format!("http://{}/transactions", addresses[0]))
            .json(&transactions)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(results.as_array().unwrap().len(), 10);
        assert!(
            results
                .as_array()
                .unwrap()
                .iter()
                .all(|r| r["status"] == "accepted")
        );
        wait_for_pool(&states, 11).await;

        // A TX injected into C must also travel back through outbound connections.
        let reverse = transaction(11);
        let reverse_hash = states[2].submit_transaction(reverse).await.unwrap();
        wait_for_pool(&states, 12).await;
        for state in &states {
            let pool = state.mempool.lock().await;
            assert!(pool.get(&first_hash).is_some());
            assert!(pool.get(&reverse_hash).is_some());
            for tx in &transactions {
                assert!(pool.get(&tx.calculate_hash().unwrap()).is_some());
            }
        }

        // A connected peer can send duplicates, invalid JSON or forged TXs;
        // none should change the pool or break the connection for the next valid TX.
        let (mut peer, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", addresses[1]))
            .await
            .unwrap();
        peer.send(TungsteniteMessage::Text("invalid json".into()))
            .await
            .unwrap();
        let mut forged = transaction(12);
        forged.amount += 1;
        for tx in [transaction(0), forged, transaction(12)] {
            peer.send(TungsteniteMessage::Text(
                serde_json::to_string(&NodeMessage::NewTx(tx))
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();
        }
        wait_for_pool(&states, 13).await;
        for state in &states {
            assert!(
                state
                    .mempool
                    .lock()
                    .await
                    .get(&transaction(12).calculate_hash().unwrap())
                    .is_some()
            );
        }
    }

    #[tokio::test]
    async fn validators_rotate_over_websockets_and_late_node_catches_up() {
        let keys: Vec<_> = (1..=3)
            .map(|seed| SigningKey::from_bytes(&[seed; 32]))
            .collect();
        let validators: Vec<_> = keys.iter().map(|key| key.verifying_key()).collect();
        let states: Vec<_> = keys
            .into_iter()
            .map(|key| Arc::new(NodeState::new(key, validators.clone()).unwrap()))
            .collect();
        let mut network = JoinSet::new();
        let mut addresses = Vec::new();
        for state in &states {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.push(listener.local_addr().unwrap());
            let app = crate::app(Arc::clone(state));
            network.spawn(async move {
                axum::serve(
                    listener,
                    app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                )
                .await
                .unwrap();
            });
        }
        for index in 0..2 {
            let (socket, _) =
                tokio_tungstenite::connect_async(format!("ws://{}/ws", addresses[index + 1]))
                    .await
                    .unwrap();
            network.spawn(handle_peer(socket, Arc::clone(&states[index])));
        }
        let transactions: Vec<_> = (0..10).map(transaction).collect();
        // Exercise the same API admission path as the benchmark.
        let axum::Json(results) = crate::api::submit_transactions(
            axum::extract::State(Arc::clone(&states[0])),
            axum::Json(transactions.clone()),
        )
        .await;
        assert!(
            results
                .iter()
                .all(|result| matches!(result, crate::api::TransactionResult::Accepted { .. }))
        );
        wait_for_pool(&states, 10).await;

        let mut producers = JoinSet::new();
        for state in &states {
            producers.spawn(
                crate::validator::Validator {
                    state: Arc::clone(state),
                    block_time: Duration::from_millis(80),
                }
                .run(),
            );
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut ready = true;
                for state in &states {
                    ready &= state.chain.lock().await.len() >= 7;
                }
                if ready {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("three validators should complete two rounds");
        producers.shutdown().await;

        // Stop production, then let the final blocks finish propagating.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut tips = Vec::new();
                for state in &states {
                    tips.push(state.chain.lock().await.last().block_data.hash.clone());
                }
                if tips.iter().all(|hash| hash == &tips[0]) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("all chain tips should converge");
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        for (state, address) in states.iter().zip(&addresses) {
            let chain = state.chain.lock().await;
            let pool = state.mempool.lock().await;
            assert!(pool.is_empty());
            for block in chain.blocks().iter().skip(1) {
                assert_eq!(
                    block.validator.as_ref(),
                    Some(chain.proposer(block.block_data.index))
                );
            }
            let included: Vec<_> = chain
                .blocks()
                .iter()
                .flat_map(|block| &block.block_data.transactions)
                .map(|tx| tx.calculate_hash().unwrap())
                .collect();
            assert_eq!(included.len(), transactions.len());
            for tx in &transactions {
                assert!(included.contains(&tx.calculate_hash().unwrap()));
            }
            assert_eq!(
                state
                    .metrics
                    .snapshot(chain.len(), pool.len())
                    .txs_committed,
                10
            );
            let expected_len = chain.len();
            let expected_hash = chain.last().block_data.hash.clone();
            drop(pool);
            drop(chain);
            let metrics: serde_json::Value = client
                .get(format!("http://{address}/metrics"))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(metrics["txs_received"], 10);
            assert_eq!(metrics["txs_committed"], 10);
            assert_eq!(metrics["mempool_len"], 0);
            assert_eq!(metrics["chain_len"], expected_len);
            assert_eq!(metrics["height"], expected_len - 1);
            assert_eq!(metrics["last_block_hash"], expected_hash);
        }

        // A fresh node can recover signed blocks, not just pending transactions.
        let late = Arc::new(NodeState::new(SigningKey::from_bytes(&[1; 32]), validators).unwrap());
        let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", addresses[1]))
            .await
            .unwrap();
        network.spawn(handle_peer(socket, Arc::clone(&late)));
        let expected = states[0].chain.lock().await.last().block_data.hash.clone();
        tokio::time::timeout(Duration::from_secs(5), async {
            while late.chain.lock().await.last().block_data.hash != expected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("late node should replay the same chain");
        // The peer marks the end of its history: the fresh node may then produce.
        tokio::time::timeout(Duration::from_secs(5), async {
            while late.sync.pending_replays() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("late node should receive the end of the peer history");
        let chain = late.chain.lock().await;
        assert_eq!(late.metrics.snapshot(chain.len(), 0).txs_committed, 10);
        assert!(late.mempool.lock().await.is_empty());
    }

    #[tokio::test]
    async fn idle_node_pings_and_drops_a_silent_peer() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = crate::app(state(1));
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        // A peer that never sends anything, like one behind a dead connection.
        let (mut silent, _) = tokio_tungstenite::connect_async(format!("ws://{address}/ws"))
            .await
            .unwrap();
        let mut pings = 0;
        tokio::time::timeout(IDLE_TIMEOUT * 4, async {
            while let Some(Ok(message)) = silent.next().await {
                if let Some(text) = message.as_text()
                    && matches!(serde_json::from_str(text), Ok(NodeMessage::Ping))
                {
                    pings += 1;
                }
            }
        })
        .await
        .expect("the node should drop a peer that stays silent");
        assert!(pings >= 2, "an idle node keeps pinging, got {pings}");
    }

    #[tokio::test]
    async fn node_stops_dialing_an_address_that_leads_back_to_itself() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let node = state(1);
        let app = crate::app(Arc::clone(&node));
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        // Like a peer announced on an address this machine also has.
        tokio::time::timeout(
            Duration::from_secs(5),
            crate::discovery::connect_peer(
                vec![format!("ws://{address}/ws")],
                Arc::clone(&node),
                Duration::from_millis(10),
            ),
        )
        .await
        .expect("dialing ourselves should be detected and given up");
        // Talking to itself never counted as a peer history.
        assert_eq!(node.sync.pending_replays(), 0);
    }
}
