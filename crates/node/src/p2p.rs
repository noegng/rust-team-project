use std::{fmt::Display, sync::Arc, time::Duration};

use axum::extract::ws::Message as AxumMessage;
use blockchain::mempool::MempoolError;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;

use crate::{NodeMessage, NodeState};

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

pub(crate) async fn handle_peer<S, M, E>(socket: S, state: Arc<NodeState>)
where
    S: Stream<Item = Result<M, E>> + Sink<M, Error = E> + Unpin,
    M: PeerMessage,
    E: Display,
{
    // Subscribe before taking the snapshot, so concurrent admissions are not missed.
    let mut outbound = state.gossip.subscribe();
    // Use the same lock order as block production. Replay history before pending TXs.
    let (blocks, pending): (Vec<_>, Vec<_>) = {
        let chain = state.chain.lock().await;
        let mempool = state.mempool.lock().await;
        (
            chain.blocks().iter().skip(1).cloned().collect(),
            mempool.txs.values().cloned().collect(),
        )
    };
    let (mut writer, mut reader) = socket.split();
    // Read while sending the snapshot, including when both peers replay at once.
    let send = async {
        for block in blocks {
            let text = serde_json::to_string(&NodeMessage::NewBlock(block))?;
            send_text(&mut writer, text).await?;
        }
        for tx in pending {
            let text = serde_json::to_string(&NodeMessage::NewTx(tx))?;
            send_text(&mut writer, text).await?;
        }
        loop {
            // A lagging peer is disconnected and catches up on reconnection.
            match outbound.recv().await {
                Ok(text) => send_text(&mut writer, text).await?,
                Err(error) => break Err::<(), _>(anyhow::Error::new(error)),
            }
        }
    };
    let receive = async {
        while let Some(message) = reader.next().await {
            let message = match message {
                Ok(message) => message,
                Err(error) => {
                    tracing::debug!(%error, "peer read failed");
                    break;
                }
            };
            if message.is_close() {
                break;
            }
            let Some(text) = message.as_text() else {
                continue;
            };
            match serde_json::from_str::<NodeMessage>(text) {
                Ok(NodeMessage::NewTx(tx)) => match state.submit_transaction(tx).await {
                    Ok(_) | Err(MempoolError::AlreadySeen(_)) => {}
                    Err(error) => tracing::warn!(%error, "invalid peer transaction"),
                },
                Ok(NodeMessage::Ping) => {}
                Ok(NodeMessage::NewBlock(block)) => {
                    if let Err(error) = state.accept_peer_block(block).await {
                        tracing::warn!(%error, "peer block rejected; reconnect to replay history");
                        break;
                    }
                }
                Err(error) => tracing::warn!(%error, "invalid peer message"),
            }
        }
    };
    tokio::select! {
        result = send => {
            if let Err(error) = result {
                tracing::debug!(%error, "peer send failed; disconnecting");
            }
        }
        () = receive => {}
    }
}

async fn send_text<S, M, E>(writer: &mut S, text: String) -> anyhow::Result<()>
where
    S: Sink<M, Error = E> + Unpin,
    M: PeerMessage,
    E: Display,
{
    tokio::time::timeout(Duration::from_secs(5), writer.send(M::text(text)))
        .await?
        .map_err(|error| anyhow::anyhow!("{error}"))
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
        let NodeMessage::NewTx(received) =
            serde_json::from_str(&events.try_recv().unwrap()).unwrap()
        else {
            panic!("expected a transaction");
        };
        assert_eq!(received.calculate_hash().unwrap(), hash);
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
            assert!(pool.txs.contains_key(&first_hash));
            assert!(pool.txs.contains_key(&reverse_hash));
            for tx in &transactions {
                assert!(pool.txs.contains_key(&tx.calculate_hash().unwrap()));
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
                    .txs
                    .contains_key(&transaction(12).calculate_hash().unwrap())
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
        let chain = late.chain.lock().await;
        assert_eq!(late.metrics.snapshot(chain.len(), 0).txs_committed, 10);
        assert!(late.mempool.lock().await.is_empty());
    }
}
