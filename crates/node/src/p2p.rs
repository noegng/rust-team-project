use blockchain::mempool::MempoolError;
use tokio::sync::broadcast::{self, error::RecvError};

use crate::{NodeMessage, NodeState, validator::accept_block};

/// Traite un message reçu d'un peer. Les TX et blocs nouveaux sont
/// re-diffusés aux peers ; les doublons sont refusés, ce qui arrête la propagation.
pub(crate) async fn handle_message(state: &NodeState, text: &str) {
    let message = match serde_json::from_str::<NodeMessage>(text) {
        Ok(message) => message,
        Err(error) => {
            tracing::warn!("invalid peer message: {error}");
            return;
        }
    };
    match message {
        NodeMessage::NewTx(tx) => {
            let added = state.mempool.lock().await.add(tx.clone());
            match added {
                Ok(_) => state.broadcast(&NodeMessage::NewTx(tx)),
                Err(MempoolError::AlreadySeen(_)) => {}
                Err(error) => tracing::debug!("peer transaction rejected: {error}"),
            }
        }
        NodeMessage::NewBlock(block) => {
            let accepted = {
                let mut chain = state.chain.lock().await;
                let mut mempool = state.mempool.lock().await;
                accept_block(&mut chain, &mut mempool, &state.metrics, block.clone())
            };
            match accepted {
                Ok(()) => {
                    tracing::info!(index = block.block_data.index, "block accepted");
                    state.broadcast(&NodeMessage::NewBlock(block));
                }
                Err(error) => tracing::debug!("peer block rejected: {error}"),
            }
        }
        NodeMessage::Ping => {}
    }
}

/// Prochain message à envoyer au peer, `None` si le canal est fermé.
/// Si le peer est trop lent, les messages perdus sont signalés et on continue.
pub(crate) async fn next_outgoing(
    outgoing: &mut broadcast::Receiver<String>,
    peer: &str,
) -> Option<String> {
    loop {
        match outgoing.recv().await {
            Ok(text) => return Some(text),
            Err(RecvError::Lagged(skipped)) => {
                tracing::warn!("{peer}: {skipped} messages dropped")
            }
            Err(RecvError::Closed) => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockchain::{block::Block, transaction::Transaction};
    use ed25519_dalek::SigningKey;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    /// Node A (seed 1) ; validateurs A puis B : le bloc 1 revient à B.
    fn state() -> NodeState {
        NodeState::new(key(1), vec![key(1).verifying_key(), key(2).verifying_key()]).unwrap()
    }

    #[tokio::test]
    async fn new_transaction_is_added_and_relayed_once() {
        let state = state();
        let mut outgoing = state.peers.subscribe();
        let tx = Transaction::new(&key(3).verifying_key(), 1, 0, 1, &key(4)).unwrap();
        let text = serde_json::to_string(&NodeMessage::NewTx(tx)).unwrap();

        handle_message(&state, &text).await;
        handle_message(&state, &text).await;

        assert_eq!(state.mempool.lock().await.len(), 1);
        assert_eq!(outgoing.try_recv().unwrap(), text);
        assert!(outgoing.try_recv().is_err());
    }

    #[tokio::test]
    async fn valid_block_is_accepted_and_relayed_once() {
        let state = state();
        let mut outgoing = state.peers.subscribe();
        let genesis_hash = state.chain.lock().await.last().block_data.hash.clone();
        let block = Block::new(1, genesis_hash, vec![], 1, &key(2)).unwrap();
        let text = serde_json::to_string(&NodeMessage::NewBlock(block)).unwrap();

        handle_message(&state, &text).await;
        handle_message(&state, &text).await;

        assert_eq!(state.chain.lock().await.len(), 2);
        assert_eq!(outgoing.try_recv().unwrap(), text);
        assert!(outgoing.try_recv().is_err());
    }

    #[tokio::test]
    async fn invalid_message_is_ignored() {
        let state = state();
        let mut outgoing = state.peers.subscribe();
        handle_message(&state, "not json").await;
        assert!(state.mempool.lock().await.is_empty());
        assert!(outgoing.try_recv().is_err());
    }
}
