use blockchain::mempool::MempoolError;
use futures_util::{Sink, SinkExt};
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::task::JoinHandle;

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
                Ok(_) => {
                    state.metrics.record_tx(&tx);
                    relay(state, text);
                }
                Err(MempoolError::AlreadySeen(_)) => {}
                Err(error) => tracing::warn!("peer transaction rejected: {error}"),
            }
        }
        NodeMessage::NewBlock(block) => {
            let index = block.block_data.index;
            let accepted = {
                let mut chain = state.chain.lock().await;
                // Le même bloc arrive par plusieurs peers : seul le premier compte.
                let known = usize::try_from(index)
                    .ok()
                    .and_then(|i| chain.blocks().get(i))
                    .is_some_and(|known| known.block_data.hash == block.block_data.hash);
                if known {
                    return;
                }
                let mut mempool = state.mempool.lock().await;
                accept_block(&mut chain, &mut mempool, &state.metrics, *block)
            };
            match accepted {
                Ok(()) => {
                    tracing::info!(index, "block accepted");
                    relay(state, text);
                }
                // Hors doublons : bloc en avance (bloc manqué), fork ou bloc invalide.
                Err(error) => tracing::warn!(index, "peer block rejected: {error}"),
            }
        }
        NodeMessage::Ping => {}
    }
}

/// Re-diffuse le message tel quel : pas besoin de le re-sérialiser.
fn relay(state: &NodeState, text: &str) {
    let _ = state.peers.send(text.to_owned());
}

/// Envoie au peer les messages diffusés, dans sa propre tâche : si la lecture
/// et l'écriture partageaient une boucle, deux nodes qui s'envoient beaucoup en
/// même temps resteraient bloqués dans `send`, chacun attendant que l'autre lise.
/// La tâche s'arrête quand l'envoi échoue.
pub(crate) fn spawn_writer<M, S>(
    state: &NodeState,
    peer: String,
    mut sink: S,
    to_message: fn(String) -> M,
) -> JoinHandle<()>
where
    M: Send + 'static,
    S: Sink<M> + Unpin + Send + 'static,
{
    let mut outgoing = state.peers.subscribe();
    tokio::spawn(async move {
        while let Some(text) = next_outgoing(&mut outgoing, &peer).await {
            if sink.send(to_message(text)).await.is_err() {
                break;
            }
        }
    })
}

/// Prochain message à envoyer au peer, `None` si le canal est fermé.
/// Si le peer est trop lent, les messages perdus sont signalés et on continue.
async fn next_outgoing(outgoing: &mut broadcast::Receiver<String>, peer: &str) -> Option<String> {
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
        assert_eq!(state.metrics.snapshot(1, 1).txs_received, 1);
        assert_eq!(outgoing.try_recv().unwrap(), text);
        assert!(outgoing.try_recv().is_err());
    }

    #[tokio::test]
    async fn valid_block_is_accepted_and_relayed_once() {
        let state = state();
        let mut outgoing = state.peers.subscribe();
        let genesis_hash = state.chain.lock().await.last().block_data.hash.clone();
        let block = Block::new(1, genesis_hash, vec![], 1, &key(2)).unwrap();
        let text = serde_json::to_string(&NodeMessage::NewBlock(Box::new(block))).unwrap();

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
