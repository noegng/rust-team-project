use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use blockchain::{
    block::{Block, BlockError},
    chain::{Chain, ChainError},
    mempool::{Mempool, MempoolError},
};
use ed25519_dalek::SigningKey;

use crate::metrics::Metrics;
use crate::{NodeMessage, NodeState};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ValidatorError {
    #[error("could not build block: {0}")]
    Block(#[from] BlockError),

    #[error("block rejected: {0}")]
    Chain(#[from] ChainError),

    #[error("could not update mempool: {0}")]
    Mempool(#[from] MempoolError),
}

/// Production de blocs PoA. Verrous pris dans l'ordre : `chain` puis `mempool`.
pub(crate) struct Validator {
    pub(crate) state: Arc<NodeState>,
    pub(crate) block_time: Duration,
}

impl Validator {
    /// Toutes les `block_time` : si c'est notre tour, produit un bloc,
    /// l'ajoute à la chaîne locale et l'envoie aux peers.
    pub(crate) async fn run(self) {
        let state = &self.state;
        let mut ticker = tokio::time::interval(self.block_time);
        ticker.tick().await; // le premier tick est immédiat
        loop {
            ticker.tick().await;
            let produced = {
                let mut chain = state.chain.lock().await;
                let mut mempool = state.mempool.lock().await;
                produce_block(&mut chain, &mut mempool, &state.signing_key, unix_secs())
            };
            match produced {
                Ok(Some(block)) => {
                    tracing::info!(
                        index = block.block_data.index,
                        txs = block.block_data.transactions.len(),
                        "block produced"
                    );
                    state.metrics.record_block(&block);
                    state.broadcast(&NodeMessage::NewBlock(block));
                }
                Ok(None) => {}
                Err(err) => tracing::error!("block production failed: {err}"),
            }
        }
    }
}

/// Produit le prochain bloc si `signing_key` est le proposant attendu
/// (round-robin), sinon retourne `None`. Le bloc est déjà ajouté à `chain`.
/// Les blocs vides sont produits aussi : sinon la rotation se bloque.
pub(crate) fn produce_block(
    chain: &mut Chain,
    mempool: &mut Mempool,
    signing_key: &SigningKey,
    now_secs: u64,
) -> Result<Option<Block>, ValidatorError> {
    let index = chain.len();
    if *chain.proposer(index) != signing_key.verifying_key() {
        return Ok(None);
    }
    let last = chain.last();
    let timestamp = now_secs.max(last.block_data.timestamp);
    let prev_hash = last.block_data.hash.clone();
    let transactions = mempool.drain(chain);
    let block = Block::new(index, prev_hash, transactions, timestamp, signing_key)?;
    chain.push(block.clone())?;
    Ok(Some(block))
}

/// Bloc reçu d'un peer (`Message::NewBlock`) : validateur autorisé et à son tour,
/// signature, lien, timestamp et TX sont vérifiés par `Chain::push`.
/// Les TX incluses sont ensuite retirées du mempool.
pub(crate) fn accept_block(
    chain: &mut Chain,
    mempool: &mut Mempool,
    metrics: &Metrics,
    block: Block,
) -> Result<(), ValidatorError> {
    chain.push(block)?;
    let block = chain.last();
    mempool.remove_included(&block.block_data.transactions)?;
    metrics.record_block(block);
    Ok(())
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockchain::transaction::Transaction;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    /// Validateurs A (seed 1) et B (seed 2) : le bloc 1 revient à B, le bloc 2 à A.
    fn chain() -> Chain {
        Chain::new(vec![key(1).verifying_key(), key(2).verifying_key()]).unwrap()
    }

    fn tx(nonce: u64) -> Transaction {
        let sender = key(9);
        Transaction::new(&key(8).verifying_key(), 5, nonce, 1, &sender).unwrap()
    }

    #[test]
    fn only_the_expected_proposer_produces() {
        let mut chain = chain();
        let mut mempool = Mempool::new();
        assert!(
            produce_block(&mut chain, &mut mempool, &key(1), 10)
                .unwrap()
                .is_none()
        );
        assert_eq!(chain.len(), 1);

        let block = produce_block(&mut chain, &mut mempool, &key(2), 10)
            .unwrap()
            .unwrap();
        assert_eq!(block.block_data.index, 1);
        assert_eq!(chain.len(), 2);
        assert!(
            produce_block(&mut chain, &mut mempool, &key(1), 11)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn produced_block_includes_mempool_and_is_accepted_by_peers() {
        let mut producer_chain = chain();
        let mut producer_mempool = Mempool::new();
        let mut peer_chain = chain();
        let mut peer_mempool = Mempool::new();
        for nonce in [1, 0] {
            producer_mempool.add(tx(nonce)).unwrap();
            peer_mempool.add(tx(nonce)).unwrap();
        }

        let block = produce_block(&mut producer_chain, &mut producer_mempool, &key(2), 10)
            .unwrap()
            .unwrap();
        let nonces: Vec<_> = block
            .block_data
            .transactions
            .iter()
            .map(|tx| tx.nonce)
            .collect();
        assert_eq!(nonces, [0, 1]);
        assert!(producer_mempool.is_empty());

        let metrics = Metrics::new();
        accept_block(&mut peer_chain, &mut peer_mempool, &metrics, block).unwrap();
        assert_eq!(
            peer_chain.last().block_data.hash,
            producer_chain.last().block_data.hash
        );
        assert!(peer_mempool.is_empty());
        assert_eq!(metrics.snapshot(peer_chain.len(), 0).txs_committed, 2);
    }

    #[test]
    fn block_timestamp_never_goes_backwards() {
        let mut chain = chain();
        let mut mempool = Mempool::new();
        produce_block(&mut chain, &mut mempool, &key(2), 50).unwrap();
        let block = produce_block(&mut chain, &mut mempool, &key(1), 40)
            .unwrap()
            .unwrap();
        assert_eq!(block.block_data.timestamp, 50);
    }

    #[test]
    fn invalid_or_duplicate_blocks_are_rejected() {
        let mut chain = chain();
        let mut mempool = Mempool::new();
        let metrics = Metrics::new();

        let out_of_turn =
            Block::new(1, chain.last().block_data.hash.clone(), vec![], 10, &key(1)).unwrap();
        assert!(matches!(
            accept_block(&mut chain, &mut mempool, &metrics, out_of_turn),
            Err(ValidatorError::Chain(ChainError::WrongProposer {
                index: 1
            }))
        ));

        let block =
            Block::new(1, chain.last().block_data.hash.clone(), vec![], 10, &key(2)).unwrap();
        accept_block(&mut chain, &mut mempool, &metrics, block.clone()).unwrap();
        assert!(matches!(
            accept_block(&mut chain, &mut mempool, &metrics, block),
            Err(ValidatorError::Chain(ChainError::InvalidIndex { .. }))
        ));
        assert_eq!(chain.len(), 2);
    }
}
