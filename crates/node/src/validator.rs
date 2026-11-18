use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::metrics::Metrics;
use crate::{NodeMessage, NodeState};
use blockchain::{
    block::{Block, BlockError},
    chain::{Chain, ChainError},
    mempool::{Mempool, MempoolError},
};
use ed25519_dalek::SigningKey;

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
    pub state: Arc<NodeState>,
    pub block_time: Duration,
}

/// Délai de découverte des peers au démarrage, en nombre de `block_time`.
const SYNC_GRACE_BLOCKS: u32 = 3;

/// Sans nouveau bloc pendant ce nombre de `block_time`, la rotation est loguée bloquée.
const STALL_LOG_BLOCKS: u32 = 5;

/// Suit depuis quand la chaîne attend le bloc `index`, pour loguer sans spammer.
struct Stall {
    index: u64,
    since: Instant,
    logged: Instant,
}

impl Default for Stall {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            index: 0,
            since: now,
            logged: now,
        }
    }
}

impl Stall {
    /// Vrai si l'on attend `index` depuis plus de `after`, au plus une fois par `after`.
    fn due(&mut self, index: u64, after: Duration) -> bool {
        let now = Instant::now();
        if index != self.index {
            *self = Self {
                index,
                since: now,
                logged: now,
            };
            return false;
        }
        if now.duration_since(self.logged) < after {
            return false;
        }
        self.logged = now;
        true
    }
}

/// Un node relancé repart de la genèse : s'il produisait tout de suite, il créerait
/// son propre bloc 1 et forkerait la chaîne des autres. Il attend donc d'abord
/// `grace` (le temps que mDNS trouve les peers), puis qu'aucun peer connecté
/// ne soit encore en train de lui rejouer son historique. Une fois prêt, il le reste.
pub(crate) struct SyncGate {
    started: Instant,
    pending_replays: AtomicUsize,
    synced: AtomicBool,
}

impl SyncGate {
    pub(crate) fn new() -> Self {
        Self {
            started: Instant::now(),
            pending_replays: AtomicUsize::new(0),
            synced: AtomicBool::new(false),
        }
    }

    /// Un peer vient de se connecter : son historique est en cours de réception.
    pub(crate) fn begin_replay(&self) -> Replay<'_> {
        self.pending_replays.fetch_add(1, Ordering::SeqCst);
        Replay {
            gate: self,
            done: false,
        }
    }

    pub(crate) fn pending_replays(&self) -> usize {
        self.pending_replays.load(Ordering::SeqCst)
    }

    fn ready(&self, grace: Duration) -> bool {
        self.ready_at(Instant::now(), grace)
    }

    fn ready_at(&self, now: Instant, grace: Duration) -> bool {
        if self.synced.load(Ordering::SeqCst) {
            return true;
        }
        let ready = now.saturating_duration_since(self.started) >= grace
            && self.pending_replays.load(Ordering::SeqCst) == 0;
        if ready {
            self.synced.store(true, Ordering::SeqCst);
        }
        ready
    }
}

/// Historique d'un peer en cours de réception ; terminé par `HistoryEnd`
/// ou par la déconnexion du peer.
pub(crate) struct Replay<'a> {
    gate: &'a SyncGate,
    done: bool,
}

impl Replay<'_> {
    pub(crate) fn finish(&mut self) {
        if !self.done {
            self.done = true;
            self.gate.pending_replays.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl Drop for Replay<'_> {
    fn drop(&mut self) {
        self.finish();
    }
}

impl Validator {
    /// Toutes les `block_time` : si c'est notre tour, produit un bloc,
    /// l'ajoute à la chaîne locale et l'envoie aux peers.
    /// Rien n'est produit avant d'avoir reçu l'historique des peers (`SyncGate`).
    pub(crate) async fn run(self) {
        let grace = self.block_time * SYNC_GRACE_BLOCKS;
        // Au-delà, on signale qui bloque la rotation (puis toutes les `STALL_LOG_BLOCKS`).
        let stall_after = self.block_time * STALL_LOG_BLOCKS;
        let mut synced = false;
        let mut stall = Stall::default();
        let mut ticker = tokio::time::interval(self.block_time);
        ticker.tick().await; // le premier tick est immédiat
        loop {
            ticker.tick().await;
            if !synced {
                if !self.state.sync.ready(grace) {
                    if stall.due(u64::MAX, stall_after) {
                        tracing::warn!(
                            pending_replays = self.state.sync.pending_replays(),
                            "block production waiting for peers' history"
                        );
                    }
                    continue;
                }
                synced = true;
                tracing::info!("peers' history received; block production enabled");
            }
            if let Err(error) = self.state.produce_next_block(unix_secs()).await {
                tracing::error!(%error, "block production failed");
            }
            let chain = self.state.chain.lock().await;
            let next = chain.len();
            if stall.due(next, stall_after) {
                let mempool_len = self.state.mempool.lock().await.len();
                tracing::warn!(
                    next_index = next,
                    proposer = %hex::encode(chain.proposer(next).to_bytes()),
                    stalled_secs = stall.since.elapsed().as_secs(),
                    mempool_len,
                    "chain stalled: waiting for the proposer's block"
                );
            }
        }
    }
}

impl NodeState {
    pub(crate) async fn produce_next_block(&self, now_secs: u64) -> Result<(), ValidatorError> {
        let mut chain = self.chain.lock().await;
        let mut mempool = self.mempool.lock().await;
        if let Some(block) = produce_block(&mut chain, &mut mempool, &self.signing_key, now_secs)? {
            self.metrics.record_block(&block);
            self.publish_block(block, "produced")?;
        }
        Ok(())
    }

    /// Ignore known blocks; reject conflicting history and invalid successors.
    pub(crate) async fn accept_peer_block(&self, block: Block) -> Result<(), ValidatorError> {
        let mut chain = self.chain.lock().await;
        if let Some(known) = chain.blocks().get(block.block_data.index as usize)
            && known.block_data.hash == block.block_data.hash
        {
            return Ok(());
        }
        let mut mempool = self.mempool.lock().await;
        accept_block(&mut chain, &mut mempool, &self.metrics, block.clone())?;
        self.publish_block(block, "received")?;
        Ok(())
    }

    fn publish_block(&self, block: Block, action: &str) -> Result<(), ValidatorError> {
        let index = block.block_data.index;
        let hash = block.block_data.hash.clone();
        let transactions = block.block_data.transactions.len();
        let text = serde_json::to_string(&NodeMessage::NewBlock(Box::new(block)))
            .map_err(BlockError::from)?;
        let _ = self.gossip.send(text);
        tracing::info!(index, %hash, transactions, %action, "block added");
        Ok(())
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
    // Drained from the mempool: every TX was verified on admission.
    chain.push_trusted(block.clone(), |tx| is_verified(mempool, tx))?;
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
    chain.push_trusted(block, |tx| is_verified(mempool, tx))?;
    let block = chain.last();
    mempool.remove_included(&block.block_data.transactions)?;
    metrics.record_block(block);
    Ok(())
}

/// Mempool `seen` only holds hashes of TXs that passed `verify` or were
/// included in an accepted block: their signature need not be checked again.
fn is_verified(mempool: &Mempool, tx: &blockchain::transaction::Transaction) -> bool {
    tx.calculate_hash()
        .is_ok_and(|hash| mempool.contains(&hash))
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

    #[tokio::test]
    async fn received_blocks_are_validated_counted_and_relayed_once() {
        let validators = vec![key(1).verifying_key(), key(2).verifying_key()];
        let state = NodeState::new(key(1), validators.clone()).unwrap();
        state.submit_transaction(tx(0)).await.unwrap();
        let mut events = state.gossip.subscribe();
        let chain = Chain::new(validators).unwrap();
        let invalid = Block::new(
            1,
            chain.last().block_data.hash.clone(),
            vec![tx(0)],
            10,
            &key(1),
        )
        .unwrap();
        assert!(state.accept_peer_block(invalid).await.is_err());
        assert_eq!(state.chain.lock().await.len(), 1);
        assert_eq!(state.mempool.lock().await.len(), 1);
        assert!(events.try_recv().is_err());

        let block = Block::new(
            1,
            chain.last().block_data.hash.clone(),
            vec![tx(0)],
            10,
            &key(2),
        )
        .unwrap();
        state.accept_peer_block(block.clone()).await.unwrap();
        assert!(matches!(
            serde_json::from_str::<NodeMessage>(&events.try_recv().unwrap()).unwrap(),
            NodeMessage::NewBlock(_)
        ));
        state.accept_peer_block(block).await.unwrap();
        assert!(events.try_recv().is_err());
        assert_eq!(state.chain.lock().await.len(), 2);
        assert!(state.mempool.lock().await.is_empty());
        assert_eq!(state.metrics.snapshot(2, 0).txs_committed, 1);
    }

    #[test]
    fn node_requires_an_authorized_key_and_unique_validators() {
        assert!(NodeState::new(key(1), vec![key(2).verifying_key()]).is_err());
        assert!(NodeState::new(key(1), vec![key(1).verifying_key(); 2]).is_err());
    }

    #[test]
    fn sync_gate_waits_for_grace_then_for_every_peer_history() {
        let gate = SyncGate::new();
        let grace = Duration::from_secs(6);
        let start = gate.started;
        // Seul au démarrage : on laisse le temps aux peers d'être découverts.
        assert!(!gate.ready_at(start + Duration::from_secs(5), grace));

        let mut first = gate.begin_replay();
        let second = gate.begin_replay();
        assert!(!gate.ready_at(start + grace, grace));
        first.finish();
        // Un historique complet ne suffit pas si un autre peer rejoue encore le sien.
        assert!(!gate.ready_at(start + grace, grace));
        // Un peer déconnecté en plein rejeu ne bloque plus la production.
        drop(second);
        assert!(gate.ready_at(start + grace, grace));

        // Une fois synchronisé, un nouveau peer ne suspend pas la production.
        let _late = gate.begin_replay();
        assert!(gate.ready_at(start + grace, grace));
        drop(first);
        assert_eq!(gate.pending_replays.load(Ordering::SeqCst), 1);
    }
}
