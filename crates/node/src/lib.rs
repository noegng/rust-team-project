mod api;
mod discovery;
mod metrics;
mod p2p;
#[cfg(any(test, feature = "testnet"))]
pub mod testnet;
mod validator;

use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Router,
    extract::{ConnectInfo, State, WebSocketUpgrade},
    http::Method,
    response::{Html, IntoResponse},
    routing::{any, get, post},
};
use blockchain::{
    block::Block,
    chain::Chain,
    mempool::{Mempool, MempoolError},
    transaction::{Transaction, VerifiedTransaction},
};
use ed25519_dalek::{SigningKey, VerifyingKey};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::{DefaultMakeSpan, TraceLayer},
};
use tracing::Instrument;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum NodeMessage {
    NewTx(Transaction),
    /// One message per admitted batch: per-TX messages flood the gossip channel.
    NewTxs(Vec<Transaction>),
    /// Boxed: a block is much larger than the other variants.
    NewBlock(Box<Block>),
    Ping,
    /// Sent once after replaying the chain to a new peer: the peer has our whole history.
    HistoryEnd,
    /// First message on every connection: the sender's public key (hex).
    /// A node that receives its own key has dialed itself.
    Hello(String),
}

struct NodeState {
    chain: Mutex<Chain>,
    mempool: Mutex<Mempool>,
    signing_key: SigningKey,
    metrics: metrics::Metrics,
    gossip: broadcast::Sender<String>,
    /// Peers vus par mDNS : ID du node → URLs HTTP candidates (une par IPv4).
    peers: std::sync::Mutex<BTreeMap<String, Vec<String>>>,
    /// Block production waits for the peers' history (see `validator::SyncGate`).
    sync: validator::SyncGate,
}

impl NodeState {
    fn new(
        signing_key: SigningKey,
        authorized_validators: Vec<VerifyingKey>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            authorized_validators.contains(&signing_key.verifying_key()),
            "SECRET_KEY must belong to AUTHORIZED_VALIDATORS"
        );
        let unique: std::collections::HashSet<_> = authorized_validators
            .iter()
            .map(|key| key.to_bytes())
            .collect();
        anyhow::ensure!(
            unique.len() == authorized_validators.len(),
            "AUTHORIZED_VALIDATORS must not contain duplicates"
        );
        let (gossip, _) = broadcast::channel(16_384);
        Ok(Self {
            chain: Mutex::new(Chain::new(authorized_validators.clone())?),
            mempool: Mutex::new(Mempool::new()),
            signing_key,
            metrics: metrics::Metrics::new(),
            gossip,
            peers: std::sync::Mutex::default(),
            sync: validator::SyncGate::new(),
        })
    }

    fn record_peer(&self, node_id: String, urls: Vec<String>) {
        self.peers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(node_id, urls);
    }

    fn peers(&self) -> BTreeMap<String, Vec<String>> {
        self.peers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Shared admission path for HTTP and peers; results follow `txs` order.
    /// Signatures are checked in parallel outside the mempool lock, known TXs
    /// (peers echo our own gossip) are skipped first, and the new valid TXs are
    /// relayed in a single message.
    async fn submit_transactions(
        &self,
        txs: Vec<Transaction>,
    ) -> Vec<Result<String, MempoolError>> {
        let hashed = blocking(move || {
            txs.into_par_iter()
                .map(|tx| {
                    let hash = tx.calculate_hash();
                    (tx, hash)
                })
                .collect::<Vec<_>>()
        })
        .await;
        let fresh: Vec<bool> = {
            let mempool = self.mempool.lock().await;
            hashed
                .iter()
                .map(|(_, hash)| hash.as_ref().is_ok_and(|hash| !mempool.contains(hash)))
                .collect()
        };
        let checked = blocking(move || {
            hashed
                .into_par_iter()
                .zip(fresh)
                .map(|((tx, hash), fresh)| match hash {
                    Err(error) => Err(MempoolError::from(error)),
                    Ok(hash) if !fresh => Err(MempoolError::AlreadySeen(hash)),
                    Ok(_) => VerifiedTransaction::new(tx).map_err(MempoolError::from),
                })
                .collect::<Vec<_>>()
        })
        .await;

        let mut admitted = Vec::new();
        let results: Vec<_> = {
            let mut mempool = self.mempool.lock().await;
            checked
                .into_iter()
                .map(|verified| {
                    let verified = verified?;
                    let hash = mempool.add_verified(verified.clone())?;
                    self.metrics.record_tx(verified.transaction());
                    admitted.push(verified.transaction().clone());
                    Ok(hash)
                })
                .collect()
        };
        if !admitted.is_empty() {
            tracing::debug!(admitted = admitted.len(), "transactions admitted");
            match serde_json::to_string(&NodeMessage::NewTxs(admitted)) {
                // No connected peers is normal; the mempool is replayed on connection.
                Ok(message) => {
                    let _ = self.gossip.send(message);
                }
                Err(error) => tracing::error!(%error, "could not encode gossip"),
            }
        }
        results
    }

    #[cfg(test)]
    async fn submit_transaction(&self, tx: Transaction) -> Result<String, MempoolError> {
        self.submit_transactions(vec![tx])
            .await
            .pop()
            .expect("one result per transaction")
    }
}

/// Runs CPU-bound work (signature checks) off the async workers.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    match tokio::task::spawn_blocking(work).await {
        Ok(value) => value,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        // Cancelled: only while the runtime shuts down, which drops this task next.
        Err(_) => std::future::pending().await,
    }
}

/// Dashboard live : interroge ce node et ses peers depuis le navigateur.
const DASHBOARD: &str = include_str!("dashboard.html");

fn app(state: Arc<NodeState>) -> Router {
    // Lecture seule : le dashboard servi par un node interroge les autres nodes.
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET]);
    Router::new()
        .route("/", get(|| async { Html(DASHBOARD) }))
        .route("/ws", any(ws_handler))
        .route("/transactions", post(api::submit_transactions))
        .route("/transactions/{hash}", get(api::get_transaction))
        .route("/blocks", get(api::get_recent_blocks))
        .route("/blocks/{index}", get(api::get_block))
        .route("/metrics", get(api::get_metrics))
        .route("/peers", get(api::get_peers))
        .with_state(state)
        .layer(cors)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true)),
        )
}

/// Everything a node needs; `main` builds it from the environment.
pub struct NodeConfig {
    pub signing_key: SigningKey,
    /// Same order on every node: it defines the proposer rotation.
    pub authorized_validators: Vec<VerifyingKey>,
    pub port: u16,
    pub block_time: Duration,
}

/// Serves the API and WebSocket, discovers peers over mDNS and produces blocks.
pub async fn run(config: NodeConfig) -> anyhow::Result<()> {
    anyhow::ensure!(
        !config.block_time.is_zero(),
        "BLOCK_TIME_SECS must be greater than zero"
    );
    let state = Arc::new(NodeState::new(
        config.signing_key,
        config.authorized_validators,
    )?);
    let app = app(Arc::clone(&state));
    let listener =
        tokio::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, config.port)).await?;
    let port = listener.local_addr()?.port();
    tracing::debug!("listening on port {port}");
    // Stable across restarts: peers keep dialing a restarted node through their
    // existing loop instead of adding one per new ID (each extra loop opened one
    // more connection, multiplying every gossip upload). 32 hex chars of the
    // public key fit the 63-byte mDNS instance label.
    let node_id = hex::encode(&state.signing_key.verifying_key().to_bytes()[..16]);
    let discovery_state = Arc::clone(&state);
    tokio::spawn(async move {
        if let Err(error) = discovery::discover_peers(node_id, port, discovery_state).await {
            eprintln!("Discovery failed: {error}");
        }
    });
    let validator = validator::Validator {
        state,
        block_time: config.block_time,
    };
    tokio::spawn(validator.run());
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

async fn ws_handler(
    State(state): State<Arc<NodeState>>,
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        tracing::info!(%addr, "incoming peer connected");
        p2p::handle_peer(socket, state)
            .instrument(tracing::info_span!("peer", %addr))
            .await;
        tracing::info!(%addr, "incoming peer disconnected");
    })
}
