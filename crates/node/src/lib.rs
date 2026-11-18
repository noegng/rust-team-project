mod api;
mod discovery;
mod metrics;
mod p2p;
#[cfg(any(test, feature = "testnet"))]
pub mod testnet;
mod validator;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Router,
    extract::{ConnectInfo, State, WebSocketUpgrade},
    response::IntoResponse,
    routing::{any, get, post},
};
use blockchain::{
    block::Block,
    chain::Chain,
    mempool::{Mempool, MempoolError},
    transaction::{Transaction, TransactionError},
};
use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast};
use tower_http::trace::{DefaultMakeSpan, TraceLayer};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum NodeMessage {
    NewTx(Transaction),
    /// Boxed: a block is much larger than the other variants.
    NewBlock(Box<Block>),
    Ping,
}

struct NodeState {
    chain: Mutex<Chain>,
    mempool: Mutex<Mempool>,
    signing_key: SigningKey,
    metrics: metrics::Metrics,
    gossip: broadcast::Sender<String>,
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
        })
    }

    /// Shared admission path for HTTP and peers. Only new, valid TXs are relayed.
    async fn submit_transaction(&self, tx: Transaction) -> Result<String, MempoolError> {
        let message = serde_json::to_string(&NodeMessage::NewTx(tx.clone()))
            .map_err(TransactionError::from)?;
        let mut mempool = self.mempool.lock().await;
        let hash = mempool.add(tx.clone())?;
        self.metrics.record_tx(&tx);
        // No connected peers is normal; the mempool is replayed on connection.
        let _ = self.gossip.send(message);
        tracing::debug!(%hash, mempool_len = mempool.len(), "transaction admitted");
        Ok(hash)
    }
}

fn app(state: Arc<NodeState>) -> Router {
    Router::new()
        .route("/ws", any(ws_handler))
        .route("/transactions", post(api::submit_transactions))
        .route("/transactions/{hash}", get(api::get_transaction))
        .route("/blocks/{index}", get(api::get_block))
        .route("/metrics", get(api::get_metrics))
        .with_state(state)
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
    let node_id = format!(
        "{:x}-{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        std::process::id(),
    );
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
        p2p::handle_peer(socket, state).await;
        tracing::info!(%addr, "incoming peer disconnected");
    })
}
