mod api;
mod discovery;
mod metrics;
mod p2p;
mod validator;

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

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
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum NodeMessage {
    NewTx(Transaction),
    /// Boxed: a block is much larger than the other variants.
    NewBlock(Box<Block>),
    Ping,
}

/// Reads `.env` from the current directory if present; shell variables keep priority.
fn read_dotenv() -> HashMap<String, String> {
    let Ok(contents) = std::fs::read_to_string(".env") else {
        return HashMap::new();
    };
    contents
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            Some((key.trim().to_string(), value.trim().to_string()))
        })
        .collect()
}

fn get_env(dotenv: &HashMap<String, String>, name: &str) -> anyhow::Result<String> {
    if let Ok(value) = std::env::var(name) {
        return Ok(value);
    }
    dotenv
        .get(name)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing environment variable {name}"))
}

fn load_signing_key(dotenv: &HashMap<String, String>) -> anyhow::Result<SigningKey> {
    let encoded = get_env(dotenv, "SECRET_KEY")?;
    let mut secret = [0u8; 32];
    hex::decode_to_slice(encoded.trim(), &mut secret)?;

    Ok(SigningKey::from_bytes(&secret))
}

fn load_authorized_validators(
    dotenv: &HashMap<String, String>,
) -> anyhow::Result<Vec<VerifyingKey>> {
    let encoded = get_env(dotenv, "AUTHORIZED_VALIDATORS")?;

    let validators = encoded
        .split(',')
        .map(|value| {
            let mut bytes = [0u8; 32];
            hex::decode_to_slice(value.trim(), &mut bytes)?;
            Ok(VerifyingKey::from_bytes(&bytes)?)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(validators)
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dotenv = read_dotenv();
    let signing_key = load_signing_key(&dotenv)?;
    let public_key = signing_key.verifying_key();

    println!("node public key: {}", hex::encode(public_key.to_bytes()));

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let authorized_validators = load_authorized_validators(&dotenv)?;
    let state = Arc::new(NodeState::new(signing_key, authorized_validators)?);
    let block_time: u64 = get_env(&dotenv, "BLOCK_TIME_SECS")
        .unwrap_or_else(|_| "2".into())
        .parse()?;
    anyhow::ensure!(block_time > 0, "BLOCK_TIME_SECS must be greater than zero");
    let app = app(Arc::clone(&state));

    // run it with hyper
    let port: u16 = get_env(&dotenv, "PORT")
        .unwrap_or_else(|_| "3000".into())
        .parse()?;
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, port)).await?;
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
        block_time: Duration::from_secs(block_time),
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
