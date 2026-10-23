mod api;
mod discovery;
mod metrics;
mod p2p;
mod validator;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Router,
    extract::{
        ConnectInfo, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::IntoResponse,
    routing::{any, get, post},
};
use blockchain::{
    block::Block,
    chain::{Chain, ChainError},
    mempool::Mempool,
    transaction::Transaction,
};
use ed25519_dalek::{SigningKey, VerifyingKey};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast};
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::{metrics::Metrics, validator::Validator};

/// Interval between two block production attempts.
const BLOCK_TIME: Duration = Duration::from_secs(5);
/// Messages buffered per peer before the slowest ones start being dropped.
const PEER_QUEUE: usize = 16_384;
/// Same default as the client's `NODE_URL`.
const DEFAULT_PORT: u16 = 8001;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum NodeMessage {
    NewTx(Transaction),
    /// Boxed: a block is much larger than the other variants.
    NewBlock(Box<Block>),
    Ping,
}

#[derive(Debug, thiserror::Error)]
enum ConfigError {
    #[error("missing environment variable {0}")]
    Missing(&'static str),

    #[error("{0} must contain 64 hex characters (32 bytes)")]
    InvalidHex(&'static str),

    #[error("{0} contains an invalid ed25519 public key")]
    InvalidPublicKey(&'static str),

    #[error("PORT must be a port number, got {0:?}")]
    InvalidPort(String),
}

/// `SECRET_KEY`: this node's ed25519 secret key, hex encoded
/// (e.g. generated with `openssl rand -hex 32`).
fn load_signing_key(var: impl Fn(&str) -> Option<String>) -> Result<SigningKey, ConfigError> {
    let value = var("SECRET_KEY").ok_or(ConfigError::Missing("SECRET_KEY"))?;
    Ok(SigningKey::from_bytes(&decode_key(
        value.trim(),
        "SECRET_KEY",
    )?))
}

/// `AUTHORIZED_VALIDATORS`: comma separated hex public keys, in the same order
/// on every node (the order defines the proposer rotation).
fn load_authorized_validators(
    var: impl Fn(&str) -> Option<String>,
) -> Result<Vec<VerifyingKey>, ConfigError> {
    const NAME: &str = "AUTHORIZED_VALIDATORS";
    let value = var(NAME).ok_or(ConfigError::Missing(NAME))?;
    value
        .split(',')
        .map(|key| {
            let bytes = decode_key(key.trim(), NAME)?;
            VerifyingKey::from_bytes(&bytes).map_err(|_| ConfigError::InvalidPublicKey(NAME))
        })
        .collect()
}

/// `PORT`: HTTP + WebSocket port, `DEFAULT_PORT` when unset.
fn load_port(var: impl Fn(&str) -> Option<String>) -> Result<u16, ConfigError> {
    match var("PORT") {
        Some(value) => value
            .trim()
            .parse()
            .map_err(|_| ConfigError::InvalidPort(value)),
        None => Ok(DEFAULT_PORT),
    }
}

fn decode_key(value: &str, name: &'static str) -> Result<[u8; 32], ConfigError> {
    let mut key = [0; 32];
    hex::decode_to_slice(value, &mut key).map_err(|_| ConfigError::InvalidHex(name))?;
    Ok(key)
}

/// State shared by the API, the peer connections and the validator.
/// Locks are always taken in this order: `chain`, then `mempool`.
/// The authorized validators live in `chain` (`Chain::validators`).
struct NodeState {
    chain: Mutex<Chain>,
    mempool: Mutex<Mempool>,
    metrics: Metrics,
    signing_key: SigningKey,
    /// Serialized `NodeMessage`s sent to every connected peer.
    peers: broadcast::Sender<String>,
}

impl NodeState {
    fn new(
        signing_key: SigningKey,
        authorized_validators: Vec<VerifyingKey>,
    ) -> Result<Self, ChainError> {
        Ok(Self {
            chain: Mutex::new(Chain::new(authorized_validators)?),
            mempool: Mutex::new(Mempool::new()),
            metrics: Metrics::new(),
            signing_key,
            peers: broadcast::channel(PEER_QUEUE).0,
        })
    }

    /// Sends the message to every connected peer (no-op without peers).
    fn broadcast(&self, message: &NodeMessage) {
        match serde_json::to_string(message) {
            Ok(text) => {
                let _ = self.peers.send(text);
            }
            Err(error) => tracing::error!("could not serialize peer message: {error}"),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Optional `.env` (see `.env.example`); variables already set take precedence.
    let _ = dotenvy::dotenv();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let var = |name: &str| std::env::var(name).ok();
    let signing_key = load_signing_key(var)?;
    tracing::info!(
        "node public key: {}",
        hex::encode(signing_key.verifying_key().as_bytes())
    );
    let authorized_validators = load_authorized_validators(var)?;
    let port = load_port(var)?;
    let state = Arc::new(NodeState::new(signing_key, authorized_validators)?);

    // run validator
    tokio::spawn(
        Validator {
            state: Arc::clone(&state),
            block_time: BLOCK_TIME,
        }
        .run(),
    );

    // build our application with some routes
    let app = Router::new()
        .route("/ws", any(ws_handler))
        .route("/transactions", post(api::submit_transactions))
        .route("/metrics", get(api::metrics))
        .with_state(Arc::clone(&state))
        // logging so we can see what's going on
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true)),
        );

    // run it with hyper
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, port)).await?;
    let port = listener.local_addr()?.port();
    tracing::info!("listening on port {port}");
    let node_id = format!(
        "{:x}-{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
        std::process::id(),
    );
    tokio::spawn(async move {
        if let Err(error) = discovery::discover_peers(node_id, port, state).await {
            tracing::error!("discovery failed: {error}");
        }
    });
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

/// The handler for the HTTP request (this gets called when the HTTP request lands at the start
/// of websocket negotiation). After this completes, the actual switching from HTTP to
/// websocket protocol will occur.
/// This is the last point where we can extract TCP/IP metadata such as IP address of the client
/// as well as things from HTTP headers such as user-agent of the browser etc.
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<NodeState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, addr, state))
}

/// Handle one incoming peer connection until it disconnects:
/// forwards gossip to the peer and handles the messages it sends.
async fn handle_socket(socket: WebSocket, who: SocketAddr, state: Arc<NodeState>) {
    tracing::info!("incoming peer connected: {who}");
    let (sink, mut stream) = socket.split();
    let mut writer = p2p::spawn_writer(&state, who.to_string(), sink, |text| {
        Message::Text(text.into())
    });
    loop {
        tokio::select! {
            incoming = stream.next() => match incoming {
                Some(Ok(Message::Text(text))) => p2p::handle_message(&state, text.as_str()).await,
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(error)) => {
                    tracing::warn!("connection error from {who}: {error}");
                    break;
                }
                Some(Ok(_)) => {}
            },
            _ = &mut writer => break,
        }
    }
    writer.abort();
    tracing::info!("peer disconnected: {who}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn public_hex(seed: u8) -> String {
        hex::encode(
            SigningKey::from_bytes(&[seed; 32])
                .verifying_key()
                .as_bytes(),
        )
    }

    #[test]
    fn signing_key_is_loaded_from_hex() {
        let key = load_signing_key(vars(&[("SECRET_KEY", &"07".repeat(32))])).unwrap();
        assert_eq!(key.to_bytes(), [7; 32]);
    }

    #[test]
    fn missing_or_malformed_signing_key_is_rejected() {
        assert!(matches!(
            load_signing_key(vars(&[])),
            Err(ConfigError::Missing("SECRET_KEY"))
        ));
        for value in ["07", &"zz".repeat(32), &"+7".repeat(32)] {
            assert!(matches!(
                load_signing_key(vars(&[("SECRET_KEY", value)])),
                Err(ConfigError::InvalidHex("SECRET_KEY"))
            ));
        }
    }

    #[test]
    fn validators_keep_their_order() {
        let list = format!("{}, {}", public_hex(2), public_hex(1));
        let validators =
            load_authorized_validators(vars(&[("AUTHORIZED_VALIDATORS", &list)])).unwrap();
        assert_eq!(
            validators,
            [
                SigningKey::from_bytes(&[2; 32]).verifying_key(),
                SigningKey::from_bytes(&[1; 32]).verifying_key(),
            ]
        );
        assert!(matches!(
            load_authorized_validators(vars(&[])),
            Err(ConfigError::Missing("AUTHORIZED_VALIDATORS"))
        ));
        assert!(matches!(
            load_authorized_validators(vars(&[("AUTHORIZED_VALIDATORS", "abc")])),
            Err(ConfigError::InvalidHex("AUTHORIZED_VALIDATORS"))
        ));
    }

    #[test]
    fn port_defaults_to_the_client_port() {
        assert_eq!(load_port(vars(&[])).unwrap(), DEFAULT_PORT);
        assert_eq!(load_port(vars(&[("PORT", "8002")])).unwrap(), 8002);
        assert!(matches!(
            load_port(vars(&[("PORT", "http")])),
            Err(ConfigError::InvalidPort(_))
        ));
    }
}
