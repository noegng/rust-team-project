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
    routing::{any, post},
};
use blockchain::{
    block::Block,
    chain::{Chain, ChainError},
    mempool::Mempool,
    transaction::Transaction,
};
use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast};
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::{metrics::Metrics, validator::Validator};

/// Interval between two block production attempts.
const BLOCK_TIME: Duration = Duration::from_secs(5);
/// Messages buffered per peer before the slowest ones start being dropped.
const PEER_QUEUE: usize = 16_384;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum NodeMessage {
    NewTx(Transaction),
    NewBlock(Block),
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
}

/// `NODE_SIGNING_KEY`: this node's ed25519 secret key, hex encoded
/// (e.g. generated with `openssl rand -hex 32`).
fn load_signing_key(var: impl Fn(&str) -> Option<String>) -> Result<SigningKey, ConfigError> {
    let value = var("NODE_SIGNING_KEY").ok_or(ConfigError::Missing("NODE_SIGNING_KEY"))?;
    Ok(SigningKey::from_bytes(&decode_key(
        value.trim(),
        "NODE_SIGNING_KEY",
    )?))
}

/// `VALIDATORS`: comma separated hex public keys, in the same order on every
/// node (the order defines the proposer rotation).
fn load_authorized_validators(
    var: impl Fn(&str) -> Option<String>,
) -> Result<Vec<VerifyingKey>, ConfigError> {
    let value = var("VALIDATORS").ok_or(ConfigError::Missing("VALIDATORS"))?;
    value
        .split(',')
        .map(|key| {
            let bytes = decode_key(key.trim(), "VALIDATORS")?;
            VerifyingKey::from_bytes(&bytes)
                .map_err(|_| ConfigError::InvalidPublicKey("VALIDATORS"))
        })
        .collect()
}

fn decode_key(value: &str, name: &'static str) -> Result<[u8; 32], ConfigError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ConfigError::InvalidHex(name));
    }
    let mut key = [0; 32];
    for (byte, pair) in key.iter_mut().zip(value.as_bytes().chunks(2)) {
        let pair = std::str::from_utf8(pair).map_err(|_| ConfigError::InvalidHex(name))?;
        *byte = u8::from_str_radix(pair, 16).map_err(|_| ConfigError::InvalidHex(name))?;
    }
    Ok(key)
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// State shared by the API, the peer connections and the validator.
/// Locks are always taken in this order: `chain`, then `mempool`.
struct NodeState {
    chain: Mutex<Chain>,
    mempool: Mutex<Mempool>,
    metrics: Metrics,
    signing_key: SigningKey,
    authorized_validators: Vec<VerifyingKey>,
    /// Serialized `NodeMessage`s sent to every connected peer.
    peers: broadcast::Sender<String>,
}

impl NodeState {
    fn new(
        signing_key: SigningKey,
        authorized_validators: Vec<VerifyingKey>,
    ) -> Result<Self, ChainError> {
        Ok(Self {
            chain: Mutex::new(Chain::new(authorized_validators.clone())?),
            mempool: Mutex::new(Mempool::new()),
            metrics: Metrics::new(),
            signing_key,
            authorized_validators,
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
        to_hex(signing_key.verifying_key().as_bytes())
    );
    let authorized_validators = load_authorized_validators(var)?;
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
        .with_state(Arc::clone(&state))
        // logging so we can see what's going on
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true)),
        );

    // run it with hyper
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".into())
        .parse()
        .expect("PORT must be a valid port number");
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, port))
        .await
        .unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    let port = listener.local_addr().unwrap().port();
    let node_id = format!(
        "{:x}-{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        std::process::id(),
    );
    tokio::spawn(async move {
        if let Err(error) = discovery::discover_peers(node_id, port, state).await {
            eprintln!("Discovery failed: {error}");
        }
    });
    let _ = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await;
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
async fn handle_socket(mut socket: WebSocket, who: SocketAddr, state: Arc<NodeState>) {
    println!("Incoming peer connected: {who}");
    let peer = who.to_string();
    let mut outgoing = state.peers.subscribe();
    loop {
        tokio::select! {
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Text(text))) => p2p::handle_message(&state, text.as_str()).await,
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(error)) => {
                    eprintln!("Connection error from {who}: {error}");
                    break;
                }
                Some(Ok(_)) => {}
            },
            message = p2p::next_outgoing(&mut outgoing, &peer) => match message {
                Some(text) => {
                    if socket.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                None => break,
            },
        }
    }
    println!("Peer disconnected: {who}");
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
        to_hex(
            SigningKey::from_bytes(&[seed; 32])
                .verifying_key()
                .as_bytes(),
        )
    }

    #[test]
    fn signing_key_is_loaded_from_hex() {
        let key = load_signing_key(vars(&[("NODE_SIGNING_KEY", &"07".repeat(32))])).unwrap();
        assert_eq!(key.to_bytes(), [7; 32]);
    }

    #[test]
    fn missing_or_malformed_signing_key_is_rejected() {
        assert!(matches!(
            load_signing_key(vars(&[])),
            Err(ConfigError::Missing("NODE_SIGNING_KEY"))
        ));
        for value in ["07", &"zz".repeat(32), &"+7".repeat(32)] {
            assert!(matches!(
                load_signing_key(vars(&[("NODE_SIGNING_KEY", value)])),
                Err(ConfigError::InvalidHex("NODE_SIGNING_KEY"))
            ));
        }
    }

    #[test]
    fn validators_keep_their_order() {
        let list = format!("{}, {}", public_hex(2), public_hex(1));
        let validators = load_authorized_validators(vars(&[("VALIDATORS", &list)])).unwrap();
        assert_eq!(
            validators,
            [
                SigningKey::from_bytes(&[2; 32]).verifying_key(),
                SigningKey::from_bytes(&[1; 32]).verifying_key(),
            ]
        );
        assert!(matches!(
            load_authorized_validators(vars(&[])),
            Err(ConfigError::Missing("VALIDATORS"))
        ));
        assert!(matches!(
            load_authorized_validators(vars(&[("VALIDATORS", "abc")])),
            Err(ConfigError::InvalidHex("VALIDATORS"))
        ));
    }
}
