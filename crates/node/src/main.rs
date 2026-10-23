mod api;
mod discovery;
mod metrics;
mod validator;

use std::{net::SocketAddr, sync::Arc};

use axum::{
    Router,
    extract::{
        ConnectInfo, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::IntoResponse,
    routing::{any, post},
};
use blockchain::{block::Block, chain::Chain, mempool::Mempool, transaction::Transaction};
use ed25519_dalek::{SecretKey, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::validator::Validator;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum NodeMessage {
    NewTx(Transaction),
    NewBlock(Block),
    Ping,
}

fn load_signing_key() -> anyhow::Result<SigningKey> {
    let encoded = std::env::var("SECRET_KEY").expect("qsdqsdd");
    let mut secret = [0u8; 32];
    hex::decode_to_slice(encoded.trim(), &mut secret)?;

    Ok(SigningKey::from_bytes(&secret))
}

fn load_authorized_validators() -> anyhow::Result<Vec<VerifyingKey>> {
    let encoded = std::env::var("AUTHORIZED_VALIDATORS")?;

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
    authorized_validators: Vec<VerifyingKey>,
}

#[tokio::main]
async fn main() {
    let signing_key = load_signing_key().unwrap();
    let public_key = signing_key.verifying_key();

    println!("Public key: {}", hex::encode(public_key.to_bytes()));

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // run validator
    let signing_key = load_signing_key().unwrap();
    let authorized_validators = load_authorized_validators().unwrap();

    let state = Arc::new(NodeState {
        chain: Mutex::new(Chain::new(authorized_validators.clone()).unwrap()),
        mempool: Mutex::new(Mempool::new()),
        signing_key,
        authorized_validators,
    });

    // build our application with some routes
    let app = Router::new()
        .route("/ws", any(ws_handler))
        .route("/transactions", post(api::submit_transactions))
        .with_state(state)
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
        if let Err(error) = discovery::discover_peers(node_id, port).await {
            eprintln!("Discovery failed: {error}");
        }
    });
    let _ = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await;
}

/// The handler for the HTTP request (this gets called when the HTTP request lands at the start
/// of websocket negotiation). After this completes, the actual switching from HTTP to
/// websocket protocol will occur.
/// This is the last point where we can extract TCP/IP metadata such as IP address of the client
/// as well as things from HTTP headers such as user-agent of the browser etc.
async fn ws_handler(
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, addr))
}

/// Handle one incoming peer connection until it disconnects.
async fn handle_socket(mut socket: WebSocket, who: SocketAddr) {
    println!("Incoming peer connected: {who}");
    if socket
        .send(Message::Text("Hello from this node".into()))
        .await
        .is_err()
    {
        return;
    }

    while let Some(message) = socket.recv().await {
        match message {
            Ok(Message::Text(text)) => println!("From {who}: {text}"),
            Ok(Message::Close(_)) => break,
            Err(error) => {
                eprintln!("Connection error from {who}: {error}");
                break;
            }
            _ => {}
        }
    }
    println!("Peer disconnected: {who}");
}
