mod api;
mod discovery;
mod metrics;
mod validator;

use std::net::SocketAddr;

use axum::{
    Router,
    extract::{
        ConnectInfo, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::IntoResponse,
    routing::any,
};
use blockchain::{block::Block, transaction::Transaction};
use serde::{Deserialize, Serialize};
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum NodeMessage {
    NewTx(Transaction),
    NewBlock(Block),
    Ping,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // build our application with some routes
    let app = Router::new()
        .route("/ws", any(ws_handler))
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
