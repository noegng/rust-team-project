use axum::{extract::WebSocketUpgrade, response::IntoResponse};
use blockchain::{block::Block, transaction::Transaction};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum Message {
    NewTx(Transaction),
    NewBlock(Block),
    Ping,
}

async fn peer_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_peer_connection)
}
