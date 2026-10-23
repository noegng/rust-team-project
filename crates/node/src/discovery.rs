use std::{collections::HashSet, sync::Arc, time::Duration};

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};

use crate::{NodeState, p2p};

const SERVICE_TYPE: &str = "_authora._tcp.local.";

pub(crate) async fn discover_peers(
    node_id: String,
    port: u16,
    state: Arc<NodeState>,
) -> anyhow::Result<()> {
    let mdns = ServiceDaemon::new()?;
    let hostname = format!("node-{node_id}.local.");

    let service = ServiceInfo::new(
        SERVICE_TYPE,
        &node_id,
        &hostname,
        "",
        port,
        &[("node_id", node_id.as_str())][..],
    )?
    .enable_addr_auto();

    mdns.register(service)?;
    let events = mdns.browse(SERVICE_TYPE)?;
    let mut peers = HashSet::new();

    while let Ok(event) = events.recv_async().await {
        let ServiceEvent::ServiceResolved(peer) = event else {
            continue;
        };
        let Some(peer_id) = peer.get_property_val_str("node_id") else {
            continue;
        };
        // Only the smaller ID initiates, so each pair has one connection.
        if node_id.as_str() >= peer_id || !peers.insert(peer_id.to_owned()) {
            continue;
        }

        let urls: Vec<_> = peer
            .get_addresses_v4()
            .into_iter()
            .map(|ip| format!("ws://{ip}:{}/ws", peer.get_port()))
            .collect();
        tokio::spawn(connect_peer(urls, Arc::clone(&state)));
    }

    Ok(())
}

async fn connect_peer(urls: Vec<String>, state: Arc<NodeState>) {
    loop {
        for url in &urls {
            let Ok(Ok((socket, _))) = tokio::time::timeout(
                Duration::from_secs(5),
                tokio_tungstenite::connect_async(url.as_str()),
            )
            .await
            else {
                continue;
            };

            println!("Connected to peer: {url}");
            p2p::handle_peer(socket, Arc::clone(&state)).await;
            println!("Peer disconnected: {url}");
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
