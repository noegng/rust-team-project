use std::{collections::HashSet, net::Ipv4Addr, sync::Arc, time::Duration};

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use tracing::Instrument;

use crate::{NodeState, p2p};

const SERVICE_TYPE: &str = "_authora._tcp.local.";
const RETRY_DELAY: Duration = Duration::from_secs(2);

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
        let mut ips: Vec<Ipv4Addr> = peer
            .get_addresses_v4()
            .into_iter()
            .map(|ip| ip.to_owned())
            .filter(is_peer_reachable)
            .collect();
        ips.sort();
        if peer_id != node_id {
            let http = ips
                .iter()
                .map(|ip| format!("http://{ip}:{}", peer.get_port()))
                .collect();
            state.record_peer(peer_id.to_owned(), http);
        }
        // Only the smaller ID initiates, so each pair has one connection.
        if ips.is_empty() || node_id.as_str() >= peer_id || !peers.insert(peer_id.to_owned()) {
            continue;
        }

        let urls: Vec<_> = ips
            .iter()
            .map(|ip| format!("ws://{ip}:{}/ws", peer.get_port()))
            .collect();
        tokio::spawn(connect_peer(urls, Arc::clone(&state), RETRY_DELAY));
    }

    Ok(())
}

/// Addresses another machine may also have, so dialing them can reach this node:
/// `192.0.0.0/24` (e.g. the IPv4 address every client of an iPhone hotspot gets
/// on an IPv6-only carrier) and link-local `169.254.0.0/16`.
fn is_peer_reachable(ip: &Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_link_local() || ip.is_unspecified() || (a, b, c) == (192, 0, 0))
}

/// Dials `urls` forever: after a disconnect (including a sync reset),
/// waits `retry` then reconnects, which replays both histories.
/// An address that leads back to this node is dropped; stops when none is left.
pub(crate) async fn connect_peer(mut urls: Vec<String>, state: Arc<NodeState>, retry: Duration) {
    while !urls.is_empty() {
        for index in 0..urls.len() {
            let url = urls[index].clone();
            let Ok(Ok((socket, _))) = tokio::time::timeout(
                Duration::from_secs(5),
                tokio_tungstenite::connect_async(url.as_str()),
            )
            .await
            else {
                continue;
            };

            tracing::info!("connected to peer: {url}");
            let exit = p2p::run_peer(socket, Arc::clone(&state))
                .instrument(tracing::info_span!("peer", %url))
                .await;
            if exit == p2p::PeerExit::Ourselves {
                tracing::warn!("{url} leads back to this node; no longer dialing it");
                urls.remove(index);
            } else {
                tracing::info!("peer disconnected: {url}");
            }
            break;
        }
        tokio::time::sleep(retry).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_and_link_local_addresses_are_not_dialed() {
        assert!(is_peer_reachable(&Ipv4Addr::new(172, 23, 76, 199)));
        assert!(is_peer_reachable(&Ipv4Addr::new(192, 168, 1, 10)));
        assert!(is_peer_reachable(&Ipv4Addr::LOCALHOST));
        assert!(!is_peer_reachable(&Ipv4Addr::new(192, 0, 0, 2)));
        assert!(!is_peer_reachable(&Ipv4Addr::new(169, 254, 170, 227)));
    }
}
