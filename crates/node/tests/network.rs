//! Integration test of the final demo, through the public API only:
//! three validators start, a client sends TXs to node A, the TXs spread by
//! gossip, the validators take turns producing signed blocks, and every node
//! ends with the same chain and an empty mempool.

use std::time::Duration;

use node::testnet::{Load, Testnet, send_load, sender_key};

const TXS: u64 = 500;

#[tokio::test]
async fn three_validators_converge_on_the_same_chain() {
    let network = Testnet::spawn(3, Duration::from_millis(100))
        .await
        .expect("three nodes start");

    let report = send_load(
        &network.http_url(0),
        Load {
            sender: sender_key(1),
            count: TXS,
            batch_size: 50,
            forged_every: None,
            per_second: None,
        },
    )
    .await;
    assert_eq!(
        report.accepted, TXS,
        "node A accepts every valid TX: {report:?}"
    );

    network
        .wait_until_synced(TXS, Duration::from_secs(30))
        .await
        .expect("all nodes include every TX in the same chain");

    for node in 0..network.len() {
        let stats = network.stats(node).await;
        assert_eq!(stats.txs_received, TXS, "node {node} received every TX");
        assert_eq!(stats.txs_rejected, 0);
        assert_eq!(stats.sync_resets, 0, "no conflicting history");
    }
}
