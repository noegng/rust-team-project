use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use blockchain::block::Block;
use serde::Deserialize;

use crate::errors::ClientError;

#[cfg(test)]
#[path = "confirmation_tests.rs"]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Inclusion {
    index: u64,
    block_hash: String,
}

struct NodeProgress {
    url: String,
    next_index: u64,
    remaining: HashSet<String>,
    last_error: Option<String>,
}

pub(crate) struct ConfirmationReport {
    pub confirmed: usize,
    pub total: usize,
    pub elapsed: Duration,
    pub nodes: Vec<(String, usize, Option<String>)>,
}

impl ConfirmationReport {
    pub fn complete(&self) -> bool {
        self.confirmed == self.total
    }
}

/// Read each block only once per node, avoiding one chain scan per transaction.
/// Confirmations concern this run's hashes, not global node counters.
pub(crate) async fn wait_for_confirmations(
    client: &reqwest::Client,
    urls: &[String],
    hashes: &HashSet<String>,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ConfirmationReport, ClientError> {
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut nodes: Vec<_> = urls
        .iter()
        .map(|url| NodeProgress {
            url: url.trim_end_matches('/').to_string(),
            next_index: 1,
            remaining: hashes.clone(),
            last_error: None,
        })
        .collect();
    let mut inclusions = HashMap::new();
    loop {
        if nodes.iter().all(|node| node.remaining.is_empty()) {
            break;
        }
        match tokio::time::timeout_at(deadline, poll_nodes(client, &mut nodes, &mut inclusions))
            .await
        {
            Ok(result) => result?,
            Err(_) => break,
        }
        if nodes.iter().all(|node| node.remaining.is_empty()) {
            break;
        }
        if tokio::time::timeout_at(deadline, tokio::time::sleep(poll_interval))
            .await
            .is_err()
        {
            break;
        }
    }
    let missing: HashSet<_> = nodes.iter().flat_map(|node| &node.remaining).collect();
    Ok(ConfirmationReport {
        confirmed: hashes.len() - missing.len(),
        total: hashes.len(),
        elapsed: started.elapsed(),
        nodes: nodes
            .into_iter()
            .map(|node| {
                (
                    node.url,
                    hashes.len() - node.remaining.len(),
                    node.last_error,
                )
            })
            .collect(),
    })
}

#[derive(Deserialize)]
struct Metrics {
    height: u64,
}

async fn poll_nodes(
    client: &reqwest::Client,
    nodes: &mut [NodeProgress],
    inclusions: &mut HashMap<String, Inclusion>,
) -> Result<(), ClientError> {
    for node in nodes {
        if node.remaining.is_empty() {
            continue;
        }
        let metrics: Metrics = match get_json(client, &format!("{}/metrics", node.url)).await {
            Ok(metrics) => metrics,
            Err(error) => {
                node.last_error = Some(error.to_string());
                continue;
            }
        };
        node.last_error = None;
        while node.next_index <= metrics.height && !node.remaining.is_empty() {
            let block: Block =
                match get_json(client, &format!("{}/blocks/{}", node.url, node.next_index)).await {
                    Ok(block) => block,
                    Err(error) => {
                        node.last_error = Some(error.to_string());
                        break;
                    }
                };
            if block.block_data.index != node.next_index {
                return Err(ClientError::InvalidConfirmation(format!(
                    "{} returned the wrong block index",
                    node.url
                )));
            }
            block
                .verify_hashes()
                .map_err(|error| ClientError::InvalidConfirmation(error.to_string()))?;
            block
                .verify_signature()
                .map_err(|error| ClientError::InvalidConfirmation(error.to_string()))?;
            for tx in &block.block_data.transactions {
                let hash = tx.calculate_hash()?;
                if !node.remaining.contains(&hash) {
                    continue;
                }
                let inclusion = Inclusion {
                    index: node.next_index,
                    block_hash: block.block_data.hash.clone(),
                };
                if let Some(expected) = inclusions.get(&hash) {
                    if expected != &inclusion {
                        return Err(ClientError::InvalidConfirmation(format!(
                            "transaction {hash} is in different blocks: {}:{} versus {}:{} on {}",
                            expected.index,
                            expected.block_hash,
                            inclusion.index,
                            inclusion.block_hash,
                            node.url
                        )));
                    }
                } else {
                    inclusions.insert(hash.clone(), inclusion);
                }
                node.remaining.remove(&hash);
            }
            node.next_index += 1;
        }
    }
    Ok(())
}

async fn get_json<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
) -> Result<T, reqwest::Error> {
    client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
}
