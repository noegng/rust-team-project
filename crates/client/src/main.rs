mod confirmation;
mod errors;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{collections::HashSet, sync::Arc};
use tokio::sync::Mutex;

use blockchain::transaction::Transaction;
use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::Deserialize;

use crate::errors::ClientError;

/// Benchmark : envoie `TX_COUNT` TX signées sur `NODE_URL` avec `CONCURRENCY`
/// tâches en parallèle, puis affiche le TPS côté client et `GET /metrics`
/// de chaque node de `METRICS_URLS` (séparées par des virgules).
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env(|name| std::env::var(name).ok())?;
    let counters = Arc::new(Counters::default());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let recipient = random_key()?.verifying_key();

    println!(
        "sending {} TX to {} with {} tasks",
        config.tx_count, config.node_url, config.concurrency
    );
    let started = Instant::now();
    let handles: Vec<_> = split_work(config.tx_count, config.concurrency)
        .into_iter()
        .map(|count| {
            let client = client.clone();
            let url = format!("{}/transactions", config.node_url.trim_end_matches('/'));
            let counters = Arc::clone(&counters);
            tokio::spawn(async move { spam(client, url, recipient, count, counters).await })
        })
        .collect();
    let mut failed_workers = 0;
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                failed_workers += 1;
                eprintln!("worker stopped: {err}");
            }
            Err(err) => {
                failed_workers += 1;
                eprintln!("worker failed: {err}");
            }
        }
    }
    let elapsed = started.elapsed().as_secs_f64();

    let accepted = counters.accepted.load(Ordering::Relaxed);
    let rejected = counters.rejected.load(Ordering::Relaxed);
    println!("accepted: {accepted}, rejected: {rejected}, elapsed: {elapsed:.2}s");
    println!(
        "client TPS: {:.0}",
        accepted as f64 / elapsed.max(f64::EPSILON)
    );

    let hashes = counters.hashes.lock().await.clone();
    println!(
        "waiting up to {}s for inclusion on {} nodes",
        config.confirm_timeout_secs,
        config.metrics_urls.len()
    );
    let confirmations = confirmation::wait_for_confirmations(
        &client,
        &config.metrics_urls,
        &hashes,
        Duration::from_secs(config.confirm_timeout_secs),
        Duration::from_millis(config.poll_interval_ms),
    )
    .await;
    if let Ok(report) = &confirmations {
        println!(
            "confirmed everywhere: {}/{}, pending: {}, confirmation wait: {:.2}s, total: {:.2}s",
            report.confirmed,
            report.total,
            report.total - report.confirmed,
            report.elapsed.as_secs_f64(),
            started.elapsed().as_secs_f64()
        );
        for (url, confirmed, error) in &report.nodes {
            println!("{url}: confirmed {confirmed}/{}", report.total);
            if let Some(error) = error {
                eprintln!("{url}: last confirmation request failed: {error}");
            }
        }
    }
    for url in &config.metrics_urls {
        match fetch_metrics(&client, url).await {
            Ok(metrics) => println!("{url}: {metrics}"),
            Err(err) => eprintln!("{url}: {err}"),
        }
    }
    let report = confirmations?;
    anyhow::ensure!(
        report.complete(),
        "confirmation deadline reached: {} transactions still await matching inclusion on all nodes",
        report.total - report.confirmed
    );
    anyhow::ensure!(
        failed_workers == 0 && rejected == 0 && accepted == config.tx_count,
        "benchmark incomplete: {accepted}/{} accepted, {rejected} rejected, {failed_workers} failed workers",
        config.tx_count
    );
    Ok(())
}

#[derive(Debug, PartialEq)]
struct Config {
    node_url: String,
    tx_count: u64,
    concurrency: u64,
    metrics_urls: Vec<String>,
    confirm_timeout_secs: u64,
    poll_interval_ms: u64,
}

impl Config {
    fn from_env(var: impl Fn(&str) -> Option<String>) -> Result<Self, ClientError> {
        let node_url = var("NODE_URL").unwrap_or_else(|| "http://127.0.0.1:3000".into());
        let tx_count = parse_number(&var, "TX_COUNT", 10_000)?;
        let concurrency = parse_number(&var, "CONCURRENCY", 50)?;
        let confirm_timeout_secs = parse_number(&var, "CONFIRM_TIMEOUT_SECS", 60)?;
        let poll_interval_ms = parse_number(&var, "POLL_INTERVAL_MS", 500)?;
        for (name, value) in [
            ("CONFIRM_TIMEOUT_SECS", confirm_timeout_secs),
            ("POLL_INTERVAL_MS", poll_interval_ms),
        ] {
            if value == 0 {
                return Err(ClientError::InvalidConfig {
                    name,
                    value: "0".into(),
                });
            }
        }
        if concurrency == 0 {
            return Err(ClientError::InvalidConfig {
                name: "CONCURRENCY",
                value: "0".into(),
            });
        }
        let mut metrics_urls: Vec<String> = match var("METRICS_URLS") {
            Some(urls) => urls.split(',').map(|url| url.trim().to_string()).collect(),
            None => vec![node_url.clone()],
        };
        if metrics_urls.iter().any(|url| url.is_empty()) {
            return Err(ClientError::InvalidConfig {
                name: "METRICS_URLS",
                value: metrics_urls.join(","),
            });
        }
        let mut seen = HashSet::new();
        metrics_urls.retain(|url| seen.insert(url.trim_end_matches('/').to_string()));
        Ok(Self {
            node_url,
            tx_count,
            concurrency,
            metrics_urls,
            confirm_timeout_secs,
            poll_interval_ms,
        })
    }
}

fn parse_number(
    var: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: u64,
) -> Result<u64, ClientError> {
    match var(name) {
        Some(value) => value
            .parse()
            .map_err(|_| ClientError::InvalidConfig { name, value }),
        None => Ok(default),
    }
}

#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    rejected: AtomicU64,
    hashes: Mutex<HashSet<String>>,
}

/// Admission results returned by the node, in the submitted order.
#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum TransactionResult {
    Accepted {
        hash: String,
    },
    Rejected {
        #[serde(rename = "error")]
        _error: String,
    },
}

/// Une tâche = un expéditeur avec sa propre clé : ses nonces partent de 0
/// et arrivent dans l'ordre sur le node.
async fn spam(
    client: reqwest::Client,
    url: String,
    recipient: VerifyingKey,
    count: u64,
    counters: Arc<Counters>,
) -> Result<(), ClientError> {
    let sender = random_key()?;
    for nonce in 0..count {
        let tx = Transaction::new(&recipient, 1, nonce, unix_secs(), &sender)?;
        let expected_hash = tx.calculate_hash()?;
        // The API accepts a batch; start with one transaction per request.
        let response = client.post(&url).json(&[tx]).send().await?;
        if !response.status().is_success() {
            counters.rejected.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let results: Vec<TransactionResult> = response.json().await?;
        if results.len() != 1 {
            return Err(ClientError::InvalidResponseCount {
                actual: results.len(),
            });
        }
        let counter = match &results[0] {
            TransactionResult::Accepted { hash } => {
                if hash != &expected_hash {
                    return Err(ClientError::InvalidTransactionHash);
                }
                counters.hashes.lock().await.insert(hash.clone());
                &counters.accepted
            }
            TransactionResult::Rejected { .. } => &counters.rejected,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

async fn fetch_metrics(
    client: &reqwest::Client,
    node_url: &str,
) -> Result<serde_json::Value, ClientError> {
    let response = client
        .get(format!("{}/metrics", node_url.trim_end_matches('/')))
        .send()
        .await?;
    Ok(response.error_for_status()?.json().await?)
}

/// Répartit `total` TX entre `workers` tâches (les premières reçoivent le reste).
fn split_work(total: u64, workers: u64) -> Vec<u64> {
    (0..workers)
        .map(|worker| total / workers + u64::from(worker < total % workers))
        .filter(|&count| count > 0)
        .collect()
}

fn random_key() -> Result<SigningKey, ClientError> {
    let mut secret = [0; 32];
    getrandom::fill(&mut secret).map_err(ClientError::KeyGeneration)?;
    Ok(SigningKey::from_bytes(&secret))
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, http::StatusCode, routing::post};
    use std::collections::HashMap;

    /// Exercise the actual HTTP encoding and response handling against the API contract.
    async fn run_with_response(
        status: StatusCode,
        body: serde_json::Value,
    ) -> (Result<(), ClientError>, Arc<Counters>) {
        let app = Router::new().route(
            "/transactions",
            post(move |Json(txs): Json<Vec<Transaction>>| async move {
                assert_eq!(txs.len(), 1);
                assert_eq!(txs[0].nonce, 0);
                txs[0].verify().unwrap();
                let mut body = body;
                // A successful admission must return the submitted transaction's hash.
                if body == serde_json::json!([{"status": "accepted", "hash": "abc"}]) {
                    body[0]["hash"] = txs[0].calculate_hash().unwrap().into();
                }
                (status, Json(body))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/transactions", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let counters = Arc::new(Counters::default());
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let result = spam(
            client,
            url,
            random_key().unwrap().verifying_key(),
            1,
            Arc::clone(&counters),
        )
        .await;
        server.abort();
        (result, counters)
    }

    #[tokio::test]
    async fn counts_admission_results_in_http_200_responses() {
        for (body, accepted, rejected) in [
            (
                serde_json::json!([{"status": "accepted", "hash": "abc"}]),
                1,
                0,
            ),
            (
                serde_json::json!([{"status": "rejected", "error": "transaction already seen"}]),
                0,
                1,
            ),
        ] {
            let (result, counters) = run_with_response(StatusCode::OK, body).await;
            result.unwrap();
            assert_eq!(counters.accepted.load(Ordering::Relaxed), accepted);
            assert_eq!(counters.rejected.load(Ordering::Relaxed), rejected);
        }
    }

    #[tokio::test]
    async fn invalid_responses_are_not_counted_as_accepted() {
        for body in [
            serde_json::json!([]),
            serde_json::json!([
                {"status": "accepted", "hash": "abc"},
                {"status": "accepted", "hash": "def"}
            ]),
            serde_json::json!([{"status": "unknown"}]),
        ] {
            let (result, counters) = run_with_response(StatusCode::OK, body).await;
            assert!(result.is_err());
            assert_eq!(counters.accepted.load(Ordering::Relaxed), 0);
            assert_eq!(counters.rejected.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn unsuccessful_http_status_is_counted_as_rejected() {
        let (result, counters) = run_with_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error": "unavailable"}),
        )
        .await;
        result.unwrap();
        assert_eq!(counters.accepted.load(Ordering::Relaxed), 0);
        assert_eq!(counters.rejected.load(Ordering::Relaxed), 1);
    }

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn config_defaults() {
        let config = Config::from_env(vars(&[])).unwrap();
        assert_eq!(
            config,
            Config {
                node_url: "http://127.0.0.1:3000".into(),
                tx_count: 10_000,
                concurrency: 50,
                metrics_urls: vec!["http://127.0.0.1:3000".into()],
                confirm_timeout_secs: 60,
                poll_interval_ms: 500,
            }
        );
    }

    #[test]
    fn config_from_vars() {
        let config = Config::from_env(vars(&[
            ("NODE_URL", "http://a:8001"),
            ("TX_COUNT", "100"),
            ("CONCURRENCY", "4"),
            ("METRICS_URLS", "http://a:8001, http://b:8002"),
            ("CONFIRM_TIMEOUT_SECS", "20"),
            ("POLL_INTERVAL_MS", "100"),
        ]))
        .unwrap();
        assert_eq!(config.tx_count, 100);
        assert_eq!(config.concurrency, 4);
        assert_eq!(config.metrics_urls, ["http://a:8001", "http://b:8002"]);
        assert_eq!(config.confirm_timeout_secs, 20);
        assert_eq!(config.poll_interval_ms, 100);
    }

    #[test]
    fn invalid_config_is_rejected() {
        for name in ["CONFIRM_TIMEOUT_SECS", "POLL_INTERVAL_MS"] {
            assert!(Config::from_env(vars(&[(name, "0")])).is_err());
        }
        assert!(Config::from_env(vars(&[("METRICS_URLS", "")])).is_err());
        assert!(Config::from_env(vars(&[("METRICS_URLS", "http://a:3000,")])).is_err());
        assert!(matches!(
            Config::from_env(vars(&[("TX_COUNT", "lots")])),
            Err(ClientError::InvalidConfig {
                name: "TX_COUNT",
                ..
            })
        ));
        assert!(matches!(
            Config::from_env(vars(&[("CONCURRENCY", "0")])),
            Err(ClientError::InvalidConfig {
                name: "CONCURRENCY",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn wrong_admission_hash_is_not_tracked_as_accepted() {
        let (result, counters) = run_with_response(
            StatusCode::OK,
            serde_json::json!([{"status": "accepted", "hash": "wrong"}]),
        )
        .await;
        assert!(matches!(result, Err(ClientError::InvalidTransactionHash)));
        assert_eq!(counters.accepted.load(Ordering::Relaxed), 0);
        assert!(counters.hashes.lock().await.is_empty());
    }

    #[test]
    fn work_is_split_without_loss() {
        assert_eq!(split_work(10, 3), [4, 3, 3]);
        assert_eq!(split_work(2, 5), [1, 1]);
        assert_eq!(split_work(10_000, 50).iter().sum::<u64>(), 10_000);
    }

    #[test]
    fn random_keys_sign_valid_transactions() {
        let sender = random_key().unwrap();
        assert_ne!(sender.to_bytes(), random_key().unwrap().to_bytes());
        let tx = Transaction::new(&sender.verifying_key(), 1, 0, unix_secs(), &sender).unwrap();
        tx.verify().unwrap();
    }
}
