mod confirmation;
mod errors;

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

use blockchain::transaction::Transaction;
use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::Deserialize;

use crate::errors::ClientError;

/// Benchmark : envoie `TX_COUNT` TX signées sur `NODE_URL/transactions`
/// par lots de `BATCH_SIZE`, avec `CONCURRENCY` tâches en parallèle, attend leur
/// confirmation sur tous les nodes de `METRICS_URLS`, puis affiche les métriques.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env(|name| std::env::var(name).ok())?;
    let counters = Arc::new(Counters::default());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let recipient = random_key()?.verifying_key();

    println!(
        "sending {} TX to {} with {} tasks, {} TX per request",
        config.tx_count, config.node_url, config.concurrency, config.batch_size
    );
    let started = Instant::now();
    let progress = tokio::spawn(report_progress(Arc::clone(&counters), started));
    let handles: Vec<_> = split_work(config.tx_count, config.concurrency)
        .into_iter()
        .map(|count| {
            let client = client.clone();
            let url = format!("{}/transactions", config.node_url.trim_end_matches('/'));
            let batch_size = config.batch_size;
            let counters = Arc::clone(&counters);
            tokio::spawn(async move {
                spam(client, url, recipient, count, batch_size, counters, started).await
            })
        })
        .collect();
    let mut failed_workers = 0;
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                failed_workers += 1;
                eprintln!(
                    "[{:.1}s] worker stopped: {err}",
                    started.elapsed().as_secs_f64()
                );
            }
            Err(err) => {
                failed_workers += 1;
                eprintln!(
                    "[{:.1}s] worker failed: {err}",
                    started.elapsed().as_secs_f64()
                );
            }
        }
    }
    progress.abort();
    let elapsed = started.elapsed().as_secs_f64();

    let accepted = counters.accepted.load(Ordering::Relaxed);
    let rejected = counters.rejected.load(Ordering::Relaxed);
    println!(
        "accepted: {accepted}, rejected: {rejected}, failed workers: {failed_workers}, elapsed: {elapsed:.2}s"
    );
    for (kind, count) in counters.rejections() {
        println!("  rejected {count}x: {kind}");
    }
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
    batch_size: u64,
    metrics_urls: Vec<String>,
    confirm_timeout_secs: u64,
    poll_interval_ms: u64,
}

impl Config {
    fn from_env(var: impl Fn(&str) -> Option<String>) -> Result<Self, ClientError> {
        let node_url = var("NODE_URL").unwrap_or_else(|| "http://127.0.0.1:3000".into());
        let tx_count = parse_number(&var, "TX_COUNT", 10_000)?;
        let concurrency = parse_positive(&var, "CONCURRENCY", 50)?;
        let batch_size = parse_positive(&var, "BATCH_SIZE", 100)?;
        let confirm_timeout_secs = parse_positive(&var, "CONFIRM_TIMEOUT_SECS", 60)?;
        let poll_interval_ms = parse_positive(&var, "POLL_INTERVAL_MS", 500)?;
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
            batch_size,
            metrics_urls,
            confirm_timeout_secs,
            poll_interval_ms,
        })
    }
}

fn parse_positive(
    var: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: u64,
) -> Result<u64, ClientError> {
    match parse_number(var, name, default)? {
        0 => Err(ClientError::InvalidConfig {
            name,
            value: "0".into(),
        }),
        value => Ok(value),
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
    rejections: StdMutex<HashMap<String, u64>>,
}

impl Counters {
    fn record_rejection(&self, error: &str, started: Instant) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        let kind = error_kind(error);
        let mut rejections = self.rejections.lock().unwrap_or_else(|e| e.into_inner());
        let count = rejections.entry(kind.to_owned()).or_default();
        if *count == 0 {
            eprintln!(
                "[{:.1}s] new rejection: {error}",
                started.elapsed().as_secs_f64()
            );
        }
        *count += 1;
    }

    fn rejections(&self) -> Vec<(String, u64)> {
        let rejections = self.rejections.lock().unwrap_or_else(|e| e.into_inner());
        let mut rejections: Vec<_> = rejections.iter().map(|(k, v)| (k.clone(), *v)).collect();
        rejections.sort_by_key(|&(_, count)| std::cmp::Reverse(count));
        rejections
    }
}

/// Regroupe les rejets sans le hash final (`"transaction already seen: <hash>"`).
fn error_kind(error: &str) -> &str {
    match error.rsplit_once(": ") {
        Some((kind, hash))
            if hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
        {
            kind
        }
        _ => error,
    }
}

async fn report_progress(counters: Arc<Counters>, started: Instant) {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.tick().await;
    let mut previous = 0;
    loop {
        ticker.tick().await;
        let accepted = counters.accepted.load(Ordering::Relaxed);
        println!(
            "[{:.0}s] accepted {accepted} (+{}/s), rejected {}",
            started.elapsed().as_secs_f64(),
            accepted - previous,
            counters.rejected.load(Ordering::Relaxed),
        );
        previous = accepted;
    }
}

/// Admission results returned by the node, in the submitted order.
#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum TransactionResult {
    Accepted { hash: String },
    Rejected { error: String },
}

/// Une tâche = un expéditeur avec sa propre clé : ses nonces partent de 0
/// et arrivent dans l'ordre sur le node, lot après lot.
async fn spam(
    client: reqwest::Client,
    url: String,
    recipient: VerifyingKey,
    count: u64,
    batch_size: u64,
    counters: Arc<Counters>,
    started: Instant,
) -> Result<(), ClientError> {
    let sender = random_key()?;
    for nonces in nonce_batches(count, batch_size) {
        let len = (nonces.end - nonces.start) as usize;
        let mut batch = Vec::with_capacity(len);
        let mut expected_hashes = Vec::with_capacity(len);
        for nonce in nonces {
            let tx = Transaction::new(&recipient, 1, nonce, unix_millis(), &sender)?;
            expected_hashes.push(tx.calculate_hash()?);
            batch.push(tx);
        }
        let response = client.post(&url).json(&batch).send().await?;
        let status = response.status();
        if !status.is_success() {
            for _ in 0..batch.len() {
                counters.record_rejection(&format!("HTTP {status}"), started);
            }
            continue;
        }
        let results: Vec<TransactionResult> = response.json().await?;
        if results.len() != batch.len() {
            return Err(ClientError::InvalidResponseCount {
                actual: results.len(),
            });
        }
        let mut batch_hashes = Vec::new();
        for (result, expected_hash) in results.iter().zip(&expected_hashes) {
            match result {
                TransactionResult::Accepted { hash } => {
                    if hash != expected_hash {
                        return Err(ClientError::InvalidTransactionHash);
                    }
                    batch_hashes.push(hash.clone());
                }
                TransactionResult::Rejected { error } => {
                    counters.record_rejection(error, started);
                }
            }
        }
        if !batch_hashes.is_empty() {
            let added = batch_hashes.len() as u64;
            counters.hashes.lock().await.extend(batch_hashes);
            counters.accepted.fetch_add(added, Ordering::Relaxed);
        }
    }
    Ok(())
}

/// Nonces `0..count` découpés en lots consécutifs d'au plus `batch_size`.
fn nonce_batches(count: u64, batch_size: u64) -> impl Iterator<Item = Range<u64>> {
    (0..count)
        .step_by(batch_size as usize)
        .map(move |start| start..count.min(start + batch_size))
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

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, http::StatusCode, routing::post};
    use std::collections::HashMap;

    /// Exercise the actual HTTP encoding and response handling against the API contract.
    async fn run_with_response(
        count: u64,
        batch_size: u64,
        status: StatusCode,
        body: serde_json::Value,
    ) -> (Result<(), ClientError>, Arc<Counters>) {
        let app = Router::new().route(
            "/transactions",
            post(move |Json(txs): Json<Vec<Transaction>>| async move {
                assert!(!txs.is_empty());
                for (index, tx) in txs.iter().enumerate() {
                    assert_eq!(tx.nonce, index as u64);
                    tx.verify().unwrap();
                }
                let mut body = body;
                if let Some(items) = body.as_array_mut() {
                    for (item, tx) in items.iter_mut().zip(&txs) {
                        if item["status"] == "accepted" && item["hash"] == "auto" {
                            item["hash"] = tx.calculate_hash().unwrap().into();
                        }
                    }
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
            count,
            batch_size,
            Arc::clone(&counters),
            Instant::now(),
        )
        .await;
        server.abort();
        (result, counters)
    }

    #[tokio::test]
    async fn counts_admission_results_in_http_200_responses() {
        let (result, counters) = run_with_response(
            2,
            2,
            StatusCode::OK,
            serde_json::json!([
                {"status": "accepted", "hash": "auto"},
                {"status": "rejected", "error": "transaction already seen"}
            ]),
        )
        .await;
        result.unwrap();
        assert_eq!(counters.accepted.load(Ordering::Relaxed), 1);
        assert_eq!(counters.rejected.load(Ordering::Relaxed), 1);
        assert_eq!(counters.hashes.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn invalid_responses_are_not_counted_as_accepted() {
        for body in [
            serde_json::json!([]),
            serde_json::json!([
                {"status": "accepted", "hash": "auto"},
                {"status": "accepted", "hash": "auto"}
            ]),
            serde_json::json!([{"status": "unknown"}]),
        ] {
            let (result, counters) = run_with_response(1, 1, StatusCode::OK, body).await;
            assert!(result.is_err());
            assert_eq!(counters.accepted.load(Ordering::Relaxed), 0);
            assert_eq!(counters.rejected.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn unsuccessful_http_status_is_counted_as_rejected() {
        let (result, counters) = run_with_response(
            2,
            2,
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error": "unavailable"}),
        )
        .await;
        result.unwrap();
        assert_eq!(counters.accepted.load(Ordering::Relaxed), 0);
        assert_eq!(counters.rejected.load(Ordering::Relaxed), 2);
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
                batch_size: 100,
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
            ("BATCH_SIZE", "25"),
            ("METRICS_URLS", "http://a:8001, http://b:8002"),
            ("CONFIRM_TIMEOUT_SECS", "20"),
            ("POLL_INTERVAL_MS", "100"),
        ]))
        .unwrap();
        assert_eq!(config.tx_count, 100);
        assert_eq!(config.concurrency, 4);
        assert_eq!(config.batch_size, 25);
        assert_eq!(config.metrics_urls, ["http://a:8001", "http://b:8002"]);
        assert_eq!(config.confirm_timeout_secs, 20);
        assert_eq!(config.poll_interval_ms, 100);
    }

    #[test]
    fn invalid_config_is_rejected() {
        for name in [
            "CONCURRENCY",
            "BATCH_SIZE",
            "CONFIRM_TIMEOUT_SECS",
            "POLL_INTERVAL_MS",
        ] {
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
    }

    #[tokio::test]
    async fn wrong_admission_hash_is_not_tracked_as_accepted() {
        let (result, counters) = run_with_response(
            1,
            1,
            StatusCode::OK,
            serde_json::json!([{"status": "accepted", "hash": "wrong"}]),
        )
        .await;
        assert!(matches!(result, Err(ClientError::InvalidTransactionHash)));
        assert_eq!(counters.accepted.load(Ordering::Relaxed), 0);
        assert!(counters.hashes.lock().await.is_empty());
    }

    #[test]
    fn nonces_are_batched_in_order() {
        assert_eq!(nonce_batches(5, 2).collect::<Vec<_>>(), [0..2, 2..4, 4..5]);
        assert_eq!(nonce_batches(0, 2).count(), 0);
    }

    #[test]
    fn rejections_are_grouped_without_the_hash() {
        let hash = "ab".repeat(32);
        assert_eq!(
            error_kind(&format!("transaction already seen: {hash}")),
            "transaction already seen"
        );
        let counters = Counters::default();
        for _ in 0..3 {
            counters.record_rejection(&format!("transaction already seen: {hash}"), Instant::now());
        }
        assert_eq!(counters.rejected.load(Ordering::Relaxed), 3);
        assert_eq!(
            counters.rejections(),
            [("transaction already seen".to_string(), 3)]
        );
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
        let tx = Transaction::new(&sender.verifying_key(), 1, 0, unix_millis(), &sender).unwrap();
        tx.verify().unwrap();
    }
}
