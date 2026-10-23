mod errors;

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use blockchain::transaction::Transaction;
use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::Deserialize;

use crate::errors::ClientError;

/// Benchmark : envoie `TX_COUNT` TX signées sur `NODE_URL/transactions`,
/// par lots de `BATCH_SIZE`, avec `CONCURRENCY` tâches en parallèle, puis affiche
/// le TPS côté client et `GET /metrics` de chaque node de `METRICS_URLS`
/// (séparées par des virgules).
/// Pendant l'envoi : une ligne de progression par seconde, et chaque nouveau
/// motif de rejet ou erreur HTTP est affiché dès qu'il apparaît.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env(|name| std::env::var(name).ok())?;
    let counters = Arc::new(Counters::default());
    let client = reqwest::Client::new();
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
            let url = format!("{}/transactions", config.node_url);
            let batch_size = config.batch_size;
            let counters = Arc::clone(&counters);
            tokio::spawn(async move {
                let result = spam(
                    client, url, recipient, count, batch_size, &counters, started,
                )
                .await;
                if let Err(err) = result {
                    counters.failed_workers.fetch_add(1, Ordering::Relaxed);
                    eprintln!(
                        "[{:.1}s] worker stopped: {err}",
                        started.elapsed().as_secs_f64()
                    );
                }
            })
        })
        .collect();
    for handle in handles {
        handle.await?;
    }
    progress.abort();
    let elapsed = started.elapsed().as_secs_f64();

    let accepted = counters.accepted.load(Ordering::Relaxed);
    let rejected = counters.rejected.load(Ordering::Relaxed);
    let failed_workers = counters.failed_workers.load(Ordering::Relaxed);
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

    for url in &config.metrics_urls {
        match fetch_metrics(&client, url).await {
            Ok(metrics) => println!("{url}: {metrics}"),
            Err(err) => eprintln!("{url}: {err}"),
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
struct Config {
    node_url: String,
    tx_count: u64,
    concurrency: u64,
    batch_size: u64,
    metrics_urls: Vec<String>,
}

impl Config {
    fn from_env(var: impl Fn(&str) -> Option<String>) -> Result<Self, ClientError> {
        let node_url = var("NODE_URL").unwrap_or_else(|| "http://127.0.0.1:8001".into());
        let tx_count = parse_number(&var, "TX_COUNT", 10_000)?;
        let concurrency = parse_positive(&var, "CONCURRENCY", 50)?;
        let batch_size = parse_positive(&var, "BATCH_SIZE", 100)?;
        let metrics_urls = match var("METRICS_URLS") {
            Some(urls) => urls.split(',').map(|url| url.trim().to_string()).collect(),
            None => vec![node_url.clone()],
        };
        Ok(Self {
            node_url,
            tx_count,
            concurrency,
            batch_size,
            metrics_urls,
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
    failed_workers: AtomicU64,
    /// Nombre de rejets par motif (voir `error_kind`).
    rejections: Mutex<HashMap<String, u64>>,
}

impl Counters {
    /// Compte le rejet ; un motif encore jamais vu est affiché tout de suite.
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

/// Motif d'un rejet sans le hash de TX final, pour regrouper les rejets
/// (`"transaction already seen: <hash>"` → `"transaction already seen"`).
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

/// Une ligne par seconde : totaux et débit sur la dernière seconde.
async fn report_progress(counters: Arc<Counters>, started: Instant) {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.tick().await;
    let mut previous = 0;
    loop {
        ticker.tick().await;
        let accepted = counters.accepted.load(Ordering::Relaxed);
        println!(
            "[{:.0}s] accepted {accepted} (+{}/s), rejected {}, failed workers {}",
            started.elapsed().as_secs_f64(),
            accepted - previous,
            counters.rejected.load(Ordering::Relaxed),
            counters.failed_workers.load(Ordering::Relaxed),
        );
        previous = accepted;
    }
}

/// Réponse de `POST /transactions` : une entrée par TX, dans le même ordre.
#[derive(Deserialize)]
struct TransactionResult {
    status: TransactionStatus,
    error: Option<String>,
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
enum TransactionStatus {
    Accepted,
    Rejected,
}

/// Une tâche = un expéditeur avec sa propre clé : ses nonces partent de 0
/// et arrivent dans l'ordre sur le node, lot après lot.
async fn spam(
    client: reqwest::Client,
    url: String,
    recipient: VerifyingKey,
    count: u64,
    batch_size: u64,
    counters: &Counters,
    started: Instant,
) -> Result<(), ClientError> {
    let sender = random_key()?;
    for nonces in nonce_batches(count, batch_size) {
        let batch = nonces
            .map(|nonce| Transaction::new(&recipient, 1, nonce, unix_millis(), &sender))
            .collect::<Result<Vec<_>, _>>()?;
        let results: Vec<TransactionResult> = client
            .post(&url)
            .json(&batch)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        for result in results {
            match result.status {
                TransactionStatus::Accepted => {
                    counters.accepted.fetch_add(1, Ordering::Relaxed);
                }
                TransactionStatus::Rejected => counters
                    .record_rejection(result.error.as_deref().unwrap_or("unknown error"), started),
            }
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
    let response = client.get(format!("{node_url}/metrics")).send().await?;
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
    use std::collections::HashMap;

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
                node_url: "http://127.0.0.1:8001".into(),
                tx_count: 10_000,
                concurrency: 50,
                batch_size: 100,
                metrics_urls: vec!["http://127.0.0.1:8001".into()],
            }
        );
    }

    #[test]
    fn config_from_vars() {
        let config = Config::from_env(vars(&[
            ("NODE_URL", "http://a:8001"),
            ("TX_COUNT", "100"),
            ("CONCURRENCY", "4"),
            ("BATCH_SIZE", "10"),
            ("METRICS_URLS", "http://a:8001, http://b:8002"),
        ]))
        .unwrap();
        assert_eq!(config.tx_count, 100);
        assert_eq!(config.concurrency, 4);
        assert_eq!(config.batch_size, 10);
        assert_eq!(config.metrics_urls, ["http://a:8001", "http://b:8002"]);
    }

    #[test]
    fn invalid_config_is_rejected() {
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
        assert!(matches!(
            Config::from_env(vars(&[("BATCH_SIZE", "0")])),
            Err(ClientError::InvalidConfig {
                name: "BATCH_SIZE",
                ..
            })
        ));
    }

    #[test]
    fn nonces_are_batched_in_order() {
        assert_eq!(nonce_batches(5, 2).collect::<Vec<_>>(), [0..2, 2..4, 4..5]);
        assert_eq!(nonce_batches(0, 2).count(), 0);
    }

    #[test]
    fn api_response_is_parsed() {
        let body = r#"[{"status":"accepted","hash":"ab"},{"status":"rejected","error":"x"}]"#;
        let results: Vec<TransactionResult> = serde_json::from_str(body).unwrap();
        let statuses: Vec<_> = results.into_iter().map(|result| result.status).collect();
        assert_eq!(
            statuses,
            [TransactionStatus::Accepted, TransactionStatus::Rejected]
        );
    }

    #[test]
    fn rejections_are_grouped_without_the_hash() {
        let hash = "ab".repeat(32);
        assert_eq!(
            error_kind(&format!("transaction already seen: {hash}")),
            "transaction already seen"
        );
        assert_eq!(
            error_kind("invalid transaction: invalid transaction signature"),
            "invalid transaction: invalid transaction signature"
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
