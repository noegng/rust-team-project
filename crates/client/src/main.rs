mod errors;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use blockchain::transaction::Transaction;
use ed25519_dalek::{SigningKey, VerifyingKey};

use crate::errors::ClientError;

/// Benchmark : envoie `TX_COUNT` TX signées sur `NODE_URL` avec `CONCURRENCY`
/// tâches en parallèle, puis affiche le TPS côté client et `GET /metrics`
/// de chaque node de `METRICS_URLS` (séparées par des virgules).
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env(|name| std::env::var(name).ok())?;
    let counters = Arc::new(Counters::default());
    let client = reqwest::Client::new();
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
            let url = format!("{}/tx", config.node_url);
            let counters = Arc::clone(&counters);
            tokio::spawn(async move { spam(client, url, recipient, count, counters).await })
        })
        .collect();
    for handle in handles {
        if let Err(err) = handle.await? {
            eprintln!("worker stopped: {err}");
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
    metrics_urls: Vec<String>,
}

impl Config {
    fn from_env(var: impl Fn(&str) -> Option<String>) -> Result<Self, ClientError> {
        let node_url = var("NODE_URL").unwrap_or_else(|| "http://127.0.0.1:8001".into());
        let tx_count = parse_number(&var, "TX_COUNT", 10_000)?;
        let concurrency = parse_number(&var, "CONCURRENCY", 50)?;
        if concurrency == 0 {
            return Err(ClientError::InvalidConfig {
                name: "CONCURRENCY",
                value: "0".into(),
            });
        }
        let metrics_urls = match var("METRICS_URLS") {
            Some(urls) => urls.split(',').map(|url| url.trim().to_string()).collect(),
            None => vec![node_url.clone()],
        };
        Ok(Self {
            node_url,
            tx_count,
            concurrency,
            metrics_urls,
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
        let response = client.post(&url).json(&tx).send().await?;
        let counter = if response.status().is_success() {
            &counters.accepted
        } else {
            &counters.rejected
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
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

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
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
            ("METRICS_URLS", "http://a:8001, http://b:8002"),
        ]))
        .unwrap();
        assert_eq!(config.tx_count, 100);
        assert_eq!(config.concurrency, 4);
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
