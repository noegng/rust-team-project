use std::{collections::HashMap, time::Duration};

use ed25519_dalek::{SigningKey, VerifyingKey};
use node::NodeConfig;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Reads `.env` from the current directory if present; shell variables keep priority.
fn read_dotenv() -> HashMap<String, String> {
    let Ok(contents) = std::fs::read_to_string(".env") else {
        return HashMap::new();
    };
    contents
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            Some((key.trim().to_string(), value.trim().to_string()))
        })
        .collect()
}

fn get_env(dotenv: &HashMap<String, String>, name: &str) -> anyhow::Result<String> {
    if let Ok(value) = std::env::var(name) {
        return Ok(value);
    }
    dotenv
        .get(name)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing environment variable {name}"))
}

fn load_signing_key(dotenv: &HashMap<String, String>) -> anyhow::Result<SigningKey> {
    let encoded = get_env(dotenv, "SECRET_KEY")?;
    let mut secret = [0u8; 32];
    hex::decode_to_slice(encoded.trim(), &mut secret)?;

    Ok(SigningKey::from_bytes(&secret))
}

fn load_authorized_validators(
    dotenv: &HashMap<String, String>,
) -> anyhow::Result<Vec<VerifyingKey>> {
    let encoded = get_env(dotenv, "AUTHORIZED_VALIDATORS")?;

    let validators = encoded
        .split(',')
        .map(|value| {
            let mut bytes = [0u8; 32];
            hex::decode_to_slice(value.trim(), &mut bytes)?;
            Ok(VerifyingKey::from_bytes(&bytes)?)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(validators)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dotenv = read_dotenv();
    let signing_key = load_signing_key(&dotenv)?;
    let public_key = signing_key.verifying_key();

    println!("node public key: {}", hex::encode(public_key.to_bytes()));

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let authorized_validators = load_authorized_validators(&dotenv)?;
    let block_time: u64 = get_env(&dotenv, "BLOCK_TIME_SECS")
        .unwrap_or_else(|_| "2".into())
        .parse()?;
    let port: u16 = get_env(&dotenv, "PORT")
        .unwrap_or_else(|_| "3000".into())
        .parse()?;
    node::run(NodeConfig {
        signing_key,
        authorized_validators,
        port,
        block_time: Duration::from_secs(block_time),
    })
    .await
}
