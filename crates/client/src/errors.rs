use blockchain::transaction::TransactionError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ClientError {
    #[error("invalid {name}: {value:?}")]
    InvalidConfig { name: &'static str, value: String },

    #[error("could not generate a key: {0}")]
    KeyGeneration(getrandom::Error),

    #[error("could not build transaction: {0}")]
    Transaction(#[from] TransactionError),

    #[error("request to node failed: {0}")]
    Connection(#[from] reqwest::Error),

    #[error("expected one transaction result from node, got {actual}")]
    InvalidResponseCount { actual: usize },

    #[error("node returned a hash that does not match the submitted transaction")]
    InvalidTransactionHash,

    #[error("could not verify confirmations: {0}")]
    InvalidConfirmation(String),
}
