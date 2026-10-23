#[derive(Debug, thiserror::Error)]
pub(crate) enum ClientError {
    #[error("Client connecting error")]
    ConnectionError,
}
