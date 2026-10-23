use thiserror::Error;

use crate::primitives::{Address, Hash};
use crate::units::{Amount, Height, Nonce};

pub type Result<T> = std::result::Result<T, ChainError>;

/// Toutes les erreurs du domaine. Aucun `unwrap()` dans la lib : on remonte
/// un `ChainError` et c'est l'appelant (node, API) qui décide quoi en faire.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ChainError {
    #[error("invalid encoding: {0}")]
    InvalidEncoding(String),

    #[error("invalid signature")]
    InvalidSignature,

    #[error("amount overflow")]
    AmountOverflow,

    #[error("insufficient balance: needed {needed}, available {available}")]
    InsufficientBalance { needed: Amount, available: Amount },

    #[error("invalid nonce: expected {expected}, got {got}")]
    InvalidNonce { expected: Nonce, got: Nonce },

    #[error("bad previous hash: expected {expected}, got {got}")]
    BadPrevHash { expected: Hash, got: Hash },

    #[error("bad height: expected {expected}, got {got}")]
    BadHeight { expected: Height, got: Height },

    #[error("invalid transaction root")]
    InvalidTxRoot,

    #[error("unknown authority: {0}")]
    UnknownAuthority(Address),

    #[error("wrong proposer: expected {expected}, got {got}")]
    WrongProposer { expected: Address, got: Address },

    #[error("no authorities configured")]
    NoAuthorities,
}

impl ChainError {
    /// Nom stable de la variante, utilisable comme label de métrique :
    /// `errors_total{kind="InvalidNonce"}`.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::InvalidEncoding(_) => "InvalidEncoding",
            Self::InvalidSignature => "InvalidSignature",
            Self::AmountOverflow => "AmountOverflow",
            Self::InsufficientBalance { .. } => "InsufficientBalance",
            Self::InvalidNonce { .. } => "InvalidNonce",
            Self::BadPrevHash { .. } => "BadPrevHash",
            Self::BadHeight { .. } => "BadHeight",
            Self::InvalidTxRoot => "InvalidTxRoot",
            Self::UnknownAuthority(_) => "UnknownAuthority",
            Self::WrongProposer { .. } => "WrongProposer",
            Self::NoAuthorities => "NoAuthorities",
        }
    }
}