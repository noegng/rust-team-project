use ed25519_dalek::{Signature, VerifyingKey};

pub struct Transaction {
    from: VerifyingKey, // the sender's public key
    to: VerifyingKey,
    amount: u64,
    nonce: u64,
    signature: Signature,
}
