use ed25519_dalek::Signature;
use sha2::Sha256;

pub type PublicKey = [u8; 32];

pub struct Block {
    index: u64,
    prev_hash: [u8; 32],
    tx_root: [u8; 32], // hash of all TXs
    timestamp: u64,
    validator: PublicKey, // who proposed this
    signature: Signature, // signed by that validator
}
