use blockchain::{block::Block, chain::Chain};
use ed25519_dalek::VerifyingKey;

fn verify_block_signature(block: &Block) -> bool {
    block.verify_signature().is_ok()
}

fn block_txs_are_valid(block: &Block) -> bool {
    let mut seen = std::collections::HashSet::new();
    block
        .block_data
        .transactions
        .iter()
        .all(|tx| tx.verify().is_ok() && seen.insert((tx.from.as_slice(), tx.nonce)))
}

fn validate_block(block: &Block, chain: &Chain, validators: &[VerifyingKey]) -> bool {
    let Some(validator) = block.validator else {
        return false;
    };

    // 1. validateur autorisé ?
    validators.contains(&validator)
    // 2. signature correcte ?
    && verify_block_signature(block)
    // 3. lien avec le bloc précédent ?
    && chain.is_valid_successor(block)
    // 4. toutes les TX valides ?
    && block_txs_are_valid(block)
}
