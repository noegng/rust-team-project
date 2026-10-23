# Authora — Mini Private Blockchain (PoA)

> Rust workspace · 3 full nodes · Proof of Authority · 2 days

---

## Stack

| Besoin | Crate |
|---|---|
| Async runtime | `tokio` |
| HTTP + WebSocket serveur | `axum` |
| WebSocket client (dial peers) | `tokio-tungstenite` |
| Hashing | `sha2` |
| Signing / keypairs | `ed25519-dalek` |
| Sérialisation | `serde` + `serde_json` |
| HTTP client (benchmark) | `reqwest` |
| Génération de clés | `rand` |

---

## Structure du workspace

```
authora/
├── Cargo.toml              ← workspace root
├── blockchain/             ← lib: types, logique core
│   └── src/
│       ├── lib.rs
│       ├── block.rs
│       ├── transaction.rs
│       ├── chain.rs
│       └── mempool.rs
├── node/                   ← binary: full node
│   └── src/
│       ├── main.rs
│       ├── p2p.rs          ← gossip WebSocket peer
│       ├── validator.rs    ← production de blocs + signing
│       └── api.rs          ← Axum HTTP (reçoit les TX du client)
└── client/                 ← binary: spammer / benchmark
    └── src/
        └── main.rs
```

---

## Cargo.toml workspace

```toml
[workspace]
members = ["blockchain", "node", "client"]
resolver = "2"

[workspace.dependencies]
tokio             = { version = "1", features = ["full"] }
axum              = { version = "0.7", features = ["ws"] }
tokio-tungstenite = "0.24"
serde             = { version = "1", features = ["derive"] }
serde_json        = "1"
sha2              = "0.10"
ed25519-dalek     = "2"
hex               = "0.4"
rand              = "0.8"
reqwest           = { version = "0.12", features = ["json"] }
```

```toml
# blockchain/Cargo.toml
[dependencies]
serde         = { workspace = true }
serde_json    = { workspace = true }
sha2          = { workspace = true }
ed25519-dalek = { workspace = true }
hex           = { workspace = true }
rand          = { workspace = true }

# node/Cargo.toml
[dependencies]
blockchain        = { path = "../blockchain" }
tokio             = { workspace = true }
axum              = { workspace = true }
tokio-tungstenite = { workspace = true }
serde_json        = { workspace = true }
ed25519-dalek     = { workspace = true }

# client/Cargo.toml
[dependencies]
blockchain = { path = "../blockchain" }
tokio      = { workspace = true }
serde_json = { workspace = true }
reqwest    = { workspace = true }
ed25519-dalek = { workspace = true }
rand          = { workspace = true }
```

---

## Data structures (blockchain crate)

### Transaction

```rust
use ed25519_dalek::{VerifyingKey, Signature};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Transaction {
    pub from:      Vec<u8>,   // VerifyingKey bytes
    pub to:        Vec<u8>,
    pub amount:    u64,
    pub nonce:     u64,       // anti-replay
    pub timestamp: u64,
    pub signature: Vec<u8>,   // Signature bytes
}
```

### Block

```rust
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Block {
    pub index:      u64,
    pub prev_hash:  String,   // hex
    pub tx_root:    String,   // hash de toutes les TX
    pub timestamp:  u64,
    pub validator:  Vec<u8>,  // VerifyingKey du proposant
    pub signature:  Vec<u8>,  // signé par le validateur
    pub hash:       String,
}
```

### Chain

```rust
pub struct Chain {
    pub blocks: Vec<Block>,
}

impl Chain {
    pub fn is_valid_successor(&self, block: &Block) -> bool {
        let last = self.blocks.last().unwrap();
        block.index == last.index + 1
            && block.prev_hash == last.hash
        // + vérifier signature du validateur
    }
}
```

### Mempool

```rust
use std::collections::{HashMap, HashSet};

pub struct Mempool {
    pub txs:      HashMap<String, Transaction>, // key = tx hash
    pub seen:     HashSet<String>,              // déduplication
}

impl Mempool {
    pub fn add(&mut self, tx: Transaction, hash: String) -> bool {
        if self.seen.contains(&hash) { return false; }
        self.seen.insert(hash.clone());
        self.txs.insert(hash, tx);
        true
    }

    pub fn drain(&mut self) -> Vec<Transaction> {
        self.txs.drain().map(|(_, tx)| tx).collect()
    }
}
```

---

## Cryptographie (ed25519-dalek)

```rust
use ed25519_dalek::{SigningKey, VerifyingKey, Signer, Verifier, Signature};
use rand::rngs::OsRng;

// Génération d'un keypair (une fois au démarrage du node)
let signing_key   = SigningKey::generate(&mut OsRng);
let verifying_key = signing_key.verifying_key();

// Signer (TX côté client, Block côté validateur)
let payload = serde_json::to_vec(&data).unwrap();
let signature: Signature = signing_key.sign(&payload);

// Vérifier (tous les nodes à la réception)
verifying_key.verify(&payload, &signature).is_ok()
```

---

## Réseau

### Qui parle à qui

```
[Client]  ──── HTTP POST /tx ────────────────► [Node A]
                                                    │
                                          gossip WS │
                                                    │
                                  ┌─────────────────┴──────────────────┐
                               [Node B]                              [Node C]
```

### Discovery : hardcodé (pas de DHT nécessaire)

```toml
# node config (env var ou config.toml)
PORT  = 8001
PEERS = ["ws://192.168.1.11:8002/peer", "ws://192.168.1.12:8003/peer"]
```

### Node : inbound (Axum)

```rust
// api.rs — reçoit les TX du client
async fn post_tx(Json(tx): Json<Transaction>) -> impl IntoResponse {
    // 1. valider signature TX
    // 2. ajouter au mempool
    // 3. gossip aux peers
}

// p2p.rs — reçoit les messages des peers
async fn peer_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_peer_connection)
}
```

### Node : outbound (tokio-tungstenite)

```rust
// p2p.rs — dial les peers au démarrage
async fn connect_to_peer(addr: &str) {
    loop {
        match connect_async(addr).await {
            Ok((ws, _)) => handle_outbound_peer(ws).await,
            Err(_) => tokio::time::sleep(Duration::from_secs(5)).await,
        }
    }
}
```

### Types de messages gossip

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum Message {
    NewTx(Transaction),
    NewBlock(Block),
    Ping,
}
```

---

## Consensus PoA

### Rotation des validateurs (round-robin)

```rust
// validators = [pubkey_A, pubkey_B, pubkey_C]
fn current_proposer(block_index: u64, validators: &[Vec<u8>]) -> &Vec<u8> {
    &validators[(block_index as usize) % validators.len()]
}
// Block 0 → A, Block 1 → B, Block 2 → C, Block 3 → A ...
```

### Boucle de production de blocs (validator.rs)

```rust
// toutes les N secondes, si c'est mon tour :
loop {
    tokio::time::sleep(Duration::from_secs(BLOCK_TIME)).await;

    if current_proposer(chain.len(), &validators) == &my_pubkey {
        let txs     = mempool.drain();
        let block   = build_block(&chain, txs);
        let signed  = sign_block(block, &signing_key);
        chain.push(signed.clone());
        broadcast_to_peers(Message::NewBlock(signed)).await;
    }
}
```

### Validation d'un bloc reçu

```rust
fn validate_block(block: &Block, chain: &Chain, validators: &[Vec<u8>]) -> bool {
    // 1. validateur autorisé ?
    validators.contains(&block.validator)
    // 2. signature correcte ?
    && verify_block_signature(block)
    // 3. lien avec le bloc précédent ?
    && chain.is_valid_successor(block)
    // 4. toutes les TX valides ?
    && block_txs_are_valid(block)
}
```

---

## Benchmark (client)

Le client 4 spam des TX sur Node A aussi vite que possible.

```rust
// Spawn N tâches concurrentes
let handles: Vec<_> = (0..CONCURRENCY).map(|_| {
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        loop {
            let tx = build_signed_tx();
            client.post("http://node-a:8001/tx")
                .json(&tx).send().await.unwrap();
        }
    })
}).collect();

join_all(handles).await;
```

### Métriques à mesurer

| Métrique | Comment |
|---|---|
| TPS (TX/sec) | compteur atomique côté node A |
| Latence propagation | timestamp à l'injection + à la réception sur B et C |
| Lag de sync | écart entre taille mempool A vs B vs C |
| Temps de finalité | TX soumise → incluse dans un bloc sur tous les nodes |

---

## Répartition des tâches

| Personne | Crate | Fichiers |
|---|---|---|
| **P1** | `blockchain` | `block.rs`, `transaction.rs`, `chain.rs`, `mempool.rs` |
| **P2** | `node` | `p2p.rs`, `api.rs`, `main.rs` |
| **P3** | `node` + `client` | `validator.rs`, `client/main.rs`, métriques |

> P1 livre les types en premier → P2 et P3 peuvent commencer dès que `blockchain` compile.

---

## Planning 2 jours

### Jour 1
- [ ] Setup workspace + Cargo.toml
- [ ] `blockchain` : Block, TX, Chain, Mempool
- [ ] Signing / verification (ed25519)
- [ ] Connexions P2P entre les 3 nodes (WebSocket)
- [ ] Endpoint HTTP POST /tx sur le node

### Jour 2
- [ ] Gossip TX entre les nodes + déduplication
- [ ] Boucle de production de blocs (validator.rs)
- [ ] Validation de blocs reçus
- [ ] Client spammer
- [ ] Métriques + démo finale

---

## Démo finale attendue

```
1. Les 3 nodes démarrent et se connectent entre eux
2. Le client envoie 10 000 TX sur Node A
3. B et C reçoivent toutes les TX par gossip
4. Toutes les N secondes, le validateur désigné produit un bloc signé
5. Les 3 nodes ont la même chaîne
6. Affichage : TPS, latence de propagation, longueur de chaîne
```
