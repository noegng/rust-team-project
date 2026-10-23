# Authora

Mini-blockchain Proof of Authority en Rust (voir `authora.md` pour la spec d'origine).

## Crates

| Crate | Rôle |
|---|---|
| `crates/blockchain` | `Transaction`, `Block`, `Chain` (validation PoA, nonces), `Mempool` |
| `crates/node` | Node : API HTTP, gossip WebSocket, découverte mDNS, production de blocs, métriques |
| `crates/client` | Benchmark : envoie des TX signées par lots et affiche le TPS + `/metrics` des nodes |

Dans `crates/node` :

- `main.rs` : configuration, `NodeState`, serveur axum, connexions entrantes ;
- `api.rs` : routes HTTP ;
- `p2p.rs` : traitement et relais des messages entre peers ;
- `discovery.rs` : découverte mDNS et connexions sortantes ;
- `validator.rs` : production et acceptation des blocs ;
- `metrics.rs` : compteurs.

## Écarts avec la spec

- **Peers :** découverts automatiquement par mDNS (`_authora._tcp.local.`) au lieu d'une liste `PEERS` fixe. Seul le node dont l'ID est le plus petit initie la connexion, ce qui donne une seule connexion par paire.
- **Route WebSocket :** `/ws` au lieu de `/peer`. Les messages sont des `NodeMessage` en JSON (`{"type": "NewTx" | "NewBlock" | "Ping", "payload": …}`).
- **Soumission des TX :** `POST /transactions` reçoit un **lot** `[Transaction]` et renvoie, dans le même ordre, `[{"status": "accepted", "hash"} | {"status": "rejected", "error"}]`.
- **Timestamp des TX :** en **millisecondes** Unix, ce qui permet de mesurer les latences à la ms près. Les blocs restent en secondes.
- **Blocs vides :** à son tour, un validateur produit et diffuse son bloc même sans TX, sinon la rotation round-robin se bloquerait.

## Lancer un réseau local

```sh
# 1. Une clé secrète par node
openssl rand -hex 32   # x3 : KEY_A, KEY_B, KEY_C

# 2. Récupérer chaque clé publique : le node l'affiche puis s'arrête
#    (AUTHORIZED_VALIDATORS manquant)
SECRET_KEY=$KEY_A cargo run -p node   # "node public key: <PUB_A>"

# 3. Lancer les nodes : même liste, même ordre partout
export AUTHORIZED_VALIDATORS=$PUB_A,$PUB_B,$PUB_C
SECRET_KEY=$KEY_A PORT=8001 cargo run -p node
SECRET_KEY=$KEY_B PORT=8002 cargo run -p node
SECRET_KEY=$KEY_C PORT=8003 cargo run -p node

# 4. Benchmark
METRICS_URLS=http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003 \
  cargo run --release -p client
```

On peut aussi mettre ces variables dans un `.env` (modèle : `.env.example`). Ne jamais commiter de `SECRET_KEY`.

### Variables du node

| Variable | Défaut | |
|---|---|---|
| `SECRET_KEY` | requis | clé ed25519 du node (hex, 32 octets) |
| `AUTHORIZED_VALIDATORS` | requis | clés publiques hex séparées par des virgules ; l'ordre définit la rotation |
| `PORT` | `8001` | API HTTP + WebSocket |
| `RUST_LOG` | `node=debug,tower_http=debug` | filtre des logs |

### Variables du client

| Variable | Défaut |
|---|---|
| `NODE_URL` | `http://127.0.0.1:8001` |
| `TX_COUNT` | `10000` |
| `CONCURRENCY` | `50` (tâches, une clé d'expéditeur chacune) |
| `BATCH_SIZE` | `100` |
| `METRICS_URLS` | `NODE_URL` |

## API du node

| Route | |
|---|---|
| `POST /transactions` | soumet un lot de TX (ajoutées au mempool puis diffusées aux peers) |
| `GET /metrics` | `txs_received`, `tps`, `avg_propagation_ms`, `txs_committed`, `avg_finality_ms`, `chain_len`, `mempool_len` |
| `GET /ws` | connexion WebSocket entre nodes |

## Tests

```sh
cargo test --workspace
cargo clippy --workspace --all-targets
```
