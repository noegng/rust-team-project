# Authora (`rust-team-project`)

Mini-blockchain Proof of Authority (PoA) en Rust (voir [authora.md](authora.md) pour la spec d'origine).

## Crates

| Crate | Rôle |
|---|---|
| `crates/blockchain` | `Transaction`, `Block`, `Chain` (validation PoA, nonces), `Mempool` |
| `crates/node` | Node : API HTTP, gossip WebSocket + replay, découverte mDNS, production de blocs, métriques |
| `crates/client` | Benchmark : envoie des TX signées par lots et vérifie leur inclusion sur tous les nodes |

## Lancer la démo dans 4 terminaux

Installer Rust et Cargo, puis ouvrir **4 terminaux dans le dossier du projet**.
Les commandes ci-dessous lancent les trois nodes sur la même machine avec des
clés de démonstration distinctes et la même liste de validateurs.

### Terminal 1 - Node A

```bash
SECRET_KEY=0101010101010101010101010101010101010101010101010101010101010101 \
AUTHORIZED_VALIDATORS=8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c,8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394,ed4928c628d1c2c6eae90338905995612959273a5c63f93636c14614ac8737d1 \
PORT=3000 \
cargo run -p node
```

### Terminal 2 - Node B

```bash
SECRET_KEY=0202020202020202020202020202020202020202020202020202020202020202 \
AUTHORIZED_VALIDATORS=8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c,8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394,ed4928c628d1c2c6eae90338905995612959273a5c63f93636c14614ac8737d1 \
PORT=3001 \
cargo run -p node
```

### Terminal 3 - Node C

```bash
SECRET_KEY=0303030303030303030303030303030303030303030303030303030303030303 \
AUTHORIZED_VALIDATORS=8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c,8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394,ed4928c628d1c2c6eae90338905995612959273a5c63f93636c14614ac8737d1 \
PORT=3002 \
cargo run -p node
```

Attendre les messages `connected to peer` / `incoming peer connected` et les premiers `block added` avant de lancer le client.

### Terminal 4 - Client

Pour un test rapide (10 TX) :

```bash
NODE_URL=http://127.0.0.1:3000 \
METRICS_URLS=http://127.0.0.1:3000,http://127.0.0.1:3001,http://127.0.0.1:3002 \
TX_COUNT=10 \
CONCURRENCY=2 \
BATCH_SIZE=5 \
CONFIRM_TIMEOUT_SECS=60 \
cargo run -p client
```

Pour simuler du trafic en continu (lots et pauses aléatoires) jusqu'à Ctrl+C :

```bash
cargo run --release -p client
```

Pour le benchmark à 10 000 TX (débit maximal, sans pause) :

```bash
NODE_URL=http://127.0.0.1:3000 \
METRICS_URLS=http://127.0.0.1:3000,http://127.0.0.1:3001,http://127.0.0.1:3002 \
TX_COUNT=10000 MIN_BATCH_SIZE=100 MAX_PAUSE_MS=0 \
cargo run --release -p client
```

Le bilan attendu contient :

```text
confirmed everywhere: 10000/10000, pending: 0
```

## Lancer sur 3 PC différents (même réseau local)

Les nodes écoutent sur `0.0.0.0` et se découvrent automatiquement via mDNS sur le même Wi-Fi ou partage de connexion. Chaque PC peut garder `PORT=3000`.

1. **Sur chaque PC**, générer une clé secrète et récupérer sa clé publique :
   ```bash
   openssl rand -hex 32
   SECRET_KEY=<CLE_PRIVEE_DU_PC> cargo run -p node
   ```
   Le programme affiche `node public key: <CLE_PUBLIQUE>` puis s'arrête.

2. **Partager les 3 clés publiques** et créer un fichier `.env` à la racine du projet sur chacun des 3 PC avec la **même liste `AUTHORIZED_VALIDATORS` dans le même ordre** :
   ```env
   SECRET_KEY=<CLE_PRIVEE_DE_CE_PC>
   AUTHORIZED_VALIDATORS=<PUB_A>,<PUB_B>,<PUB_C>
   PORT=3000
   BLOCK_TIME_SECS=2
   ```

3. **Lancer un node sur chaque PC** :
   ```bash
   cargo run --release -p node
   ```
   Attendre les messages `connected to peer` / `incoming peer connected` et `block added` sur les 3 machines. (Autoriser le port `3000` si le pare-feu demande confirmation.)

4. **Lancer le client depuis l'un des PC**. Il envoie les TX au node local (`http://127.0.0.1:3000` par défaut), qui les diffuse aux autres via les blocs :
   ```bash
   cargo run --release -p client
   ```
   Optionnel : pour vérifier que les TX sont incluses sur **les 3 nodes** (et afficher leurs métriques), ajouter leurs IP locales (`ipconfig getifaddr en0` sur macOS, `hostname -I` sur Linux, `ipconfig` sur Windows) :
   ```bash
   METRICS_URLS=http://127.0.0.1:3000,http://<IP_PC_B>:3000,http://<IP_PC_C>:3000 \
   cargo run --release -p client
   ```

5. **Suivre en direct** : ouvrir `http://<IP_PC_A>:3000/` dans un navigateur (voir [Dashboard](#dashboard)).

> **Astuce jour J** : si le Wi-Fi d'une salle ou d'une école bloque la découverte mDNS entre appareils ("client isolation"), connectez les 3 PC sur un partage de connexion téléphone.

## Dashboard

Chaque node sert un dashboard live sur `/` (par exemple `http://127.0.0.1:3000/`) :
débit de TX reçues par node sur les 60 dernières secondes, mempool, hauteur,
propagation et finalité de chaque node, derniers blocs avec leur producteur, et
un état du consensus (synchronisés, node en retard ou bloqué, aucun nouveau bloc, divergence).

Les autres nodes sont trouvés via `GET /peers` (découverte mDNS). Si mDNS ne
passe pas, fixer la liste dans l'URL :
`http://<IP_PC_A>:3000/?nodes=http://<IP_PC_A>:3000,http://<IP_PC_B>:3000,http://<IP_PC_C>:3000`.

## Stress test (`scripts/stress.sh`)

Nécessite `curl` et `jq` (et `openssl` en local). Le script affiche la
progression des clients et les métriques de chaque node toutes les 2 s, puis
attend que tous les nodes aient le même dernier bloc, les mêmes TX incluses et
un mempool vide (code de sortie 1 sinon).

**Sur une machine** (3 nodes lancés par le script sur les ports 8001 à 8003, 2 clients) :

```bash
scripts/stress.sh
```

**Sur plusieurs machines** : lancer un node sur chaque PC (voir plus haut), puis
depuis un seul PC pilote, avec un client par node :

```bash
NODES=http://<IP_PC_A>:3000,http://<IP_PC_B>:3000,http://<IP_PC_C>:3000 scripts/stress.sh
```

| Variable | Défaut | Rôle |
|---|---|---|
| `NODES` | vide (mode local) | URLs des nodes déjà lancés |
| `CLIENTS` | nombre de nodes (2 en local) | Clients en parallèle, répartis sur les nodes |
| `TX_COUNT` | `50000` | TX **par client** |
| `CONCURRENCY` / `BATCH_SIZE` | `50` / `100` | Transmis à chaque client |
| `CONFIRM_TIMEOUT_SECS` | `120` | Délai de confirmation de chaque client |
| `SETTLE_TIMEOUT_SECS` | `60` | Délai d'attente de la convergence finale |
| `LOG_DIR` | `target/stress` | Logs des nodes locaux |

> **Limite connue** : un bloc de plus d'environ 30 000 TX dépasse 16 Mo de JSON,
> la taille de trame maximale des WebSockets entre nodes. Les autres nodes ne
> peuvent pas le recevoir et la chaîne s'arrête. Garder `CLIENTS × TX_COUNT`
> sous ce seuil par intervalle de bloc tant que la taille des blocs n'est pas plafonnée.

## Configuration (`.env` optionnel)

Un modèle est disponible dans `.env.example`. Les variables déjà définies dans le shell restent prioritaires.

### Variables du node

| Variable | Défaut | Rôle |
|---|---|---|
| `SECRET_KEY` | requis | Clé secrète ed25519 (hex, 32 octets) |
| `AUTHORIZED_VALIDATORS` | requis | Clés publiques hex séparées par des virgules (ordre = rotation PoA) |
| `PORT` | `3000` | Port HTTP + WebSocket |
| `BLOCK_TIME_SECS` | `2` | Intervalle de production des blocs (en secondes) |
| `RUST_LOG` | `node=debug,tower_http=debug` | Filtre des logs |

### Variables du client

| Variable | Défaut | Rôle |
|---|---|---|
| `NODE_URL` | `http://127.0.0.1:3000` | Node cible des soumissions |
| `TX_COUNT` | vide (continu) | Nombre total de TX à envoyer, puis attente des confirmations. Vide : envoi jusqu'à Ctrl+C |
| `CONCURRENCY` | `50` | Nombre de tâches parallèles (1 clé expéditeur chacune) |
| `BATCH_SIZE` | `100` | Taille max d'un lot `POST /transactions` |
| `MIN_BATCH_SIZE` | `1` | Taille min d'un lot (tirage uniforme jusqu'à `BATCH_SIZE`) |
| `MAX_PAUSE_MS` | `1000` | Pause aléatoire max avant chaque lot, par tâche (`0` = débit maximal) |
| `METRICS_URLS` | `NODE_URL` | Liste des nodes où vérifier les confirmations et lire `/metrics` |
| `CONFIRM_TIMEOUT_SECS` | `60` | Délai max pour la confirmation sur tous les nodes |
| `POLL_INTERVAL_MS` | `500` | Fréquence de polling des blocs pour la confirmation |

## API du node

| Route | Rôle |
|---|---|
| `POST /transactions` | Soumet un lot `[Transaction]` et diffuse les TX valides aux peers |
| `GET /transactions/{hash}` | État d'une TX (`pending` ou `included` avec numéro et hash de bloc) |
| `GET /blocks?limit=N` | Résumé des N derniers blocs (20 par défaut, 100 max), du plus récent au plus ancien, sans les TX |
| `GET /blocks/{index}` | Récupère un bloc par son index |
| `GET /metrics` | `txs_received`, `tps` (débit réel du dernier envoi : TX reçues entre la première et la dernière réception ; un envoi se termine après 1 s sans TX), `avg_propagation_ms`, `txs_committed`, `avg_finality_ms`, `chain_len`, `mempool_len`, `txs_rejected` (TX refusées par `POST /transactions`), `sync_resets` (connexions peer coupées sur un bloc rejeté), `height`, `last_block_hash`, `public_key` |
| `GET /peers` | Peers découverts par mDNS : `[{node_id, urls}]` |
| `GET /` | Dashboard live |
| `GET /ws` | Connexion WebSocket entre nodes |

## Tests

```bash
cargo test --workspace
cargo clippy --workspace --all-targets
```

## Benchmark réseau (Criterion)

3 nodes validateurs tournent chacun dans une tâche Tokio (ports loopback) et une 4e tâche leur envoie des TX en HTTP (`node::testnet`, feature `testnet`).

```bash
cargo bench -p node --features testnet            # mesure complète
cargo bench -p node --features testnet -- --test  # vérification rapide (1 itération)
```

| Bench | Mesure |
|---|---|
| `sync/load_to_sync/{100,1000,5000}` | Envoi de N TX au node A jusqu'à ce que les 3 nodes aient les mêmes blocs et un mempool vide |
| `bad_sync/reset_on_bad_block` | Temps pour qu'un node coupe un peer qui envoie un bloc invalide |
| `bad_sync/load_with_bad_blocks/1000` | Même charge, pendant qu'un peer malveillant envoie 5 blocs invalides au node B |
| `catch_up/late_node/1000` | Rattrapage d'un nouveau node sur une chaîne contenant 1000 TX |

Après Criterion, le bench affiche et écrit dans `target/criterion/network_stats.json` : temps moyen / p50 / p95 / max de sync, lag après la fin de l'envoi, taux d'erreur (TX rejetées ou perdues / TX envoyées), nombre de resets de sync (total, par bloc invalide, inattendus), propagation et finalité moyennes, TPS jusqu'à la sync.

## Flamegraph par node (samply)

`scripts/profile-nodes.sh` lance 3 nodes (processus séparés `node-1..3`, ports 3101-3103, découverte mDNS) sous un seul [samply](https://github.com/mstange/samply), puis le client leur envoie `TX_COUNT` TX (défaut 50000, échantillonnage `SAMPLE_RATE` = 4000 Hz). Un seul terminal suffit. Le profil `target/profiles/nodes.json.gz` contient une piste par node (build `--profile profiling` : release + symboles).

Un seul samply pour les 3 nodes : plusieurs instances épuisent `kernel.perf_event_mlock_kb` (`Failed to start profiling: mmap failed`).

```bash
cargo install --locked samply                # une fois
sudo sysctl kernel.perf_event_paranoid=1     # Linux, une fois par démarrage
scripts/profile-nodes.sh                     # TX_COUNT=50000 BLOCK_TIME_SECS=1 scripts/profile-nodes.sh
samply load target/profiles/nodes.json.gz    # déplier node-N, Ctrl+clic sur toutes ses pistes tokio-rt-worker, onglet "Flame Graph"
```
