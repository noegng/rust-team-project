# Authora (`rust-team-project`)

Mini-blockchain privée en **Proof of Authority (PoA)**, écrite en Rust : un
workspace, **3 full nodes** validateurs et un **client de benchmark**. La
consigne d'origine est dans [authora.md](authora.md) ; ce README suit son plan
et indique, pour chaque partie, ce qui a été réalisé et où se trouve le code.

**Sommaire** : [Objectif](#objectif) · [Stack](#stack) · [Workspace](#structure-du-workspace) ·
[Structures de données](#structures-de-données-crate-blockchain) · [Cryptographie](#cryptographie) ·
[Réseau](#réseau) · [Consensus PoA](#consensus-poa) · [Benchmark et métriques](#benchmark-et-métriques) ·
[Démo finale](#démo-finale-attendue) · [Robustesse réseau](#robustesse-réseau-ce-que-les-tests-multi-pc-ont-révélé) ·
[Écarts avec la spec](#écarts-avec-la-spec) · [Lancer le projet](#lancer-la-démo-dans-4-terminaux)

## Objectif

Trois nodes, chacun détenteur d'une clé ed25519 autorisée, reçoivent des
transactions signées, se les diffusent (gossip) et produisent à tour de rôle un
bloc signé toutes les `BLOCK_TIME_SECS` secondes. À la fin, les trois nodes ont
**la même chaîne**, et l'on mesure le **TPS**, la **latence de propagation**, le
**lag de synchronisation** et le **temps de finalité**.

## Stack

| Besoin | Crate utilisé |
|---|---|
| Runtime async | `tokio` |
| Serveur HTTP + WebSocket | `axum` 0.8 (`ws`) |
| Client WebSocket (connexion aux peers) | `tokio-tungstenite` |
| Hachage | `sha2` (SHA-256) |
| Signatures, paires de clés | `ed25519-dalek` |
| Sérialisation | `serde` + `serde_json` |
| Client HTTP (benchmark) | `reqwest` |
| Génération de clés | `rand` |
| *Ajouts* : découverte des peers | `mdns-sd` |
| *Ajouts* : vérification parallèle des signatures | `rayon` |
| *Ajouts* : logs, CORS du dashboard | `tracing`, `tower-http` |

## Structure du workspace

```
rust-team-project/
├── Cargo.toml                 ← workspace (blockchain, node, client)
├── crates/
│   ├── blockchain/            ← lib : types et règles du protocole
│   │   └── src/ block.rs · transaction.rs · chain.rs · mempool.rs
│   ├── node/                  ← binaire : full node
│   │   └── src/ main.rs       ← lecture de la config (.env / variables)
│   │            lib.rs        ← état partagé, routes, démarrage
│   │            api.rs        ← HTTP : soumission des TX, blocs, métriques
│   │            p2p.rs        ← gossip WebSocket entre nodes
│   │            discovery.rs  ← découverte mDNS + reconnexion
│   │            validator.rs  ← production et acceptation des blocs
│   │            metrics.rs    ← compteurs (TPS, propagation, finalité…)
│   │            dashboard.html← dashboard live servi sur /
│   │            testnet.rs    ← réseau de 3 nodes en mémoire (tests, benchs)
│   └── client/                ← binaire : spammer / benchmark
│       └── src/ main.rs · network.rs · confirmation.rs
└── scripts/ stress.sh · profile-nodes.sh
```

La répartition de la consigne (P1 `blockchain`, P2 réseau du node, P3
validateur + client) se retrouve dans ce découpage : `blockchain` ne dépend
d'aucun réseau et se teste seul, `node` et `client` s'appuient dessus.

## Structures de données (crate `blockchain`)

| Type | Contenu | Règles vérifiées |
|---|---|---|
| `Transaction` | `from`, `to` (clés publiques), `amount`, `nonce`, `timestamp`, `signature` | Signature ed25519 de l'expéditeur sur tous les autres champs ; hash SHA-256 = identifiant de la TX |
| `Block` | `BlockData` (`index`, `timestamp`, `prev_hash`, `hash`, `transactions`, `tx_root`) + `validator` + `signature` | `tx_root` = hash des TX, `hash` recalculé, signature du validateur sur l'en-tête |
| `Chain` | Blocs depuis la genèse + liste ordonnée des validateurs | Successeur valide : bon index, bon `prev_hash`, horodatage croissant, validateur autorisé **et** proposant attendu, signatures, nonces des expéditeurs, au plus `MAX_TXS_PER_BLOCK` (20 000) TX |
| `Mempool` | TX en attente, indexées par hash, avec déduplication | Refuse les doublons ; `drain` ne prend que les TX dont le nonce suit celui de la chaîne (ordre par expéditeur) |

**Anti-rejeu** : chaque expéditeur a un `nonce` qui doit valoir exactement le
suivant attendu par la chaîne. Une TX rejouée ou hors ordre est refusée.

## Cryptographie

- **Clé du node** : `SECRET_KEY` (32 octets hex). Le node affiche sa clé
  publique au démarrage (`node public key: …`), à mettre dans
  `AUTHORIZED_VALIDATORS` sur les trois machines.
- **Signature des TX** côté client, **signature des blocs** côté validateur.
- **Vérification** de toutes les signatures à la réception, par tous les nodes.
  Pour tenir la charge, les signatures d'un lot sont vérifiées **en parallèle**
  (`rayon`) et **hors des verrous** de la chaîne et du mempool.

## Réseau

```
[Client] ── HTTP POST /transactions (lots de TX) ──► [Node A]
                                                        │  gossip WebSocket (/ws)
                                        ┌───────────────┴───────────────┐
                                     [Node B] ◄──────── WS ────────► [Node C]
```

- **Entrée HTTP (`api.rs`)** : `POST /transactions` accepte un **lot** de TX,
  vérifie chacune, ajoute les valides au mempool et les diffuse aux peers en un
  seul message (`NewTxs`).
- **Gossip (`p2p.rs`)** : chaque paire de nodes partage une connexion
  WebSocket. Messages échangés (JSON `{"type", "payload"}`) :

  | Message | Rôle |
  |---|---|
  | `Hello(clé)` | Premier message : identité de l'émetteur (détecte une connexion à soi-même) |
  | `NewTx` / `NewTxs` | Transactions admises |
  | `NewBlock` | Bloc produit ou relayé |
  | `HistoryEnd` | Fin de l'historique envoyé à un nouveau peer |
  | `Ping` | Battement de cœur (connexion vivante) |

- **À la connexion**, chaque node renvoie à l'autre toute sa chaîne puis son
  mempool (*replay*) : un node relancé ou en retard rattrape ainsi les autres.
- **Découverte** : la consigne prévoyait des adresses codées en dur. Nous
  utilisons **mDNS** (`discovery.rs`) : les nodes d'un même réseau se trouvent
  seuls, sans configuration. Seul le node d'identifiant le plus petit initie la
  connexion de chaque paire (une seule connexion par paire), et il la rétablit
  après toute coupure.

## Consensus PoA

- **Rotation (round-robin)** : le bloc d'index *n* revient à
  `AUTHORIZED_VALIDATORS[n % 3]`. L'ordre de la liste doit donc être
  **identique sur les trois nodes**.
- **Production (`validator.rs`)** : toutes les `BLOCK_TIME_SECS`, si c'est son
  tour, le node vide le mempool (dans la limite de 20 000 TX), construit le
  bloc, le signe, l'ajoute à sa chaîne et le diffuse. Les **blocs vides** sont
  produits aussi, sinon la rotation se bloquerait.
- **Acceptation d'un bloc reçu** : toutes les règles de `Chain` ci-dessus. Un
  bloc déjà connu est ignoré ; un bloc invalide fait couper la connexion, qui
  est rétablie et rejoue l'historique (`sync_resets` dans les métriques).
- **Synchronisation au démarrage** : un node ne produit rien avant d'avoir reçu
  l'historique de ses peers (voir [Robustesse](#robustesse-réseau-ce-que-les-tests-multi-pc-ont-révélé)).

## Benchmark et métriques

Le **client** (`crates/client`) signe des TX avec plusieurs clés expéditeurs,
les envoie en lots par `CONCURRENCY` tâches parallèles, puis **vérifie leur
inclusion sur tous les nodes** (`METRICS_URLS`) et affiche le bilan. Il peut
aussi simuler un trafic continu (lots et pauses aléatoires).

| Métrique de la consigne | Où la lire | Calcul |
|---|---|---|
| TPS | `GET /metrics` → `tps`, bilan du client | Débit réel de la dernière rafale : TX reçues entre la première et la dernière réception (une rafale se termine après 1 s sans TX) |
| Latence de propagation | `avg_propagation_ms` | Horodatage de la TX (posé par le client) → réception par chaque node |
| Lag de synchronisation | `mempool_len`, `height`, `last_block_hash` de chaque node | Écart entre les nodes, visible sur le dashboard |
| Temps de finalité | `avg_finality_ms`, bilan du client | TX soumise → incluse dans un bloc sur **tous** les nodes |

Outils complémentaires : [dashboard live](#dashboard),
[stress test multi-machines](#stress-test-scriptsstresssh),
[benchmarks Criterion](#benchmark-réseau-criterion) et
[flamegraphs](#flamegraph-par-node-samply).

## Démo finale attendue

| Étape de la consigne | Comment la montrer |
|---|---|
| 1. Les 3 nodes démarrent et se connectent | `connected to peer` dans les logs ; les 3 nodes sur le dashboard |
| 2. Le client envoie 10 000 TX sur le node A | `TX_COUNT=10000 … cargo run --release -p client` (voir plus bas) |
| 3. B et C reçoivent toutes les TX par gossip | `txs_received` de chaque node, courbes TX/s du dashboard |
| 4. Le validateur désigné produit un bloc signé toutes les N s | Histogramme « TX par bloc », coloré par node producteur ; tableau des derniers blocs |
| 5. Les 3 nodes ont la même chaîne | Bandeau « Synchronisés : 3 nodes sur le bloc #… » ; bilan `confirmed everywhere: 10000/10000` |
| 6. Affichage TPS, propagation, longueur de chaîne | Cartes des nodes du dashboard ; `GET /metrics` |

## Robustesse réseau (ce que les tests multi-PC ont révélé)

Les tests sur 3 PC reliés par ZeroTier ou un partage de connexion ont montré
des pannes absentes en local. Chacune a un correctif dédié :

| Problème observé | Correctif |
|---|---|
| Un node relancé repart de la genèse et crée son propre bloc 1 → **fourche** et reconnexions en boucle | Le node ne produit qu'après la découverte des peers et la réception de **tout l'historique** de chacun (`HistoryEnd`) |
| Une connexion morte sans erreur laisse un node **isolé** ; la chaîne se bloque à son tour | **Heartbeat** : `Ping` toutes les 2 s, connexion coupée après 15 s de silence puis rétablie |
| Sur un partage de connexion iPhone, tous les appareils ont l'adresse `192.0.0.2` → un node **se connecte à lui-même** | Ces adresses sont ignorées, et le message `Hello` (clé publique) fait refuser toute connexion à soi-même |
| TPS moyen depuis le démarrage, trompeur | TPS mesuré sur la **dernière rafale** |
| Bloc trop gros pour une trame WebSocket (16 Mo) | Plafond de **20 000 TX par bloc** |

**Limites encore ouvertes** : si un validateur est arrêté, la chaîne attend son
tour (pas de saut de tour) ; une fourche déjà formée n'est pas réparée
automatiquement (pas de règle de la chaîne la plus longue) ; sur un réseau lent
ou relayé, le débit entre nodes chute nettement. Préférer un même réseau local.

## Écarts avec la spec

| Spec | Implémentation | Raison |
|---|---|---|
| `POST /tx`, une TX | `POST /transactions`, un lot de TX | Débit : moins de requêtes et un seul message de gossip par lot |
| Peers codés en dur | Découverte mDNS | Aucune IP à saisir, fonctionne sur 3 PC |
| `Message { NewTx, NewBlock, Ping }` | + `NewTxs`, `HistoryEnd`, `Hello` | Lots, rattrapage au démarrage, détection de soi-même |
| `mempool.drain()` de toutes les TX | TX dans l'ordre des nonces, 20 000 max par bloc | Anti-rejeu et taille de trame |
| Validation du successeur | + horodatage croissant, proposant attendu, nonces | Sécurité du consensus |
| Bloc produit seulement s'il y a des TX | Blocs vides produits | La rotation ne doit pas se bloquer |

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

Le dashboard suit aussi les tests et benchmarks :

- `scripts/stress.sh` et `scripts/profile-nodes.sh` affichent son URL au démarrage ;
- les nodes du `Testnet` (tests, Criterion) se déclarent les uns aux autres dans `GET /peers`, donc le `/` de n'importe lequel montre tout le réseau ;
- `DASHBOARD_PORT=3200 cargo bench -p node --features testnet -- stress` fixe les ports (3200-3202) de chaque réseau lancé par le bench, et affiche l'URL à ouvrir une fois : le même onglet suit toutes les itérations (chaque itération repart d'une chaîne neuve). Le polling du navigateur ajoute un peu de charge aux mesures.

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
| `GET /mempool` | Contenu du mempool, regroupé par expéditeur (affiché par le dashboard) |
| `GET /` | Dashboard live |
| `GET /ws` | Connexion WebSocket entre nodes |

## Tests

```bash
cargo test --workspace --all-features          # tests unitaires + test d'intégration
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
```

Le test d'intégration `crates/node/tests/network.rs` rejoue la démo finale par
l'API publique : 3 validateurs, 500 TX envoyées au node A, puis vérification
que les 3 nodes ont la même chaîne, toutes les TX incluses et un mempool vide.
Il a besoin de la feature `testnet` (d'où `--all-features`).

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
| `stress/ramp/{tps}` | Montée en charge, blocs toutes les 10 ms : 12 expéditeurs cadencés (boucle ouverte) répartis sur les 3 nodes envoient `tps` TX/s pendant 2 s, puis 5 s pour tout committer. 3 runs par palier (hors Criterion, dont l'échantillonnage impose au moins 10 runs + chauffe ; 1 run avec `--test`). Débit offert ×1,5 à chaque palier à partir de 5 000 TX/s ; arrêt au premier palier dont le taux d'erreur (TX rejetées + perdues + non committées partout à temps) dépasse 1 % |

`cargo bench -p node --features testnet -- stress` ne lance que le stress test ; il affiche le taux d'erreur de chaque palier puis le meilleur TPS jusqu'à la sync avec au plus 1 % d'erreur.

Après Criterion, le bench affiche et écrit dans `target/criterion/network_stats.json` : temps moyen / p50 / p95 / max de sync, lag après la fin de l'envoi, taux d'erreur (TX rejetées ou perdues / TX envoyées), nombre de resets de sync (total, par bloc invalide, inattendus), propagation et finalité moyennes, TPS jusqu'à la sync, plus gros bloc (limite `MAX_TXS_PER_BLOCK` = 20 000).

## Flamegraph par node (samply)

`scripts/profile-nodes.sh` lance 3 nodes (processus séparés `node-1..3`, ports 3101-3103, découverte mDNS) sous un seul [samply](https://github.com/mstange/samply), puis le client leur envoie `TX_COUNT` TX (défaut 50000, échantillonnage `SAMPLE_RATE` = 4000 Hz). Un seul terminal suffit. Le profil `target/profiles/nodes.json.gz` contient une piste par node (build `--profile profiling` : release + symboles).

Un seul samply pour les 3 nodes : plusieurs instances épuisent `kernel.perf_event_mlock_kb` (`Failed to start profiling: mmap failed`).

```bash
cargo install --locked samply                # une fois
sudo sysctl kernel.perf_event_paranoid=1     # Linux, une fois par démarrage
scripts/profile-nodes.sh                     # TX_COUNT=50000 BLOCK_TIME_SECS=1 scripts/profile-nodes.sh
samply load target/profiles/nodes.json.gz    # déplier node-N, Ctrl+clic sur toutes ses pistes tokio-rt-worker, onglet "Flame Graph"
```
