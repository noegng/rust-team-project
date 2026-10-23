# rust-team-project

## Lancer le projet dans 4 terminaux

Installer Rust et Cargo, puis ouvrir **4 terminaux dans le dossier du projet**.
Les commandes ci-dessous lancent les trois nodes sur la même machine avec des
clés de démonstration distinctes et la même liste de validateurs.
Arrêter les anciennes instances avec `Ctrl+C` avant de commencer.

### Terminal 1 — Node A

```bash
SECRET_KEY=0101010101010101010101010101010101010101010101010101010101010101 \
AUTHORIZED_VALIDATORS=8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c,8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394,ed4928c628d1c2c6eae90338905995612959273a5c63f93636c14614ac8737d1 \
PORT=3000 \
cargo run -p node
```

### Terminal 2 — Node B

```bash
SECRET_KEY=0202020202020202020202020202020202020202020202020202020202020202 \
AUTHORIZED_VALIDATORS=8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c,8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394,ed4928c628d1c2c6eae90338905995612959273a5c63f93636c14614ac8737d1 \
PORT=3001 \
cargo run -p node
```

### Terminal 3 — Node C

```bash
SECRET_KEY=0303030303030303030303030303030303030303030303030303030303030303 \
AUTHORIZED_VALIDATORS=8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c,8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394,ed4928c628d1c2c6eae90338905995612959273a5c63f93636c14614ac8737d1 \
PORT=3002 \
cargo run -p node
```

Laisser ces trois terminaux ouverts. Les nodes se découvrent automatiquement
par mDNS. Attendre les messages `Connected to peer` ou `incoming peer connected`
et les premiers `block added` avant de lancer le client.

### Terminal 4 — Client

```bash
NODE_URL=http://127.0.0.1:3000 \
METRICS_URLS=http://127.0.0.1:3000,http://127.0.0.1:3001,http://127.0.0.1:3002 \
TX_COUNT=10 \
CONCURRENCY=2 \
CONFIRM_TIMEOUT_SECS=60 \
cargo run -p client
```

Le client envoie 10 transactions à A, puis attend leur inclusion dans les mêmes
blocs sur les trois nodes. Le bilan attendu contient :

```text
confirmed everywhere: 10/10, pending: 0
```

Pour arrêter la démonstration, faire `Ctrl+C` dans chaque terminal des nodes.
