# Authora (`rust-team-project`)

A private **Proof of Authority (PoA)** mini-blockchain written in Rust: one
workspace, **3 validating full nodes** and a **benchmark client**. The original
assignment is in [authora.md](authora.md); this README follows its outline and
states, for each part, what was built and where the code lives.

**Contents**: [Goal](#goal) · [Stack](#stack) · [Workspace](#workspace-layout) ·
[Data structures](#data-structures-blockchain-crate) · [Cryptography](#cryptography) ·
[Network](#network) · [PoA consensus](#poa-consensus) · [Benchmark and metrics](#benchmark-and-metrics) ·
[Final demo](#expected-final-demo) · [Network robustness](#network-robustness-what-multi-pc-testing-revealed) ·
[Deviations from the spec](#deviations-from-the-spec) · [Running the project](#running-the-demo-in-4-terminals)

## Goal

Three nodes, each holding an authorized ed25519 key, receive signed
transactions, gossip them to each other and take turns producing a signed
block every `BLOCK_TIME_SECS` seconds. At the end, all three nodes have
**the same chain**, and we measure **TPS**, **propagation latency**,
**synchronization lag** and **time to finality**.

## Stack

| Need | Crate used |
|---|---|
| Async runtime | `tokio` |
| HTTP + WebSocket server | `axum` 0.8 (`ws`) |
| WebSocket client (connecting to peers) | `tokio-tungstenite` |
| Hashing | `sha2` (SHA-256) |
| Signatures, key pairs | `ed25519-dalek` |
| Serialization | `serde` + `serde_json` |
| HTTP client (benchmark) | `reqwest` |
| Key generation | `rand` |
| *Additions*: peer discovery | `mdns-sd` |
| *Additions*: parallel signature verification | `rayon` |
| *Additions*: logging, dashboard CORS | `tracing`, `tower-http` |

## Workspace layout

```
rust-team-project/
├── Cargo.toml                 ← workspace (blockchain, node, client)
├── crates/
│   ├── blockchain/            ← lib: protocol types and rules
│   │   └── src/ block.rs · transaction.rs · chain.rs · mempool.rs
│   ├── node/                  ← binary: full node
│   │   └── src/ main.rs       ← config loading (.env / variables)
│   │            lib.rs        ← shared state, routes, startup
│   │            api.rs        ← HTTP: TX submission, blocks, metrics
│   │            p2p.rs        ← WebSocket gossip between nodes
│   │            discovery.rs  ← mDNS discovery + reconnection
│   │            validator.rs  ← block production and acceptance
│   │            metrics.rs    ← counters (TPS, propagation, finality…)
│   │            dashboard.html← live dashboard served on /
│   │            testnet.rs    ← in-memory 3-node network (tests, benches)
│   └── client/                ← binary: spammer / benchmark
│       └── src/ main.rs · network.rs · confirmation.rs
└── scripts/ stress.sh · profile-nodes.sh
```

The assignment's split (P1 `blockchain`, P2 node networking, P3 validator +
client) maps onto this layout: `blockchain` has no network dependency and is
tested on its own, while `node` and `client` build on it.

## Data structures (`blockchain` crate)

| Type | Contents | Rules checked |
|---|---|---|
| `Transaction` | `from`, `to` (public keys), `amount`, `nonce`, `timestamp`, `signature` | Sender's ed25519 signature over all other fields; SHA-256 hash = TX identifier |
| `Block` | `BlockData` (`index`, `timestamp`, `prev_hash`, `hash`, `transactions`, `tx_root`) + `validator` + `signature` | `tx_root` = hash of the TXs, `hash` recomputed, validator signature over the header |
| `Chain` | Blocks since genesis + ordered list of validators | Valid successor: correct index, correct `prev_hash`, increasing timestamp, authorized validator **and** expected proposer, signatures, sender nonces, at most `MAX_TXS_PER_BLOCK` (20,000) TXs |
| `Mempool` | Pending TXs, indexed by hash, deduplicated | Rejects duplicates; `drain` only takes TXs whose nonce follows the chain's (per-sender ordering) |

**Replay protection**: each sender has a `nonce` that must be exactly the next
one the chain expects. A replayed or out-of-order TX is rejected.

## Cryptography

- **Node key**: `SECRET_KEY` (32 hex bytes). The node prints its public key at
  startup (`node public key: …`), to be put in `AUTHORIZED_VALIDATORS` on all
  three machines.
- **TX signing** on the client side, **block signing** on the validator side.
- **Verification** of every signature on receipt, by every node. To keep up
  with load, the signatures of a batch are verified **in parallel** (`rayon`)
  and **outside the locks** of the chain and the mempool.

## Network

```
[Client] ── HTTP POST /transactions (TX batches) ──► [Node A]
                                                        │  WebSocket gossip (/ws)
                                        ┌───────────────┴───────────────┐
                                     [Node B] ◄──────── WS ────────► [Node C]
```

- **HTTP entry point (`api.rs`)**: `POST /transactions` accepts a **batch** of
  TXs, checks each one, adds the valid ones to the mempool and broadcasts them
  to peers in a single message (`NewTxs`).
- **Gossip (`p2p.rs`)**: each pair of nodes shares one WebSocket connection.
  Messages exchanged (JSON `{"type", "payload"}`):

  | Message | Purpose |
  |---|---|
  | `Hello(key)` | First message: sender identity (detects a connection to itself) |
  | `NewTx` / `NewTxs` | Accepted transactions |
  | `NewBlock` | Produced or relayed block |
  | `HistoryEnd` | End of the history sent to a new peer |
  | `Ping` | Heartbeat (connection alive) |

- **On connection**, each node sends the other its whole chain followed by its
  mempool (*replay*): a restarted or lagging node catches up this way.
- **Discovery**: the assignment called for hard-coded addresses. We use
  **mDNS** (`discovery.rs`): nodes on the same network find each other with no
  configuration. Only the node with the smaller identifier initiates the
  connection of each pair (one connection per pair), and it re-establishes it
  after any drop.

## PoA consensus

- **Round-robin rotation**: block index *n* belongs to
  `AUTHORIZED_VALIDATORS[n % 3]`. The list order must therefore be
  **identical on all three nodes**.
- **Production (`validator.rs`)**: every `BLOCK_TIME_SECS`, if it is its turn,
  the node drains the mempool (up to 20,000 TXs), builds the block, signs it,
  appends it to its chain and broadcasts it. **Empty blocks** are produced
  too, otherwise the rotation would stall.
- **Accepting a received block**: all the `Chain` rules above. An already known
  block is ignored; an invalid block drops the connection, which is then
  re-established and replays the history (`sync_resets` in the metrics).
- **Startup synchronization**: a node produces nothing until it has received
  its peers' history (see [Robustness](#network-robustness-what-multi-pc-testing-revealed)).

## Benchmark and metrics

The **client** (`crates/client`) signs TXs with several sender keys, sends them
in batches from `CONCURRENCY` parallel tasks, then **checks their inclusion on
every node** (`METRICS_URLS`) and prints a summary. It can also simulate
continuous traffic (random batches and pauses).

| Assignment metric | Where to read it | Computation |
|---|---|---|
| TPS | `GET /metrics` → `tps`, client summary | Actual throughput of the last burst: TXs received between the first and last receipt (a burst ends after 1 s with no TX) |
| Propagation latency | `avg_propagation_ms` | TX timestamp (set by the client) → receipt by each node |
| Synchronization lag | `mempool_len`, `height`, `last_block_hash` of each node | Gap between nodes, visible on the dashboard |
| Time to finality | `avg_finality_ms`, client summary | TX submitted → included in a block on **all** nodes |

Additional tools: [live dashboard](#dashboard),
[multi-machine stress test](#stress-test-scriptsstresssh),
[Criterion benchmarks](#network-benchmark-criterion) and
[flamegraphs](#per-node-flamegraph-samply).

## Expected final demo

| Assignment step | How to show it |
|---|---|
| 1. The 3 nodes start and connect | `connected to peer` in the logs; all 3 nodes on the dashboard |
| 2. The client sends 10,000 TXs to node A | `TX_COUNT=10000 … cargo run --release -p client` (see below) |
| 3. B and C receive every TX via gossip | `txs_received` of each node, dashboard TX/s charts |
| 4. The designated validator produces a signed block every N s | "TXs per block" histogram, colored by producing node; latest blocks table |
| 5. The 3 nodes have the same chain | "Synchronized: 3 nodes at block #…" banner; summary `confirmed everywhere: 10000/10000` |
| 6. Display of TPS, propagation, chain length | Dashboard node cards; `GET /metrics` |

## Network robustness (what multi-PC testing revealed)

Tests on 3 PCs linked through ZeroTier or a phone hotspot exposed failures that
never showed up locally. Each one has a dedicated fix:

| Observed problem | Fix |
|---|---|
| A restarted node starts again from genesis and creates its own block 1 → **fork** and reconnection loops | The node only produces after discovering its peers and receiving **their whole history** (`HistoryEnd`) |
| A connection that dies silently leaves a node **isolated**; the chain stalls on its turn | **Heartbeat**: `Ping` every 2 s, connection dropped after 15 s of silence, then re-established |
| On an iPhone hotspot, every device has the address `192.0.0.2` → a node **connects to itself** | Those addresses are ignored, and the `Hello` message (public key) rejects any connection to oneself |
| TPS averaged since startup, misleading | TPS measured over the **last burst** |
| Block too large for a WebSocket frame (16 MB) | Cap of **20,000 TXs per block** |

**Still-open limitations**: if a validator is stopped, the chain waits for its
turn (no turn skipping); a fork that has already formed is not repaired
automatically (no longest-chain rule); on a slow or relayed network,
throughput between nodes drops sharply. Prefer a single local network.

## Deviations from the spec

| Spec | Implementation | Reason |
|---|---|---|
| `POST /tx`, one TX | `POST /transactions`, a batch of TXs | Throughput: fewer requests and a single gossip message per batch |
| Hard-coded peers | mDNS discovery | No IP to type in, works across 3 PCs |
| `Message { NewTx, NewBlock, Ping }` | + `NewTxs`, `HistoryEnd`, `Hello` | Batches, startup catch-up, self-detection |
| `mempool.drain()` of every TX | TXs in nonce order, 20,000 max per block | Replay protection and frame size |
| Successor validation | + increasing timestamp, expected proposer, nonces | Consensus safety |
| Block produced only when there are TXs | Empty blocks produced | The rotation must not stall |

## Running the demo in 4 terminals

Install Rust and Cargo, then open **4 terminals in the project folder**.
The commands below start the three nodes on the same machine with distinct
demo keys and the same validator list.

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

Wait for the `connected to peer` / `incoming peer connected` messages and the first `block added` before starting the client.

### Terminal 4 - Client

For a quick test (10 TXs):

```bash
NODE_URL=http://127.0.0.1:3000 \
METRICS_URLS=http://127.0.0.1:3000,http://127.0.0.1:3001,http://127.0.0.1:3002 \
TX_COUNT=10 \
CONCURRENCY=2 \
BATCH_SIZE=5 \
CONFIRM_TIMEOUT_SECS=60 \
cargo run -p client
```

To simulate continuous traffic (random batches and pauses) until Ctrl+C:

```bash
cargo run --release -p client
```

For the 10,000-TX benchmark (maximum throughput, no pauses):

```bash
NODE_URL=http://127.0.0.1:3000 \
METRICS_URLS=http://127.0.0.1:3000,http://127.0.0.1:3001,http://127.0.0.1:3002 \
TX_COUNT=10000 MIN_BATCH_SIZE=100 MAX_PAUSE_MS=0 \
cargo run --release -p client
```

The expected summary contains:

```text
confirmed everywhere: 10000/10000, pending: 0
```

## Running on 3 different PCs (same local network)

Nodes listen on `0.0.0.0` and discover each other automatically via mDNS on the same Wi-Fi or hotspot. Every PC can keep `PORT=3000`.

1. **On each PC**, generate a secret key and get its public key:
   ```bash
   openssl rand -hex 32
   SECRET_KEY=<PC_PRIVATE_KEY> cargo run -p node
   ```
   The program prints `node public key: <PUBLIC_KEY>` and exits.

2. **Share the 3 public keys** and create a `.env` file at the project root on each of the 3 PCs with the **same `AUTHORIZED_VALIDATORS` list in the same order**:
   ```env
   SECRET_KEY=<THIS_PC_PRIVATE_KEY>
   AUTHORIZED_VALIDATORS=<PUB_A>,<PUB_B>,<PUB_C>
   PORT=3000
   BLOCK_TIME_SECS=2
   ```

3. **Start a node on each PC**:
   ```bash
   cargo run --release -p node
   ```
   Wait for the `connected to peer` / `incoming peer connected` and `block added` messages on all 3 machines. (Allow port `3000` if the firewall asks.)

4. **Start the client from one of the PCs**. It sends TXs to the local node (`http://127.0.0.1:3000` by default), which spreads them to the others through blocks:
   ```bash
   cargo run --release -p client
   ```
   Optional: to check that TXs are included on **all 3 nodes** (and show their metrics), add their local IPs (`ipconfig getifaddr en0` on macOS, `hostname -I` on Linux, `ipconfig` on Windows):
   ```bash
   METRICS_URLS=http://127.0.0.1:3000,http://<IP_PC_B>:3000,http://<IP_PC_C>:3000 \
   cargo run --release -p client
   ```

5. **Watch live**: open `http://<IP_PC_A>:3000/` in a browser (see [Dashboard](#dashboard)).

> **Demo-day tip**: if a room's or school's Wi-Fi blocks mDNS discovery between devices ("client isolation"), connect the 3 PCs to a phone hotspot.

## Dashboard

Each node serves a live dashboard on `/` (for example `http://127.0.0.1:3000/`):
received-TX throughput per node over the last 60 seconds, mempool, height,
propagation and finality of each node, latest blocks with their producer, and
a consensus status (synchronized, node lagging or stuck, no new block, divergence).

The other nodes are found via `GET /peers` (mDNS discovery). If mDNS does not
get through, set the list in the URL:
`http://<IP_PC_A>:3000/?nodes=http://<IP_PC_A>:3000,http://<IP_PC_B>:3000,http://<IP_PC_C>:3000`.

The dashboard also follows tests and benchmarks:

- `scripts/stress.sh` and `scripts/profile-nodes.sh` print its URL at startup;
- `Testnet` nodes (tests, Criterion) announce each other in `GET /peers`, so the `/` of any of them shows the whole network;
- `DASHBOARD_PORT=3200 cargo bench -p node --features testnet -- stress` pins the ports (3200-3202) of every network the bench starts, and prints the URL to open once: the same tab follows every iteration (each iteration starts from a fresh chain). Browser polling adds a little load to the measurements.

## Stress test (`scripts/stress.sh`)

Requires `curl` and `jq` (and `openssl` locally). The script shows client
progress and each node's metrics every 2 s, then waits until all nodes have
the same last block, the same included TXs and an empty mempool (exit code 1
otherwise).

**On one machine** (3 nodes started by the script on ports 8001 to 8003, 2 clients):

```bash
scripts/stress.sh
```

**On several machines**: start a node on each PC (see above), then from a
single driver PC, with one client per node:

```bash
NODES=http://<IP_PC_A>:3000,http://<IP_PC_B>:3000,http://<IP_PC_C>:3000 scripts/stress.sh
```

| Variable | Default | Purpose |
|---|---|---|
| `NODES` | empty (local mode) | URLs of already running nodes |
| `CLIENTS` | number of nodes (2 locally) | Parallel clients, spread across nodes |
| `TX_COUNT` | `50000` | TXs **per client** |
| `CONCURRENCY` / `BATCH_SIZE` | `50` / `100` | Passed to each client |
| `CONFIRM_TIMEOUT_SECS` | `120` | Confirmation timeout of each client |
| `SETTLE_TIMEOUT_SECS` | `60` | Timeout for final convergence |
| `LOG_DIR` | `target/stress` | Logs of local nodes |

> **Known limitation**: a block of more than about 30,000 TXs exceeds 16 MB of
> JSON, the maximum WebSocket frame size between nodes. The other nodes cannot
> receive it and the chain stops. Keep `CLIENTS × TX_COUNT` below that
> threshold per block interval as long as block size is not capped.

## Configuration (optional `.env`)

A template is available in `.env.example`. Variables already set in the shell take precedence.

### Node variables

| Variable | Default | Purpose |
|---|---|---|
| `SECRET_KEY` | required | ed25519 secret key (hex, 32 bytes) |
| `AUTHORIZED_VALIDATORS` | required | Comma-separated hex public keys (order = PoA rotation) |
| `PORT` | `3000` | HTTP + WebSocket port |
| `BLOCK_TIME_SECS` | `2` | Block production interval (in seconds) |
| `RUST_LOG` | `node=debug,tower_http=debug` | Log filter |

### Client variables

| Variable | Default | Purpose |
|---|---|---|
| `NODE_URL` | `http://127.0.0.1:3000` | Target node for submissions |
| `TX_COUNT` | empty (continuous) | Total number of TXs to send, then wait for confirmations. Empty: send until Ctrl+C |
| `CONCURRENCY` | `50` | Number of parallel tasks (1 sender key each) |
| `BATCH_SIZE` | `100` | Max size of a `POST /transactions` batch |
| `MIN_BATCH_SIZE` | `1` | Min batch size (uniform draw up to `BATCH_SIZE`) |
| `MAX_PAUSE_MS` | `1000` | Max random pause before each batch, per task (`0` = maximum throughput) |
| `METRICS_URLS` | `NODE_URL` | Nodes on which to check confirmations and read `/metrics` |
| `CONFIRM_TIMEOUT_SECS` | `60` | Max time for confirmation on all nodes |
| `POLL_INTERVAL_MS` | `500` | Block polling frequency for confirmation |

## Node API

| Route | Purpose |
|---|---|
| `POST /transactions` | Submits a `[Transaction]` batch and broadcasts the valid TXs to peers |
| `GET /transactions/{hash}` | Status of a TX (`pending` or `included` with block number and hash) |
| `GET /blocks?limit=N` | Summary of the last N blocks (20 by default, 100 max), newest first, without TXs |
| `GET /blocks/{index}` | Fetches a block by index |
| `GET /metrics` | `txs_received`, `tps` (actual throughput of the last send: TXs received between the first and last receipt; a send ends after 1 s with no TX), `avg_propagation_ms`, `txs_committed`, `avg_finality_ms`, `chain_len`, `mempool_len`, `txs_rejected` (TXs refused by `POST /transactions`), `sync_resets` (peer connections dropped over a rejected block), `height`, `last_block_hash`, `public_key` |
| `GET /peers` | Peers discovered via mDNS: `[{node_id, urls}]` |
| `GET /mempool` | Mempool contents, grouped by sender (shown by the dashboard) |
| `GET /` | Live dashboard |
| `GET /ws` | WebSocket connection between nodes |

## Tests

```bash
cargo test --workspace --all-features          # unit tests + integration test
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
```

The integration test `crates/node/tests/network.rs` replays the final demo
through the public API: 3 validators, 500 TXs sent to node A, then a check
that all 3 nodes have the same chain, every TX included and an empty mempool.
It needs the `testnet` feature (hence `--all-features`).

## Network benchmark (Criterion)

3 validator nodes each run in a Tokio task (loopback ports) and a 4th task sends them TXs over HTTP (`node::testnet`, `testnet` feature).

```bash
cargo bench -p node --features testnet            # full measurement
cargo bench -p node --features testnet -- --test  # quick check (1 iteration)
```

| Bench | Measures |
|---|---|
| `sync/load_to_sync/{100,1000,5000}` | Sending N TXs to node A until all 3 nodes have the same blocks and an empty mempool |
| `bad_sync/reset_on_bad_block` | Time for a node to drop a peer that sends an invalid block |
| `bad_sync/load_with_bad_blocks/1000` | Same load, while a malicious peer sends 5 invalid blocks to node B |
| `catch_up/late_node/1000` | Catch-up of a new node on a chain holding 1000 TXs |
| `stress/ramp/{tps}` | Load ramp, blocks every 10 ms: 12 paced senders (open loop) spread over the 3 nodes send `tps` TX/s for 2 s, then 5 s to commit everything. 3 runs per step (outside Criterion, whose sampling requires at least 10 runs + warm-up; 1 run with `--test`). Offered load ×1.5 at each step from 5,000 TX/s; stops at the first step whose error rate (rejected + lost + not committed everywhere in time) exceeds 1% |

`cargo bench -p node --features testnet -- stress` runs only the stress test; it prints each step's error rate, then the best TPS-to-sync with at most 1% errors.

After Criterion, the bench prints and writes to `target/criterion/network_stats.json`: mean / p50 / p95 / max sync time, lag after sending ends, error rate (rejected or lost TXs / TXs sent), number of sync resets (total, from invalid blocks, unexpected), mean propagation and finality, TPS until sync, largest block (limit `MAX_TXS_PER_BLOCK` = 20,000).

## Per-node flamegraph (samply)

`scripts/profile-nodes.sh` starts 3 nodes (separate processes `node-1..3`, ports 3101-3103, mDNS discovery) under a single [samply](https://github.com/mstange/samply), then the client sends them `TX_COUNT` TXs (default 50000, sampling `SAMPLE_RATE` = 4000 Hz). A single terminal is enough. The profile `target/profiles/nodes.json.gz` holds one track per node (`--profile profiling` build: release + symbols).

One samply for all 3 nodes: several instances exhaust `kernel.perf_event_mlock_kb` (`Failed to start profiling: mmap failed`).

```bash
cargo install --locked samply                # once
sudo sysctl kernel.perf_event_paranoid=1     # Linux, once per boot
scripts/profile-nodes.sh                     # TX_COUNT=50000 BLOCK_TIME_SECS=1 scripts/profile-nodes.sh
samply load target/profiles/nodes.json.gz    # expand node-N, Ctrl+click all its tokio-rt-worker tracks, "Flame Graph" tab
```
