#!/usr/bin/env bash
# Profiles 3 validator nodes with samply (https://github.com/mstange/samply)
# while the client sends them a load. The nodes are separate processes named
# node-1..node-3, recorded by ONE samply (several samply instances exhaust the
# kernel's perf_event_mlock_kb budget: "Failed to start profiling: mmap failed").
# Output: target/profiles/nodes.json.gz, one process track per node.
# macOS: samply cannot attach to a signed system binary such as bash, so each
# node runs under its own samply instead -> target/profiles/node-N.json.gz.
#
# Usage: scripts/profile-nodes.sh   (TX_COUNT, SAMPLE_RATE, BLOCK_TIME_SECS, BASE_PORT overridable)
# View:  samply load target/profiles/nodes.json.gz
#        Each tokio worker is its own track: expand node-N, Ctrl+click all its
#        tokio-rt-worker tracks to merge them, then open the "Flame Graph" tab.
#
# Linux needs: sudo sysctl kernel.perf_event_paranoid=1
set -euo pipefail

cd "$(dirname "$0")/.."
TX_COUNT=${TX_COUNT:-50000}
SAMPLE_RATE=${SAMPLE_RATE:-4000}
BLOCK_TIME_SECS=${BLOCK_TIME_SECS:-1}
BASE_PORT=${BASE_PORT:-3101}
OUT=$PWD/target/profiles
NODES=3

command -v samply >/dev/null || { echo "samply missing: cargo install --locked samply" >&2; exit 1; }
if [[ -r /proc/sys/kernel/perf_event_paranoid ]] && (($(cat /proc/sys/kernel/perf_event_paranoid) > 1)); then
    echo "perf_event_paranoid > 1: run 'sudo sysctl kernel.perf_event_paranoid=1' first" >&2
    exit 1
fi

# Release code with symbols, so the flamegraph shows function names.
cargo build --profile profiling -p node -p client
mkdir -p "$OUT"
rm -f "$OUT"/node-*.log "$OUT"/node-*.json.gz "$OUT"/nodes.json.gz "$OUT"/samply.log
NODE_BIN=$PWD/target/profiling/node
CLIENT_BIN=$PWD/target/profiling/client

# Throwaway deterministic keys: seed i repeated over 32 bytes.
secret() { printf '%02x' $(for _ in $(seq 32); do echo "$1"; done); }
public() {
    # PKCS#8 header for a raw ed25519 seed, then keep the 32 raw public key bytes.
    { printf '302e020100300506032b657004220420' ; secret "$1"; } | xxd -r -p |
        openssl pkey -inform DER -pubout -outform DER | tail -c 32 | xxd -p -c 64
}
VALIDATORS=$(for i in $(seq $NODES); do public "$i"; done | paste -sd, -)

# Empty working dir: the repo's .env must not override this setup.
RUN_DIR=$(mktemp -d)
LAUNCHER=$RUN_DIR/launch.sh
{
    echo 'cd "$(dirname "$0")"'
    for i in $(seq $NODES); do
        port=$((BASE_PORT + i - 1))
        # One symlink per node: the process name (and samply track) is node-N.
        ln -s "$NODE_BIN" "$RUN_DIR/node-$i"
        echo "SECRET_KEY=$(secret "$i") AUTHORIZED_VALIDATORS=$VALIDATORS PORT=$port" \
            "BLOCK_TIME_SECS=$BLOCK_TIME_SECS RUST_LOG=${RUST_LOG:-warn}" \
            "./node-$i >'$OUT/node-$i.log' 2>&1 &"
        echo "echo \$! >> pids"
    done
    echo 'wait'
} >"$LAUNCHER"

MACOS=false
[[ $(uname) == Darwin ]] && MACOS=true
SAMPLY_PIDS=()
if $MACOS; then
    for i in $(seq $NODES); do
        (cd "$RUN_DIR" && SECRET_KEY=$(secret "$i") AUTHORIZED_VALIDATORS=$VALIDATORS \
            PORT=$((BASE_PORT + i - 1)) BLOCK_TIME_SECS=$BLOCK_TIME_SECS RUST_LOG=${RUST_LOG:-warn} \
            exec samply record --rate "$SAMPLE_RATE" --save-only -o "$OUT/node-$i.json.gz" -- "$NODE_BIN") \
            >"$OUT/node-$i.log" 2>&1 &
        SAMPLY_PIDS+=($!)
    done
else
    samply record --rate "$SAMPLE_RATE" --save-only -o "$OUT/nodes.json.gz" -- bash "$LAUNCHER" >"$OUT/samply.log" 2>&1 &
    SAMPLY_PIDS+=($!)
fi

# PIDs of the node processes themselves (macOS: found by their listening port).
node_pids() {
    if $MACOS; then
        for i in $(seq $NODES); do lsof -t -iTCP:$((BASE_PORT + i - 1)) -sTCP:LISTEN 2>/dev/null || true; done
    elif [[ -f $RUN_DIR/pids ]]; then
        cat "$RUN_DIR/pids"
    fi
}
cleanup() {
    # Stop the nodes; samply then sees its target exit and writes the profile.
    # SIGTERM: background jobs of a script ignore SIGINT.
    node_pids | xargs kill -TERM 2>/dev/null || true
    for pid in "${SAMPLY_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
    rm -rf "$RUN_DIR"
}
trap cleanup EXIT

fail() {
    echo "$1" >&2
    tail -n 5 "$OUT/samply.log" "$OUT"/node-*.log >&2 2>/dev/null || true
    exit 1
}

URLS=()
for i in $(seq $NODES); do URLS+=("http://127.0.0.1:$((BASE_PORT + i - 1))"); done

# Blocks rotate between validators, so a common height >= 4 means the mesh is up.
echo "waiting for the 3 nodes to discover each other (mDNS)..."
min=0
for _ in $(seq 120); do
    for pid in "${SAMPLY_PIDS[@]}"; do kill -0 "$pid" 2>/dev/null || fail "samply or a node stopped early"; done
    if [[ -f $RUN_DIR/pids ]]; then
        while read -r pid; do kill -0 "$pid" 2>/dev/null || fail "a node stopped early"; done <"$RUN_DIR/pids"
    fi
    heights=$(for url in "${URLS[@]}"; do
        curl -sf "$url/metrics" | grep -o '"height":[0-9]*' | cut -d: -f2 || echo 0
    done)
    min=$(sort -n <<<"$heights" | head -1)
    ((${min:-0} >= 4)) && break
    sleep 0.5
done
((${min:-0} >= 4)) || fail "nodes did not sync within 60s"

echo "sending $TX_COUNT transactions to node 1..."
NODE_URL=${URLS[0]} METRICS_URLS=$(IFS=,; echo "${URLS[*]}") TX_COUNT=$TX_COUNT \
    MIN_BATCH_SIZE=100 MAX_PAUSE_MS=0 \
    "$CLIENT_BIN" | tee "$OUT/client.log"

cleanup
trap - EXIT
if $MACOS; then
    for i in $(seq $NODES); do [[ -s $OUT/node-$i.json.gz ]] || fail "samply wrote no profile for node $i"; done
    echo
    echo "view: samply load target/profiles/node-1.json.gz  (node-2, node-3 likewise)"
else
    [[ -s $OUT/nodes.json.gz ]] || fail "samply wrote no profile"
    echo
    echo "view: samply load target/profiles/nodes.json.gz  (expand node-N)"
fi
echo "      Ctrl+click every tokio-rt-worker track to merge them, then open 'Flame Graph'"
