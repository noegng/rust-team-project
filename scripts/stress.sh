#!/usr/bin/env bash
# Stress test : 3 nodes locaux (A, B, C) et 2 clients qui spamment A et B en même temps
# (C ne reçoit les TX que par gossip).
#
# En direct :
#   [A] / [B] / [C]   warnings, erreurs, blocs produits/acceptés et connexions des nodes
#   [client 1|2]      progression chaque seconde, nouveaux motifs de rejet, erreurs HTTP
#   [metrics]         chain_len / mempool / reçues / commitées de chaque node toutes les 2 s
# À la fin : attend que les mempools se vident puis vérifie que les 3 nodes sont d'accord.
# Logs complets dans $LOG_DIR. Nécessite openssl, curl et jq.
#
#   scripts/stress.sh
#   TX_COUNT=200000 CONCURRENCY=100 BATCH_SIZE=500 scripts/stress.sh
set -euo pipefail
cd "$(dirname "$0")/.."

TX_COUNT=${TX_COUNT:-50000}   # par client
CONCURRENCY=${CONCURRENCY:-50}
BATCH_SIZE=${BATCH_SIZE:-100}
LOG_DIR=${LOG_DIR:-target/stress}
NAMES=(A B C)
PORTS=(8001 8002 8003)
export NO_COLOR=1 RUST_LOG=${RUST_LOG:-node=info,tower_http=warn}

for tool in openssl curl jq; do
  command -v "$tool" >/dev/null || { echo "$tool est requis" >&2; exit 1; }
done

cargo build --release -q -p node -p client
NODE=target/release/node
CLIENT=target/release/client
mkdir -p "$LOG_DIR"
trap 'kill $(jobs -p) 2>/dev/null; wait 2>/dev/null' EXIT

ESC=$(printf '\033')
# Préfixe chaque ligne, retire les couleurs et la date des logs tracing.
prefix() {
  sed -u -e "s/$ESC\[[0-9;]*m//g" -e 's/^[0-9-]*T\([0-9:]*\)\.[0-9]*Z */\1 /' -e "s/^/[$1] /"
}

metrics() {
  curl -s --max-time 1 "http://127.0.0.1:$1/metrics"
}

# Le node affiche sa clé publique puis s'arrête : AUTHORIZED_VALIDATORS vide
# (prioritaire sur un éventuel .env) est refusé.
pubkey() {
  SECRET_KEY=$1 AUTHORIZED_VALIDATORS='' "$NODE" 2>&1 |
    sed -n 's/.*node public key: \([0-9a-f]*\).*/\1/p' || true
}

SECRETS=()
PUBKEYS=()
for i in 0 1 2; do
  SECRETS[i]=$(openssl rand -hex 32)
  PUBKEYS[i]=$(pubkey "${SECRETS[i]}")
done
AUTHORIZED_VALIDATORS=$(IFS=,; echo "${PUBKEYS[*]}")
export AUTHORIZED_VALIDATORS

for i in 0 1 2; do
  name=${NAMES[i]}
  log=$LOG_DIR/$name.log
  : >"$log"
  SECRET_KEY=${SECRETS[i]} PORT=${PORTS[i]} "$NODE" >>"$log" 2>&1 &
  tail -F -n +1 "$log" 2>/dev/null |
    grep --line-buffered -E "WARN|ERROR|block (produced|accepted)|peer|listening" |
    prefix "$name" &
done

for port in "${PORTS[@]}"; do
  for _ in $(seq 50); do
    metrics "$port" >/dev/null && break
    sleep 0.2
  done
  metrics "$port" >/dev/null || { echo "le node du port $port ne répond pas" >&2; exit 1; }
done
sleep 3 # découverte mDNS des peers

running=$LOG_DIR/.running
touch "$running"
monitor() {
  while sleep 2 && [[ -e $running ]]; do
    line=""
    for i in 0 1 2; do
      state=$(metrics "${PORTS[i]}" |
        jq -r '"len=\(.chain_len) mempool=\(.mempool_len) recv=\(.txs_received) committed=\(.txs_committed)"' \
          2>/dev/null) || state=DOWN
      line+="${NAMES[i]} ${state:-DOWN} | "
    done
    echo "${line% | }"
  done | prefix metrics
}
monitor &

run_client() {
  NODE_URL=http://127.0.0.1:$2 METRICS_URLS=http://127.0.0.1:$2 \
    TX_COUNT=$TX_COUNT CONCURRENCY=$CONCURRENCY BATCH_SIZE=$BATCH_SIZE \
    "$CLIENT" 2>&1 | prefix "client $1"
}
echo "=== 2 clients x $TX_COUNT TX ($CONCURRENCY tâches, lots de $BATCH_SIZE) sur A et B ==="
run_client 1 "${PORTS[0]}" &
client1=$!
run_client 2 "${PORTS[1]}" &
client2=$!
wait "$client1" "$client2"

echo "=== clients terminés, attente de la vidange des mempools (60 s max) ==="
for _ in $(seq 30); do
  sleep 2
  states=$(for port in "${PORTS[@]}"; do
    metrics "$port" | jq -c '[.chain_len, .mempool_len, .txs_committed]' 2>/dev/null || echo DOWN
  done)
  if [[ $(sort -u <<<"$states" | wc -l) -eq 1 && $(jq '.[1]' <<<"${states%%$'\n'*}" 2>/dev/null) == 0 ]]; then
    break
  fi
done
rm -f "$running"
sleep 2.5 # dernière ligne du moniteur

echo "=== résultat ==="
for i in 0 1 2; do
  echo "${NAMES[i]}: $(metrics "${PORTS[i]}" || echo DOWN)"
  echo "   warnings: $(grep -c WARN "$LOG_DIR/${NAMES[i]}.log" || true), erreurs: $(grep -c ERROR "$LOG_DIR/${NAMES[i]}.log" || true)"
done
if [[ $(sort -u <<<"$states" | wc -l) -eq 1 && $states != DOWN* ]]; then
  echo "OK : les 3 nodes ont la même chain_len, le même nombre de TX commitées et un mempool vide"
else
  echo "DIVERGENCE : [chain_len, mempool_len, txs_committed] par node :"
  echo "$states"
  exit 1
fi
