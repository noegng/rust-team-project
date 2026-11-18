#!/usr/bin/env bash
# Stress test d'Authora. Deux modes :
#
#   Local (3 nodes A, B, C lancés sur cette machine, 2 clients sur A et B) :
#     scripts/stress.sh
#
#   Multi-machines (les nodes tournent déjà sur leurs PC, ce PC pilote) :
#     NODES=http://192.168.1.10:3000,http://192.168.1.11:3000,http://192.168.1.12:3000 scripts/stress.sh
#   Un client par node (CLIENTS pour changer), chacun vérifie l'inclusion sur tous les nodes.
#
# En direct : progression des clients, métriques de chaque node toutes les 2 s
# (et, en local, warnings / erreurs / blocs des nodes). Dashboard : <node>/ dans un navigateur.
# À la fin : attend que les mempools se vident puis vérifie que tous les nodes
# ont la même hauteur, le même dernier bloc et le même nombre de TX incluses.
#
#   TX_COUNT=200000 CONCURRENCY=100 BATCH_SIZE=500 scripts/stress.sh
# Nécessite curl et jq (et openssl en local).
set -euo pipefail
cd "$(dirname "$0")/.."

TX_COUNT=${TX_COUNT:-50000} # par client
CONCURRENCY=${CONCURRENCY:-50}
BATCH_SIZE=${BATCH_SIZE:-100}
CONFIRM_TIMEOUT_SECS=${CONFIRM_TIMEOUT_SECS:-120}
SETTLE_TIMEOUT_SECS=${SETTLE_TIMEOUT_SECS:-60}
LOG_DIR=${LOG_DIR:-target/stress}
export NO_COLOR=1 RUST_LOG=${RUST_LOG:-node=info,tower_http=warn}

tools=(curl jq)
[[ -z ${NODES:-} ]] && tools+=(openssl)
for tool in "${tools[@]}"; do
  command -v "$tool" >/dev/null || { echo "$tool est requis" >&2; exit 1; }
done

cargo build --release -q -p client $([[ -z ${NODES:-} ]] && echo "-p node")
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
  curl -s --max-time 2 "$1/metrics"
}

if [[ -n ${NODES:-} ]]; then
  IFS=, read -r -a URLS <<<"${NODES// /}"
  URLS=("${URLS[@]%/}")
  NAMES=()
  for url in "${URLS[@]}"; do NAMES+=("${url#*://}"); done
  CLIENTS=${CLIENTS:-${#URLS[@]}}
else
  # Le node affiche sa clé publique puis s'arrête : AUTHORIZED_VALIDATORS vide
  # (prioritaire sur un éventuel .env) est refusé.
  pubkey() {
    SECRET_KEY=$1 AUTHORIZED_VALIDATORS='' "$NODE" 2>&1 |
      sed -n 's/.*node public key: \([0-9a-f]*\).*/\1/p' || true
  }
  NAMES=(A B C)
  PORTS=(8001 8002 8003)
  URLS=()
  SECRETS=()
  PUBKEYS=()
  for i in 0 1 2; do
    URLS+=("http://127.0.0.1:${PORTS[i]}")
    SECRETS[i]=$(openssl rand -hex 32)
    PUBKEYS[i]=$(pubkey "${SECRETS[i]}")
  done
  AUTHORIZED_VALIDATORS=$(IFS=,; echo "${PUBKEYS[*]}")
  export AUTHORIZED_VALIDATORS
  for i in 0 1 2; do
    log=$LOG_DIR/${NAMES[i]}.log
    : >"$log"
    SECRET_KEY=${SECRETS[i]} PORT=${PORTS[i]} "$NODE" >>"$log" 2>&1 &
    tail -F -n +1 "$log" 2>/dev/null |
      grep --line-buffered -E "WARN|ERROR|block added|peer|listening" |
      prefix "${NAMES[i]}" &
  done
  CLIENTS=${CLIENTS:-2}
fi
ALL_URLS=$(IFS=,; echo "${URLS[*]}")

for i in "${!URLS[@]}"; do
  for _ in $(seq 50); do
    metrics "${URLS[i]}" >/dev/null && break
    sleep 0.2
  done
  metrics "${URLS[i]}" >/dev/null || { echo "le node ${NAMES[i]} (${URLS[i]}) ne répond pas" >&2; exit 1; }
done
[[ -z ${NODES:-} ]] && sleep 3 # découverte mDNS des peers locaux

echo "=== dashboard : ${URLS[0]}/?nodes=$ALL_URLS ==="

running=$LOG_DIR/.running
touch "$running"
monitor() {
  while sleep 2 && [[ -e $running ]]; do
    line=""
    for i in "${!URLS[@]}"; do
      state=$(metrics "${URLS[i]}" |
        jq -r '"h=\(.height) mempool=\(.mempool_len) recv=\(.txs_received) committed=\(.txs_committed)"' \
          2>/dev/null) || state=DOWN
      line+="${NAMES[i]} ${state:-DOWN} | "
    done
    echo "${line% | }"
  done | prefix metrics
}
monitor &

run_client() {
  NODE_URL=$2 METRICS_URLS=$ALL_URLS CONFIRM_TIMEOUT_SECS=$CONFIRM_TIMEOUT_SECS \
    TX_COUNT=$TX_COUNT CONCURRENCY=$CONCURRENCY BATCH_SIZE=$BATCH_SIZE \
    "$CLIENT" 2>&1 | prefix "client $1"
}
echo "=== $CLIENTS clients x $TX_COUNT TX ($CONCURRENCY tâches, lots de $BATCH_SIZE) ==="
pids=()
for ((c = 0; c < CLIENTS; c++)); do
  target=${URLS[c % ${#URLS[@]}]}
  echo "client $((c + 1)) → $target"
  run_client $((c + 1)) "$target" &
  pids+=($!)
done
failed_clients=0
for pid in "${pids[@]}"; do
  wait "$pid" || failed_clients=$((failed_clients + 1))
done

echo "=== clients terminés, attente de la convergence (${SETTLE_TIMEOUT_SECS} s max) ==="
# Convergé : mempools vides, même nombre de TX incluses, même hauteur et même dernier bloc.
converged=false
for _ in $(seq $((SETTLE_TIMEOUT_SECS * 2))); do
  states=$(for url in "${URLS[@]}"; do
    metrics "$url" | jq -c '[.height, .last_block_hash[:16], .mempool_len, .txs_committed]' 2>/dev/null || echo DOWN
  done)
  if [[ $states != *DOWN* && $(sort -u <<<"$states" | wc -l) -eq 1 && $(jq '.[2]' <<<"${states%%$'\n'*}") == 0 ]]; then
    converged=true
    break
  fi
  sleep 0.5
done
rm -f "$running"
sleep 2.5 # dernière ligne du moniteur

echo "=== résultat ==="
for i in "${!URLS[@]}"; do
  echo "${NAMES[i]}: $(metrics "${URLS[i]}" | jq -c 'del(.public_key)' 2>/dev/null || echo DOWN)"
  log=$LOG_DIR/${NAMES[i]}.log
  if [[ -f $log ]]; then
    echo "   warnings: $(grep -c WARN "$log" || true), erreurs: $(grep -c ERROR "$log" || true)"
  fi
done
if [[ $failed_clients -gt 0 ]]; then
  echo "ÉCHEC : $failed_clients client(s) en erreur (TX rejetées ou non confirmées, voir ci-dessus)"
  exit 1
fi
if $converged; then
  echo "OK : ${#URLS[@]} nodes au même bloc, mêmes TX incluses, mempools vides"
else
  echo "DIVERGENCE : [hauteur, dernier hash, mempool, TX incluses] par node :"
  echo "$states"
  exit 1
fi
