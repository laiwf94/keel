#!/usr/bin/env bash
# Benchmarks a three-node keel cluster and saves the table under bench/results/.
#
#   bench/run.sh local    three JVMs on this machine (default)
#   bench/run.sh docker   the docker compose cluster; on macOS this mostly measures Docker
#                         Desktop's port forwarding, which adds milliseconds to every request
#
# CLIENTS, DURATION, WARMUP and VALUE_SIZE override the defaults.
set -euo pipefail
cd "$(dirname "$0")/.."

mode=${1:-local}
clients=${CLIENTS:-16}
duration=${DURATION:-15s}
warmup=${WARMUP:-3s}
value_size=${VALUE_SIZE:-100}
jar=build/libs/keel-0.1.0.jar
secret=$(openssl rand -hex 24)
mkdir -p bench/results
out=bench/results/$mode-$(date +%H%M%S).txt
pids=()
data=""

cleanup() {
  if [[ $mode == local ]]; then
    for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
    [[ -n $data ]] && rm -rf "$data"
  else
    KEEL_CLUSTER_SECRET=$secret docker compose down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

./gradlew -q jar

case $mode in
  local)
    data=$(mktemp -d)
    members="n1@127.0.0.1:17101@127.0.0.1:18101,n2@127.0.0.1:17102@127.0.0.1:18102,n3@127.0.0.1:17103@127.0.0.1:18103"
    for i in 1 2 3; do
      java -jar "$jar" server --id "n$i" --members "$members" --secret "$secret" \
        --peer-bind "127.0.0.1:1710$i" --http-bind "127.0.0.1:1810$i" --data "$data/n$i" \
        --log-level warn >"$data/n$i.log" 2>&1 &
      pids+=($!)
    done
    endpoints=http://127.0.0.1:18101,http://127.0.0.1:18102,http://127.0.0.1:18103
    ;;
  docker)
    KEEL_CLUSTER_SECRET=$secret docker compose up -d --build --wait >/dev/null
    endpoints=http://localhost:8081,http://localhost:8082,http://localhost:8083
    ;;
  *)
    echo "usage: bench/run.sh [local|docker]" >&2
    exit 2
    ;;
esac

for _ in $(seq 1 50); do
  if java -jar "$jar" status --endpoints "$endpoints" --retry-for 1s 2>/dev/null | grep -q '"leader":"n'; then
    break
  fi
  sleep 0.2
done

{
  echo "mode: $mode"
  echo "machine: $(uname -sm), $(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu) cpus"
  echo "java: $(java -version 2>&1 | head -1)"
  echo "clients: $clients  value size: $value_size bytes  warmup: $warmup  measured: $duration"
  echo
  java -jar "$jar" bench --endpoints "$endpoints" --clients "$clients" --value-size "$value_size" \
    --warmup "$warmup" --duration "$duration"
} | tee "$out"
