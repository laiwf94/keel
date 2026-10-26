#!/usr/bin/env bash
# Live demo on the docker compose cluster. It checks its own results and exits non-zero if any
# check fails.
#
#   1. Starts three nodes and a write load that records every acknowledged write.
#   2. Kills the leader with SIGKILL mid-load, shows a new leader in a higher term, restarts the
#      old one, and reads back every acknowledged write.
#   3. Cuts the current leader off from its peers, shows that it cannot commit while the majority
#      elects a new leader and keeps serving, then heals the partition and shows the old leader
#      catch up and drop its uncommitted write.
set -euo pipefail
cd "$(dirname "$0")/.."

jar=build/libs/keel-0.1.0.jar
export KEEL_CLUSTER_SECRET=${KEEL_CLUSTER_SECRET:-$(openssl rand -hex 24)}
export KEEL_ENDPOINTS=http://localhost:8081,http://localhost:8082,http://localhost:8083
work=$(mktemp -d)
load_pid=""

cleanup() {
  [[ -n $load_pid ]] && kill "$load_pid" 2>/dev/null || true
  docker compose down -v >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

say() { printf '\n== %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
keel() { java -jar "$jar" "$@"; }

# Fields from a node's /v1/status: id role term leader commit_index applied_index, with "-" for
# no known leader.
status() {
  local s
  s=$(curl -s -m 2 "http://localhost:$1/v1/status") || return 1
  [[ -n $s ]] || return 1
  local leader
  leader=$(sed -E 's/.*"leader":("([^"]*)"|null).*/\2/' <<<"$s")
  printf '%s %s %s %s %s %s\n' \
    "$(sed -E 's/^\{"id":"([^"]*)".*/\1/' <<<"$s")" \
    "$(sed -E 's/.*"role":"([^"]*)".*/\1/' <<<"$s")" \
    "$(sed -E 's/.*"term":([0-9]+).*/\1/' <<<"$s")" \
    "${leader:--}" \
    "$(sed -E 's/.*"commit_index":([0-9]+).*/\1/' <<<"$s")" \
    "$(sed -E 's/.*"applied_index":([0-9]+).*/\1/' <<<"$s")"
}

show() {
  printf '%-4s %-14s %-5s %-7s %-7s\n' node role term leader applied
  for p in 8081 8082 8083; do
    if read -r id role term leader _ applied < <(status "$p"); then
      printf '%-4s %-14s %-5s %-7s %-7s\n' "$id" "$role" "$term" "$leader" "$applied"
    else
      printf '%-4s %s\n' "n${p: -1}" "(down)"
    fi
  done
}

# Prints "id term port" of the leader that the given ports agree on, if any.
leader() {
  for p in "$@"; do
    if read -r id role term _ _ _ < <(status "$p") && [[ $role == leader ]]; then
      echo "$id $term $p"
      return 0
    fi
  done
  return 1
}

wait_for() {
  local what=$1 tries=$2
  shift 2
  for _ in $(seq 1 "$tries"); do
    if "$@"; then return 0; fi
    sleep 0.5
  done
  fail "timed out waiting for $what"
}

new_leader_after() {
  local old_id=$1 old_term=$2
  shift 2
  read -r id term _ < <(leader "$@") || return 1
  [[ $id != "$old_id" && $term -gt $old_term ]]
}

say "building the client and starting three nodes"
./gradlew -q jar
docker compose up -d --build --wait >"$work/up.log" 2>&1 || { cat "$work/up.log"; fail "cluster did not start"; }
wait_for "a leader" 60 leader 8081 8082 8083 >/dev/null
show

say "write load: 4 clients for 25 s, every acknowledged write recorded"
keel load --duration 25s --clients 4 --out "$work/acked.log" >"$work/load.out" 2>&1 &
load_pid=$!
sleep 6

read -r victim term1 port1 < <(leader 8081 8082 8083)
service=keel${victim#n}
say "killing the leader $victim (term $term1) with SIGKILL, mid-load"
docker compose kill -s SIGKILL "$service" >/dev/null 2>&1
others=()
for p in 8081 8082 8083; do [[ $p != "$port1" ]] && others+=("$p"); done
wait_for "a new leader" 60 new_leader_after "$victim" "$term1" "${others[@]}"
read -r leader2 term2 _ < <(leader "${others[@]}")
echo "new leader: $leader2 in term $term2 (was $victim in term $term1)"
show

sleep 4
say "restarting $victim from its own disk"
docker compose start "$service" >/dev/null 2>&1
wait_for "$victim to rejoin" 60 bash -c "curl -sf -m 2 localhost:$port1/healthz >/dev/null"

wait "$load_pid"
load_pid=""
tail -n 4 "$work/load.out"
acked=$(wc -l <"$work/acked.log" | tr -d ' ')
[[ $acked -gt 100 ]] || fail "only $acked writes were acknowledged"

say "reading back all $acked acknowledged writes with linearizable reads"
keel verify --in "$work/acked.log" | tee "$work/verify.out"
grep -q "lost=0" "$work/verify.out" || fail "acknowledged writes were lost"
show

read -r isolated term3 port3 < <(leader 8081 8082 8083)
service=keel${isolated#n}
container=keel-$service-1
say "partition: cutting the leader $isolated off from its peers"
docker network disconnect keel_peers "$container"
code=$(curl -s -o "$work/minority.out" -w '%{http_code}' -m 10 -X PUT --data lost \
  "localhost:$port3/v1/kv/demo-minority")
echo "write sent to the isolated $isolated: HTTP $code $(cat "$work/minority.out")"
[[ $code != 200 ]] || fail "the isolated leader acknowledged a write"
majority=()
for p in 8081 8082 8083; do [[ $p != "$port3" ]] && majority+=("$p"); done
wait_for "the majority to elect a leader" 60 new_leader_after "$isolated" "$term3" "${majority[@]}"
read -r leader4 term4 _ < <(leader "${majority[@]}")
echo "majority leader: $leader4 in term $term4"
keel put demo-majority kept >/dev/null
echo "write through the majority: acknowledged"
show

say "healing the partition"
docker network connect --alias "$service-peer" keel_peers "$container"
caught_up() {
  read -r _ role _ leader _ applied < <(status "$port3") || return 1
  read -r _ _ _ _ commit _ < <(status "$(leader "${majority[@]}" | cut -d' ' -f3)") || return 1
  [[ $role == follower && $leader == "$leader4" && $applied -ge $commit ]]
}
wait_for "$isolated to catch up" 60 caught_up
show
got=$(curl -s -o /dev/null -w '%{http_code}' "localhost:$port3/v1/kv/demo-minority?consistency=stale")
[[ $got == 404 ]] || fail "the minority write survived (HTTP $got)"
echo "on $isolated: the uncommitted minority write is gone (HTTP 404)"
value=$(curl -s "localhost:$port3/v1/kv/demo-majority?consistency=stale")
[[ $value == kept ]] || fail "the majority write did not reach $isolated"
echo "on $isolated: the majority write is present (\"$value\")"

say "PASS: no acknowledged write lost across a leader kill; an isolated leader could not commit"
