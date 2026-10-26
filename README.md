# keel

keel is a replicated key-value store built on the Raft consensus algorithm, written in Java 21 with no runtime dependencies. A cluster of three or five nodes keeps serving linearizable reads and writes while a minority of them crash or are cut off, and a client that retries a write after a timeout gets it applied exactly once. It comes with a deterministic simulator that runs thousands of seeded fault schedules and checks every client history for linearizability.

## Starting a three-node cluster

You need Docker and a JDK 21.

```sh
export KEEL_CLUSTER_SECRET=$(openssl rand -hex 24)
docker compose up -d --build --wait     # three nodes, HTTP on localhost:8081-8083
./gradlew jar                           # the command-line client
export KEEL_ENDPOINTS=http://localhost:8081,http://localhost:8082,http://localhost:8083
java -jar build/libs/keel-0.1.0.jar put greeting hello
java -jar build/libs/keel-0.1.0.jar get greeting
java -jar build/libs/keel-0.1.0.jar status
```

To run the live failure demo, which kills the leader mid-load and then partitions it away from its peers, run `examples/demo.sh`. It starts and removes its own cluster.

To run a node without Docker:

```sh
java -jar build/libs/keel-0.1.0.jar server --id n1 \
  --members n1@127.0.0.1:7001@127.0.0.1:8081,n2@127.0.0.1:7002@127.0.0.1:8082,n3@127.0.0.1:7003@127.0.0.1:8083 \
  --peer-bind 127.0.0.1:7001 --http-bind 127.0.0.1:8081 --data data/n1 --secret "$KEEL_CLUSTER_SECRET"
```

Every flag can also be set as an environment variable, for example `KEEL_ID` for `--id`. Each member is `id@peer-address@client-address`; the client address is what followers put in their redirects, so it must be reachable by clients.

## Talking to it over HTTP

The API is plain HTTP. A key's version is the log index of its last write and doubles as its ETag. These are real responses from a fresh compose cluster whose leader was n1.

```sh
$ curl -si -X PUT --data apple localhost:8082/v1/kv/fruit     # n2 is a follower
HTTP/1.1 307
Location: http://localhost:8081/v1/kv/fruit

$ curl -s -X PUT --data apple localhost:8081/v1/kv/fruit
{"version":2}

$ curl -s -X PUT -H 'If-Match: "1"' --data pear localhost:8081/v1/kv/fruit
{"error":"version conflict","current_version":2}

$ curl -s localhost:8081/v1/kv/fruit                          # linearizable (ReadIndex)
apple
$ curl -s 'localhost:8081/v1/kv/fruit?consistency=lease'       # leader lease, no round trip
apple
```

For exactly-once writes, register a session and number your requests. A retry with the same number returns the first result instead of applying again.

```sh
$ curl -s -X POST localhost:8081/v1/sessions
{"client_id":4}
$ curl -s -X PUT -H 'Keel-Client: 4' -H 'Keel-Seq: 1' --data 1 localhost:8081/v1/kv/counter
{"version":5}
$ curl -s -X PUT -H 'Keel-Client: 4' -H 'Keel-Seq: 1' --data 1 localhost:8081/v1/kv/counter
{"version":5}
```

The Java client in `io.github.laiwf94.keel.client.KeelClient` does all of this for you: it follows redirects, keeps a session, and retries under the same sequence number.

Other endpoints are `DELETE /v1/kv/{key}`, `GET /v1/status`, `GET /v1/members`, `POST /v1/members?id=&peer=&client=` and `DELETE /v1/members/{id}` (both need the admin token), `GET /metrics` in the Prometheus text format, and `GET /healthz`. Set `KEEL_CLIENT_TOKEN` to require a bearer token on the client API and `KEEL_ADMIN_TOKEN` to enable membership changes.

## What happens when things break

A write is acknowledged only after a majority of nodes has it on disk, so losing any minority of nodes loses no acknowledged write. When the leader dies, the others elect a new one within one election timeout (250 to 500 ms by default) and the client retries there. A leader cut off from the majority stops accepting writes within an election timeout and never serves a linearizable read; its uncommitted writes are discarded when the partition heals. A write whose fate is unknown, because the leader changed or the request timed out, is reported as such (503 or 504), never as a success or a failure, and retrying it under the same session is safe. A torn final write to the log is cut off on restart; corruption anywhere else stops the node rather than silently dropping data.

This is what `examples/demo.sh` shows on the compose cluster:

```
== building the client and starting three nodes
node role           term  leader  applied
n1   follower       1     n3      1      
n2   follower       1     n3      1      
n3   leader         1     n3      1      

== write load: 4 clients for 25 s, every acknowledged write recorded

== killing the leader n3 (term 1) with SIGKILL, mid-load
new leader: n1 in term 2 (was n3 in term 1)
node role           term  leader  applied
n1   leader         2     n1      478    
n2   follower       2     n1      482    
n3   (down)

== restarting n3 from its own disk
t=23s acked=1755 unknown=0 errors=0
t=24s acked=1839 unknown=0 errors=0
t=25s acked=1892 unknown=0 errors=0
load done: acked=1892 unknown=0 errors=0

== reading back all 1892 acknowledged writes with linearizable reads
verify: checked=1892 present=1892 lost=0
node role           term  leader  applied
n1   leader         2     n1      1898   
n2   follower       2     n1      1898   
n3   follower       2     n1      1898   

== partition: cutting the leader n1 off from its peers
write sent to the isolated n1: HTTP 503 {"error":"leadership changed before the entry was applied; retry with the same Keel-Seq to be safe","outcome":"unknown"}
majority leader: n2 in term 3
write through the majority: acknowledged
node role           term  leader  applied
n1   pre_candidate  2     -       1898   
n2   leader         3     n2      1900   
n3   follower       3     n2      1900   

== healing the partition
node role           term  leader  applied
n1   follower       3     n2      1900   
n2   leader         3     n2      1900   
n3   follower       3     n2      1900   
on n1: the uncommitted minority write is gone (HTTP 404)
on n1: the majority write is present ("kept")

== PASS: no acknowledged write lost across a leader kill; an isolated leader could not commit
```

## How fast it is, and why

`bench/run.sh local` starts three nodes as separate JVMs on one machine, a 16-core arm64 Mac, with the benchmark clients on the same machine. Values are 100 bytes, each run warms up for 3 s and measures for 15 s, and every client sends its next request as soon as the last one is answered. Nothing failed in any run.

| workload | clients | ops/s | p50 ms | p99 ms |
|---|---:|---:|---:|---:|
| write | 16 | 745 | 22.0 | 41.0 |
| write | 64 | 2,246 | 28.2 | 63.0 |
| write | 256 | 5,766 | 42.5 | 93.2 |
| linearizable read | 16 | 16,484 | 0.75 | 4.74 |
| linearizable read | 64 | 29,267 | 2.11 | 4.10 |
| lease read | 16 | 22,648 | 0.52 | 3.65 |
| lease read | 64 | 31,568 | 1.94 | 4.29 |

A write is acknowledged only after it is on disk on the leader and on at least one follower. On macOS the JDK's `FileChannel.force` asks the drive to flush its cache, which takes about 5 ms here. The leader's and the follower's fsyncs overlap, but under load a write also waits for the round already in progress, so latency is a few fsyncs. Throughput grows with the number of clients because each event-loop round syncs every write that arrived in it with one fsync. Reads skip the log and the disk; a lease read also skips the heartbeat round. With 256 clients the machine itself is saturated by the client processes, and reads fall to about 12,000 per second. Through Docker Desktop on macOS every request also pays for port forwarding, which dominates the numbers, so the table uses local processes.

## What it does not do

All data lives in memory and is snapshotted to disk, so the data set is bounded by the heap. Snapshots are sent in one message of at most 256 MiB. Only the leader serves linearizable and lease reads. Each session allows one outstanding request. Peer traffic is authenticated with HMAC-SHA256 but not encrypted, so run it on a private network. Lease reads assume clock rates differ by less than 25% between nodes; ReadIndex, the default, assumes nothing about clocks. There is no leadership transfer. Every founding node must start with the same member list, and a node that loses its disk must rejoin under a new id.

docs/DESIGN.md explains the design, the reasons behind it, the failure modes and the trade-offs in detail.

## Working on keel

```sh
./gradlew check                                     # format, Error Prone, tests, 90% line coverage
./gradlew test --tests '*SimulationTest*' -Dkeel.sim.seeds=10000
./gradlew test --tests '*KvSimulationTest*' -Dkeel.sim.seed=1234   # replay one seed
bench/run.sh local                                  # benchmark three local JVMs
scripts/verify-commits.sh                           # run the gate on every commit
```

The source is organised by layer: `raft` is the algorithm as a deterministic state machine, `replica` drives it, `kv` is the key-value state machine with sessions, `storage` is the disk format, `net` the peer transport, `server` the event loop and HTTP API, and `client` the Java client. The simulator and the linearizability checker are under `src/test/java/.../sim`.

## License

MIT
