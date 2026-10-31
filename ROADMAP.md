# keel: roadmap, November 2026 to October 2027

## Vision

By October 2027 keel is an enterprise-grade distributed key-value store with feature parity with etcd, Consul KV and ZooKeeper for coordination, and the sharding, distributed transactions, backups and multi-region placement of FoundationDB, TiKV, CockroachDB and Spanner, while staying a single dependency-free Java service. It speaks the etcd v3 gRPC API well enough to run a Kubernetes API server, offers MVCC revisions, leases, watches at scale, range-partitioned Multi-Raft with online split, merge and placement, serializable cross-range transactions, mutual TLS, encryption at rest, RBAC, tenants with quotas and a hash-chained audit log, a web admin console with OIDC sign-in, a Kubernetes operator, incremental backups with point-in-time restore, cross-cluster disaster recovery and multi-region placement. It wins where a small project can: a native single binary that starts in under 100 ms, a lower resident memory per million keys than etcd, and equal or better p99 latency and throughput, all published from a reproducible benchmark against etcd and Consul, re-measured every quarter and enforced as a CI regression gate. Every feature is checked by the deterministic simulator, linearizability and serializability checkers, a TLA+ model and a real-cluster fault harness.

## Benchmarked against

Commercial leaders first: Consul Enterprise and HCP Consul, CockroachDB (self-hosted Enterprise and Cloud) and Google Cloud Spanner; then the major open source stores: etcd, Apache ZooKeeper, FoundationDB and TiKV. The benchmark in week 13 measures keel against etcd, Consul and ZooKeeper on the same Linux host with explicit targets: p99 write latency at or below etcd's, throughput at or above it, resident memory per million 100-byte keys at least 30% lower, cold start under 100 ms for the native build, and a container image under 30 MB. The numbers are re-measured each quarter (weeks 13, 25, 46 and 52) and a CI gate fails any change that regresses them by more than 5%.

Weeks 1 to 12 deliver the twelve v1 monthly themes, one per week; weeks 13 to 52 close the ranked gaps, and each week names the matrix rows it closes.

## 2026-11 · Raft operations, storage limits, TLS, KV API and throughput

Due 2026-11-30. The first five v1 themes, one per week: leadership transfer and simulator faults, a segmented log with chunked snapshots, TLS and cluster identity, range scans, TTLs, transactions and watches, then pipelined sessions, follower reads and Linux benchmarks.

| Week | Dates | Deliverable | Why |
|---:|---|---|---|
| 1 | 2026-11-02 to 2026-11-08 | Leadership transfer, graceful handoff and richer simulator faults | Planned maintenance on the leader stops costing an election timeout, and the simulator finds bugs in failure shapes it cannot produce today, with failing seeds shrunk to something a person can read. Comparison matrix rows: B6, G5. |
| 2 | 2026-11-09 to 2026-11-15 | Segmented log and chunked, streamed snapshots | Removes the two storage limits in DESIGN.md: the full log rewrite after every snapshot and the 256 MiB single-message snapshot, so large data sets compact and catch up with bounded memory and pauses. Comparison matrix rows: B8, B1. |
| 3 | 2026-11-16 to 2026-11-22 | Encrypted transport, secret rotation and cluster identity | Peer and client traffic is encrypted with the JDK's TLS so keel can run on a shared network, secrets rotate without downtime, and a cluster id stops a copied data directory from joining the wrong cluster. No new dependency: javax.net.ssl and the JDK HttpsServer only. Comparison matrix rows: C1, C2. |
| 4 | 2026-11-23 to 2026-11-29 | Range scans, TTLs, multi-key transactions and watches | Adds the API most real uses of a key-value store need, each operation checked by the linearizability model so the new surface keeps keel's guarantees. Comparison matrix rows: A3, A4, A6, A8. |
| 5 | 2026-11-30 to 2026-12-06 | Pipelined sessions, back-pressure, follower reads and Linux benchmarks | One client gets far higher write throughput, overload sheds work instead of growing latency, read load spreads across all nodes, and published numbers come from Linux where anyone can rerun them. Comparison matrix rows: B5, G4. |

## 2026-12 · Operations, overload control, upgrades and larger-than-heap data

Due 2026-12-31. Backups and an admin command line, metrics, tracing and rate limits, versioned formats with rolling upgrades, and a disk-backed state machine that holds more than the heap.

| Week | Dates | Deliverable | Why |
|---:|---|---|---|
| 6 | 2026-12-07 to 2026-12-13 | Online backups, restore, bootstrap with join, and an admin command line | Operators can back up a live cluster, turn the backup into a working cluster, start from one node and grow by joining, and do all of it with one command instead of hand-written curl calls. Comparison matrix rows: E1, E6. |
| 7 | 2026-12-14 to 2026-12-20 | Metrics, tracing, per-client rate limits, readiness and disk watermarks | Operators can see replication and storage health and follow one request through the log, clients cannot starve each other, and a filling disk stops writes cleanly before it stops the node. Comparison matrix rows: E9, D2. |
| 8 | 2026-12-21 to 2026-12-27 | Versioned wire and disk formats with a rolling upgrade drill | Every message and file carries a version and golden files pin the formats, so a cluster can be upgraded one node at a time under load. Comparison matrix rows: E5. |
| 9 | 2026-12-28 to 2027-01-03 | A disk-backed state machine for data sets larger than the heap | The data set stops being bounded by the heap: values live in data files with an in-memory key index, snapshots come from those files, and background merging reclaims space. Comparison matrix rows: B1. |

## 2027-01 · Verification, embedding, 1.0 and the etcd benchmark

Due 2027-01-31. A model checker and soak runs, a public embedding API with a lock service, the 1.0 release on Kubernetes, then a reproducible benchmark against etcd and Consul that becomes a CI gate.

| Week | Dates | Deliverable | Why |
|---:|---|---|---|
| 10 | 2027-01-04 to 2027-01-10 | Model checker, real-cluster pause faults, mutation testing and soak | Verification goes beyond random seeds: an exhaustive small-scope model checker, process pauses on real clusters, mutation testing of the Raft core, and a scheduled long soak. Comparison matrix rows: G5, G6. |
| 11 | 2027-01-11 to 2027-01-17 | Embedding API, lock service with fencing, OpenAPI and signed artifacts | keel becomes a library as well as a server: a public API for custom state machines, a lock service example with fencing tokens, an OpenAPI description checked against the server, and signed release artifacts. Comparison matrix rows: F3, A9. |
| 12 | 2027-01-18 to 2027-01-24 | Kubernetes manifests, extended failure demo, hot-path allocations and 1.0 | keel 1.0: runnable on Kubernetes, demonstrated under a wider set of failures, with fewer allocations on the hot path and written stability guarantees. Comparison matrix rows: E8, G1. |
| 13 | 2027-01-25 to 2027-01-31 | Reproducible benchmark against etcd and Consul with a CI regression gate | Claims about being lighter and faster than the leaders need numbers anyone can rerun. This sets the targets the rest of the year is measured against and makes regressions fail the build. Comparison matrix rows: G1, G2, G3, G4. |

## 2027-02 · MVCC storage engine and the etcd data model

Due 2027-02-28. Revision history with historical reads, a leveled LSM engine, first-class leases and watches that scale to a hundred thousand watchers.

| Week | Dates | Deliverable | Why |
|---:|---|---|---|
| 14 | 2027-02-01 to 2027-02-07 | MVCC revision store with historical reads and compaction | etcd's data model is revisions: every write gets a cluster-wide revision, reads can ask for any retained revision, and watches resume from one. Matching it is the foundation for etcd compatibility and consistent snapshots. Comparison matrix rows: A2. |
| 15 | 2027-02-08 to 2027-02-14 | Leveled LSM storage engine with bloom filters and a block cache | A leveled LSM keeps writes sequential and reads bounded on data sets far beyond memory, where etcd's bbolt is bound by its mmap and page cache. This is where keel can beat etcd on large data and write throughput. Comparison matrix rows: B1, G3. |
| 16 | 2027-02-15 to 2027-02-21 | First-class leases with keepalive streams and checkpointing | Kubernetes and most coordination code use etcd leases, not per-key TTLs: one lease owns many keys, clients keep it alive over a stream, and revoking it deletes them all. Leases must survive leader changes without resetting. Comparison matrix rows: A7. |
| 17 | 2027-02-22 to 2027-02-28 | Watches at scale: a hundred thousand watchers with low memory | Kubernetes keeps tens of thousands of watches open on etcd. keel's watches should match etcd semantics and use less memory per watcher, with progress notifications and clean errors for compacted revisions. Comparison matrix rows: A6, G3. |

## 2027-03 · Security, tenancy and audit

Due 2027-03-31. Encryption at rest, users and roles with prefix permissions, tenants with quotas, a tamper-evident audit log and per-tenant metering.

| Week | Dates | Deliverable | Why |
|---:|---|---|---|
| 18 | 2027-03-01 to 2027-03-07 | Encryption at rest with envelope keys and online rotation | Enterprise buyers require data encrypted on disk. Every log segment, SSTable and snapshot is encrypted with AES-256-GCM from the JDK, under data keys wrapped by a key provider, with rotation that needs no downtime. Comparison matrix rows: C4. |
| 19 | 2027-03-08 to 2027-03-14 | Users, roles and prefix permissions with signed tokens | etcd ships authentication and role-based access per key range; enterprises need it to share a cluster safely. Permissions live in the replicated state so every node enforces the same rules. Comparison matrix rows: C3. |
| 20 | 2027-03-15 to 2027-03-21 | Tenants with quotas and noisy-neighbour isolation | One cluster serving many teams needs hard boundaries: each tenant gets its own key space, quotas on keys, bytes, watches and leases, and its own rate limits so one tenant cannot hurt another's latency. Comparison matrix rows: D1, D2. |
| 21 | 2027-03-22 to 2027-03-28 | Tamper-evident audit log with signed checkpoints | Compliance needs a record of who changed what that cannot be silently edited. Admin and data changes are written to a hash-chained audit log with signed checkpoints and export to external systems. Comparison matrix rows: C5. |
| 22 | 2027-03-29 to 2027-04-04 | Per-tenant usage metering and OpenTelemetry export | Platform teams charge back and plan capacity per tenant. keel meters storage, requests and watch time per tenant and exports traces and metrics over OTLP so they land in any standard backend. Comparison matrix rows: D3, E9. |

## 2027-04 · etcd API compatibility and SDKs

Due 2027-04-30. An HTTP/2 and gRPC stack on the JDK, the etcd v3 API, a Kubernetes API server running on keel, and Go and Python SDKs.

| Week | Dates | Deliverable | Why |
|---:|---|---|---|
| 23 | 2027-04-05 to 2027-04-11 | HTTP/2 server, protobuf codec and gRPC on the JDK | The etcd ecosystem speaks gRPC. The JDK has no HTTP/2 server and keel adds no runtime dependency, so this week builds a small, fuzzed HTTP/2 server with HPACK, a protobuf codec and gRPC framing. Comparison matrix rows: A10. |
| 24 | 2027-04-12 to 2027-04-18 | etcd v3 API compatibility | With the etcd v3 API, existing tools and clients (etcdctl, client libraries in every language) work against keel unchanged, which is the fastest path to adoption. Comparison matrix rows: A10. |
| 25 | 2027-04-19 to 2027-04-25 | Running a Kubernetes API server on keel | The strongest proof of etcd parity is Kubernetes itself. kube-apiserver runs with keel as its store and passes a conformance subset, and the benchmark compares it with etcd under a Kubernetes workload. Comparison matrix rows: F2. |
| 26 | 2027-04-26 to 2027-05-02 | Go and Python SDKs with sessions, transactions and watches | Most users of a coordination store write Go or Python. Native SDKs for keel's HTTP and gRPC APIs, standard library only, make keel usable without the Java client. Comparison matrix rows: F1. |

## 2027-05 · Multi-Raft sharding and distributed transactions

Due 2027-05-31. Many Raft groups per process, range partitioning, online split and merge, a placement driver and serializable cross-range transactions.

| Week | Dates | Deliverable | Why |
|---:|---|---|---|
| 27 | 2027-05-03 to 2027-05-09 | Multi-Raft runtime with thousands of groups per process | Scaling beyond one Raft group is how TiKV and CockroachDB grow past one machine's write capacity. Many groups share one transport, one event loop and one fsync, and idle groups go quiet. Comparison matrix rows: B2. |
| 28 | 2027-05-10 to 2027-05-16 | Range partitioning with a meta range and client routing | Keys are split into ranges, each its own Raft group, with a meta range describing where every range lives. Clients route directly and recover from stale routing. Comparison matrix rows: B2. |
| 29 | 2027-05-17 to 2027-05-23 | Online range split and merge by size and load | Ranges grow and shrink with data and traffic. Splits and merges happen online, atomically in the meta range, with no client-visible errors beyond a retry. Comparison matrix rows: B3. |
| 30 | 2027-05-24 to 2027-05-30 | Placement driver with zone-aware rebalancing and decommission | A placement driver keeps replicas balanced, spreads them across zones and moves leaders to where load is, and lets an operator drain a node safely. Comparison matrix rows: B4. |
| 31 | 2027-05-31 to 2027-06-06 | Serializable cross-range transactions with parallel commits | Multi-key transactions across ranges need distributed commit. keel uses hybrid logical clock timestamps, write intents and a transaction record with parallel commits, the approach CockroachDB uses, for one round trip commits. Comparison matrix rows: A5. |

## 2027-06 · Verified transactions and coordination features

Due 2027-06-30. A serializability checker for transactions, Consul KV compatibility, coordination recipes in every SDK and a durable change feed.

| Week | Dates | Deliverable | Why |
|---:|---|---|---|
| 32 | 2027-06-07 to 2027-06-13 | Serializability checker for transactions on real clusters | Distributed transactions are where databases lose correctness. An Elle-style checker finds isolation anomalies from histories, and it runs in the simulator and on real clusters. Comparison matrix rows: A5, G5. |
| 33 | 2027-06-14 to 2027-06-20 | Consul KV compatibility: blocking queries, sessions and semaphores | Consul users depend on blocking queries, sessions with lock-delay and KV-based semaphores. Serving Consul's KV HTTP API lets those applications move to keel without rewrites. Comparison matrix rows: A11. |
| 34 | 2027-06-21 to 2027-06-27 | Coordination recipes in every SDK | Leader election, locks, barriers, queues and read-write locks are what most users build on a coordination store, and getting them wrong is easy. Tested recipes in Java, Go and Python with fencing tokens. Comparison matrix rows: A9. |
| 35 | 2027-06-28 to 2027-07-04 | Durable change feed with resumable cursors and sinks | Change data capture lets other systems follow keel without polling: a durable feed per prefix with cursors, delivered to webhooks, files or a pull API, at least once and in revision order. Comparison matrix rows: E10. |

## 2027-07 · Admin console, SSO, operator and backups

Due 2027-07-31. A web admin console with OIDC sign-in, a Kubernetes operator and incremental backups to object storage with point-in-time restore.

| Week | Dates | Deliverable | Why |
|---:|---|---|---|
| 36 | 2027-07-05 to 2027-07-11 | Web admin console | etcd has third-party consoles and Consul ships a UI. keel serves its own console from the server: cluster topology, ranges, leaders, key browser, live watches, all with no build toolchain or frontend framework. Comparison matrix rows: E7. |
| 37 | 2027-07-12 to 2027-07-18 | OIDC sign-in and role-based access for the admin console | An enterprise console needs single sign-on and roles. Operators sign in with any OIDC provider, groups map to keel roles, and every console action is audited. Comparison matrix rows: C6, E7. |
| 38 | 2027-07-19 to 2027-07-25 | Kubernetes operator and Helm chart | Running a stateful store on Kubernetes safely needs an operator: scaling through membership changes, rolling upgrades with leadership handoff, backups on a schedule and repair after node loss. Comparison matrix rows: E8. |
| 39 | 2027-07-26 to 2027-08-01 | Incremental backups to object storage with point-in-time restore | Enterprises need backups off the cluster and the ability to restore to a moment before a bad write. Incremental backups ship SSTables and log to object storage, encrypted, and restore to any second within retention. Comparison matrix rows: E2. |

## 2027-08 · Disaster recovery and global deployments

Due 2027-08-31. Cross-cluster replication, recovery from majority loss, migration from etcd and Consul, multi-region placement and tiered storage.

| Week | Dates | Deliverable | Why |
|---:|---|---|---|
| 40 | 2027-08-02 to 2027-08-08 | Cross-cluster replication and disaster recovery failover | Losing a whole region needs a standby cluster, which FoundationDB DR, CockroachDB physical cluster replication and Spanner multi-region all offer. Asynchronous replication streams changes to a second cluster with bounded lag, and a promoted standby takes over with a measured recovery point. Comparison matrix rows: E3. |
| 41 | 2027-08-09 to 2027-08-15 | Recovery from majority loss and automated node replacement | When a majority is gone for good, an operator must recover the survivors with clear warnings, as etcd's snapshot restore and CockroachDB's loss-of-quorum recovery allow. Dead nodes are also replaced automatically. Comparison matrix rows: E12. |
| 42 | 2027-08-16 to 2027-08-22 | Migration from etcd and Consul with live mirroring | Adoption needs a safe migration path from etcd and Consul KV: import snapshots, mirror a live source into keel, verify and cut over without downtime. Comparison matrix rows: E11. |
| 43 | 2027-08-23 to 2027-08-29 | Multi-region placement with witnesses and region-local reads | Global deployments are a headline feature of Spanner, CockroachDB and FoundationDB. keel places ranges across regions, keeps leaders near their users, uses witnesses for cheap tie-breaking and serves consistent reads in region. Comparison matrix rows: E4, B5. |
| 44 | 2027-08-30 to 2027-09-05 | Tiered storage for cold data | Most data is cold, and object storage costs a fraction of block storage. Older SSTables move to object storage while reads still work through a local cache, as CockroachDB and TiKV do for backups and FoundationDB's newer backup design does for logs. Comparison matrix rows: B7. |

## 2027-09 · Lightweight and fast

Due 2027-09-30. A native single binary, a faster write path, a smaller memory footprint and a Linux fault harness that checks it all on real clusters.

| Week | Dates | Deliverable | Why |
|---:|---|---|---|
| 45 | 2027-09-06 to 2027-09-12 | Native single binary with fast cold start | A small static binary that starts in under 100 ms beats JVM start-up and matches etcd's single binary while using less memory. GraalVM native-image is a build-time tool only and is justified by the cold start, image size and RSS targets in the comparison matrix. Comparison matrix rows: G1, G2. |
| 46 | 2027-09-13 to 2027-09-19 | A faster write path: group commit, direct I/O and zero-copy reads | Tail latency is where keel should beat etcd and Consul. Group commit across Raft groups, direct I/O for the log, zero-copy value reads and fewer allocations push p99 down under load. Comparison matrix rows: G4. |
| 47 | 2027-09-20 to 2027-09-26 | Smaller memory footprint with off-heap arenas | Memory per key is the main cost at scale and the lightweight target in the matrix. Off-heap arenas with the Foreign Function and Memory API, compact key encoding and smaller watcher and lease structures cut RSS, measured against etcd. Comparison matrix rows: G3. |
| 48 | 2027-09-27 to 2027-10-03 | Real-cluster fault harness on Linux | The simulator checks the algorithm; real clusters fail in other ways. A Jepsen-style harness injects network partitions, clock skew and disk faults into real processes, the kind of testing FoundationDB and etcd are known for. Comparison matrix rows: G5. |

## 2027-10 · Formal models, compliance and 2.0

Due 2027-10-31. TLA+ models with trace validation, fuzzing and a threat model, a compliance pack and keel 2.0 with the final benchmark against etcd and Consul.

| Week | Dates | Deliverable | Why |
|---:|---|---|---|
| 49 | 2027-10-04 to 2027-10-10 | TLA+ models with trace validation | A formal model catches design errors that tests miss, as etcd's Raft and CockroachDB's parallel commits specs show. TLA+ specs are model checked with TLC, a build-time tool, and simulator traces are validated against them. Comparison matrix rows: G6. |
| 50 | 2027-10-11 to 2027-10-17 | Continuous fuzzing and a threat model | Every input that crosses a trust boundary is fuzzed, and a written threat model ties each threat to a control and a test, so the security rows of the matrix are backed by evidence. Comparison matrix rows: C1, C3, C4. |
| 51 | 2027-10-18 to 2027-10-24 | Compliance pack: FIPS mode, retention, legal hold and evidence export | Regulated buyers ask for FIPS-approved crypto, retention rules, legal holds and audit evidence, which CockroachDB and Spanner offer. keel provides them as configuration, each checked by tests. Comparison matrix rows: C7. |
| 52 | 2027-10-25 to 2027-10-31 | keel 2.0 with the final benchmark against etcd and Consul | Close the year with a release that states what keel guarantees, how it compares with etcd, Consul and the rest of the matrix, and how to upgrade from 1.0. Comparison matrix rows: G4. |

## Next year

Gaps from the comparison matrix still open after October 2027:

- ZooKeeper wire protocol compatibility (matrix row A12), so ZooKeeper clients and Curator recipes can move to keel.
- A managed hosted service (row E13).
- Service discovery and health checking in the style of Consul (row F4): keel stays a KV store; recipes cover registration but not health checks.
- A SQL layer (row F5): out of scope; keel is the KV layer.
