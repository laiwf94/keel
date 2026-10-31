# How keel works

## What keel promises

keel is a replicated key-value store that stays correct when machines crash, disks tear writes, and networks drop, delay, duplicate and partition traffic. Correct here means three things. Raft's safety properties hold: at most one leader per term, committed entries never change, and every replica applies the same commands in the same order. Clients see a linearizable store: every operation appears to take effect at one instant between its call and its return. And a write a client retries after a timeout takes effect at most once.

Throughput matters less than those three properties, but the design avoids the obvious costs: one fsync per batch rather than per write, reads that do not go through the log, and a leader that writes to its disk in parallel with its followers.

It is deliberately small. There are no runtime dependencies beyond the JDK, and the build adds only formatting, static analysis, test and coverage tools.

## Where the algorithm lives

The code is split so that the hard part, the Raft algorithm, can be tested without threads, sockets or clocks.

`RaftNode` is the algorithm as a deterministic state machine. A driver calls `tick()` at a fixed interval, passes each received message to `step()`, and collects what the node produced from `ready()`: messages to send, entries that are now committed, a snapshot to restore, and reads that are now safe to serve. The node writes its term, vote and log through a `Storage` interface but never waits for the disk; the driver decides when to sync.

`Replica` is the driver. In one round it takes the node's output, sends what may go before the fsync, syncs storage, sends the rest, applies committed entries to the state machine, and completes the callbacks of the clients that proposed them. It has no thread of its own.

The simulator calls `Replica` directly from one thread with a virtual clock. The server wraps it in `NodeLoop`, a single platform thread that owns the replica. HTTP handlers and the peer transport never touch Raft state; they put tasks on the loop's bounded queue and wait on futures. Because one thread owns everything, there are no locks around Raft state, and a burst of requests that arrive together costs one fsync and one AppendEntries message per follower.

Around the loop sit `FileStorage` (the disk format), `PeerTransport` (authenticated TCP between nodes), `HttpApi` (the client API on the JDK HTTP server and virtual threads), and `KvMachine` (the key-value state machine with client sessions).

## Choosing a leader

Election follows section 5.2 of the Raft paper with three additions from Ongaro's thesis.

Pre-vote (thesis section 9.6). When its election timer fires, a node first asks the others whether they would vote for it in the next term, without changing anyone's term. Only if a majority says yes does it increment its term and run a real election. A node that was cut off and kept timing out therefore comes back with its old term and cannot force the cluster into an election it would lose anyway.

Leader stickiness (thesis section 4.2.3). A node that has heard from a live leader within the minimum election timeout ignores requests to vote in a higher term, real or pre-vote. This stops a removed server, or one with a broken link to the leader only, from deposing a healthy leader. It is also what makes lease reads safe, below.

Check quorum (thesis section 6.2). A leader that has not heard from a majority within an election timeout steps down. A leader stranded in a minority partition stops accepting writes instead of collecting them until it times out.

A fourth rule is less often written down. A node that has just started behaves as if it had just heard from a leader until one election timeout has passed. Without it, a follower that restarts would grant votes immediately and could help elect a new leader while the old leader still believes its lease holds.

Leadership transfer (thesis section 3.10). `RaftNode.transferLeadership(target)` hands leadership to another voter without waiting for a timeout. The leader stops accepting proposals (they are refused with the target as the leader hint), sends the target whatever it is missing, and once the target's log matches its own sends it `TimeoutNow`. The target starts a real election at once, skipping the pre-vote, and marks its vote requests as a transfer. Voters grant a transfer vote even while they still hear from the leader, because the leader asked for it; stickiness would otherwise make the transfer wait out a timeout. With an up-to-date log the target wins in about one round trip. If it has not taken over within one election timeout, the leader gives up and takes proposals again. Membership changes are refused while a transfer is in progress. Because a transfer vote passes the stickiness rule that lease reads rely on, a leader that has sent `TimeoutNow` voids its lease for the rest of its term, even if the transfer fails, since a delayed `TimeoutNow` could still start an election; its lease reads then wait for a heartbeat round like ReadIndex reads.

Election timeouts are drawn from `[electionTicks, 2 * electionTicks)` with a seeded random source. The defaults are a 25 ms tick, 10 election ticks (250 to 500 ms) and 2 heartbeat ticks (50 ms).

## Replication and the commit rule

AppendEntries follows section 5.3 of the paper. A rejection carries a hint, the follower's last index plus one if its log is short or the first index of the conflicting term otherwise, so a leader can skip a whole divergent term in one round trip instead of backing up one entry at a time. A test builds a follower with fifty uncommitted entries from an old term and checks that it converges in at most four messages.

Per follower the leader is in one of three modes. While probing it sends one batch and waits for the answer. Once a batch is accepted it switches to replicating and streams batches without waiting, up to a window of unacknowledged entries. If the follower needs entries that were compacted, it sends a snapshot and waits. Heartbeats double as probes, so a lost batch is noticed within one heartbeat.

The commit rule is the part that is easy to get wrong. A leader advances its commit index to the highest index stored on a majority only if that entry is from the leader's current term (section 5.4.2). Counting replicas of an older entry is unsafe, as Figure 8 of the paper shows: the entry can reach a majority and still be overwritten by a node elected later with a newer last term. Every new leader appends a no-op so that older entries commit indirectly as soon as possible. `ReplicationTest` reproduces Figure 8 step by step on five nodes, and it fails if the term check is removed.

A leader writes its own new entries to disk in parallel with its followers (thesis section 10.2.1). AppendEntries and InstallSnapshot leave before the leader's fsync, because they promise nothing about the leader's disk. The node keeps the last index known to be durable and counts itself toward a commit only up to there. Every other message, a vote or an acknowledgement, is a promise about durable state and waits for the fsync. Without this the leader's fsync and the follower's ran one after the other; now they overlap.

## On disk

Each node has a directory with three files and a lock file.

`state` holds the current term and vote. It is replaced atomically: write `state.tmp`, fsync it, rename it over `state`, fsync the directory. The rename is what makes the replacement atomic on POSIX file systems, and the directory fsync is what makes the rename itself durable; Pillai et al. show how often applications get this sequence wrong.

`log` is append-only. Each record is a length, a CRC32C of the payload, and the payload (index, term, type, data). Appends go to the OS page cache and one `FileChannel.force(false)` per loop round makes them durable; that is `fdatasync` on Linux and a full cache flush on macOS. When a follower must drop a conflicting suffix, the file is truncated at the first record to drop; the node keeps the byte offset of every record in memory for that.

`snapshot` holds the latest snapshot, replaced the same way as `state`. After a snapshot, the log is rewritten into a temporary file holding only the later entries and renamed into place.

Recovery reads the log until the first record that fails its length or CRC check. If nothing valid follows, that record was a torn final write. Its bytes were never synced, so no peer was told about them, and the file is cut there. If a valid record does follow, the damage is in the middle of the log, which a torn write cannot cause. The node refuses to start rather than silently drop entries it may have acknowledged; the operator can wipe it and let it rejoin through a snapshot. A CRC alone cannot tell these two cases apart, which is why recovery looks past the bad record. The tests cut the last record at every byte offset and flip a byte in the middle of the log.

A crash between writing a snapshot received from the leader and rewriting the log can leave a log that conflicts with the snapshot. Recovery applies the same rule as InstallSnapshot: if the log holds the snapshot's last entry with the same term, the entries after it are kept, otherwise the log is dropped.

Any storage error stops the node. A node that cannot persist cannot keep the promises it has made to its peers, so continuing would be worse than stopping. The server halts with exit code 70.

## Snapshots and compaction

The replica snapshots the state machine every 10,000 applied entries by default and compacts the log to that point. A follower that needs compacted entries gets the whole snapshot in one message, keeps any suffix of its log that matches, and restores its state machine from it. A lost snapshot is resent after an election timeout. Snapshots are sent whole and held in memory, which caps them at 256 MiB; chunked transfer would remove that limit and is listed below.

## Reading without the log

Reads do not go through the log. A linearizable read uses ReadIndex (thesis section 6.4). The leader records its commit index, then confirms it is still leader by collecting acknowledgements of a heartbeat round sent after the read arrived. Once a majority has acknowledged that round and the state machine has applied up to the recorded index, the read is served. Reads that arrive together share one round. A new leader does not know the latest commit index until it commits an entry of its own term, so reads wait for its no-op to commit.

A lease read skips the round trip (thesis section 6.4.1). When a majority acknowledges a heartbeat round, the leader knows that each of those followers will refuse to vote for anyone else until one minimum election timeout after it received that heartbeat. The leader therefore treats itself as leader until `leaseTicks` after it sent the round, and serves reads locally until then. `leaseTicks` is `electionTicks - leaseDriftTicks`, 8 of 10 ticks by default.

That argument depends on clocks. The followers' timeout is measured on their clocks from when they received the heartbeat; the lease is measured on the leader's clock from when it sent it. The lease is safe as long as no follower's clock runs faster than the leader's by more than a factor of `electionTicks / leaseTicks`, 1.25 with the defaults. Clock offset does not matter, only rate, and ticks are driven by `System.nanoTime`, which is monotonic. The simulator runs lease reads with every node's clock rate up to 3% (and, in one test, 10%) away from nominal and checks every history. `ReadTest.leaseReadsBreakWhenClocksDriftBeyondTheBound` shows what happens outside the bound: with the followers' clocks running much faster, they elect a new leader that commits a write while the old leader still serves a stale value from its lease. ReadIndex is the default for that reason.

Stale reads are also offered. They answer from whatever the contacted node has applied and may be arbitrarily old.

## Membership changes

keel changes membership one server at a time (thesis chapter 4) rather than with joint consensus. When only one voter is added or removed, every majority of the old configuration overlaps every majority of the new one, so the two cannot elect separate leaders, and no intermediate joint configuration is needed. Joint consensus is required only to change several voters at once, which an operator rarely needs and which can be done as a sequence of single changes. The single-server approach is also what the thesis recommends for its simplicity.

A configuration takes effect on each server as soon as the entry is appended to its log, not when it commits, and a configuration entry removed by a new leader rolls the server back to the previous one. Two rules make single-server changes safe. A leader starts a change only when no earlier change is uncommitted. And it starts one only after it has committed an entry in its own term. Without the second rule, a change from a previous leader that never committed can combine with the new leader's change into two configurations without overlapping majorities; Ongaro described this bug on the raft-dev list after the thesis was published.

A new server joins as a learner: it receives the log but does not vote, so a slow newcomer cannot hurt availability. Once its log reaches the leader's commit index, the leader promotes it to voter in a second change. A leader that removes itself keeps leading until the change commits, then steps down. A removed server does not campaign once it learns it was removed, and stickiness stops it from disrupting the cluster if it never learns.

The simulator adds and removes servers under faults. Its first version reused node ids with an empty disk when re-adding a server, and the invariant checker found two leaders in one term: the server had forgotten a vote it cast before it was wiped. The rule now is that a server that loses its disk rejoins under a new id.

## Exactly-once writes

A client that times out does not know whether its write applied. Retrying blindly can apply it twice. keel uses client sessions (thesis section 6.3). A client registers through the log and receives an id, then numbers its requests. For each session the state machine keeps the highest sequence number applied and its result. A request with that number gets the stored result without being applied again; a request with a lower number is refused as stale. A client may have one request outstanding, which is enough because the Java client serializes its calls.

Sessions expire after a period without requests. Expiry must be identical on every replica, so it cannot use local clocks. Each command carries the proposing leader's wall-clock time, the state machine keeps the maximum time it has seen, and sessions are expired against that. A leader whose clock is far ahead could expire sessions early; that only turns a duplicate into a "session expired" error, never into a double application. The session table is also bounded, evicting the least recently used session, again deterministically.

The HTTP API exposes this through `POST /v1/sessions` and the `Keel-Client` and `Keel-Seq` headers. A write without them is applied each time it is received.

## Peer transport

Raft needs six message types, most of them small and frequent. keel encodes them in a length-prefixed binary format over persistent TCP connections. gRPC would bring a large dependency tree and code generation for very little here; the codec and transport together are a few hundred lines with every length checked before anything is allocated.

Each node dials one connection to each peer and only writes on it, and only reads on connections peers dial to it. Each connection starts with a mutual challenge-response handshake over a shared cluster secret. Each side proves knowledge of the secret by computing an HMAC-SHA256 over a transcript that includes the other side's fresh nonce, with distinct labels for the two directions so a proof cannot be reflected back. Both sides then derive a per-connection key, and every frame carries an HMAC over a sequence number and the payload, so frames cannot be forged, altered, replayed or reordered within a connection. A peer may only send messages from its own authenticated id. Traffic is authenticated, not encrypted.

Sending never blocks the event loop. Each peer has a bounded queue drained by its own virtual thread, which reconnects with exponential backoff. When the queue is full or the peer is down, messages are dropped; Raft retries everything that matters.

## Client API

The HTTP API runs on the JDK's built-in server with a virtual thread per request. Values are raw bytes in the body; versions are log indexes and appear as ETags, so compare-and-set is `If-Match` and create-only is `If-None-Match: *`. A follower answers with a redirect to the leader's advertised address, or 503 with `Retry-After` while there is no leader. A write whose outcome is unknown, because leadership changed or the request timed out, gets 503 or 504 and says so in the body.

Every input is checked before anything is proposed. Keys are strictly percent-decoded, must be valid UTF-8 without control characters, and are limited to 1 KiB. Values are limited to 1 MiB, and that is checked against `Content-Length` before the body is read. Bearer tokens are compared as SHA-256 hashes with a constant-time comparison. Membership changes need a separate admin token and are disabled when none is configured.

## How it is checked

The simulator is the main test. It runs the real `RaftNode` and `Replica` on one thread with a virtual clock and one seeded random source. The network drops, delays, reorders and duplicates messages and applies random partitions, including isolating the leader. Nodes crash, losing whatever they had not synced, and restart. Occasionally a sync fails as if power were lost in the middle of an fsync, after a leader has already sent that batch to its followers. Clock rates differ between nodes. After every step the checker verifies election safety, state machine safety and leader completeness. Log matching is checked across all live nodes at the end of the fault period and again after the cluster settles, and by then every acknowledged write must be present on every replica. A seed replays the identical run, and a failure prints the command that replays it. `./gradlew check` runs 1,000 seeds of each simulation test, and CI runs 10,000.

Simulated clients keep a session, retry a write under the same sequence number until it is answered, read with ReadIndex or leases, and give up after a deadline. Their histories go through a linearizability checker: the Wing and Gong search with Lowe's memoization, the algorithm Porcupine and Knossos use, per key since linearizability is compositional (Herlihy and Wing). The model is a register that also tracks versions, which makes it strict: a put must produce a version above the current one, and a conditional write must see exactly the version it expected. An operation that never got an answer may take effect at any point after its call or not at all; its version is unknown until a later operation reports it, and because an unanswered conditional write may or may not have matched, the search runs over sets of states. A replica that ignores sessions, so that retries apply twice, is caught by the checker.

Real-process tests cover what the simulator cannot. Three servers in one JVM lose their leader under load and keep every acknowledged write. A proxy that reads the dialer's id from the handshake cuts a leader off from its peers in both directions: it cannot commit, the majority elects a new leader, and after healing the old leader catches up and its uncommitted write is gone. Three separate JVMs run from the real classpath, and the leader is killed with SIGKILL mid-load and restarted from its files. Concurrent clients against a real cluster have their history checked across a leader change. Garbage is thrown at both ports.

Java has no race detector. The single-owner event loop removes most of the need for one, and the remaining concurrency (transport queues, HTTP futures) is covered by the real-server tests.

## What each failure costs

A leader crash costs one election timeout, 250 to 500 ms by default, before a new leader is elected. Writes in flight get an unknown outcome and are safe to retry under the same session. No acknowledged write is lost, because a write is acknowledged only after a majority has it on disk.

A follower crash costs nothing while a majority is up. The follower catches up from the log, or from a snapshot if it fell too far behind.

A minority partition cannot commit. A leader in the minority steps down within an election timeout and its pending writes get an unknown outcome. Linearizable reads there are never served; lease reads stop when the lease runs out.

A torn final write is cut on restart. Corruption in the middle of the log, or in the state or snapshot files, stops the node.

A disk that fails or fills up stops the node.

A slow or unreachable peer fills its send queue and its messages are dropped until it recovers. An overloaded node answers 503 rather than queueing without bound.

A full session table evicts the least recently used session, and that client gets "session expired" on its next write.

## What was traded away

The whole key space and the session table live in memory, and snapshots are serialized in memory. That keeps the state machine simple and fast but bounds the data set by heap size.

Snapshots are sent in one message of at most 256 MiB, and the log is rewritten after each snapshot. Chunked snapshot transfer and a segmented log would remove both limits.

Only the leader serves linearizable and lease reads. Followers serve only stale reads.

There is no leadership transfer, so planned maintenance on the leader costs an election timeout.

Each session allows one outstanding request. Concurrent writers need one session each.

Peer traffic is authenticated but not encrypted. Run it on a private network, or put TLS in front of it.

Every founding node must be started with the same initial membership. A server that loses its disk must rejoin under a new id.

Benchmarks on macOS measure the JDK's `force`, which on that platform asks the drive to flush its cache. That makes each fsync a few milliseconds and bounds write latency.

## What it is built with

There are no runtime dependencies. The build uses Gradle with the wrapper, Spotless with google-java-format, Error Prone, JUnit 5 and JaCoCo, each pinned to an exact version, and every resolved version is recorded in `gradle.lockfile`. CI scans that lockfile with osv-scanner. One has been moved to a newer release for a security fix: Guava, which Error Prone brings into the annotation processor path, is held at 33.7.2-jre or later by a constraint in `build.gradle.kts` because of GHSA-xxph-c9ww-hj94. It never reaches the runtime classpath.

## References

Diego Ongaro and John Ousterhout. In Search of an Understandable Consensus Algorithm (extended version). USENIX Annual Technical Conference.

Diego Ongaro. Consensus: Bridging Theory and Practice. PhD thesis, Stanford University.

Diego Ongaro. Bug in single-server membership changes. raft-dev mailing list.

Maurice Herlihy and Jeannette Wing. Linearizability: A Correctness Condition for Concurrent Objects. ACM Transactions on Programming Languages and Systems.

Jeannette Wing and Chun Gong. Testing and Verifying Concurrent Objects. Journal of Parallel and Distributed Computing.

Gavin Lowe. Testing for Linearizability. Concurrency and Computation: Practice and Experience.

Anish Athalye. Porcupine, a fast linearizability checker. github.com/anishathalye/porcupine.

Cary Gray and David Cheriton. Leases: An Efficient Fault-Tolerant Mechanism for Distributed File Cache Consistency. ACM Symposium on Operating Systems Principles.

Thanumalayan Sankaranarayana Pillai et al. All File Systems Are Not Created Equal: On the Complexity of Crafting Crash-Consistent Applications. USENIX Symposium on Operating Systems Design and Implementation.

Hugo Krawczyk, Mihir Bellare and Ran Canetti. HMAC: Keyed-Hashing for Message Authentication. RFC 2104.

Guy Castagnoli, Stefan Bräuer and Martin Herrmann. Optimization of Cyclic Redundancy-Check Codes with 24 and 32 Parity Bits. IEEE Transactions on Communications.
