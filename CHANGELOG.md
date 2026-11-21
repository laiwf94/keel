# Changelog

All notable changes to keel. Dates are when the work landed on main.

## Unreleased

### Week 1 (2026-11-02 to 2026-11-08, finished 2026-11-21): leadership transfer, graceful handoff and richer simulator faults

Added
- Leadership transfer (Raft thesis section 3.10): a `TimeoutNow` message; the leader stops taking proposals, catches the target up, sends `TimeoutNow`, and gives up after one election timeout.
- `POST /v1/leader[?to=<id>]`, behind the admin token, which answers with the new leader and term once it has taken over.
- Graceful handoff on SIGTERM: a leader hands over to the most caught-up voter before it closes its HTTP and peer ports, so a rolling restart neither waits out an election timeout nor fails a write.
- Simulator faults: one-way partitions; per-node disk latency, stalls and fsync failures; random leadership transfers and graceful shutdowns. All of them run in every simulation test and in the 10,000 seeds CI runs.
- `sim/Shrinker.java`: replays a failing seed while removing faults and client steps by delta debugging, and prints the minimal schedule and the command that replays it.
- `./gradlew linearizabilityBenchmark`, which times the old and new checkers in JVMs of their own and says whether the new checker's 100,000-operation rows are within the old search's 10,000-operation time.
- `examples/demo.sh` transfers leadership and restarts the leader with SIGTERM under load, and fails unless every write succeeds and reads back.

Changed
- The linearizability checker checks histories ten times longer in the same time: P-compositionality partitioning by key, state hashing, an early cut-off on unanswered operations, a forward pass without a stack, parallel stretches between pins, and per-key setup kept in arrays. 100,000-operation histories now check within the old search's time for 10,000.

Fixed
- A leader paused in an fsync could serve a stale lease read (#77).
