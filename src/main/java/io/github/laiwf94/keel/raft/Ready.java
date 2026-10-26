package io.github.laiwf94.keel.raft;

import java.util.List;
import java.util.Optional;

/**
 * Everything a node produced since the last call to {@link RaftNode#ready()}. The driver must sync
 * storage before sending {@link #messages}, then restore {@link #snapshot} if present, then apply
 * {@link #committed} in order.
 */
public record Ready(
    List<Message> messages,
    Optional<Snapshot> snapshot,
    List<Entry> committed,
    List<ReadState> reads,
    Status status) {
  public Ready {
    messages = List.copyOf(messages);
    committed = List.copyOf(committed);
    reads = List.copyOf(reads);
  }

  /** A read request that may be served once the state machine has applied {@code index}. */
  public record ReadState(long context, long index) {}

  /** A summary of the node's volatile state, for drivers, status pages and metrics. */
  public record Status(
      String id,
      RaftNode.Role role,
      long term,
      String leader,
      long commitIndex,
      long lastIndex,
      long snapshotIndex) {}
}
