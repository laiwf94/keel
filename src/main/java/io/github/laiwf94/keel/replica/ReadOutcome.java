package io.github.laiwf94.keel.replica;

import java.util.Optional;

/** How a read request ended. */
public sealed interface ReadOutcome {
  /**
   * Safe to answer now from the state machine, which has applied at least {@code index}. The
   * callback runs on the replica's thread, so the state machine can be read directly.
   */
  record Served(long index) implements ReadOutcome {}

  /** This node is not the leader. */
  record NotLeader(Optional<String> leaderHint) implements ReadOutcome {}

  /** Leadership changed before the read could be confirmed; retrying is safe. */
  record Failed(String reason) implements ReadOutcome {}
}
