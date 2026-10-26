package io.github.laiwf94.keel.raft;

import java.util.Optional;

/** Thrown when a request needs the leader and this node is not it. */
public final class NotLeaderException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private final String leaderHint;

  public NotLeaderException(String leaderHint) {
    super(leaderHint.isEmpty() ? "no known leader" : "not the leader; try " + leaderHint);
    this.leaderHint = leaderHint;
  }

  /** The node this one believes leads, if any. */
  public Optional<String> leaderHint() {
    return leaderHint.isEmpty() ? Optional.empty() : Optional.of(leaderHint);
  }
}
