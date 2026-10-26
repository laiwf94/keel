package io.github.laiwf94.keel.raft;

import java.util.Objects;

/**
 * The state a node must persist before answering any message: its current term and whom it voted
 * for in that term ({@link #NO_VOTE} when it has not voted).
 */
public record HardState(long term, String votedFor) {
  public static final String NO_VOTE = "";
  public static final HardState INITIAL = new HardState(0, NO_VOTE);

  public HardState {
    if (term < 0) {
      throw new IllegalArgumentException("negative term");
    }
    Objects.requireNonNull(votedFor, "votedFor");
    if (!votedFor.isEmpty()) {
      Member.validateId(votedFor);
    }
  }
}
