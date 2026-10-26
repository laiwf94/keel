package io.github.laiwf94.keel.replica;

import java.util.Optional;

/** How a proposal ended, as reported to the caller. */
public sealed interface Outcome {
  /** Committed and applied at {@code index}; {@code result} is the state machine's reply. */
  final class Applied implements Outcome {
    private final long index;
    private final byte[] result;

    public Applied(long index, byte[] result) {
      this.index = index;
      this.result = result;
    }

    public long index() {
      return index;
    }

    /** The state machine's reply. Callers must not modify it. */
    public byte[] result() {
      return result;
    }

    @Override
    public String toString() {
      return "Applied{" + index + "}";
    }
  }

  /** The request was refused before anything was proposed; {@code reason} says why. */
  record Rejected(String reason) implements Outcome {}

  /** This node is not the leader. Nothing was proposed, so retrying elsewhere is safe. */
  record NotLeader(Optional<String> leaderHint) implements Outcome {}

  /**
   * The node lost leadership or its entry was replaced before it was applied. The command may or
   * may not take effect; only a retry under the same client session is safe.
   */
  record Unknown(String reason) implements Outcome {}
}
