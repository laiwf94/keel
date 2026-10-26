package io.github.laiwf94.keel.replica;

/**
 * The replicated application. It must be deterministic: replicas that apply the same commands in
 * the same order must end in the same state and return the same results.
 */
public interface StateMachine {
  /** Applies the committed command at {@code index} and returns the reply for the client. */
  byte[] apply(long index, byte[] command);

  /** Serializes the full state. */
  byte[] snapshot();

  /** Replaces the full state with a snapshot taken by {@link #snapshot()}. */
  void restore(byte[] snapshot);
}
