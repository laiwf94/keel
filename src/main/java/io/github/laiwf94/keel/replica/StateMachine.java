package io.github.laiwf94.keel.replica;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The replicated application. It must be deterministic: replicas that apply the same commands in
 * the same order must end in the same state and return the same results.
 *
 * <p>Snapshots are streams so that the replica never needs the serialized state in memory: {@link
 * #snapshot} writes straight into the snapshot file and {@link #restore} reads straight from it.
 */
public interface StateMachine {
  /** Applies the committed command at {@code index} and returns the reply for the client. */
  byte[] apply(long index, byte[] command);

  /** Serializes the full state to {@code out}. The caller closes the stream. */
  void snapshot(OutputStream out) throws IOException;

  /**
   * Replaces the full state with a snapshot written by {@link #snapshot}, read from {@code in} to
   * its end. The caller closes the stream. On failure the state must be left as it was.
   */
  void restore(InputStream in) throws IOException;
}
