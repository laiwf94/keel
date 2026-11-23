package io.github.laiwf94.keel.replica;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/** Byte-array helpers over the streaming snapshot methods of a {@link StateMachine}, for tests. */
public final class MachineSnapshots {
  private MachineSnapshots() {}

  /** The machine's snapshot as one array. */
  public static byte[] bytes(StateMachine m) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      m.snapshot(out);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out.toByteArray();
  }

  /** Restores the machine from a snapshot held in an array. */
  public static void restore(StateMachine m, byte[] snapshot) {
    try {
      m.restore(new ByteArrayInputStream(snapshot));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
