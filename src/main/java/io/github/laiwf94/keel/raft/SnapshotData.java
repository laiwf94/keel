package io.github.laiwf94.keel.raft;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * The state machine bytes of a snapshot, read in ranges or as a stream so that no caller has to
 * hold the whole snapshot in memory. {@link io.github.laiwf94.keel.raft.MemStorage} keeps them in
 * an array; file storage keeps them in the snapshot file and reads them from disk on demand.
 *
 * <p>Data kept in a file is reference counted. Whoever stores the snapshot holds one reference, and
 * a leader holds another for each follower it is sending the snapshot to, so a transfer can finish
 * reading a snapshot that storage has since replaced. In-memory data ignores the counts.
 */
public interface SnapshotData {
  /** Number of bytes. */
  long size();

  /**
   * Reads {@code length} bytes at {@code offset} into a new array.
   *
   * @throws IllegalArgumentException if the range is not inside the data
   * @throws UncheckedIOException if the bytes cannot be read
   */
  byte[] read(long offset, int length);

  /** Opens a stream over all the bytes, from the first. The caller closes it. */
  InputStream open();

  /** Takes another reference; see the class comment. */
  default void retain() {}

  /** Drops a reference; data in a file is closed when the last one goes. */
  default void release() {}

  /**
   * Copies every byte into one array, for tests and small snapshots.
   *
   * @throws IllegalStateException if the data does not fit in an array
   */
  default byte[] toByteArray() {
    long n = size();
    if (n > Integer.MAX_VALUE - 8) {
      throw new IllegalStateException(n + " snapshot bytes do not fit in an array");
    }
    return read(0, (int) n);
  }

  /** Whether {@code a} and {@code b} hold the same bytes, compared a block at a time. */
  static boolean sameBytes(SnapshotData a, SnapshotData b) {
    long n = a.size();
    if (n != b.size()) {
      return false;
    }
    int block = 64 << 10;
    for (long at = 0; at < n; at += block) {
      int len = (int) Math.min(block, n - at);
      if (!Arrays.equals(a.read(at, len), b.read(at, len))) {
        return false;
      }
    }
    return true;
  }

  /** Data held in {@code bytes}, which the caller must not modify afterwards. */
  static SnapshotData of(byte[] bytes) {
    return new Heap(bytes);
  }

  /** Throws unless {@code [offset, offset + length)} lies inside {@code [0, size)}. */
  static void checkRange(long offset, int length, long size) {
    if (offset < 0 || length < 0 || offset > size - length) {
      throw new IllegalArgumentException(
          "range " + offset + "+" + length + " outside snapshot of " + size + " bytes");
    }
  }

  /** Snapshot bytes in an array. */
  final class Heap implements SnapshotData {
    private final byte[] bytes;

    private Heap(byte[] bytes) {
      this.bytes = Objects.requireNonNull(bytes, "bytes");
    }

    @Override
    public long size() {
      return bytes.length;
    }

    @Override
    public byte[] read(long offset, int length) {
      checkRange(offset, length, bytes.length);
      return Arrays.copyOfRange(bytes, (int) offset, (int) offset + length);
    }

    @Override
    public InputStream open() {
      return new ByteArrayInputStream(bytes);
    }

    @Override
    public byte[] toByteArray() {
      return bytes.clone();
    }
  }

  /** Wraps an {@link IOException} from reading snapshot bytes. */
  static UncheckedIOException failure(String what, IOException e) {
    return new UncheckedIOException(what + ": " + e.getMessage(), e);
  }
}
