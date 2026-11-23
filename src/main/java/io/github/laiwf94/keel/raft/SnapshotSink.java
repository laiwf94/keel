package io.github.laiwf94.keel.raft;

import java.io.OutputStream;

/**
 * Receives the state machine bytes of a new snapshot from {@link Storage#beginSnapshot()}: the
 * bytes a state machine serializes when the log is compacted, or the chunks a follower is sent by
 * its leader. File storage writes them to a temporary file, so a snapshot of any size is written
 * with a bounded buffer.
 *
 * <p>{@link #finish} seals the bytes into a {@link Snapshot} that {@link Storage#saveSnapshot} then
 * installs; {@link #abort} throws them away. A sink is used once and by one thread.
 */
public interface SnapshotSink {
  /**
   * Appends bytes.
   *
   * @throws java.io.UncheckedIOException or a storage exception if they cannot be written
   */
  void write(byte[] b, int off, int len);

  /** Number of bytes written so far. */
  long size();

  /**
   * Seals the bytes written so far as the snapshot described by {@code meta}. For file storage the
   * bytes are durable when this returns, but they become the node's snapshot only when the result
   * is passed to {@link Storage#saveSnapshot}.
   */
  Snapshot finish(SnapshotMeta meta);

  /** Throws the bytes away. Safe to call more than once, and after {@link #finish}. */
  void abort();

  /** An output stream that writes to this sink, for state machines that serialize to a stream. */
  default OutputStream stream() {
    SnapshotSink sink = this;
    return new OutputStream() {
      @Override
      public void write(int b) {
        sink.write(new byte[] {(byte) b}, 0, 1);
      }

      @Override
      public void write(byte[] b, int off, int len) {
        sink.write(b, off, len);
      }
    };
  }

  /** A sink that collects the bytes in memory, for storage that keeps snapshots in memory. */
  static SnapshotSink inMemory() {
    return new SnapshotSink() {
      private byte[] buf = new byte[256];
      private int size;
      private boolean closed;

      @Override
      public void write(byte[] b, int off, int len) {
        if (closed) {
          throw new IllegalStateException("snapshot sink is closed");
        }
        java.util.Objects.checkFromIndexSize(off, len, b.length);
        if (len > Integer.MAX_VALUE - 8 - size) {
          throw new IllegalStateException("snapshot too large to hold in memory");
        }
        if (size + len > buf.length) {
          buf =
              java.util.Arrays.copyOf(
                  buf,
                  (int) Math.min(Integer.MAX_VALUE - 8, Math.max(size + len, 2L * buf.length)));
        }
        System.arraycopy(b, off, buf, size, len);
        size += len;
      }

      @Override
      public long size() {
        return size;
      }

      @Override
      public Snapshot finish(SnapshotMeta meta) {
        if (closed) {
          throw new IllegalStateException("snapshot sink is closed");
        }
        closed = true;
        byte[] data = java.util.Arrays.copyOf(buf, size);
        buf = null;
        return new Snapshot(meta, data);
      }

      @Override
      public void abort() {
        closed = true;
        buf = null;
      }
    };
  }
}
