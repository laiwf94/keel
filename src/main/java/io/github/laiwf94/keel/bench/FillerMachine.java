package io.github.laiwf94.keel.bench;

import io.github.laiwf94.keel.replica.StateMachine;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A state machine whose state is a count and a digest of the commands applied, and whose snapshot
 * adds a fixed number of filler bytes generated from that state. Restore checks every filler byte,
 * so a snapshot of any size is verified end to end without either side holding it in memory. It
 * stands in for a data set of that size when measuring snapshots and catch-up.
 */
public final class FillerMachine implements StateMachine {
  /** Snapshot bytes written per call to the stream, the way a state machine writes records. */
  private static final int BLOCK = 64 << 10;

  private final long filler;
  private long applied;
  private long digest = 17;
  private int restores;
  private long snapshotNanos;
  private long restoreNanos;

  /** A machine whose snapshots carry {@code filler} bytes after a 16-byte header. */
  public FillerMachine(long filler) {
    if (filler < 0) {
      throw new IllegalArgumentException("negative filler: " + filler);
    }
    this.filler = filler;
  }

  @Override
  public byte[] apply(long index, byte[] command) {
    applied++;
    for (byte b : command) {
      digest = digest * 31 + b;
    }
    digest = digest * 31 + index;
    return new byte[0];
  }

  private static void fill(ByteBuffer b, long[] seed) {
    b.clear();
    while (b.remaining() >= 8) {
      long x = seed[0];
      x ^= x << 13;
      x ^= x >>> 7;
      x ^= x << 17;
      seed[0] = x;
      b.putLong(x);
    }
    b.flip();
  }

  @Override
  public void snapshot(OutputStream out) throws IOException {
    long start = System.nanoTime();
    ByteBuffer head = ByteBuffer.allocate(16).putLong(applied).putLong(digest);
    out.write(head.array());
    long[] seed = {digest | 1};
    ByteBuffer b = ByteBuffer.allocate(BLOCK);
    for (long left = filler; left > 0; left -= BLOCK) {
      fill(b, seed);
      out.write(b.array(), 0, (int) Math.min(BLOCK, left));
    }
    snapshotNanos += System.nanoTime() - start;
  }

  @Override
  public void restore(InputStream in) throws IOException {
    long start = System.nanoTime();
    byte[] head = in.readNBytes(16);
    if (head.length != 16) {
      throw new IOException("snapshot header truncated");
    }
    ByteBuffer h = ByteBuffer.wrap(head);
    long a = h.getLong();
    long d = h.getLong();
    long[] seed = {d | 1};
    ByteBuffer want = ByteBuffer.allocate(BLOCK);
    byte[] got = new byte[BLOCK];
    for (long left = filler; left > 0; left -= BLOCK) {
      int n = (int) Math.min(BLOCK, left);
      fill(want, seed);
      if (in.readNBytes(got, 0, n) != n || !Arrays.equals(got, 0, n, want.array(), 0, n)) {
        throw new IOException("snapshot filler differs before byte " + (filler - left + n));
      }
    }
    if (in.read() >= 0) {
      throw new IOException("snapshot has trailing bytes");
    }
    applied = a;
    digest = d;
    restores++;
    restoreNanos += System.nanoTime() - start;
  }

  /** Commands applied, counting those a restored snapshot covers. */
  public long applied() {
    return applied;
  }

  /** A digest of every command applied and its index. */
  public long digest() {
    return digest;
  }

  /** Snapshots restored so far. */
  public int restores() {
    return restores;
  }

  /** Time spent writing snapshots, including the time the stream took to accept the bytes. */
  public long snapshotNanos() {
    return snapshotNanos;
  }

  /** Time spent reading and checking restored snapshots. */
  public long restoreNanos() {
    return restoreNanos;
  }

  /** Whether {@code other} has applied the same commands at the same indexes. */
  public boolean sameStateAs(FillerMachine other) {
    return applied == other.applied && digest == other.digest;
  }
}
