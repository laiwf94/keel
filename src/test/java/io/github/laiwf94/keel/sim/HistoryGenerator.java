package io.github.laiwf94.keel.sim;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * Generates long key-value histories with a known answer, for testing and benchmarking the
 * linearizability checker without running a cluster.
 *
 * <p>Clients issue one operation at a time, as {@link SimKvClient} does: puts, conditional puts
 * against the last version they saw, deletes, conditional deletes and reads. Each operation takes
 * effect on an atomic register at a random instant between its call and its return, so the history
 * is linearizable by construction. Versions come from one counter shared by all keys, like log
 * indexes. A fraction of writes never return: half of those take effect and half do not, and the
 * client moves on. {@link #staleRead} turns such a history into one that is not linearizable.
 */
final class HistoryGenerator {
  private HistoryGenerator() {}

  /** What to generate. {@code openRate} is the fraction of operations that never return. */
  record Shape(int ops, int clients, int keys, double openRate) {
    /** Four clients on three keys with a few unanswered operations, like the simulator's runs. */
    static Shape simulatorLike(int ops) {
      return new Shape(ops, 4, 3, 0.002);
    }
  }

  private record Planned(History.Op op, long lin, boolean open, boolean applies) {}

  static List<History.Op> linearizable(long seed, Shape shape) {
    SplittableRandom rnd = new SplittableRandom(seed);
    // Times are drawn on a fine grid and every call, return and effect gets its own instant.
    List<Planned> planned = new ArrayList<>(shape.ops());
    long[] free = new long[shape.clients()];
    int[] seqs = new int[shape.clients()];
    for (int i = 0; i < shape.ops(); i++) {
      int c = rnd.nextInt(shape.clients());
      long call = free[c] + 1 + rnd.nextInt(40) * 4L;
      long length = 4 + rnd.nextInt(60) * 4L;
      long ret = call + length;
      long lin = call + 1 + rnd.nextLong(length - 2);
      boolean open = rnd.nextDouble() < shape.openRate();
      String key = "k" + rnd.nextInt(shape.keys());
      int pick = rnd.nextInt(13);
      History.Kind kind =
          pick >= 10 ? History.Kind.GET : pick < 8 ? History.Kind.PUT : History.Kind.DELETE;
      boolean conditional = pick == 6 || pick == 7 || pick == 9;
      String value = kind == History.Kind.PUT ? "c" + c + "-" + ++seqs[c] : null;
      // The expected version is filled in when the operation is applied (see below); -1 marks an
      // unconditional one, and -3 a conditional one that is still to be decided.
      // Scaled by the client count and offset by the client, so no two instants coincide.
      int n = shape.clients();
      History.Op op = new History.Op(i, c, kind, key, value, conditional ? -3 : -1, call * n + c);
      if (!open) {
        op.ret = ret * n + c;
      }
      planned.add(new Planned(op, lin * n + c, open, !open || rnd.nextBoolean()));
      free[c] = open ? ret + 200 : ret;
    }
    // Apply every operation at its instant, in order.
    planned.sort(Comparator.comparingLong(Planned::lin));
    Map<String, String> values = new HashMap<>();
    Map<String, Long> versions = new HashMap<>();
    Map<Long, Long> lastSeen = new HashMap<>(); // (client << 32 | key index) -> version
    long index = 0;
    List<History.Op> out = new ArrayList<>(planned.size());
    for (Planned p : planned) {
      History.Op op = p.op();
      long seenKey = ((long) op.client << 32) | Integer.parseInt(op.key.substring(1));
      long current = versions.getOrDefault(op.key, 0L);
      long expected = op.expected == -3 ? lastSeen.getOrDefault(seenKey, 0L) : -1;
      History.Op real =
          new History.Op(op.id, op.client, op.kind, op.key, op.value, expected, op.call);
      real.ret = op.ret;
      out.add(real);
      index += 1 + (p.lin() & 1);
      if (!p.applies()) {
        continue;
      }
      if (op.kind == History.Kind.GET) {
        real.ok = true;
        real.readValue = values.get(op.key);
        real.version = current;
      } else if (expected != -1 && expected != current) {
        real.ok = false;
        real.version = current;
      } else if (op.kind == History.Kind.PUT) {
        values.put(op.key, op.value);
        versions.put(op.key, index);
        real.ok = true;
        real.version = index;
      } else {
        values.remove(op.key);
        versions.remove(op.key);
        real.ok = true;
        real.version = current;
      }
      if (!p.open()) {
        long seen = real.ok && op.kind == History.Kind.DELETE ? 0 : real.version;
        lastSeen.put(seenKey, seen);
      }
    }
    out.sort(Comparator.comparingInt(o -> o.id));
    return out;
  }

  /**
   * Makes one read in the second half of {@code ops} return a value that two completed writes had
   * already overwritten before the read was called, which no linearization explains because values
   * are unique. Returns the read it changed.
   */
  static History.Op staleRead(List<History.Op> ops, long seed) {
    SplittableRandom rnd = new SplittableRandom(seed);
    for (int attempt = 0; attempt < 10_000; attempt++) {
      History.Op read = ops.get(ops.size() / 2 + rnd.nextInt(ops.size() / 2));
      if (read.kind != History.Kind.GET || read.ret == History.OPEN) {
        continue;
      }
      History.Op newest = null;
      History.Op older = null;
      for (int i = read.id - 1; i >= 0 && older == null; i--) {
        History.Op w = ops.get(i);
        if (!w.key.equals(read.key)
            || w.kind != History.Kind.PUT
            || !w.ok
            || w.ret == History.OPEN
            || w.ret >= read.call) {
          continue;
        }
        if (newest == null) {
          newest = w;
        } else if (w.ret < newest.call) {
          older = w;
        }
      }
      if (older != null && !older.value.equals(read.readValue)) {
        read.readValue = older.value;
        read.version = older.version;
        return read;
      }
    }
    throw new IllegalStateException("no read to make stale");
  }
}
