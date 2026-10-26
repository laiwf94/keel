package io.github.laiwf94.keel.sim;

import java.util.ArrayList;
import java.util.List;

/**
 * A record of client operations with their call and return points. Points come from one counter so
 * ordering is exact. An operation that never got an answer keeps {@link #OPEN} as its return point:
 * it may have taken effect at any time after its call, or not at all.
 */
final class History {
  static final long OPEN = Long.MAX_VALUE;

  enum Kind {
    PUT,
    DELETE,
    GET
  }

  /** Version that matches any version; used when an open write's version is unknown. */
  static final long ANY = -2;

  static final class Op {
    final int id;
    final int client;
    final Kind kind;
    final String key;
    final String value;
    final long expected;
    final long call;
    long ret = OPEN;
    boolean ok;
    long version;
    String readValue;

    Op(int id, int client, Kind kind, String key, String value, long expected, long call) {
      this.id = id;
      this.client = client;
      this.kind = kind;
      this.key = key;
      this.value = value;
      this.expected = expected;
      this.call = call;
    }

    @Override
    public String toString() {
      String out = ret == OPEN ? "?" : (ok ? "ok" : "conflict") + "@" + version;
      if (kind == Kind.GET && ret != OPEN) {
        out = readValue + "@" + version;
      }
      return "c"
          + client
          + " "
          + kind
          + "("
          + key
          + (value == null ? "" : "=" + value)
          + (expected == -1 ? "" : " if " + expected)
          + ") ["
          + call
          + ","
          + (ret == OPEN ? "inf" : ret)
          + "] -> "
          + out;
    }
  }

  final List<Op> ops = new ArrayList<>();
  private long clock;

  Op call(int client, Kind kind, String key, String value, long expected) {
    Op op = new Op(ops.size(), client, kind, key, value, expected, ++clock);
    ops.add(op);
    return op;
  }

  /** A write finished: {@code ok} if it applied, else it hit a version conflict. */
  void write(Op op, boolean ok, long version) {
    op.ok = ok;
    op.version = version;
    op.ret = ++clock;
  }

  /** A read returned {@code value} (null if absent) at {@code version} (0 if absent). */
  void read(Op op, String value, long version) {
    op.ok = true;
    op.readValue = value;
    op.version = version;
    op.ret = ++clock;
  }

  int completed() {
    return (int) ops.stream().filter(o -> o.ret != OPEN).count();
  }
}
