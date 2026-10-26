package io.github.laiwf94.keel.kv;

import io.github.laiwf94.keel.codec.ByteReader;
import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.codec.CodecException;
import io.github.laiwf94.keel.replica.StateMachine;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The replicated key-value store, with client sessions for exactly-once writes (thesis section
 * 6.3).
 *
 * <p>A client registers once and then numbers its requests. The machine remembers, per session, the
 * highest sequence number applied and its result. A retry of that request returns the stored result
 * instead of applying again; anything older is refused as stale. Sessions expire after a period
 * without requests, measured with the leader timestamps carried in the log, never the local clock,
 * so every replica expires the same sessions at the same index.
 */
public final class KvMachine implements StateMachine {
  /** A stored value and its version, which is the log index of the write that set it. */
  public static final class Value {
    private final byte[] bytes;
    private final long version;

    Value(byte[] bytes, long version) {
      this.bytes = bytes;
      this.version = version;
    }

    /** The stored bytes. Callers must not modify them. */
    public byte[] bytes() {
      return bytes;
    }

    public long version() {
      return version;
    }
  }

  private static final class Session {
    long lastSeq;
    long lastActive;
    Result lastResult;

    Session(long lastSeq, long lastActive, Result lastResult) {
      this.lastSeq = lastSeq;
      this.lastActive = lastActive;
      this.lastResult = lastResult;
    }
  }

  private static final int MAX_SNAPSHOT_KEYS = 50_000_000;

  private final long sessionTimeoutMs;
  private final int maxSessions;
  private final TreeMap<String, Value> data = new TreeMap<>();
  // Access order: the first session is always the one used least recently. Because the clock
  // only moves forward, that is also the one that expires first.
  private final LinkedHashMap<Long, Session> sessions = new LinkedHashMap<>(16, 0.75f, true);
  private long clock;

  public KvMachine(long sessionTimeoutMs, int maxSessions) {
    if (sessionTimeoutMs < 1 || maxSessions < 1) {
      throw new IllegalArgumentException("session limits must be positive");
    }
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.maxSessions = maxSessions;
  }

  @Override
  public byte[] apply(long index, byte[] bytes) {
    Command c;
    try {
      c = Command.decode(bytes);
    } catch (CodecException e) {
      return new Result(Result.Status.INVALID, 0).encode();
    }
    clock = Math.max(clock, c.timestamp());
    expireSessions();
    return execute(index, c).encode();
  }

  private Result execute(long index, Command c) {
    if (c.op() == Command.Op.REGISTER) {
      sessions.put(index, new Session(0, clock, null));
      while (sessions.size() > maxSessions) {
        Iterator<Long> it = sessions.keySet().iterator();
        it.next();
        it.remove();
      }
      return new Result(Result.Status.OK, index);
    }
    Session s = null;
    if (c.clientId() != 0) {
      s = sessions.get(c.clientId());
      if (s == null) {
        return new Result(Result.Status.SESSION_EXPIRED, 0);
      }
      s.lastActive = clock;
      if (c.seq() == s.lastSeq) {
        return s.lastResult;
      }
      if (c.seq() < s.lastSeq) {
        return new Result(Result.Status.STALE, 0);
      }
    }
    Result r = write(index, c);
    if (s != null) {
      s.lastSeq = c.seq();
      s.lastResult = r;
    }
    return r;
  }

  private Result write(long index, Command c) {
    Value current = data.get(c.key());
    long currentVersion = current == null ? 0 : current.version();
    if (c.expectedVersion() != Command.ANY_VERSION && c.expectedVersion() != currentVersion) {
      return new Result(Result.Status.CONFLICT, currentVersion);
    }
    if (c.op() == Command.Op.PUT) {
      data.put(c.key(), new Value(c.value(), index));
      return new Result(Result.Status.OK, index);
    }
    data.remove(c.key());
    return new Result(Result.Status.OK, currentVersion);
  }

  private void expireSessions() {
    Iterator<Session> it = sessions.values().iterator();
    while (it.hasNext()) {
      Session s = it.next();
      if (clock - s.lastActive <= sessionTimeoutMs) {
        break;
      }
      it.remove();
    }
  }

  public Optional<Value> get(String key) {
    return Optional.ofNullable(data.get(key));
  }

  public int size() {
    return data.size();
  }

  public int sessionCount() {
    return sessions.size();
  }

  @Override
  public byte[] snapshot() {
    ByteWriter w = new ByteWriter(1024);
    w.writeLong(clock);
    w.writeInt(data.size());
    for (Map.Entry<String, Value> e : data.entrySet()) {
      w.writeString(e.getKey()).writeBytes(e.getValue().bytes()).writeLong(e.getValue().version());
    }
    w.writeInt(sessions.size());
    for (Map.Entry<Long, Session> e : sessions.entrySet()) {
      Session s = e.getValue();
      w.writeLong(e.getKey()).writeLong(s.lastSeq).writeLong(s.lastActive);
      w.writeBoolean(s.lastResult != null);
      if (s.lastResult != null) {
        s.lastResult.writeTo(w);
      }
    }
    return w.toByteArray();
  }

  @Override
  public void restore(byte[] snapshot) {
    ByteReader r = new ByteReader(snapshot);
    long newClock = r.readNonNegativeLong();
    TreeMap<String, Value> newData = new TreeMap<>();
    int n = r.readCount(MAX_SNAPSHOT_KEYS);
    for (int i = 0; i < n; i++) {
      String key = r.readString(Command.MAX_KEY_BYTES);
      byte[] value = r.readBytes(Command.MAX_VALUE_BYTES);
      newData.put(key, new Value(value, r.readNonNegativeLong()));
    }
    LinkedHashMap<Long, Session> newSessions = new LinkedHashMap<>(16, 0.75f, true);
    int m = r.readCount(maxSessions);
    for (int i = 0; i < m; i++) {
      long id = r.readNonNegativeLong();
      long seq = r.readNonNegativeLong();
      long active = r.readNonNegativeLong();
      Result last = r.readBoolean() ? Result.readFrom(r) : null;
      newSessions.put(id, new Session(seq, active, last));
    }
    r.expectEnd();
    clock = newClock;
    data.clear();
    data.putAll(newData);
    sessions.clear();
    sessions.putAll(newSessions);
  }
}
