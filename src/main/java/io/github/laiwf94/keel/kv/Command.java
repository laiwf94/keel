package io.github.laiwf94.keel.kv;

import io.github.laiwf94.keel.codec.ByteReader;
import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.codec.CodecException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * A write to the key-value store, as stored in the log. Every command carries the proposing
 * leader's wall-clock time so that all replicas expire client sessions at the same log position.
 */
public final class Command {
  public enum Op {
    REGISTER(1),
    PUT(2),
    DELETE(3);

    private final int code;

    Op(int code) {
      this.code = code;
    }

    static Op fromCode(int code) {
      for (Op op : values()) {
        if (op.code == code) {
          return op;
        }
      }
      throw new CodecException("unknown op " + code);
    }
  }

  /** {@code expectedVersion} that matches anything. */
  public static final long ANY_VERSION = -1;

  /** {@code expectedVersion} that matches only a key that does not exist. */
  public static final long ABSENT = 0;

  public static final int MAX_KEY_BYTES = 1024;
  public static final int MAX_VALUE_BYTES = 1 << 20;

  private final Op op;
  private final long timestamp;
  private final long clientId;
  private final long seq;
  private final String key;
  private final byte[] value;
  private final long expectedVersion;

  private Command(
      Op op,
      long timestamp,
      long clientId,
      long seq,
      String key,
      byte[] value,
      long expectedVersion) {
    if (timestamp < 0 || clientId < 0 || seq < 0 || expectedVersion < ANY_VERSION) {
      throw new IllegalArgumentException("negative field");
    }
    if (clientId > 0 && seq == 0) {
      throw new IllegalArgumentException("a session request needs a sequence number");
    }
    if (op != Op.REGISTER) {
      validateKey(key);
    }
    if (value.length > MAX_VALUE_BYTES) {
      throw new IllegalArgumentException("value larger than " + MAX_VALUE_BYTES + " bytes");
    }
    this.op = op;
    this.timestamp = timestamp;
    this.clientId = clientId;
    this.seq = seq;
    this.key = key;
    this.value = value;
    this.expectedVersion = expectedVersion;
  }

  /** Throws if the key is empty, too long or contains control characters. */
  public static String validateKey(String key) {
    Objects.requireNonNull(key, "key");
    int bytes = key.getBytes(StandardCharsets.UTF_8).length;
    if (bytes == 0 || bytes > MAX_KEY_BYTES) {
      throw new IllegalArgumentException("key must be 1 to " + MAX_KEY_BYTES + " bytes");
    }
    for (int i = 0; i < key.length(); i++) {
      if (Character.isISOControl(key.charAt(i))) {
        throw new IllegalArgumentException("key contains a control character");
      }
    }
    return key;
  }

  public static Command register(long timestamp) {
    return new Command(Op.REGISTER, timestamp, 0, 0, "", new byte[0], ANY_VERSION);
  }

  public static Command put(
      long timestamp, long clientId, long seq, String key, byte[] value, long expectedVersion) {
    return new Command(Op.PUT, timestamp, clientId, seq, key, value, expectedVersion);
  }

  public static Command delete(
      long timestamp, long clientId, long seq, String key, long expectedVersion) {
    return new Command(Op.DELETE, timestamp, clientId, seq, key, new byte[0], expectedVersion);
  }

  public Op op() {
    return op;
  }

  public long timestamp() {
    return timestamp;
  }

  public long clientId() {
    return clientId;
  }

  public long seq() {
    return seq;
  }

  public String key() {
    return key;
  }

  /** The value to write. Callers must not modify it. */
  public byte[] value() {
    return value;
  }

  public long expectedVersion() {
    return expectedVersion;
  }

  public byte[] encode() {
    ByteWriter w = new ByteWriter(32 + key.length() + value.length);
    w.writeByte(op.code).writeLong(timestamp);
    if (op != Op.REGISTER) {
      w.writeLong(clientId).writeLong(seq).writeString(key).writeLong(expectedVersion);
      if (op == Op.PUT) {
        w.writeBytes(value);
      }
    }
    return w.toByteArray();
  }

  public static Command decode(byte[] bytes) {
    ByteReader r = new ByteReader(bytes);
    Op op = Op.fromCode(r.readByte());
    long ts = r.readLong();
    Command c;
    try {
      if (op == Op.REGISTER) {
        c = register(ts);
      } else {
        long client = r.readLong();
        long seq = r.readLong();
        String key = r.readString(MAX_KEY_BYTES);
        long expected = r.readLong();
        c =
            op == Op.PUT
                ? put(ts, client, seq, key, r.readBytes(MAX_VALUE_BYTES), expected)
                : delete(ts, client, seq, key, expected);
      }
    } catch (IllegalArgumentException e) {
      throw new CodecException(e.getMessage());
    }
    r.expectEnd();
    return c;
  }
}
