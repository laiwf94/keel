package io.github.laiwf94.keel.kv;

import io.github.laiwf94.keel.codec.ByteReader;
import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.codec.CodecException;

/**
 * The reply to a {@link Command}.
 *
 * @param status what happened
 * @param version for {@code OK} writes, the key's new version (the log index of the write); for
 *     {@code OK} deletes, the version that was removed (0 if the key was absent); for {@code
 *     CONFLICT}, the key's current version; for {@code REGISTER}, the new client id
 */
public record Result(Status status, long version) {
  public enum Status {
    OK(0),
    /** The key's version did not match {@code expectedVersion}; nothing changed. */
    CONFLICT(1),
    /** The client session does not exist or has expired; nothing changed. */
    SESSION_EXPIRED(2),
    /** The request is older than one the session already completed; nothing changed. */
    STALE(3),
    /** The command bytes did not decode; nothing changed. */
    INVALID(4);

    private final int code;

    Status(int code) {
      this.code = code;
    }

    static Status fromCode(int code) {
      for (Status s : values()) {
        if (s.code == code) {
          return s;
        }
      }
      throw new CodecException("unknown status " + code);
    }
  }

  public byte[] encode() {
    ByteWriter w = new ByteWriter(9);
    writeTo(w);
    return w.toByteArray();
  }

  void writeTo(ByteWriter w) {
    w.writeByte(status.code).writeLong(version);
  }

  static Result readFrom(ByteReader r) {
    Status s = Status.fromCode(r.readByte());
    return new Result(s, r.readLong());
  }

  public static Result decode(byte[] bytes) {
    ByteReader r = new ByteReader(bytes);
    Result res = readFrom(r);
    r.expectEnd();
    return res;
  }
}
