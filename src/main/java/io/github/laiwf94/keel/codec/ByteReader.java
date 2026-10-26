package io.github.laiwf94.keel.codec;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Reads values written by {@link ByteWriter}. Every length is checked against both a caller-given
 * maximum and the bytes actually left before anything is allocated, so a hostile length field
 * cannot make the reader allocate more than the input it was given.
 */
public final class ByteReader {
  private final byte[] buf;
  private int pos;
  private final int limit;

  public ByteReader(byte[] buf) {
    this(buf, 0, buf.length);
  }

  public ByteReader(byte[] buf, int offset, int length) {
    if (offset < 0 || length < 0 || offset + length > buf.length) {
      throw new IllegalArgumentException("bad slice");
    }
    this.buf = buf;
    this.pos = offset;
    this.limit = offset + length;
  }

  public int remaining() {
    return limit - pos;
  }

  private void need(int n) {
    if (n > limit - pos) {
      throw new CodecException("truncated input: need " + n + " bytes, have " + (limit - pos));
    }
  }

  public int readByte() {
    need(1);
    return buf[pos++] & 0xff;
  }

  public boolean readBoolean() {
    int b = readByte();
    if (b > 1) {
      throw new CodecException("bad boolean " + b);
    }
    return b == 1;
  }

  public int readInt() {
    need(4);
    int v =
        ((buf[pos] & 0xff) << 24)
            | ((buf[pos + 1] & 0xff) << 16)
            | ((buf[pos + 2] & 0xff) << 8)
            | (buf[pos + 3] & 0xff);
    pos += 4;
    return v;
  }

  public long readLong() {
    long hi = readInt() & 0xffffffffL;
    long lo = readInt() & 0xffffffffL;
    return (hi << 32) | lo;
  }

  /** Reads a long that must be zero or positive, as indexes and terms are. */
  public long readNonNegativeLong() {
    long v = readLong();
    if (v < 0) {
      throw new CodecException("negative value " + v);
    }
    return v;
  }

  /** Reads a count that must lie in {@code [0, max]}. */
  public int readCount(int max) {
    int n = readInt();
    if (n < 0 || n > max) {
      throw new CodecException("count " + n + " outside [0, " + max + "]");
    }
    return n;
  }

  public byte[] readBytes(int max) {
    int n = readCount(max);
    need(n);
    byte[] out = new byte[n];
    System.arraycopy(buf, pos, out, 0, n);
    pos += n;
    return out;
  }

  public String readString(int maxBytes) {
    byte[] raw = readBytes(maxBytes);
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(java.nio.ByteBuffer.wrap(raw))
          .toString();
    } catch (CharacterCodingException e) {
      throw new CodecException("invalid UTF-8");
    }
  }

  /** Fails if any bytes are left, so trailing garbage is never silently accepted. */
  public void expectEnd() {
    if (pos != limit) {
      throw new CodecException((limit - pos) + " unexpected trailing bytes");
    }
  }
}
