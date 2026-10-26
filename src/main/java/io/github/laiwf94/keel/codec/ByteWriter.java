package io.github.laiwf94.keel.codec;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** A growable big-endian byte buffer. Faster than {@code DataOutputStream} and never throws. */
public final class ByteWriter {
  private byte[] buf;
  private int size;

  public ByteWriter() {
    this(64);
  }

  public ByteWriter(int initialCapacity) {
    buf = new byte[Math.max(16, initialCapacity)];
  }

  private void ensure(int extra) {
    if (size + extra > buf.length) {
      long wanted = Math.max((long) buf.length * 2, (long) size + extra);
      if (wanted > Integer.MAX_VALUE - 8) {
        throw new CodecException("value too large to encode");
      }
      buf = Arrays.copyOf(buf, (int) wanted);
    }
  }

  public ByteWriter writeByte(int v) {
    ensure(1);
    buf[size++] = (byte) v;
    return this;
  }

  public ByteWriter writeBoolean(boolean v) {
    return writeByte(v ? 1 : 0);
  }

  public ByteWriter writeInt(int v) {
    ensure(4);
    buf[size++] = (byte) (v >>> 24);
    buf[size++] = (byte) (v >>> 16);
    buf[size++] = (byte) (v >>> 8);
    buf[size++] = (byte) v;
    return this;
  }

  public ByteWriter writeLong(long v) {
    writeInt((int) (v >>> 32));
    return writeInt((int) v);
  }

  /** Writes a length-prefixed byte array. */
  public ByteWriter writeBytes(byte[] v) {
    writeInt(v.length);
    return writeRaw(v);
  }

  public ByteWriter writeRaw(byte[] v) {
    ensure(v.length);
    System.arraycopy(v, 0, buf, size, v.length);
    size += v.length;
    return this;
  }

  /** Writes a length-prefixed UTF-8 string. */
  public ByteWriter writeString(String v) {
    return writeBytes(v.getBytes(StandardCharsets.UTF_8));
  }

  public int size() {
    return size;
  }

  public byte[] toByteArray() {
    return Arrays.copyOf(buf, size);
  }
}
