package io.github.laiwf94.keel.codec;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Reads values written by {@link ByteWriter} from a stream, with the same checks as {@link
 * ByteReader}. The stream's length is not known in advance, so a length field is checked against
 * the caller's maximum and a large value is read into a buffer that starts at 64 KiB and doubles
 * only once the bytes before it have arrived: a hostile length makes the reader allocate at most
 * twice what the input holds, plus 64 KiB. Truncated or malformed input throws {@link
 * CodecException}; an {@link IOException} means the stream itself failed.
 */
public final class StreamReader {
  private static final int STEP = 64 << 10;

  private final InputStream in;
  private final byte[] scratch = new byte[8];

  /** Reads from {@code in}, which should be buffered; this class reads a few bytes at a time. */
  public StreamReader(InputStream in) {
    this.in = in;
  }

  private void readFully(byte[] b, int off, int len) throws IOException {
    while (len > 0) {
      int n = in.read(b, off, len);
      if (n < 0) {
        throw new CodecException("truncated input: " + len + " more bytes expected");
      }
      off += n;
      len -= n;
    }
  }

  public int readByte() throws IOException {
    int b = in.read();
    if (b < 0) {
      throw new CodecException("truncated input: 1 more byte expected");
    }
    return b;
  }

  public boolean readBoolean() throws IOException {
    int b = readByte();
    if (b > 1) {
      throw new CodecException("bad boolean " + b);
    }
    return b == 1;
  }

  public int readInt() throws IOException {
    readFully(scratch, 0, 4);
    return ByteBuffer.wrap(scratch, 0, 4).getInt();
  }

  public long readLong() throws IOException {
    readFully(scratch, 0, 8);
    return ByteBuffer.wrap(scratch, 0, 8).getLong();
  }

  /** Reads a long that must be zero or positive. */
  public long readNonNegativeLong() throws IOException {
    long v = readLong();
    if (v < 0) {
      throw new CodecException("negative value " + v);
    }
    return v;
  }

  /** Reads a count that must lie in {@code [0, max]}. */
  public int readCount(int max) throws IOException {
    int n = readInt();
    if (n < 0 || n > max) {
      throw new CodecException("count " + n + " outside [0, " + max + "]");
    }
    return n;
  }

  public byte[] readBytes(int max) throws IOException {
    int n = readCount(max);
    byte[] out = new byte[Math.min(n, STEP)];
    int have = 0;
    while (have < n) {
      if (have == out.length) {
        out = Arrays.copyOf(out, (int) Math.min(n, 2L * out.length));
      }
      int len = out.length - have;
      readFully(out, have, len);
      have += len;
    }
    return out;
  }

  public String readString(int maxBytes) throws IOException {
    byte[] raw = readBytes(maxBytes);
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(raw))
          .toString();
    } catch (CharacterCodingException e) {
      throw new CodecException("invalid UTF-8");
    }
  }

  /** Fails if the stream has any bytes left, so trailing garbage is never silently accepted. */
  public void expectEnd() throws IOException {
    if (in.read() >= 0) {
      throw new CodecException("unexpected trailing bytes");
    }
  }
}
