package io.github.laiwf94.keel.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import org.junit.jupiter.api.Test;

class StreamReaderTest {
  static StreamReader reader(byte[] b) {
    return new StreamReader(new ByteArrayInputStream(b));
  }

  /** A stream that hands out one byte per read, the worst case for a reader's loops. */
  static InputStream trickle(byte[] b) {
    return new ByteArrayInputStream(b) {
      @Override
      public synchronized int read(byte[] out, int off, int len) {
        return super.read(out, off, Math.min(len, 1));
      }
    };
  }

  @Test
  void readsWhatByteWriterWrites() throws IOException {
    byte[] big = new byte[300_000];
    new Random(1).nextBytes(big);
    ByteWriter w = new ByteWriter();
    w.writeByte(200).writeBoolean(true).writeBoolean(false).writeInt(-7).writeLong(Long.MIN_VALUE);
    w.writeLong(42).writeInt(3).writeBytes(big).writeString("héllo");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    w.drainTo(out);
    assertEquals(0, w.size());
    for (InputStream in :
        new InputStream[] {
          new ByteArrayInputStream(out.toByteArray()), trickle(out.toByteArray())
        }) {
      StreamReader r = new StreamReader(in);
      assertEquals(200, r.readByte());
      assertTrue(r.readBoolean());
      assertFalse(r.readBoolean());
      assertEquals(-7, r.readInt());
      assertEquals(Long.MIN_VALUE, r.readLong());
      assertEquals(42, r.readNonNegativeLong());
      assertEquals(3, r.readCount(3));
      assertArrayEquals(big, r.readBytes(big.length));
      assertEquals("héllo", r.readString(16));
      r.expectEnd();
    }
  }

  @Test
  void rejectsBadValuesTruncationAndTrailingBytes() {
    assertThrows(CodecException.class, () -> reader(new byte[] {2}).readBoolean());
    assertThrows(CodecException.class, () -> reader(new byte[0]).readByte());
    assertThrows(CodecException.class, () -> reader(new byte[3]).readInt());
    byte[] negative = new ByteWriter().writeLong(-1).toByteArray();
    assertThrows(CodecException.class, () -> reader(negative).readNonNegativeLong());
    byte[] five = new ByteWriter().writeInt(5).toByteArray();
    assertThrows(CodecException.class, () -> reader(five).readCount(4));
    byte[] minus = new ByteWriter().writeInt(-1).toByteArray();
    assertThrows(CodecException.class, () -> reader(minus).readCount(4));
    assertThrows(CodecException.class, () -> reader(new byte[1]).expectEnd());
    byte[] badUtf8 = new ByteWriter().writeInt(1).writeByte(0xff).toByteArray();
    assertThrows(CodecException.class, () -> reader(badUtf8).readString(4));
  }

  @Test
  void aHostileLengthAllocatesInStepsAndFailsWhenTheInputEnds() {
    // Claims 100 MiB, holds 10 bytes: the reader fails without allocating anywhere near that.
    byte[] lie = new ByteWriter().writeInt(100 << 20).writeRaw(new byte[10]).toByteArray();
    CodecException e = assertThrows(CodecException.class, () -> reader(lie).readBytes(200 << 20));
    assertTrue(e.getMessage().contains("truncated"), e.getMessage());
    // Claims 1 MiB, holds 200 KiB: the buffer doubles past the first step before the input ends.
    byte[] partial = new ByteWriter().writeInt(1 << 20).writeRaw(new byte[200 << 10]).toByteArray();
    assertThrows(CodecException.class, () -> reader(partial).readBytes(2 << 20));
  }
}
