package io.github.laiwf94.keel.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ByteCodecTest {
  @Test
  void roundTripsEveryType() {
    ByteWriter w = new ByteWriter(1);
    w.writeByte(7)
        .writeBoolean(true)
        .writeBoolean(false)
        .writeInt(-5)
        .writeLong(Long.MIN_VALUE + 3)
        .writeBytes(new byte[] {1, 2, 3})
        .writeString("héllo")
        .writeLong(42);
    for (int i = 0; i < 100; i++) {
      w.writeInt(i);
    }
    ByteReader r = new ByteReader(w.toByteArray());
    assertEquals(7, r.readByte());
    assertTrue(r.readBoolean());
    assertFalse(r.readBoolean());
    assertEquals(-5, r.readInt());
    assertEquals(Long.MIN_VALUE + 3, r.readLong());
    assertArrayEquals(new byte[] {1, 2, 3}, r.readBytes(10));
    assertEquals("héllo", r.readString(10));
    assertEquals(42, r.readNonNegativeLong());
    for (int i = 0; i < 100; i++) {
      assertEquals(i, r.readInt());
    }
    assertEquals(0, r.remaining());
    r.expectEnd();
  }

  @Test
  void rejectsTruncatedInput() {
    byte[] b = new ByteWriter().writeLong(1).toByteArray();
    ByteReader r = new ByteReader(b, 0, 5);
    assertThrows(CodecException.class, r::readLong);
  }

  @Test
  void rejectsHostileLengthsBeforeAllocating() {
    byte[] b = new ByteWriter().writeInt(Integer.MAX_VALUE).toByteArray();
    assertThrows(CodecException.class, () -> new ByteReader(b).readBytes(Integer.MAX_VALUE));
    assertThrows(CodecException.class, () -> new ByteReader(b).readBytes(100));
    byte[] neg = new ByteWriter().writeInt(-1).toByteArray();
    assertThrows(CodecException.class, () -> new ByteReader(neg).readCount(10));
  }

  @Test
  void rejectsBadBooleansNegativeLongsAndTrailingBytes() {
    assertThrows(CodecException.class, () -> new ByteReader(new byte[] {2}).readBoolean());
    byte[] neg = new ByteWriter().writeLong(-1).toByteArray();
    assertThrows(CodecException.class, () -> new ByteReader(neg).readNonNegativeLong());
    ByteReader r = new ByteReader(new byte[] {1, 2});
    r.readByte();
    assertThrows(CodecException.class, r::expectEnd);
  }

  @Test
  void rejectsInvalidUtf8() {
    byte[] b = new ByteWriter().writeBytes(new byte[] {(byte) 0xc3, (byte) 0x28}).toByteArray();
    assertThrows(CodecException.class, () -> new ByteReader(b).readString(10));
  }

  @Test
  void rejectsBadSlices() {
    assertThrows(IllegalArgumentException.class, () -> new ByteReader(new byte[2], 1, 2));
    assertThrows(IllegalArgumentException.class, () -> new ByteReader(new byte[2], -1, 1));
    assertThrows(IllegalArgumentException.class, () -> new ByteReader(new byte[2], 0, -1));
  }

  @Test
  void codecExceptionKeepsMessage() {
    assertEquals("x", new CodecException("x").getMessage());
  }
}
