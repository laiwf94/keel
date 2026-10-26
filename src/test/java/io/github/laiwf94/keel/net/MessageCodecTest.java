package io.github.laiwf94.keel.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.codec.CodecException;
import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.Entry;
import io.github.laiwf94.keel.raft.EntryType;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.Message;
import io.github.laiwf94.keel.raft.Snapshot;
import io.github.laiwf94.keel.raft.SnapshotMeta;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class MessageCodecTest {
  static final Configuration CONFIG =
      new Configuration(
          List.of(new Member("a", "a:1", "a:2", true), new Member("b", "b:1", "b:2", false)));

  static List<Message> samples() {
    return List.of(
        new Message.VoteRequest("a", "b", 3, 10, 2, true),
        new Message.VoteResponse("b", "a", 3, true, false),
        new Message.AppendRequest(
            "a",
            "b",
            3,
            4,
            2,
            List.of(
                new Entry(5, 3, EntryType.COMMAND, new byte[] {1, 2}),
                new Entry(6, 3, EntryType.CONFIG, CONFIG.encode())),
            4,
            9),
        new Message.AppendRequest("a", "b", 3, 4, 2, List.of(), 4, 9),
        new Message.AppendResponse("b", "a", 3, false, 0, 4, 2, 1, 9),
        new Message.SnapshotRequest(
            "a", "b", 3, new Snapshot(new SnapshotMeta(7, 2, CONFIG), new byte[] {5, 6, 7}), 4),
        new Message.SnapshotResponse("b", "a", 3, 7, 4));
  }

  @Test
  void everyMessageRoundTrips() {
    for (Message m : samples()) {
      assertEquals(m, MessageCodec.decode(MessageCodec.encode(m)));
    }
  }

  @Test
  void everyTruncationIsRejected() {
    for (Message m : samples()) {
      byte[] b = MessageCodec.encode(m);
      for (int cut = 0; cut < b.length; cut++) {
        byte[] t = Arrays.copyOf(b, cut);
        assertThrows(CodecException.class, () -> MessageCodec.decode(t), m + " cut at " + cut);
      }
      byte[] extra = Arrays.copyOf(b, b.length + 1);
      assertThrows(CodecException.class, () -> MessageCodec.decode(extra));
    }
  }

  @Test
  void randomBytesOnlyEverFailAsCodecErrors() {
    Random r = new Random(1);
    for (int i = 0; i < 20_000; i++) {
      byte[] b = new byte[r.nextInt(200)];
      r.nextBytes(b);
      if (b.length > 0) {
        b[0] = (byte) (1 + r.nextInt(6));
      }
      try {
        MessageCodec.decode(b);
      } catch (CodecException expected) {
        // the only acceptable failure
      }
    }
    // Flipping single bytes of valid messages must also fail cleanly or decode to something valid.
    for (Message m : samples()) {
      byte[] b = MessageCodec.encode(m);
      for (int i = 0; i < b.length; i++) {
        byte[] f = b.clone();
        f[i] ^= (byte) 0xff;
        try {
          MessageCodec.decode(f);
        } catch (CodecException expected) {
          // fine
        }
      }
    }
  }

  @Test
  void invalidFieldsAreCodecErrors() {
    byte[] unknownType =
        new ByteWriter().writeByte(99).writeString("a").writeString("b").writeLong(1).toByteArray();
    assertThrows(CodecException.class, () -> MessageCodec.decode(unknownType));
    byte[] negativeTerm =
        new ByteWriter()
            .writeByte(2)
            .writeString("a")
            .writeString("b")
            .writeLong(-1)
            .writeBoolean(true)
            .writeBoolean(false)
            .toByteArray();
    assertThrows(CodecException.class, () -> MessageCodec.decode(negativeTerm));
    byte[] tooMany =
        new ByteWriter()
            .writeByte(3)
            .writeString("a")
            .writeString("b")
            .writeLong(1)
            .writeLong(0)
            .writeLong(0)
            .writeLong(0)
            .writeLong(0)
            .writeInt(MessageCodec.MAX_ENTRIES + 1)
            .toByteArray();
    assertThrows(CodecException.class, () -> MessageCodec.decode(tooMany));
    byte[] badEntry =
        new ByteWriter()
            .writeByte(3)
            .writeString("a")
            .writeString("b")
            .writeLong(1)
            .writeLong(0)
            .writeLong(0)
            .writeLong(0)
            .writeLong(0)
            .writeInt(1)
            .writeLong(0)
            .writeLong(1)
            .writeByte(1)
            .writeBytes(new byte[0])
            .toByteArray();
    assertThrows(CodecException.class, () -> MessageCodec.decode(badEntry));
    byte[] badSnapshot =
        new ByteWriter()
            .writeByte(5)
            .writeString("a")
            .writeString("b")
            .writeLong(1)
            .writeLong(0)
            .writeLong(0)
            .writeLong(3)
            .writeInt(0)
            .writeBytes(new byte[0])
            .toByteArray();
    assertThrows(CodecException.class, () -> MessageCodec.decode(badSnapshot));
  }
}
