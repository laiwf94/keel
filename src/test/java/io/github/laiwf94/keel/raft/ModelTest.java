package io.github.laiwf94.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.codec.CodecException;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ModelTest {
  static Member m(String id, boolean voter) {
    return new Member(id, id + ":7000", id + ":8080", voter);
  }

  @Test
  void entryValidatesAndCompares() {
    Entry a = new Entry(1, 2, EntryType.COMMAND, new byte[] {1});
    assertEquals(a, new Entry(1, 2, EntryType.COMMAND, new byte[] {1}));
    assertEquals(a.hashCode(), new Entry(1, 2, EntryType.COMMAND, new byte[] {1}).hashCode());
    assertNotEquals(a, new Entry(1, 2, EntryType.COMMAND, new byte[] {2}));
    assertNotEquals(a, new Entry(1, 3, EntryType.COMMAND, new byte[] {1}));
    assertNotEquals(a, new Entry(2, 2, EntryType.COMMAND, new byte[] {1}));
    assertNotEquals(a, new Entry(1, 2, EntryType.NOOP, new byte[] {1}));
    assertNotEquals(a, (Object) "x");
    assertEquals(33, a.size());
    assertTrue(a.toString().contains("1@2"));
    assertThrows(
        IllegalArgumentException.class, () -> new Entry(0, 1, EntryType.NOOP, new byte[0]));
    assertThrows(
        IllegalArgumentException.class, () -> new Entry(1, -1, EntryType.NOOP, new byte[0]));
  }

  @Test
  void entryTypeCodes() {
    assertEquals(EntryType.CONFIG, EntryType.fromCode(2));
    assertThrows(CodecException.class, () -> EntryType.fromCode(3));
    assertThrows(CodecException.class, () -> EntryType.fromCode(-1));
  }

  @Test
  void memberValidation() {
    assertThrows(IllegalArgumentException.class, () -> m("", true));
    assertThrows(IllegalArgumentException.class, () -> m("has space", true));
    assertThrows(IllegalArgumentException.class, () -> Member.validateId(null));
    assertThrows(IllegalArgumentException.class, () -> new Member("a", "x y", "h:1", true));
    assertThrows(IllegalArgumentException.class, () -> new Member("a", "h:1", "", true));
    assertTrue(m("a", false).asVoter().voter());
    new Member("ipv6", "[::1]:7000", "[::1]:8080", true);
  }

  @Test
  void configurationRules() {
    Configuration c = new Configuration(List.of(m("c", true), m("a", true), m("b", false)));
    assertEquals(List.of("a", "b", "c"), c.ids());
    assertEquals(List.of("a", "c"), c.voterIds());
    assertEquals(2, c.quorum());
    assertTrue(c.isVoter("a"));
    assertFalse(c.isVoter("b"));
    assertFalse(c.isVoter("zz"));
    assertTrue(c.contains("b"));
    assertTrue(c.hasQuorum(Set.of("a", "c")));
    assertFalse(c.hasQuorum(Set.of("a", "b")));
    assertFalse(c.isEmpty());
    assertTrue(Configuration.EMPTY.isEmpty());

    Configuration promoted = c.withPromoted("b");
    assertEquals(List.of("a", "b", "c"), promoted.voterIds());
    assertEquals(2, promoted.quorum());
    assertThrows(IllegalArgumentException.class, () -> promoted.withPromoted("b"));
    assertThrows(IllegalArgumentException.class, () -> c.withPromoted("zz"));
    Configuration added = c.withLearner(m("d", true));
    assertFalse(added.isVoter("d"));
    assertThrows(IllegalArgumentException.class, () -> c.withLearner(m("a", false)));
    assertEquals(List.of("a", "c"), c.without("b").ids());
    assertThrows(IllegalArgumentException.class, () -> c.without("zz"));
    assertEquals(List.of("a", "b", "c"), Configuration.ofVoters(c.members()).voterIds());
  }

  @Test
  void configurationRejectsImpossibleShapes() {
    assertThrows(IllegalArgumentException.class, () -> new Configuration(List.of(m("a", false))));
    assertThrows(
        IllegalArgumentException.class,
        () -> new Configuration(List.of(m("a", true), m("a", false))));
    List<Member> many =
        java.util.stream.IntStream.range(0, 65).mapToObj(i -> m("n" + i, true)).toList();
    assertThrows(IllegalArgumentException.class, () -> new Configuration(many));
  }

  @Test
  void configurationCodec() {
    Configuration c = new Configuration(List.of(m("a", true), m("b", false)));
    assertEquals(c, Configuration.decode(c.encode()));
    assertEquals(Configuration.EMPTY, Configuration.decode(Configuration.EMPTY.encode()));
    byte[] trailing = new ByteWriter().writeRaw(c.encode()).writeByte(0).toByteArray();
    assertThrows(CodecException.class, () -> Configuration.decode(trailing));
    byte[] badMember =
        new ByteWriter()
            .writeInt(1)
            .writeString("bad id")
            .writeString("h:1")
            .writeString("h:2")
            .writeBoolean(true)
            .toByteArray();
    assertThrows(CodecException.class, () -> Configuration.decode(badMember));
    byte[] noVoter =
        new ByteWriter()
            .writeInt(1)
            .writeString("a")
            .writeString("h:1")
            .writeString("h:2")
            .writeBoolean(false)
            .toByteArray();
    assertThrows(CodecException.class, () -> Configuration.decode(noVoter));
  }

  @Test
  void hardStateAndSnapshotMeta() {
    assertEquals(0, HardState.INITIAL.term());
    assertEquals("a", new HardState(3, "a").votedFor());
    assertThrows(IllegalArgumentException.class, () -> new HardState(-1, ""));
    assertThrows(IllegalArgumentException.class, () -> new HardState(1, "bad id"));
    assertThrows(
        IllegalArgumentException.class, () -> new SnapshotMeta(-1, 0, Configuration.EMPTY));
    assertThrows(IllegalArgumentException.class, () -> new SnapshotMeta(0, 1, Configuration.EMPTY));
    Snapshot s = new Snapshot(new SnapshotMeta(5, 2, Configuration.EMPTY), new byte[] {9});
    assertEquals(s, new Snapshot(new SnapshotMeta(5, 2, Configuration.EMPTY), new byte[] {9}));
    assertEquals(
        s.hashCode(),
        new Snapshot(new SnapshotMeta(5, 2, Configuration.EMPTY), new byte[] {9}).hashCode());
    assertNotEquals(s, new Snapshot(new SnapshotMeta(5, 2, Configuration.EMPTY), new byte[0]));
    assertNotEquals(s, (Object) "x");
    assertEquals(1, s.data().length);
    assertTrue(s.toString().contains("5@2"));
  }

  @Test
  void messagesValidate() {
    Entry e1 = new Entry(4, 1, EntryType.NOOP, new byte[0]);
    Entry e2 = new Entry(5, 1, EntryType.NOOP, new byte[0]);
    var ok = new Message.AppendRequest("a", "b", 1, 3, 1, List.of(e1, e2), 3, 0);
    assertEquals(2, ok.entries().size());
    assertThrows(
        IllegalArgumentException.class,
        () -> new Message.AppendRequest("a", "b", 1, 2, 1, List.of(e1), 3, 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> new Message.AppendRequest("a", "b", 1, -1, 1, List.of(), 3, 0));
    assertThrows(
        IllegalArgumentException.class, () -> new Message.VoteRequest("a", "b", -1, 0, 0, false));
    assertThrows(
        IllegalArgumentException.class,
        () -> new Message.VoteResponse("bad id", "b", 1, true, false));
    assertThrows(
        IllegalArgumentException.class,
        () -> new Message.AppendResponse("a", "b", 1, true, -1, 0, 0, 0, 0));
    assertThrows(
        NullPointerException.class, () -> new Message.SnapshotRequest("a", "b", 1, null, 0));
    assertThrows(
        IllegalArgumentException.class, () -> new Message.SnapshotResponse("a", "b", 1, 0, -1));
  }
}
