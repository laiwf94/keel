package io.github.laiwf94.keel.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.Entry;
import io.github.laiwf94.keel.raft.EntryType;
import io.github.laiwf94.keel.raft.HardState;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.Snapshot;
import io.github.laiwf94.keel.raft.SnapshotMeta;
import io.github.laiwf94.keel.raft.Storage;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileStorageTest {
  @TempDir Path tmp;

  static final Configuration CONFIG =
      Configuration.ofVoters(List.of(new Member("a", "a:1", "a:2", true)));

  static Entry e(long index, long term) {
    return new Entry(
        index,
        term,
        EntryType.COMMAND,
        ("value-" + index).getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  static List<Entry> entries(long from, long to, long term) {
    List<Entry> out = new ArrayList<>();
    for (long i = from; i <= to; i++) {
      out.add(e(i, term));
    }
    return out;
  }

  static Storage.State reopen(Path dir) {
    try (FileStorage s = new FileStorage(dir)) {
      return s.load();
    }
  }

  @Test
  void entriesAndHardStateSurviveARestart() {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      Storage.State empty = s.load();
      assertEquals(HardState.INITIAL, empty.hardState());
      assertTrue(empty.entries().isEmpty());
      assertTrue(empty.snapshot().isEmpty());
      s.append(entries(1, 10, 1));
      s.saveHardState(new HardState(3, "b"));
      s.sync();
      s.sync();
      assertEquals(3, s.fsyncs());
      assertThrows(IllegalStateException.class, s::load);
    }
    Storage.State st = reopen(dir);
    assertEquals(new HardState(3, "b"), st.hardState());
    assertEquals(entries(1, 10, 1), st.entries());
  }

  @Test
  void conflictingSuffixIsReplaced() {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.append(entries(1, 10, 1));
      s.append(entries(6, 8, 2));
      s.append(entries(9, 9, 2));
      s.sync();
      assertThrows(IllegalStateException.class, () -> s.append(entries(20, 20, 2)));
    }
    List<Entry> expect = new ArrayList<>(entries(1, 5, 1));
    expect.addAll(entries(6, 9, 2));
    assertEquals(expect, reopen(dir).entries());
  }

  @Test
  void unsyncedHardStateIsNotWritten() {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.saveHardState(new HardState(1, "a"));
      s.sync();
      s.saveHardState(new HardState(2, ""));
    }
    assertEquals(new HardState(1, "a"), reopen(dir).hardState());
  }

  @Test
  void snapshotsReplaceTheLogPrefix() {
    Path dir = tmp.resolve("n");
    Snapshot snap = new Snapshot(new SnapshotMeta(6, 1, CONFIG), new byte[] {1, 2, 3});
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.append(entries(1, 10, 1));
      s.sync();
      s.saveSnapshot(snap, entries(7, 10, 1));
      s.append(entries(11, 12, 1));
      s.append(entries(8, 8, 2));
      s.sync();
      assertThrows(IllegalStateException.class, () -> s.append(entries(5, 5, 2)));
    }
    Storage.State st = reopen(dir);
    assertEquals(snap, st.snapshot().orElseThrow());
    List<Entry> expect = new ArrayList<>(entries(7, 7, 1));
    expect.add(e(8, 2));
    assertEquals(expect, st.entries());

    Path empty = tmp.resolve("e");
    try (FileStorage s = new FileStorage(empty)) {
      s.load();
      s.saveSnapshot(snap, List.of());
      s.append(entries(7, 7, 1));
      s.sync();
    }
    assertEquals(entries(7, 7, 1), reopen(empty).entries());
  }

  @Test
  void crashBetweenSnapshotAndLogRewriteIsRepaired() throws IOException {
    // The log holds entries the snapshot conflicts with: the installed snapshot wins.
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.append(entries(1, 5, 1));
      s.sync();
    }
    Path other = tmp.resolve("o");
    try (FileStorage s = new FileStorage(other)) {
      s.load();
      s.saveSnapshot(new Snapshot(new SnapshotMeta(3, 2, CONFIG), new byte[0]), List.of());
    }
    Files.copy(other.resolve("snapshot"), dir.resolve("snapshot"));
    assertTrue(reopen(dir).entries().isEmpty());
    assertTrue(reopen(dir).entries().isEmpty());

    // The log still holds the snapshot's last entry with the same term: keep what follows.
    Path keep = tmp.resolve("k");
    try (FileStorage s = new FileStorage(keep)) {
      s.load();
      s.append(entries(1, 5, 1));
      s.sync();
    }
    Path other2 = tmp.resolve("o2");
    try (FileStorage s = new FileStorage(other2)) {
      s.load();
      s.saveSnapshot(new Snapshot(new SnapshotMeta(3, 1, CONFIG), new byte[0]), List.of());
    }
    Files.copy(other2.resolve("snapshot"), keep.resolve("snapshot"));
    assertEquals(entries(4, 5, 1), reopen(keep).entries());
  }

  /** Cuts the last record at every byte; recovery must keep exactly the records before it. */
  @Test
  void tornTailIsCutAtEveryOffset() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.append(entries(1, 4, 1));
      s.sync();
    }
    long before = Files.size(dir.resolve("log"));
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.append(entries(5, 5, 1));
      s.sync();
    }
    long after = Files.size(dir.resolve("log"));
    byte[] full = Files.readAllBytes(dir.resolve("log"));
    for (long cut = before; cut < after; cut++) {
      Path copy = tmp.resolve("cut" + cut);
      Files.createDirectories(copy);
      Files.write(copy.resolve("log"), java.util.Arrays.copyOf(full, (int) cut));
      try (FileStorage s = new FileStorage(copy)) {
        assertEquals(entries(1, 4, 1), s.load().entries(), "cut at " + cut);
        assertEquals(cut - before, s.tornBytesDropped());
        // The repaired log accepts the next append where the torn one was.
        s.append(entries(5, 6, 1));
        s.sync();
      }
      assertEquals(entries(1, 6, 1), reopen(copy).entries());
    }
  }

  @Test
  void zeroFilledTailIsTreatedAsTorn() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.append(entries(1, 3, 1));
      s.sync();
    }
    Files.write(dir.resolve("log"), new byte[4096], StandardOpenOption.APPEND);
    assertEquals(entries(1, 3, 1), reopen(dir).entries());
  }

  @Test
  void corruptionFollowedByValidRecordsStopsStartup() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.append(entries(1, 5, 1));
      s.sync();
    }
    byte[] full = Files.readAllBytes(dir.resolve("log"));
    int mid = full.length / 2;
    try (RandomAccessFile f = new RandomAccessFile(dir.resolve("log").toFile(), "rw")) {
      f.seek(mid);
      f.write(full[mid] ^ 0x40);
    }
    StorageException ex = assertThrows(StorageException.class, () -> new FileStorage(dir));
    assertTrue(ex.getMessage().contains("valid record follows"), ex.getMessage());
  }

  @Test
  void discontinuousRecordsStopStartup() throws IOException {
    Path dir = tmp.resolve("n");
    Files.createDirectories(dir);
    ByteWriter w = new ByteWriter();
    for (Entry x : List.of(e(1, 1), e(3, 1))) {
      ByteWriter p = new ByteWriter();
      io.github.laiwf94.keel.raft.Codec.writeEntry(p, x);
      byte[] payload = p.toByteArray();
      CRC32C c = new CRC32C();
      c.update(payload);
      w.writeInt(payload.length).writeInt((int) c.getValue()).writeRaw(payload);
    }
    Files.write(dir.resolve("log"), w.toByteArray());
    assertThrows(StorageException.class, () -> new FileStorage(dir));
  }

  @Test
  void corruptStateOrSnapshotFilesStopStartup() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.saveHardState(new HardState(2, "a"));
      s.saveSnapshot(new Snapshot(new SnapshotMeta(1, 1, CONFIG), new byte[] {9}), List.of());
      s.sync();
    }
    for (String name : List.of("state", "snapshot")) {
      Path copy = tmp.resolve("c-" + name);
      Files.createDirectories(copy);
      try (Stream<Path> files = Files.list(dir)) {
        for (Path f : files.toList()) {
          Files.copy(f, copy.resolve(f.getFileName()));
        }
      }
      byte[] b = Files.readAllBytes(copy.resolve(name));
      b[b.length - 1] ^= 1;
      Files.write(copy.resolve(name), b);
      assertThrows(StorageException.class, () -> new FileStorage(copy), name);
      Files.write(copy.resolve(name), java.util.Arrays.copyOf(b, b.length - 1));
      assertThrows(StorageException.class, () -> new FileStorage(copy), name + " truncated");
      Files.write(copy.resolve(name), new byte[3]);
      assertThrows(StorageException.class, () -> new FileStorage(copy), name + " tiny");
    }
    // A state file whose checksum matches but whose content is invalid.
    Path bad = tmp.resolve("bad");
    Files.createDirectories(bad);
    byte[] payload = new ByteWriter().writeLong(-4).writeString("").toByteArray();
    CRC32C c = new CRC32C();
    c.update(payload);
    Files.write(
        bad.resolve("state"),
        new ByteWriter()
            .writeInt(payload.length)
            .writeInt((int) c.getValue())
            .writeRaw(payload)
            .toByteArray());
    assertThrows(StorageException.class, () -> new FileStorage(bad));
  }

  @Test
  void aDirectoryIsUsedByOneProcessAtATime() {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      StorageException ex = assertThrows(StorageException.class, () -> new FileStorage(dir));
      assertTrue(ex.getMessage().contains("in use"));
      s.load();
    }
    reopen(dir);
  }

  @Test
  void unwritableDirectoryFailsClearly() throws IOException {
    Path file = tmp.resolve("file");
    Files.write(file, new byte[] {1});
    assertThrows(StorageException.class, () -> new FileStorage(file.resolve("sub")));
  }

  @Test
  void largeEntriesRoundTrip() {
    Path dir = tmp.resolve("n");
    byte[] big = new byte[2 << 20];
    big[12345] = 7;
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.append(List.of(new Entry(1, 1, EntryType.COMMAND, big)));
      s.sync();
    }
    assertArrayEquals(big, reopen(dir).entries().get(0).data());
  }

  @Test
  void raftNodeRecoversFromFileStorage() {
    Path dir = tmp.resolve("n");
    long term;
    long last;
    try (FileStorage s = new FileStorage(dir)) {
      var node =
          new io.github.laiwf94.keel.raft.RaftNode(
              "a",
              CONFIG,
              io.github.laiwf94.keel.raft.RaftOptions.DEFAULTS,
              s,
              new java.util.Random(1));
      while (node.role() != io.github.laiwf94.keel.raft.RaftNode.Role.LEADER) {
        node.tick();
      }
      for (int i = 0; i < 5; i++) {
        node.propose(new byte[] {(byte) i});
      }
      node.ready();
      s.sync();
      node.markDurable();
      node.ready();
      node.compact(3, new byte[] {42});
      term = node.term();
      last = node.log().lastIndex();
    }
    try (FileStorage s = new FileStorage(dir)) {
      var node =
          new io.github.laiwf94.keel.raft.RaftNode(
              "a",
              CONFIG,
              io.github.laiwf94.keel.raft.RaftOptions.DEFAULTS,
              s,
              new java.util.Random(1));
      assertEquals(term, node.term());
      assertEquals(last, node.log().lastIndex());
      assertEquals(3, node.log().snapshot().index());
      assertArrayEquals(new byte[] {42}, node.ready().snapshot().orElseThrow().data());
    }
  }
}
