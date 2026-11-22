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
import java.util.Arrays;
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
      // Creating the log directory.
      assertEquals(1, s.fsyncs());
      s.append(entries(1, 10, 1));
      s.saveHardState(new HardState(3, "b"));
      s.sync();
      s.sync();
      // The new segment's header and directory, the state file and its directory, the segment.
      assertEquals(6, s.fsyncs());
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

  /** Segment size used where a test wants many segments: two of the small entries fit in one. */
  static final long SMALL = 100;

  static List<Path> segments(Path dir) throws IOException {
    try (Stream<Path> files = Files.list(dir.resolve("wal"))) {
      return files.filter(f -> f.getFileName().toString().endsWith(".seg")).sorted().toList();
    }
  }

  static Storage.State reopen(Path dir, long segmentBytes) {
    try (FileStorage s = new FileStorage(dir, segmentBytes)) {
      return s.load();
    }
  }

  static void copyTree(Path from, Path to) throws IOException {
    try (Stream<Path> walk = Files.walk(from)) {
      for (Path f : walk.toList()) {
        Path t = to.resolve(from.relativize(f).toString());
        if (Files.isDirectory(f)) {
          Files.createDirectories(t);
        } else {
          Files.copy(f, t);
        }
      }
    }
  }

  static byte[] records(List<Entry> es) {
    ByteWriter w = new ByteWriter();
    for (Entry x : es) {
      w.writeRaw(SegmentedLog.entryRecord(x));
    }
    return w.toByteArray();
  }

  static void flip(Path file, long at) throws IOException {
    try (RandomAccessFile f = new RandomAccessFile(file.toFile(), "rw")) {
      f.seek(at);
      int b = f.read();
      f.seek(at);
      f.write(b ^ 0x40);
    }
  }

  @Test
  void entriesRollIntoSegmentsOfTheGivenSize() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 5, 1));
      s.sync();
      s.append(entries(6, 9, 1));
      s.sync();
      assertEquals(5, s.segmentCount());
    }
    List<Path> segs = segments(dir);
    assertEquals(5, segs.size());
    assertEquals("00000000000000000001.seg", segs.get(0).getFileName().toString());
    assertEquals("00000000000000000009.seg", segs.get(4).getFileName().toString());
    for (Path f : segs.subList(0, 4)) {
      assertTrue(Files.size(f) <= SMALL, f + " is " + Files.size(f) + " bytes");
    }
    assertEquals(entries(1, 9, 1), reopen(dir, SMALL).entries());
    // The size applies to new segments; existing ones are read whatever their size.
    try (FileStorage s = new FileStorage(dir, 1 << 20)) {
      assertEquals(entries(1, 9, 1), s.load().entries());
      s.append(entries(10, 30, 1));
      s.sync();
      assertEquals(5, s.segmentCount());
    }
    assertEquals(entries(1, 30, 1), reopen(dir, SMALL).entries());
    assertThrows(StorageException.class, () -> new FileStorage(tmp.resolve("x"), 10));
    assertThrows(StorageException.class, () -> new FileStorage(tmp.resolve("y"), 2L << 30));
  }

  @Test
  void aNewSegmentIsDurableBeforeItTakesRecords() {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      long opened = s.fsyncs();
      s.append(entries(1, 2, 1));
      // Header forced and directory fsynced when the first segment starts.
      assertEquals(opened + 2, s.fsyncs());
      s.sync();
      assertEquals(opened + 3, s.fsyncs());
      s.append(entries(3, 3, 1));
      // The full segment was synced, so only the new one is created as above.
      assertEquals(opened + 5, s.fsyncs());
      s.append(entries(4, 5, 1));
      // Entry 5 starts a segment while entry 4 is not synced: that segment is forced first.
      assertEquals(opened + 8, s.fsyncs());
      s.sync();
      assertEquals(opened + 9, s.fsyncs());
      s.sync();
      assertEquals(opened + 9, s.fsyncs());
    }
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
    Path seg = segments(dir).get(0);
    long before = Files.size(seg);
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.append(entries(5, 5, 1));
      s.sync();
    }
    long after = Files.size(seg);
    byte[] full = Files.readAllBytes(seg);
    for (long cut = before; cut < after; cut++) {
      Path copy = tmp.resolve("cut" + cut);
      Files.createDirectories(copy.resolve("wal"));
      Files.write(copy.resolve("wal").resolve(seg.getFileName()), Arrays.copyOf(full, (int) cut));
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

  /**
   * Cuts the log at every byte from the last record of one segment to the end of the next, the
   * shapes a crash leaves while a segment fills and the next one starts: the earlier segment cut
   * with the later one not yet created, then the later one cut inside its header and inside its
   * records. Recovery must keep exactly the records before the cut.
   */
  @Test
  void aCutAtEveryByteOfASegmentBoundaryKeepsTheRecordsBeforeIt() throws IOException {
    Path dir = tmp.resolve("n");
    List<Entry> all = entries(1, 6, 1);
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(all);
      s.sync();
    }
    List<Path> segs = segments(dir);
    assertEquals(3, segs.size());
    byte[] a = Files.readAllBytes(segs.get(1));
    byte[] b = Files.readAllBytes(segs.get(2));
    // Where each record of the second and third segments ends, counted across both.
    long r3 = SegmentedLog.entryRecord(all.get(2)).length;
    long r4 = SegmentedLog.entryRecord(all.get(3)).length;
    long r5 = SegmentedLog.entryRecord(all.get(4)).length;
    long r6 = SegmentedLog.entryRecord(all.get(5)).length;
    long h = SegmentedLog.SEGMENT_HEADER;
    assertEquals(h + r3 + r4, a.length);
    assertEquals(h + r5 + r6, b.length);
    long[] ends = {h + r3, a.length, a.length + h + r5, a.length + b.length};
    for (long cut = h + r3; cut <= a.length + b.length; cut++) {
      Path copy = tmp.resolve("cut" + cut);
      Files.createDirectories(copy.resolve("wal"));
      Files.copy(segs.get(0), copy.resolve("wal").resolve(segs.get(0).getFileName()));
      Path wa = copy.resolve("wal").resolve(segs.get(1).getFileName());
      Path wb = copy.resolve("wal").resolve(segs.get(2).getFileName());
      if (cut <= a.length) {
        Files.write(wa, Arrays.copyOf(a, (int) cut));
      } else {
        Files.write(wa, a);
        Files.write(wb, Arrays.copyOf(b, (int) (cut - a.length)));
      }
      int kept = 2;
      long keptEnd = h;
      for (int i = 0; i < ends.length; i++) {
        if (ends[i] <= cut) {
          kept = 3 + i;
          keptEnd = ends[i];
        }
      }
      // A cut inside the header of the third segment drops the whole file; one after it leaves
      // the third segment empty.
      long torn;
      if (cut <= a.length) {
        torn = cut - keptEnd;
      } else if (cut - a.length < h) {
        torn = cut - a.length;
      } else {
        torn = cut - Math.max(keptEnd, a.length + h);
      }
      try (FileStorage s = new FileStorage(copy, SMALL)) {
        assertEquals(entries(1, kept, 1), s.load().entries(), "cut at " + cut);
        assertEquals(torn, s.tornBytesDropped(), "cut at " + cut);
        s.append(entries(kept + 1, 8, 2));
        s.sync();
      }
      List<Entry> expect = new ArrayList<>(entries(1, kept, 1));
      expect.addAll(entries(kept + 1, 8, 2));
      assertEquals(expect, reopen(copy, SMALL).entries(), "cut at " + cut);
    }
  }

  @Test
  void zeroFilledTailIsTreatedAsTorn() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 3, 1));
      s.sync();
    }
    Files.write(segments(dir).get(1), new byte[4096], StandardOpenOption.APPEND);
    assertEquals(entries(1, 3, 1), reopen(dir, SMALL).entries());
    // A zero-filled segment created by a crash during a roll is dropped.
    Files.write(dir.resolve("wal").resolve(SegmentedLog.name(4)), new byte[4096]);
    assertEquals(entries(1, 3, 1), reopen(dir, SMALL).entries());
    assertEquals(2, segments(dir).size());
  }

  @Test
  void corruptionFollowedByValidRecordsStopsStartup() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.append(entries(1, 5, 1));
      s.sync();
    }
    Path seg = segments(dir).get(0);
    flip(seg, Files.size(seg) / 2);
    StorageException ex = assertThrows(StorageException.class, () -> new FileStorage(dir));
    assertTrue(ex.getMessage().contains("valid record follows"), ex.getMessage());
  }

  /**
   * A segment is forced before the next one starts, so damage at the end of any segment but the
   * last is damage to durable data, not a torn write, even with no valid record after it in the
   * same file.
   */
  @Test
  void corruptionAtTheEndOfAnEarlierSegmentStopsStartup() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 6, 1));
      s.sync();
    }
    List<Path> segs = segments(dir);
    for (int i = 0; i < 2; i++) {
      for (long at : new long[] {Files.size(segs.get(i)) - 1, Files.size(segs.get(i)) - 20}) {
        Path copy = tmp.resolve("c" + i + "-" + at);
        copyTree(dir, copy);
        flip(segments(copy).get(i), at);
        StorageException ex =
            assertThrows(StorageException.class, () -> new FileStorage(copy, SMALL));
        assertTrue(ex.getMessage().contains("later segments follow"), ex.getMessage());
      }
    }
    // The same damage in the last segment is a torn tail.
    Path last = tmp.resolve("last");
    copyTree(dir, last);
    Path seg = segments(last).get(2);
    flip(seg, Files.size(seg) - 1);
    assertEquals(entries(1, 5, 1), reopen(last, SMALL).entries());
  }

  @Test
  void corruptSegmentHeadersStopStartup() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 6, 1));
      s.sync();
    }
    for (int i = 0; i < 3; i++) {
      for (int at : new int[] {0, 5, 9, 17}) {
        Path copy = tmp.resolve("h" + i + "-" + at);
        copyTree(dir, copy);
        flip(segments(copy).get(i), at);
        StorageException ex =
            assertThrows(StorageException.class, () -> new FileStorage(copy, SMALL));
        assertTrue(
            ex.getMessage().contains(i < 2 ? "later segments follow" : "valid record follows"),
            ex.getMessage());
      }
    }
    // A segment whose header names another first index than its file name.
    Path renamed = tmp.resolve("renamed");
    copyTree(dir, renamed);
    Path wal = renamed.resolve("wal");
    Files.move(wal.resolve(SegmentedLog.name(5)), wal.resolve(SegmentedLog.name(7)));
    assertThrows(StorageException.class, () -> new FileStorage(renamed, SMALL));
  }

  @Test
  void aMissingSegmentStopsStartup() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 6, 1));
      s.sync();
    }
    Files.delete(segments(dir).get(1));
    StorageException ex = assertThrows(StorageException.class, () -> new FileStorage(dir, SMALL));
    assertTrue(ex.getMessage().contains("does not follow"), ex.getMessage());
  }

  static void writeSegment(Path file, long first, List<Entry> es) throws IOException {
    Files.createDirectories(file.getParent());
    ByteWriter w = new ByteWriter();
    w.writeRaw(SegmentedLog.header(first)).writeRaw(records(es));
    Files.write(file, w.toByteArray());
  }

  @Test
  void discontinuousRecordsStopStartup() throws IOException {
    Path dir = tmp.resolve("n");
    Path wal = dir.resolve("wal");
    writeSegment(wal.resolve(SegmentedLog.name(1)), 1, List.of(e(1, 1), e(3, 1)));
    assertThrows(StorageException.class, () -> new FileStorage(dir));
    // The first record must carry the index in the header.
    writeSegment(wal.resolve(SegmentedLog.name(1)), 1, List.of(e(2, 1)));
    assertThrows(StorageException.class, () -> new FileStorage(dir));
    // Terms may not go down across a segment boundary.
    writeSegment(wal.resolve(SegmentedLog.name(1)), 1, List.of(e(1, 2)));
    writeSegment(wal.resolve(SegmentedLog.name(2)), 2, List.of(e(2, 1)));
    assertThrows(StorageException.class, () -> new FileStorage(dir));
    writeSegment(wal.resolve(SegmentedLog.name(2)), 2, List.of(e(2, 2)));
    assertEquals(List.of(e(1, 2), e(2, 2)), reopen(dir).entries());
  }

  @Test
  void aConflictingSuffixAcrossSegmentsIsReplaced() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 10, 1));
      s.sync();
      assertEquals(5, s.segmentCount());
      // Entry 5 starts the third segment: the later two are deleted and the third emptied.
      s.append(entries(5, 5, 2));
      assertEquals(3, s.segmentCount());
      s.sync();
      // Entry 4 is in the middle of the second segment.
      s.append(entries(4, 7, 3));
      s.sync();
      assertThrows(IllegalStateException.class, () -> s.append(entries(9, 9, 3)));
      assertThrows(IllegalStateException.class, () -> s.append(List.of(e(8, 3), e(10, 3))));
    }
    List<Entry> expect = new ArrayList<>(entries(1, 3, 1));
    expect.addAll(entries(4, 8, 3));
    assertEquals(expect, reopen(dir, SMALL).entries());
    assertEquals(4, segments(dir).size());
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      // Back to the first entry: one empty segment is left.
      s.append(entries(1, 1, 4));
      s.sync();
      assertEquals(1, s.segmentCount());
    }
    assertEquals(entries(1, 1, 4), reopen(dir, SMALL).entries());
  }

  static Object fileKey(Path f) throws IOException {
    return Files.readAttributes(f, java.nio.file.attribute.BasicFileAttributes.class).fileKey();
  }

  @Test
  void snapshotsDeleteWholeSegmentsWithoutRewritingTheRest() throws IOException {
    Path dir = tmp.resolve("n");
    Snapshot snap = new Snapshot(new SnapshotMeta(15, 1, CONFIG), new byte[] {4});
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 20, 1));
      s.sync();
      assertEquals(10, s.segmentCount());
      Path kept = dir.resolve("wal").resolve(SegmentedLog.name(17));
      Object key = fileKey(kept);
      byte[] bytes = Files.readAllBytes(kept);
      long before = s.fsyncs();
      // Segments 1..13 end at or below entry 15; segment 15 holds entry 16 and stays.
      s.saveSnapshot(snap, entries(16, 20, 1));
      assertEquals(3, s.segmentCount());
      assertEquals(7, s.segmentsCompacted());
      assertEquals(0, s.logRewrites());
      // The snapshot file and its directory, then a directory fsync per deleted segment.
      assertEquals(before + 2 + 7, s.fsyncs());
      assertEquals(key, fileKey(kept));
      assertArrayEquals(bytes, Files.readAllBytes(kept));
      // Entry 15 is still on disk in the first segment but is compacted.
      assertThrows(IllegalStateException.class, () -> s.append(entries(15, 15, 2)));
      s.append(entries(21, 22, 1));
      s.sync();
    }
    assertEquals(entries(16, 22, 1), reopen(dir, SMALL).entries());
    assertEquals(SegmentedLog.name(15), segments(dir).get(0).getFileName().toString());
    try (Stream<Path> files = Files.list(dir)) {
      assertEquals(
          List.of("lock", "snapshot", "wal"),
          files.map(f -> f.getFileName().toString()).sorted().toList());
    }
    // Reopening leaves the partly compacted first segment alone.
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      assertEquals(entries(16, 22, 1), s.load().entries());
      assertEquals(0, s.segmentsCompacted());
      assertEquals(0, s.logRewrites());
      // A later snapshot past the end of segment 15 deletes it.
      s.saveSnapshot(
          new Snapshot(new SnapshotMeta(18, 1, CONFIG), new byte[0]), entries(19, 22, 1));
      assertEquals(2, s.segmentsCompacted());
      // A snapshot at the same point again changes nothing.
      s.saveSnapshot(
          new Snapshot(new SnapshotMeta(18, 1, CONFIG), new byte[0]), entries(19, 22, 1));
      assertEquals(2, s.segmentsCompacted());
      assertEquals(0, s.logRewrites());
    }
    assertEquals(entries(19, 22, 1), reopen(dir, SMALL).entries());
    assertEquals(SegmentedLog.name(19), segments(dir).get(0).getFileName().toString());
  }

  @Test
  void aSnapshotOfTheWholeLogDeletesEverySegment() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 9, 1));
      s.sync();
      // Entry 9 is not synced when the snapshot covers it; nothing of it is needed.
      s.append(entries(10, 10, 1));
      s.saveSnapshot(new Snapshot(new SnapshotMeta(10, 1, CONFIG), new byte[0]), List.of());
      assertEquals(0, s.segmentCount());
      assertEquals(5, s.segmentsCompacted());
      assertEquals(0, s.logRewrites());
      assertTrue(segments(dir).isEmpty());
      assertThrows(IllegalStateException.class, () -> s.append(entries(12, 12, 1)));
      s.append(entries(11, 13, 2));
      s.sync();
    }
    assertEquals(entries(11, 13, 2), reopen(dir, SMALL).entries());
    assertEquals(SegmentedLog.name(11), segments(dir).get(0).getFileName().toString());
  }

  @Test
  void aSnapshotEndingASegmentDeletesThatSegmentToo() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 10, 1));
      s.sync();
      s.saveSnapshot(new Snapshot(new SnapshotMeta(6, 1, CONFIG), new byte[0]), entries(7, 10, 1));
      assertEquals(3, s.segmentsCompacted());
      assertEquals(2, s.segmentCount());
    }
    assertEquals(SegmentedLog.name(7), segments(dir).get(0).getFileName().toString());
    assertEquals(entries(7, 10, 1), reopen(dir, SMALL).entries());
  }

  @Test
  void aSuffixInsideThePartlyCompactedSegmentIsCut() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 10, 1));
      s.sync();
      // Segment 5 holds entries 5 and 6; 5 is compacted, 6 is not.
      s.saveSnapshot(new Snapshot(new SnapshotMeta(5, 1, CONFIG), new byte[0]), entries(6, 10, 1));
      assertEquals(3, s.segmentCount());
      // Replacing from entry 6 deletes the later segments and cuts segment 5 after entry 5.
      s.append(entries(6, 6, 2));
      assertEquals(1, s.segmentCount());
      s.sync();
      assertThrows(IllegalStateException.class, () -> s.append(entries(5, 5, 2)));
    }
    assertEquals(entries(6, 6, 2), reopen(dir, SMALL).entries());
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(7, 9, 2));
      s.sync();
    }
    assertEquals(entries(6, 9, 2), reopen(dir, SMALL).entries());
  }

  @Test
  void aCompactionInterruptedByACrashIsFinishedOnOpen() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 12, 1));
      s.sync();
    }
    Path snapped = tmp.resolve("s");
    try (FileStorage s = new FileStorage(snapped, SMALL)) {
      s.load();
      s.saveSnapshot(new Snapshot(new SnapshotMeta(9, 1, CONFIG), new byte[] {9}), List.of());
    }
    // The snapshot was durable and the crash came before, or between, the segment deletions.
    for (int deletedBeforeCrash = 0; deletedBeforeCrash <= 4; deletedBeforeCrash++) {
      Path crashed = tmp.resolve("c" + deletedBeforeCrash);
      copyTree(dir, crashed);
      Files.copy(snapped.resolve("snapshot"), crashed.resolve("snapshot"));
      for (int k = 0; k < deletedBeforeCrash; k++) {
        Files.delete(crashed.resolve("wal").resolve(SegmentedLog.name(1 + 2L * k)));
      }
      try (FileStorage s = new FileStorage(crashed, SMALL)) {
        Storage.State st = s.load();
        assertEquals(entries(10, 12, 1), st.entries());
        assertEquals(9, st.snapshot().orElseThrow().meta().index());
        assertEquals(4 - deletedBeforeCrash, s.segmentsCompacted());
        assertEquals(0, s.logRewrites());
      }
      assertEquals(SegmentedLog.name(9), segments(crashed).get(0).getFileName().toString());
      assertEquals(entries(10, 12, 1), reopen(crashed, SMALL).entries());
    }
  }

  @Test
  void aSnapshotTheLogDisagreesWithReplacesIt() throws IOException {
    // Installed from a leader whose entry 6 has another term: the whole log goes.
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 10, 1));
      s.sync();
      s.saveSnapshot(new Snapshot(new SnapshotMeta(6, 2, CONFIG), new byte[0]), List.of());
      assertEquals(1, s.logRewrites());
      assertEquals(0, s.segmentsCompacted());
      assertEquals(0, s.segmentCount());
      s.append(entries(7, 7, 2));
      s.sync();
    }
    assertEquals(entries(7, 7, 2), reopen(dir, SMALL).entries());

    // Past the end of the log.
    Path behind = tmp.resolve("b");
    try (FileStorage s = new FileStorage(behind, SMALL)) {
      s.load();
      s.append(entries(1, 4, 1));
      s.saveSnapshot(new Snapshot(new SnapshotMeta(30, 1, CONFIG), new byte[0]), List.of());
      assertEquals(1, s.logRewrites());
    }
    assertTrue(reopen(behind, SMALL).entries().isEmpty());

    // Remaining entries that do not match the log on disk, in length or at either end.
    Entry other7 = new Entry(7, 1, EntryType.COMMAND, new byte[] {7});
    Entry other10 = new Entry(10, 1, EntryType.COMMAND, new byte[] {10});
    List<List<Entry>> wrong =
        List.of(
            entries(7, 9, 1),
            entries(7, 11, 1),
            List.of(other7, e(8, 1), e(9, 1), e(10, 1)),
            List.of(e(7, 1), e(8, 1), e(9, 1), other10),
            List.of(e(7, 1), e(8, 1), e(9, 1), e(10, 2)));
    for (int i = 0; i < wrong.size(); i++) {
      Path d = tmp.resolve("w" + i);
      List<Entry> remaining = wrong.get(i);
      try (FileStorage s = new FileStorage(d, SMALL)) {
        s.load();
        s.append(entries(1, 10, 1));
        s.sync();
        s.saveSnapshot(new Snapshot(new SnapshotMeta(6, 1, CONFIG), new byte[0]), remaining);
        assertEquals(1, s.logRewrites(), "case " + i);
      }
      assertEquals(remaining, reopen(d, SMALL).entries(), "case " + i);
    }
  }

  @Test
  void anEntryThatNoLongerReadsBackStopsCompaction() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 10, 1));
      s.sync();
      // Damage the record of entry 6, the snapshot's last entry, at the start of segment 5.
      flip(dir.resolve("wal").resolve(SegmentedLog.name(5)), SegmentedLog.SEGMENT_HEADER + 50);
      Snapshot snap = new Snapshot(new SnapshotMeta(6, 1, CONFIG), new byte[0]);
      assertThrows(StorageException.class, () -> s.saveSnapshot(snap, entries(7, 10, 1)));
    }
  }

  @Test
  void anEmptySegmentAfterASnapshotOpens() throws IOException {
    // A crash right after entry 11 started a segment, before its record was synced.
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 10, 1));
      s.saveSnapshot(new Snapshot(new SnapshotMeta(10, 1, CONFIG), new byte[0]), List.of());
      s.append(entries(11, 11, 1));
      s.sync();
    }
    Path seg = dir.resolve("wal").resolve(SegmentedLog.name(11));
    try (var ch = java.nio.channels.FileChannel.open(seg, StandardOpenOption.WRITE)) {
      ch.truncate(SegmentedLog.SEGMENT_HEADER);
    }
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      assertTrue(s.load().entries().isEmpty());
      assertEquals(0, s.logRewrites());
      s.append(entries(11, 12, 2));
      s.sync();
    }
    assertEquals(entries(11, 12, 2), reopen(dir, SMALL).entries());

    // The same with an empty segment the snapshot does not lead into: the log is replaced.
    writeSegment(dir.resolve("wal").resolve(SegmentedLog.name(11)), 11, List.of());
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.saveSnapshot(new Snapshot(new SnapshotMeta(20, 3, CONFIG), new byte[0]), List.of());
    }
    writeSegment(dir.resolve("wal").resolve(SegmentedLog.name(5)), 5, List.of());
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      assertTrue(s.load().entries().isEmpty());
      assertEquals(1, s.logRewrites());
      s.append(entries(21, 21, 3));
      s.sync();
    }
    assertEquals(entries(21, 21, 3), reopen(dir, SMALL).entries());
  }

  @Test
  void aRewriteInterruptedByACrashIsFinishedOrAbandoned() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      s.load();
      s.append(entries(1, 5, 1));
      s.sync();
    }
    Path prefix = tmp.resolve("p");
    try (FileStorage s = new FileStorage(prefix, SMALL)) {
      s.load();
      s.append(entries(1, 3, 2));
      s.sync();
    }

    // Crash while wal.new was being written: wal stands and wal.new is removed.
    Path writing = tmp.resolve("writing");
    copyTree(dir, writing);
    copyTree(prefix.resolve("wal"), writing.resolve("wal.new"));
    Files.write(writing.resolve("wal.new").resolve("00000000000000000001.seg"), new byte[3]);
    assertEquals(entries(1, 5, 1), reopen(writing, SMALL).entries());
    assertTrue(Files.notExists(writing.resolve("wal.new")));

    // Crash between the two renames: the complete wal.new is renamed into place.
    Path between = tmp.resolve("between");
    copyTree(dir, between);
    Files.move(between.resolve("wal"), between.resolve("wal.old"));
    copyTree(prefix.resolve("wal"), between.resolve("wal.new"));
    assertEquals(entries(1, 3, 2), reopen(between, SMALL).entries());
    assertTrue(Files.notExists(between.resolve("wal.new")));
    assertTrue(Files.notExists(between.resolve("wal.old")));

    // Crash before wal.old was deleted.
    Path after = tmp.resolve("after");
    copyTree(dir, after);
    copyTree(prefix.resolve("wal"), after.resolve("wal.old"));
    assertEquals(entries(1, 5, 1), reopen(after, SMALL).entries());
    assertTrue(Files.notExists(after.resolve("wal.old")));
  }

  @Test
  void aSingleFileLogFromBeforeSegmentsIsMoved() throws IOException {
    Path dir = tmp.resolve("n");
    Files.createDirectories(dir);
    Files.write(dir.resolve("log"), records(entries(1, 7, 1)));
    try (FileStorage s = new FileStorage(dir, SMALL)) {
      assertEquals(entries(1, 7, 1), s.load().entries());
      assertEquals(4, s.segmentCount());
      s.append(entries(8, 8, 1));
      s.sync();
    }
    assertTrue(Files.notExists(dir.resolve("log")));
    assertEquals(entries(1, 8, 1), reopen(dir, SMALL).entries());

    // A torn tail is cut as before.
    Path torn = tmp.resolve("torn");
    Files.createDirectories(torn);
    byte[] full = records(entries(1, 3, 1));
    Files.write(torn.resolve("log"), Arrays.copyOf(full, full.length - 5));
    try (FileStorage s = new FileStorage(torn)) {
      assertEquals(entries(1, 2, 1), s.load().entries());
      assertEquals(full.length - 5 - records(entries(1, 2, 1)).length, s.tornBytesDropped());
    }

    // Damage in the middle stops startup and leaves the file as it was.
    Path bad = tmp.resolve("bad");
    Files.createDirectories(bad);
    Files.write(bad.resolve("log"), full);
    flip(bad.resolve("log"), full.length / 2);
    StorageException ex = assertThrows(StorageException.class, () -> new FileStorage(bad));
    assertTrue(ex.getMessage().contains("valid record follows"), ex.getMessage());
    assertTrue(Files.exists(bad.resolve("log")));
    assertTrue(Files.notExists(bad.resolve("wal")));

    // So do records that do not follow each other.
    Path gap = tmp.resolve("gap");
    Files.createDirectories(gap);
    Files.write(gap.resolve("log"), records(List.of(e(1, 1), e(3, 1))));
    assertThrows(StorageException.class, () -> new FileStorage(gap));

    // An empty file moves to an empty log; a snapshot gives the next index.
    Path empty = tmp.resolve("empty");
    Files.createDirectories(empty);
    Files.write(empty.resolve("log"), new byte[0]);
    try (FileStorage s = new FileStorage(empty)) {
      assertTrue(s.load().entries().isEmpty());
      s.append(entries(1, 1, 1));
      s.sync();
    }

    // A crash after the move but before the old file was deleted: the moved log wins.
    Files.write(dir.resolve("log"), records(entries(1, 2, 9)));
    assertEquals(entries(1, 8, 1), reopen(dir, SMALL).entries());
    assertTrue(Files.notExists(dir.resolve("log")));
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
