package io.github.laiwf94.keel.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogBenchTest {
  @TempDir Path tmp;

  private static List<String> files(Path dir) throws IOException {
    try (Stream<Path> s = Files.list(dir)) {
      return s.map(p -> p.getFileName().toString()).sorted().toList();
    }
  }

  @Test
  void compactionDeletesTheCoveredSegmentsAndRewritesNothing() throws IOException {
    Path dir = tmp.resolve("c");
    LogBench.Compaction c = LogBench.compaction(dir, 6 << 20, 1024, 64 << 10, 1 << 20, 100);
    assertEquals(6 * 1024, c.entries());
    assertTrue(c.logBytes() > 6 << 20, c.toString());
    assertTrue(c.segmentsBefore() >= 6, c.toString());
    // The snapshot sits 100 entries before the end, so at most the last two segments hold an
    // entry after it.
    assertTrue(c.segmentsAfter() >= 1 && c.segmentsAfter() <= 2, c.toString());
    assertEquals(c.segmentsBefore(), c.segmentsDeleted() + c.segmentsAfter(), c.toString());
    assertEquals(0, c.logRewrites());
    // One directory fsync after the snapshot's rename, then one after each deletion.
    assertEquals(c.segmentsDeleted() + 1, c.pauseFsyncs(), c.toString());
    assertTrue(c.pauseNanos() > 0 && c.writeNanos() > 0);
    assertTrue(c.peakHeapBytes() > 0);
    assertEquals(c.segmentsAfter(), files(dir.resolve("wal")).size());
  }

  @Test
  void aSnapshotOfTheWholeLogDeletesEverySegment() throws IOException {
    Path dir = tmp.resolve("c");
    LogBench.Compaction c = LogBench.compaction(dir, 3 << 20, 4096, 1 << 20, 1 << 20, 0);
    assertEquals(3 * 256, c.entries());
    assertEquals(c.segmentsBefore(), c.segmentsDeleted());
    assertEquals(0, c.segmentsAfter());
    assertEquals(0, c.logRewrites());
    assertEquals(List.of(), files(dir.resolve("wal")));
  }

  @Test
  void aNewServerCatchesUpThroughChunksAndTheEntriesAfterThem() {
    long state = 3 << 20;
    LogBench.CatchUp c = LogBench.catchUp(tmp.resolve("u"), state, 50, 256, 256 << 10);
    assertTrue(c.statesMatch(), c.toString());
    assertEquals(state, c.stateBytes());
    assertTrue(c.snapshotBytes() > state, c.toString());
    // Every chunk carries at most 256 KiB, and the window means some may be resent.
    assertTrue(c.chunks() >= c.snapshotBytes() / (256 << 10), c.toString());
    assertTrue(c.wireBytes() > c.snapshotBytes(), c.toString());
    // The tail, then the configuration entry that adds the new server.
    assertTrue(c.tailEntries() > 50, c.toString());
    assertTrue(c.leaderPauseNanos() > 0 && c.transferNanos() > 0 && c.restoreNanos() > 0);
    assertTrue(c.totalNanos() >= c.transferNanos() + c.restoreNanos() + c.tailNanos());
    assertTrue(c.peakHeapBytes() > 0);
  }

  @Test
  void runPrintsBothTablesAndLeavesNoDataBehind() throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    LogBench.Options o =
        new LogBench.Options(
            List.of(2L << 20, 1L << 20), tmp, 1024, 64 << 10, 1 << 20, 10, 20, 256 << 10);
    LogBench.run(o, new PrintStream(buf, true, StandardCharsets.UTF_8));
    String out = buf.toString(StandardCharsets.UTF_8);
    assertTrue(out.contains("compaction"), out);
    assertTrue(out.contains("catch-up (snapshot chunks of 256 KiB"), out);
    assertTrue(out.contains("segments of 1 MiB"), out);
    assertTrue(out.contains("2 MiB"), out);
    assertFalse(out.contains(" NO "), out);
    // A header and two rows in each table.
    assertEquals(
        4, out.lines().filter(l -> l.startsWith("2 MiB") || l.startsWith("1 MiB")).count());
    assertEquals(List.of(), files(tmp));
  }

  @Test
  void parsesSizesAndOptions() {
    assertEquals(512, LogBench.parseSize("512"));
    assertEquals(64 << 10, LogBench.parseSize("64k"));
    assertEquals(8 << 20, LogBench.parseSize(" 8M "));
    assertEquals(16L << 30, LogBench.parseSize("16g"));
    for (String bad : new String[] {"0", "-1", "", "g", "x", "9000000000g"}) {
      assertThrows(IllegalArgumentException.class, () -> LogBench.parseSize(bad), bad);
    }
    LogBench.Options d = LogBench.parse(new String[0]);
    assertEquals(List.of(1L << 30, 4L << 30, 16L << 30), d.sizes());
    assertEquals(1024, d.entryBytes());
    assertEquals(64L << 20, d.segmentBytes());
    LogBench.Options o =
        LogBench.parse(
            new String[] {
              "--sizes",
              "1m,2m",
              "--dir",
              tmp.toString(),
              "--entry-bytes",
              "2k",
              "--batch-bytes",
              "256k",
              "--segment-bytes",
              "4m",
              "--keep",
              "7",
              "--tail",
              "9",
              "--chunk-bytes",
              "128k"
            });
    assertEquals(
        new LogBench.Options(
            List.of(1L << 20, 2L << 20), tmp, 2048, 256 << 10, 4L << 20, 7, 9, 128 << 10),
        o);
    assertThrows(IllegalArgumentException.class, () -> LogBench.parse(new String[] {"--keep"}));
    assertThrows(
        IllegalArgumentException.class, () -> LogBench.parse(new String[] {"--what", "1"}));
    assertThrows(
        IllegalArgumentException.class,
        () -> new LogBench.Options(List.of(), tmp, 1, 1, 1, 0, 0, 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> new LogBench.Options(List.of(1L), tmp, 1024, 512, 1, 0, 0, 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> new LogBench.Options(List.of(1L), tmp, 1, 1, 1, -1, 0, 1));
  }

  @Test
  void deletingAMissingDirectoryDoesNothing() {
    LogBench.delete(tmp.resolve("absent"));
    assertTrue(Files.exists(tmp));
  }

  @Test
  void fillerRestoreRejectsDamagedSnapshotsAndKeepsItsState() throws IOException {
    assertThrows(IllegalArgumentException.class, () -> new FillerMachine(-1));
    FillerMachine a = new FillerMachine(100_000);
    a.apply(1, new byte[] {1, 2, 3});
    a.apply(2, new byte[] {4});
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    a.snapshot(buf);
    byte[] good = buf.toByteArray();
    assertEquals(16 + 100_000, good.length);
    assertTrue(a.snapshotNanos() > 0);

    FillerMachine b = new FillerMachine(100_000);
    b.apply(1, new byte[] {9});
    long digest = b.digest();
    byte[] flipped = good.clone();
    flipped[16 + 70_000] ^= 1;
    byte[] trailing = Arrays.copyOf(good, good.length + 1);
    for (byte[] bad :
        new byte[][] {
          Arrays.copyOf(good, 10), Arrays.copyOf(good, good.length - 1), flipped, trailing
        }) {
      assertThrows(IOException.class, () -> b.restore(new ByteArrayInputStream(bad)));
      assertEquals(1, b.applied());
      assertEquals(digest, b.digest());
      assertEquals(0, b.restores());
    }
    b.restore(new ByteArrayInputStream(good));
    assertTrue(b.sameStateAs(a));
    assertEquals(1, b.restores());
    assertTrue(b.restoreNanos() > 0);
    assertFalse(b.sameStateAs(new FillerMachine(100_000)));
  }
}
