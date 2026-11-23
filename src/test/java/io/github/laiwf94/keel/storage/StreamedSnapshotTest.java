package io.github.laiwf94.keel.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.raft.Codec;
import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.Entry;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.RaftNode;
import io.github.laiwf94.keel.raft.RaftOptions;
import io.github.laiwf94.keel.raft.Snapshot;
import io.github.laiwf94.keel.raft.SnapshotData;
import io.github.laiwf94.keel.raft.SnapshotMeta;
import io.github.laiwf94.keel.raft.SnapshotSink;
import io.github.laiwf94.keel.raft.Storage;
import io.github.laiwf94.keel.replica.Replica;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Snapshots streamed through files: the file layout, its recovery, and transfers between nodes. */
class StreamedSnapshotTest {
  static final Configuration CONFIG =
      new Configuration(List.of(new Member("a", "h:1", "h:2", true)));

  @TempDir Path tmp;

  static byte[] random(int n, long seed) {
    byte[] b = new byte[n];
    new Random(seed).nextBytes(b);
    return b;
  }

  static List<String> files(Path dir) throws IOException {
    try (Stream<Path> s = Files.list(dir)) {
      return s.map(f -> f.getFileName().toString()).sorted().toList();
    }
  }

  static byte[] readAll(SnapshotData d) throws IOException {
    try (InputStream in = d.open()) {
      return in.readAllBytes();
    }
  }

  /** Writes {@code data} to a sink in uneven pieces, the way a state machine writes records. */
  static void writeUneven(SnapshotSink sink, byte[] data) {
    Random r = new Random(data.length);
    for (int at = 0; at < data.length; ) {
      int n = Math.min(data.length - at, 1 + r.nextInt(300_000));
      sink.write(data, at, n);
      at += n;
    }
  }

  @Test
  void aSnapshotIsWrittenToATemporaryFileAndRenamedIntoPlace() throws IOException {
    Path dir = tmp.resolve("n");
    byte[] data = random(3_000_000, 1);
    SnapshotMeta meta = new SnapshotMeta(4, 2, CONFIG);
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      long fsyncs = s.fsyncs();
      SnapshotSink sink = s.beginSnapshot();
      writeUneven(sink, data);
      assertEquals(data.length, sink.size());
      assertTrue(files(dir).contains("snapshot.0.tmp"));
      assertFalse(files(dir).contains("snapshot"));
      Snapshot snap = sink.finish(meta);
      // The file is fsynced once, when the sink is finished.
      assertEquals(fsyncs + 1, s.fsyncs());
      assertThrows(IllegalStateException.class, () -> sink.write(new byte[1], 0, 1));
      assertThrows(IllegalStateException.class, () -> sink.finish(meta));
      sink.abort(); // a finished sink is not undone
      s.saveSnapshot(snap, List.of());
      assertEquals(List.of("lock", "snapshot", "wal"), files(dir));
      assertEquals(Files.size(dir.resolve("snapshot")), 8 + data.length + metaBytes(meta) + 24);
      // The snapshot is read from the file, in ranges and as a stream.
      SnapshotData d = snap.data();
      assertInstanceOf(SnapshotFile.Data.class, d);
      assertArrayEquals(
          java.util.Arrays.copyOfRange(data, 1_000_000, 1_000_123), d.read(1_000_000, 123));
      assertArrayEquals(new byte[0], d.read(data.length, 0));
      assertThrows(IllegalArgumentException.class, () -> d.read(data.length - 1, 2));
      assertThrows(IllegalArgumentException.class, () -> d.read(-1, 1));
      assertArrayEquals(data, readAll(d));
      try (InputStream in = d.open()) {
        assertEquals(data[0] & 0xff, in.read());
        assertEquals(0, in.read(new byte[4], 0, 0));
        assertTrue(in.available() > 0);
        in.skipNBytes(data.length - 1);
        assertEquals(-1, in.read());
        assertEquals(-1, in.read(new byte[4], 0, 4));
      }
    }
    try (FileStorage s = new FileStorage(dir)) {
      Snapshot loaded = s.load().snapshot().orElseThrow();
      assertEquals(meta, loaded.meta());
      assertArrayEquals(data, loaded.data().toByteArray());
      assertEquals(new Snapshot(meta, data), loaded);
    }
  }

  static int metaBytes(SnapshotMeta meta) {
    ByteWriter w = new ByteWriter();
    Codec.writeSnapshotMeta(w, meta);
    return w.size();
  }

  @Test
  void aSnapshotFromElsewhereIsCopiedInAndAnEmptyOneWorks() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.saveSnapshot(new Snapshot(new SnapshotMeta(1, 1, CONFIG), new byte[0]), List.of());
      assertEquals(0, s.currentSnapshot().size());
      byte[] data = random(70_000, 2);
      s.saveSnapshot(new Snapshot(new SnapshotMeta(2, 1, CONFIG), data), List.of());
      assertArrayEquals(data, readAll(s.currentSnapshot()));
      assertEquals(List.of("lock", "snapshot", "wal"), files(dir));
    }
  }

  @Test
  void abortedAndLeftoverTemporaryFilesAreDeleted() throws IOException {
    Path dir = tmp.resolve("n");
    byte[] data = random(1000, 3);
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.saveSnapshot(new Snapshot(new SnapshotMeta(1, 1, CONFIG), data), List.of());
      SnapshotSink aborted = s.beginSnapshot();
      aborted.write(new byte[10], 0, 10);
      aborted.abort();
      aborted.abort();
      assertThrows(IllegalStateException.class, () -> aborted.write(new byte[1], 0, 1));
      assertThrows(IllegalStateException.class, () -> aborted.finish(null));
      // A crash mid-write, and a crash after the file was finished but before it was installed.
      SnapshotSink partial = s.beginSnapshot();
      partial.write(new byte[500_000], 0, 500_000);
      SnapshotSink sealed = s.beginSnapshot();
      sealed.write(new byte[] {1}, 0, 1);
      sealed.finish(new SnapshotMeta(9, 9, CONFIG));
      assertEquals(
          List.of("lock", "snapshot", "snapshot.2.tmp", "snapshot.3.tmp", "wal"), files(dir));
    }
    // An interrupted rewrite of the older single-record layout leaves this name.
    Files.write(dir.resolve("snapshot.tmp"), new byte[] {1, 2});
    try (FileStorage s = new FileStorage(dir)) {
      Snapshot loaded = s.load().snapshot().orElseThrow();
      assertEquals(1, loaded.meta().index());
      assertArrayEquals(data, loaded.data().toByteArray());
      assertEquals(List.of("lock", "snapshot", "wal"), files(dir));
    }
  }

  @Test
  void everyDamagedByteStopsStartup() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.saveSnapshot(new Snapshot(new SnapshotMeta(3, 1, CONFIG), random(40, 4)), List.of());
    }
    byte[] good = Files.readAllBytes(dir.resolve("snapshot"));
    for (int i = 0; i < good.length; i++) {
      for (int bit : new int[] {0, 7}) {
        byte[] b = good.clone();
        b[i] ^= (byte) (1 << bit);
        Files.write(dir.resolve("snapshot"), b);
        int at = i;
        assertThrows(StorageException.class, () -> new FileStorage(dir), "byte " + at);
      }
    }
    for (int len = 0; len < good.length; len++) {
      Files.write(dir.resolve("snapshot"), java.util.Arrays.copyOf(good, len));
      int at = len;
      assertThrows(StorageException.class, () -> new FileStorage(dir), "cut at " + at);
    }
    byte[] longer = java.util.Arrays.copyOf(good, good.length + 1);
    Files.write(dir.resolve("snapshot"), longer);
    assertThrows(StorageException.class, () -> new FileStorage(dir));
    Files.write(dir.resolve("snapshot"), good);
    try (FileStorage s = new FileStorage(dir)) {
      assertEquals(3, s.load().snapshot().orElseThrow().meta().index());
    }
  }

  /**
   * A file whose checksums all match but whose lengths or metadata are wrong, as a bug rather than
   * a bit flip would leave it.
   */
  @Test
  void consistentChecksumsDoNotHideBadLengthsOrMetadata() throws IOException {
    Path dir = tmp.resolve("n");
    Files.createDirectories(dir);
    byte[] meta = {1, 2, 3}; // not valid metadata
    Files.write(dir.resolve("snapshot"), layout(1, new byte[] {5}, meta, 0, 0));
    StorageException e = assertThrows(StorageException.class, () -> new FileStorage(dir));
    assertTrue(e.getMessage().contains("corrupt"), e.getMessage());
    ByteWriter mw = new ByteWriter();
    Codec.writeSnapshotMeta(mw, new SnapshotMeta(1, 1, CONFIG));
    byte[] goodMeta = mw.toByteArray();
    // Lengths that do not add up to the file size.
    Files.write(dir.resolve("snapshot"), layout(1, new byte[] {5}, goodMeta, 1, 0));
    assertThrows(StorageException.class, () -> new FileStorage(dir));
    // A metadata length past the limit.
    Files.write(dir.resolve("snapshot"), layout(1, new byte[] {5}, goodMeta, 0, 2 << 20));
    assertThrows(StorageException.class, () -> new FileStorage(dir));
    // A version from the future.
    Files.write(dir.resolve("snapshot"), layout(2, new byte[] {5}, goodMeta, 0, 0));
    e = assertThrows(StorageException.class, () -> new FileStorage(dir));
    assertTrue(e.getMessage().contains("version"), e.getMessage());
    Files.write(dir.resolve("snapshot"), layout(1, new byte[] {5}, goodMeta, 0, 0));
    try (FileStorage s = new FileStorage(dir)) {
      assertArrayEquals(new byte[] {5}, s.load().snapshot().orElseThrow().data().toByteArray());
    }
  }

  /** A snapshot file with valid checksums whose recorded lengths are off by the given amounts. */
  static byte[] layout(int version, byte[] data, byte[] meta, int dataSkew, int metaOverride) {
    java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(8 + data.length + meta.length + 24);
    b.putInt(SnapshotFile.MAGIC).putInt(version).put(data).put(meta);
    byte[] t = new byte[24];
    java.nio.ByteBuffer tb = java.nio.ByteBuffer.wrap(t);
    tb.putLong(data.length + dataSkew).putInt(metaOverride != 0 ? metaOverride : meta.length);
    tb.putInt(SnapshotFile.crc(data, 0, data.length))
        .putInt(SnapshotFile.crc(meta, 0, meta.length));
    tb.putInt(SnapshotFile.crc(t, 0, 20));
    b.put(t);
    return b.array();
  }

  @Test
  void aSnapshotFileFromBeforeStreamingIsRewritten() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.append(List.of(new Entry(1, 1, io.github.laiwf94.keel.raft.EntryType.NOOP, new byte[0])));
      s.append(List.of(new Entry(2, 1, io.github.laiwf94.keel.raft.EntryType.NOOP, new byte[0])));
      s.sync();
    }
    byte[] data = random(5000, 5);
    Snapshot old = new Snapshot(new SnapshotMeta(1, 1, CONFIG), data);
    ByteWriter w = new ByteWriter();
    Codec.writeLegacySnapshot(w, old);
    Files.write(dir.resolve("snapshot"), SegmentedLog.record(w.toByteArray()));
    assertFalse(SnapshotFile.isStreamed(dir.resolve("snapshot")));
    try (FileStorage s = new FileStorage(dir)) {
      Storage.State st = s.load();
      assertEquals(old, st.snapshot().orElseThrow());
      assertEquals(1, st.entries().size());
      assertEquals(2, st.entries().get(0).index());
    }
    assertTrue(SnapshotFile.isStreamed(dir.resolve("snapshot")));
    assertEquals(List.of("lock", "snapshot", "wal"), files(dir));
    try (FileStorage s = new FileStorage(dir)) {
      assertEquals(old, s.load().snapshot().orElseThrow());
    }
    // A corrupt old-layout file still stops startup.
    byte[] bad = SegmentedLog.record(w.toByteArray());
    bad[bad.length - 1] ^= 1;
    Files.write(dir.resolve("snapshot"), bad);
    assertThrows(StorageException.class, () -> new FileStorage(dir));
    Files.write(dir.resolve("snapshot"), SegmentedLog.record(new byte[] {1, 2, 3}));
    assertThrows(StorageException.class, () -> new FileStorage(dir));
  }

  @Test
  void aReplacedSnapshotStaysReadableWhileReferencedAndClosesAfter() throws IOException {
    Path dir = tmp.resolve("n");
    byte[] first = random(100_000, 6);
    SnapshotFile.Data now;
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      s.saveSnapshot(new Snapshot(new SnapshotMeta(1, 1, CONFIG), first), List.of());
      SnapshotFile.Data old = s.currentSnapshot();
      old.retain(); // a leader starting a transfer of it
      s.saveSnapshot(new Snapshot(new SnapshotMeta(2, 1, CONFIG), new byte[] {1}), List.of());
      assertTrue(old.isOpen());
      assertArrayEquals(first, readAll(old));
      old.release();
      assertFalse(old.isOpen());
      assertThrows(IllegalStateException.class, old::retain);
      assertThrows(IllegalStateException.class, old::release);
      UncheckedIOException e = assertThrows(UncheckedIOException.class, () -> old.read(0, 1));
      assertTrue(e.getMessage().contains("snapshot"), e.getMessage());
      now = s.currentSnapshot();
      assertTrue(now.isOpen());
    }
    assertFalse(now.isOpen());
  }

  @Test
  void aSinkThatCannotWriteFailsCleanly() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      s.load();
      SnapshotSink sink = s.beginSnapshot();
      sink.write(new byte[10], 0, 10);
      // The file system goes away under it: writing past the buffer and finishing both fail.
      Files.delete(dir.resolve("snapshot.0.tmp"));
      Files.createDirectory(dir.resolve("snapshot.1.tmp"));
      StorageException e = assertThrows(StorageException.class, s::beginSnapshot);
      assertTrue(e.getMessage().contains("snapshot.1.tmp"), e.getMessage());
      Files.delete(dir.resolve("snapshot.1.tmp"));
      // Unlinked under it, the file still takes the bytes, but it cannot be reopened to read.
      SnapshotMeta meta = new SnapshotMeta(1, 1, CONFIG);
      assertThrows(StorageException.class, () -> sink.finish(meta));
      assertThrows(IllegalStateException.class, () -> sink.write(new byte[1], 0, 1));
    }
  }

  @Test
  void raftNodeCompactsIntoASinkAndAbortsOnesItCannotUse() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      RaftNode node = new RaftNode("a", CONFIG, RaftOptions.DEFAULTS, s, new Random(1));
      while (node.role() != RaftNode.Role.LEADER) {
        node.tick();
      }
      node.propose(new byte[] {1});
      node.ready();
      s.sync();
      node.markDurable();
      node.ready();
      SnapshotSink early = s.beginSnapshot();
      assertThrows(IllegalArgumentException.class, () -> node.compact(5, early));
      SnapshotSink sink = s.beginSnapshot();
      sink.write(new byte[] {4, 2}, 0, 2);
      node.compact(2, sink);
      assertEquals(2, node.log().snapshot().index());
      SnapshotSink stale = s.beginSnapshot();
      node.compact(2, stale);
      node.propose(new byte[] {2});
      node.ready();
      s.sync();
      node.markDurable();
      node.ready();
      SnapshotSink bad = s.beginSnapshot();
      bad.write(new byte[] {1}, 0, 1);
      bad.abort();
      assertThrows(IllegalStateException.class, () -> node.compact(3, bad));
      assertEquals(2, node.log().snapshot().index());
      assertEquals(List.of("lock", "snapshot", "state", "wal"), files(dir));
      assertArrayEquals(new byte[] {4, 2}, readAll(s.currentSnapshot()));
    }
  }

  /** A state machine whose snapshot fails part of the way through. */
  static final class FailingMachine implements io.github.laiwf94.keel.replica.StateMachine {
    @Override
    public byte[] apply(long index, byte[] command) {
      return command;
    }

    @Override
    public void snapshot(OutputStream out) throws IOException {
      out.write(new byte[100_000]);
      throw new IOException("out of disk");
    }

    @Override
    public void restore(InputStream in) {}
  }

  @Test
  void aFailedStateMachineSnapshotLeavesNoFileBehind() throws IOException {
    Path dir = tmp.resolve("n");
    try (FileStorage s = new FileStorage(dir)) {
      Replica r =
          new Replica(
              "a",
              CONFIG,
              RaftOptions.DEFAULTS,
              s,
              new FailingMachine(),
              new Random(1),
              m -> {},
              2);
      while (r.node().role() != RaftNode.Role.LEADER) {
        r.tick();
        r.process();
      }
      r.propose(new byte[] {1}, o -> {});
      UncheckedIOException e = assertThrows(UncheckedIOException.class, r::process);
      assertTrue(e.getMessage().contains("out of disk"), e.getMessage());
      assertEquals(List.of("lock", "state", "wal"), files(dir));
    }
  }

  @Test
  void aLargeSnapshotStreamsToANewServerThroughFiles() throws IOException {
    StreamedTransfer.Result r = StreamedTransfer.run(tmp, 6_000_000, 64 << 10, 0);
    assertTrue(r.statesMatch(), r.toString());
    assertTrue(r.promoted(), r.toString());
    assertEquals(1, r.restores(), r.toString());
    assertEquals(r.leaderApplied(), r.followerApplied(), r.toString());
    assertTrue(r.chunksSent() >= 6_000_000 / (64 << 10), r.toString());
    assertEquals(List.of("lock", "snapshot", "state", "wal"), files(tmp.resolve("n2")));
    assertTrue(Files.size(tmp.resolve("n2/snapshot")) > 6_000_000);
  }

  @Test
  void aTransferThatLosesChunksResumesFromTheFile() throws IOException {
    StreamedTransfer.Result r = StreamedTransfer.run(tmp, 2_000_000, 32 << 10, 7);
    assertTrue(r.statesMatch(), r.toString());
    assertTrue(r.chunksDropped() > 5, r.toString());
    assertEquals(1, r.restores(), r.toString());
    assertEquals(List.of("lock", "snapshot", "state", "wal"), files(tmp.resolve("n2")));
  }

  /**
   * The bound the whole design is for: a snapshot larger than the old 256 MiB limit and ten times
   * the heap goes from a leader's state machine to a new server's state machine in a JVM with a 32
   * MiB heap. Holding the snapshot in memory anywhere on the way would run out of heap.
   */
  @Test
  void aSnapshotTenTimesTheHeapTransfersInAJvmWithA32MibHeap() throws Exception {
    long filler = 320L << 20;
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    ProcessBuilder pb =
        new ProcessBuilder(
            java,
            "-Xmx32m",
            "-XX:+UseSerialGC",
            "-cp",
            System.getProperty("java.class.path"),
            StreamedTransfer.class.getName(),
            tmp.toString(),
            Long.toString(filler),
            Integer.toString(1 << 20),
            "0");
    pb.redirectErrorStream(true);
    Process p = pb.start();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (InputStream in = p.getInputStream()) {
      in.transferTo(out);
    }
    assertTrue(p.waitFor(5, TimeUnit.MINUTES));
    String text = out.toString(StandardCharsets.UTF_8);
    assertEquals(0, p.exitValue(), text);
    assertTrue(text.contains("statesMatch=true"), text);
    assertTrue(text.contains("promoted=true"), text);
    assertTrue(text.contains("restores=1"), text);
    assertTrue(text.contains("maxHeap="), text);
    long heap = Long.parseLong(text.substring(text.indexOf("maxHeap=") + 8).trim());
    assertTrue(heap <= 40L << 20, text);
    assertTrue(Files.size(tmp.resolve("n2/snapshot")) > filler, text);
  }

  /**
   * Delivers queued messages between replicas until none are left; drops what {@code drop} says.
   */
  static void pump(
      java.util.Map<String, Replica> replicas,
      java.util.ArrayDeque<io.github.laiwf94.keel.raft.Message> wire,
      java.util.function.Predicate<io.github.laiwf94.keel.raft.Message> drop) {
    for (int i = 0; i < 100_000; i++) {
      replicas.values().forEach(Replica::process);
      if (wire.isEmpty()) {
        return;
      }
      io.github.laiwf94.keel.raft.Message m = wire.poll();
      Replica to = replicas.get(m.to());
      if (to != null && !drop.test(m)) {
        to.receive(m);
      }
    }
    throw new AssertionError("messages never stopped");
  }

  /**
   * A leader keeps the snapshot it is sending open after taking a newer one, and drops it when the
   * transfer ends: when the follower installs it, and when the leader steps down mid-transfer.
   */
  @Test
  void aLeaderHoldsTheSnapshotItSendsOnlyWhileTheTransferRuns() {
    for (boolean stepDown : new boolean[] {false, true}) {
      Path dir = tmp.resolve("run-" + stepDown);
      java.util.ArrayDeque<io.github.laiwf94.keel.raft.Message> wire = new java.util.ArrayDeque<>();
      RaftOptions opts = RaftOptions.DEFAULTS.withSnapshotChunks(1024, 2);
      boolean[] hold = {true};
      int[] sent = {0};
      try (FileStorage s1 = new FileStorage(dir.resolve("n1"));
          FileStorage s2 = new FileStorage(dir.resolve("n2"))) {
        StreamedTransfer.FillerMachine m1 = new StreamedTransfer.FillerMachine(50_000);
        StreamedTransfer.FillerMachine m2 = new StreamedTransfer.FillerMachine(50_000);
        Replica leader =
            new Replica(
                "n1",
                new Configuration(List.of(StreamedTransfer.member("n1"))),
                opts,
                s1,
                m1,
                new Random(1),
                wire::add,
                10);
        Replica follower =
            new Replica("n2", Configuration.EMPTY, opts, s2, m2, new Random(2), wire::add, 1000);
        java.util.Map<String, Replica> all = java.util.Map.of("n1", leader, "n2", follower);
        while (leader.node().role() != RaftNode.Role.LEADER) {
          leader.tick();
          leader.process();
        }
        for (int i = 0; i < 10; i++) {
          leader.propose(new byte[] {(byte) i}, o -> {});
        }
        pump(all, wire, m -> false);
        leader.addServer(StreamedTransfer.member("n2"), o -> {});
        // The follower's acknowledgements are held back, so the transfer stays open.
        java.util.function.Predicate<io.github.laiwf94.keel.raft.Message> drop =
            m -> {
              if (m instanceof io.github.laiwf94.keel.raft.Message.SnapshotRequest) {
                sent[0]++;
              }
              return hold[0] && m instanceof io.github.laiwf94.keel.raft.Message.SnapshotResponse;
            };
        for (int t = 0; t < 50 && sent[0] == 0; t++) {
          leader.tick();
          pump(all, wire, drop);
        }
        assertTrue(sent[0] > 0, "no transfer started");
        SnapshotFile.Data first = s1.currentSnapshot();
        assertTrue(first.isOpen());
        for (int i = 0; i < 10; i++) {
          leader.propose(new byte[] {(byte) (10 + i)}, o -> {});
        }
        pump(all, wire, drop);
        SnapshotFile.Data second = s1.currentSnapshot();
        assertTrue(second != first, "the leader did not take a newer snapshot");
        assertTrue(first.isOpen(), "the snapshot being sent was closed under the transfer");
        if (stepDown) {
          leader.receive(
              new io.github.laiwf94.keel.raft.Message.AppendRequest(
                  "n3", "n1", 99, 0, 0, List.of(), 0, 0));
          leader.process();
          assertEquals(RaftNode.Role.FOLLOWER, leader.node().role());
        } else {
          hold[0] = false;
          for (int t = 0; t < 50 && follower.appliedIndex() < leader.appliedIndex(); t++) {
            leader.tick();
            pump(all, wire, drop);
          }
          assertEquals(leader.appliedIndex(), follower.appliedIndex());
          // The first transfer ends with the older snapshot; the leader compacted past it
          // meanwhile, so a second transfer sends the newer one.
          assertEquals(2, m2.restores);
          assertEquals(m1.digest, m2.digest);
        }
        assertFalse(first.isOpen(), "the finished transfer still holds its snapshot");
        assertTrue(second.isOpen());
      }
    }
  }
}
