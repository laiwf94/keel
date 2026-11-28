package io.github.laiwf94.keel.bench;

import com.sun.management.GarbageCollectionNotificationInfo;
import io.github.laiwf94.keel.net.MessageCodec;
import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.Entry;
import io.github.laiwf94.keel.raft.EntryType;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.Message;
import io.github.laiwf94.keel.raft.RaftNode;
import io.github.laiwf94.keel.raft.RaftOptions;
import io.github.laiwf94.keel.raft.Snapshot;
import io.github.laiwf94.keel.raft.SnapshotMeta;
import io.github.laiwf94.keel.raft.SnapshotSink;
import io.github.laiwf94.keel.replica.Replica;
import io.github.laiwf94.keel.storage.FileStorage;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

/**
 * Measures the two costs a large log used to make expensive, on {@link FileStorage} in a real
 * directory:
 *
 * <ul>
 *   <li><b>Compaction pause.</b> A log of a given size is written in synced batches, then a
 *       snapshot covering all but its last entries is installed. The pause is how long {@link
 *       FileStorage#saveSnapshot} blocks the node's thread: the snapshot file's rename and the
 *       deletion of every segment the snapshot covers, each followed by a directory fsync.
 *   <li><b>Catch-up.</b> A leader whose state is a given size snapshots it, then a new server
 *       joins. The time runs from the configuration change until the new server is a voter and has
 *       applied everything the leader has: the chunked InstallSnapshot transfer into a file, the
 *       restore that reads and checks every byte, and the entries after the snapshot. Messages go
 *       through the wire codec in memory, so the network itself is not measured.
 * </ul>
 *
 * Both run in one JVM whose heap the caller sets ({@code ./gradlew logBenchmark} uses 256 MiB), and
 * each result reports the most heap in use right after a collection, so a run also shows that
 * neither path holds the log or the snapshot in memory.
 */
public final class LogBench {
  /** One compaction measurement. */
  public record Compaction(
      long logBytes,
      long entries,
      int segmentsBefore,
      long segmentsDeleted,
      int segmentsAfter,
      long writeNanos,
      long pauseNanos,
      long pauseFsyncs,
      long logRewrites,
      long peakHeapBytes) {}

  /** One catch-up measurement. */
  public record CatchUp(
      long stateBytes,
      long snapshotBytes,
      long leaderPauseNanos,
      long transferNanos,
      long restoreNanos,
      long tailNanos,
      long totalNanos,
      long tailEntries,
      long chunks,
      long wireBytes,
      boolean statesMatch,
      long peakHeapBytes) {}

  /** What to run; sizes are in bytes. */
  public record Options(
      List<Long> sizes,
      Path dir,
      int entryBytes,
      int batchBytes,
      long segmentBytes,
      int keep,
      int tail,
      int chunkBytes) {
    public Options {
      sizes = List.copyOf(sizes);
      if (sizes.isEmpty()) {
        throw new IllegalArgumentException("no sizes given");
      }
      if (entryBytes < 1 || batchBytes < entryBytes || keep < 0 || tail < 0 || chunkBytes < 1) {
        throw new IllegalArgumentException("invalid benchmark options");
      }
    }

    static Options defaults(Path dir) {
      return new Options(
          List.of(1L << 30, 4L << 30, 16L << 30),
          dir,
          1024,
          1 << 20,
          64L << 20,
          1024,
          1000,
          1 << 20);
    }
  }

  private static final long TERM = 1;

  private LogBench() {}

  static Member member(String id) {
    return new Member(id, id + ":7000", id + ":8080", true);
  }

  /**
   * Writes at least {@code logBytes} of log in entries of {@code entryBytes}, syncing every {@code
   * batchBytes}, then installs a snapshot at the entry {@code keep} entries before the end and
   * times it.
   */
  public static Compaction compaction(
      Path dir, long logBytes, int entryBytes, int batchBytes, long segmentBytes, int keep) {
    byte[] payload = new byte[entryBytes];
    new Random(1).nextBytes(payload);
    int perBatch = batchBytes / entryBytes;
    ArrayDeque<Entry> kept = new ArrayDeque<>(keep + 1);
    resetPeakHeap();
    try (FileStorage s = new FileStorage(dir, segmentBytes)) {
      s.load();
      long next = 1;
      long written = 0;
      long start = System.nanoTime();
      while (written < logBytes) {
        List<Entry> batch = new ArrayList<>(perBatch);
        for (int i = 0; i < perBatch; i++) {
          Entry e = new Entry(next++, TERM, EntryType.COMMAND, payload);
          batch.add(e);
          if (keep > 0) {
            if (kept.size() == keep) {
              kept.poll();
            }
            kept.add(e);
          }
        }
        s.append(batch);
        s.sync();
        written += (long) perBatch * entryBytes;
      }
      long writeNanos = System.nanoTime() - start;
      long last = next - 1;
      long index = last - kept.size();
      long onDisk = sizeOf(dir.resolve("wal"));
      int before = s.segmentCount();
      // Measured before compaction too, while the index of every segment is still in memory.
      long heap = peakHeap();
      SnapshotSink sink = s.beginSnapshot();
      sink.write(payload, 0, payload.length);
      Snapshot snap =
          sink.finish(new SnapshotMeta(index, TERM, new Configuration(List.of(member("n1")))));
      long fsyncs = s.fsyncs();
      long t0 = System.nanoTime();
      s.saveSnapshot(snap, List.copyOf(kept));
      long pause = System.nanoTime() - t0;
      return new Compaction(
          onDisk,
          last,
          before,
          s.segmentsCompacted(),
          s.segmentCount(),
          writeNanos,
          pause,
          s.fsyncs() - fsyncs,
          s.logRewrites(),
          Math.max(heap, peakHeap()));
    }
  }

  /**
   * A leader whose state machine snapshot carries {@code stateBytes}, with {@code tail} entries of
   * {@code entryBytes} after its snapshot, adds a new server and runs until it has caught up.
   */
  public static CatchUp catchUp(Path dir, long stateBytes, int tail, int entryBytes, int chunk) {
    RaftOptions opts = RaftOptions.DEFAULTS.withSnapshotChunks(chunk, 4);
    Map<String, ArrayDeque<byte[]>> wire =
        Map.of("n1", new ArrayDeque<>(), "n2", new ArrayDeque<>());
    long[] counts = new long[2];
    // Snapshot once, after the first batch, and not again while the tail and the configuration
    // changes for the new server are appended.
    long snapshotEvery = tail + 10L;
    byte[] command = new byte[entryBytes];
    new Random(2).nextBytes(command);
    resetPeakHeap();
    FillerMachine m1 = new FillerMachine(stateBytes);
    FillerMachine m2 = new FillerMachine(stateBytes);
    try (FileStorage s1 = new FileStorage(dir.resolve("n1"));
        FileStorage s2 = new FileStorage(dir.resolve("n2"))) {
      java.util.function.Consumer<Message> net =
          msg -> {
            byte[] bytes = MessageCodec.encode(msg);
            counts[1] += bytes.length;
            if (msg instanceof Message.SnapshotRequest) {
              counts[0]++;
            }
            ArrayDeque<byte[]> q = wire.get(msg.to());
            if (q != null) {
              q.add(bytes);
            }
          };
      Replica leader =
          new Replica(
              "n1",
              new Configuration(List.of(member("n1"))),
              opts,
              s1,
              m1,
              new Random(1),
              net,
              snapshotEvery);
      while (leader.node().role() != RaftNode.Role.LEADER) {
        leader.tick();
        leader.process();
      }
      long leaderPause = 0;
      while (leader.node().log().snapshot().index() == 0) {
        leader.propose(command, o -> {});
        long t0 = System.nanoTime();
        leader.process();
        leaderPause = Math.max(leaderPause, System.nanoTime() - t0);
      }
      for (int i = 0; i < tail; i++) {
        leader.propose(command, o -> {});
        leader.process();
      }
      long snapshotBytes = Files.size(dir.resolve("n1").resolve("snapshot"));
      Replica follower =
          new Replica("n2", Configuration.EMPTY, opts, s2, m2, new Random(2), net, snapshotEvery);
      Map<String, Replica> replicas = Map.of("n1", leader, "n2", follower);
      counts[0] = 0;
      counts[1] = 0;
      long start = System.nanoTime();
      long installed = 0;
      leader.addServer(member("n2"), o -> {});
      leader.process();
      for (long round = 0; ; round++) {
        if (round == 100_000_000L) {
          throw new IllegalStateException("the new server did not catch up");
        }
        boolean moved = false;
        for (Map.Entry<String, Replica> e : replicas.entrySet()) {
          ArrayDeque<byte[]> q = wire.get(e.getKey());
          while (!q.isEmpty()) {
            e.getValue().receive(MessageCodec.decode(q.poll()));
            moved = true;
          }
          e.getValue().process();
        }
        if (installed == 0 && m2.restores() > 0) {
          installed = System.nanoTime();
        }
        if (leader.node().config().isVoter("n2")
            && follower.appliedIndex() == leader.appliedIndex()) {
          break;
        }
        if (!moved) {
          leader.tick();
          follower.tick();
          leader.process();
          follower.process();
        }
      }
      long end = System.nanoTime();
      if (installed == 0) {
        throw new IllegalStateException("the new server caught up without a snapshot");
      }
      long restore = m2.restoreNanos();
      return new CatchUp(
          stateBytes,
          snapshotBytes,
          leaderPause,
          installed - start - restore,
          restore,
          end - installed,
          end - start,
          leader.appliedIndex() - leader.node().log().snapshot().index(),
          counts[0],
          counts[1],
          m1.sameStateAs(m2),
          peakHeap());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The most heap in use right after any collection since the last {@link #resetPeakHeap}: an upper
   * bound on the live data, which is what a smaller heap would have to hold. A peak of heap use
   * without a collection mostly measures how much garbage the young generation is sized for.
   */
  private static final AtomicLong PEAK_AFTER_GC = new AtomicLong();

  private static final Set<String> HEAP_POOLS = new HashSet<>();

  static {
    for (MemoryPoolMXBean p : ManagementFactory.getMemoryPoolMXBeans()) {
      if (p.getType() == MemoryType.HEAP) {
        HEAP_POOLS.add(p.getName());
      }
    }
    NotificationListener listener =
        (n, handback) -> {
          if (!n.getType()
              .equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
            return;
          }
          GarbageCollectionNotificationInfo info =
              GarbageCollectionNotificationInfo.from((CompositeData) n.getUserData());
          long used = 0;
          for (Map.Entry<String, MemoryUsage> e :
              info.getGcInfo().getMemoryUsageAfterGc().entrySet()) {
            if (HEAP_POOLS.contains(e.getKey())) {
              used += e.getValue().getUsed();
            }
          }
          PEAK_AFTER_GC.accumulateAndGet(used, Math::max);
        };
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      if (gc instanceof NotificationEmitter emitter) {
        emitter.addNotificationListener(listener, null, null);
      }
    }
  }

  private static void resetPeakHeap() {
    System.gc();
    PEAK_AFTER_GC.set(0);
  }

  /** Collects once more, so the result includes what is live at the end, and reads the peak. */
  private static long peakHeap() {
    System.gc();
    // Notifications arrive on a JMX thread shortly after the collection.
    long deadline = System.nanoTime() + 1_000_000_000L;
    while (PEAK_AFTER_GC.get() == 0 && System.nanoTime() < deadline) {
      Thread.onSpinWait();
    }
    return PEAK_AFTER_GC.get();
  }

  static long sizeOf(Path dir) {
    try (Stream<Path> files = Files.walk(dir)) {
      long sum = 0;
      for (Path p : (Iterable<Path>) files::iterator) {
        if (Files.isRegularFile(p)) {
          sum += Files.size(p);
        }
      }
      return sum;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static void delete(Path dir) {
    if (!Files.exists(dir)) {
      return;
    }
    try (Stream<Path> files = Files.walk(dir)) {
      for (Path p : files.sorted(Comparator.reverseOrder()).toList()) {
        Files.delete(p);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Parses a size such as {@code 512}, {@code 64k}, {@code 8m} or {@code 16g} (powers of 1024). */
  static long parseSize(String s) {
    String t = s.trim().toLowerCase(Locale.ROOT);
    int shift =
        switch (t.isEmpty() ? ' ' : t.charAt(t.length() - 1)) {
          case 'k' -> 10;
          case 'm' -> 20;
          case 'g' -> 30;
          default -> 0;
        };
    long n = Long.parseLong(shift == 0 ? t : t.substring(0, t.length() - 1));
    if (n < 1 || n > (Long.MAX_VALUE >> shift)) {
      throw new IllegalArgumentException("size out of range: " + s);
    }
    return n << shift;
  }

  static Options parse(String[] args) {
    Options d = Options.defaults(Path.of(System.getProperty("java.io.tmpdir")));
    List<Long> sizes = d.sizes();
    Path dir = d.dir();
    int entry = d.entryBytes();
    int batch = d.batchBytes();
    long segment = d.segmentBytes();
    int keep = d.keep();
    int tail = d.tail();
    int chunk = d.chunkBytes();
    for (int i = 0; i < args.length; i += 2) {
      if (i + 1 >= args.length) {
        throw new IllegalArgumentException("missing value for " + args[i]);
      }
      String v = args[i + 1];
      switch (args[i]) {
        case "--sizes" -> {
          List<Long> out = new ArrayList<>();
          for (String part : v.split(",", -1)) {
            out.add(parseSize(part));
          }
          sizes = out;
        }
        case "--dir" -> dir = Path.of(v);
        case "--entry-bytes" -> entry = Math.toIntExact(parseSize(v));
        case "--batch-bytes" -> batch = Math.toIntExact(parseSize(v));
        case "--segment-bytes" -> segment = parseSize(v);
        case "--keep" -> keep = Integer.parseInt(v);
        case "--tail" -> tail = Integer.parseInt(v);
        case "--chunk-bytes" -> chunk = Math.toIntExact(parseSize(v));
        default -> throw new IllegalArgumentException("unknown option " + args[i]);
      }
    }
    return new Options(sizes, dir, entry, batch, segment, keep, tail, chunk);
  }

  private static String size(long bytes) {
    if (bytes >= 1L << 30 && bytes % (1L << 30) == 0) {
      return (bytes >> 30) + " GiB";
    }
    if (bytes >= 1L << 20 && bytes % (1L << 20) == 0) {
      return (bytes >> 20) + " MiB";
    }
    if (bytes >= 1L << 10 && bytes % (1L << 10) == 0) {
      return (bytes >> 10) + " KiB";
    }
    return bytes + " B";
  }

  private static double ms(long nanos) {
    return nanos / 1e6;
  }

  private static double mib(long bytes) {
    return bytes / (double) (1 << 20);
  }

  private static double mibPerSecond(long bytes, long nanos) {
    return nanos == 0 ? 0 : mib(bytes) / (nanos / 1e9);
  }

  /** Runs every size, compaction then catch-up, and prints both tables. */
  public static void run(Options o, PrintStream out) {
    Path root = o.dir().resolve("keel-log-bench-" + ProcessHandle.current().pid());
    out.printf(
        Locale.ROOT,
        "java %s, %d cpus, max heap %.0f MiB, data in %s%n",
        System.getProperty("java.version"),
        Runtime.getRuntime().availableProcessors(),
        mib(Runtime.getRuntime().maxMemory()),
        o.dir());
    out.printf(
        Locale.ROOT,
        "entries of %d bytes synced every %s, segments of %s, %d entries kept after the"
            + " snapshot%n%n",
        o.entryBytes(),
        size(o.batchBytes()),
        size(o.segmentBytes()),
        o.keep());
    out.println(
        "heap MiB is the most heap in use right after a garbage collection during the case");
    out.println();
    out.println("compaction");
    out.printf(
        Locale.ROOT,
        "%-8s %10s %9s %10s %9s %9s %8s %8s %10s%n",
        "log",
        "entries",
        "segments",
        "write MB/s",
        "deleted",
        "pause ms",
        "fsyncs",
        "rewrites",
        "heap MiB");
    List<CatchUp> catchUps = new ArrayList<>();
    try {
      for (long size : o.sizes()) {
        Path dir = root.resolve("compaction");
        Compaction c =
            compaction(dir, size, o.entryBytes(), o.batchBytes(), o.segmentBytes(), o.keep());
        delete(dir);
        out.printf(
            Locale.ROOT,
            "%-8s %10d %9d %10.0f %9d %9.1f %8d %8d %10.1f%n",
            size(size),
            c.entries(),
            c.segmentsBefore(),
            mibPerSecond(c.logBytes(), c.writeNanos()),
            c.segmentsDeleted(),
            ms(c.pauseNanos()),
            c.pauseFsyncs(),
            c.logRewrites(),
            mib(c.peakHeapBytes()));
        out.flush();
      }
      out.println();
      out.printf(
          Locale.ROOT,
          "catch-up (snapshot chunks of %s, %d entries of %d bytes after the snapshot)%n",
          size(o.chunkBytes()),
          o.tail(),
          o.entryBytes());
      out.printf(
          Locale.ROOT,
          "%-8s %12s %11s %12s %10s %8s %9s %8s %10s %6s %10s%n",
          "state",
          "snapshot ms",
          "transfer s",
          "transfer MB/s",
          "restore s",
          "tail ms",
          "total s",
          "chunks",
          "wire MiB",
          "match",
          "heap MiB");
      for (long size : o.sizes()) {
        Path dir = root.resolve("catch-up");
        CatchUp c = catchUp(dir, size, o.tail(), o.entryBytes(), o.chunkBytes());
        delete(dir);
        catchUps.add(c);
        out.printf(
            Locale.ROOT,
            "%-8s %12.0f %11.2f %12.0f %10.2f %8.1f %9.2f %8d %10.0f %6s %10.1f%n",
            size(size),
            ms(c.leaderPauseNanos()),
            c.transferNanos() / 1e9,
            mibPerSecond(c.snapshotBytes(), c.transferNanos()),
            c.restoreNanos() / 1e9,
            ms(c.tailNanos()),
            c.totalNanos() / 1e9,
            c.chunks(),
            mib(c.wireBytes()),
            c.statesMatch() ? "yes" : "NO",
            mib(c.peakHeapBytes()));
        out.flush();
      }
    } finally {
      delete(root);
    }
    if (catchUps.stream().anyMatch(c -> !c.statesMatch())) {
      throw new IllegalStateException("a new server ended in a different state");
    }
  }

  /**
   * {@code LogBench [--sizes 1g,4g,16g] [--dir tmp] [--entry-bytes 1k] [--batch-bytes 1m]
   * [--segment-bytes 64m] [--keep 1024] [--tail 1000] [--chunk-bytes 1m]}.
   */
  public static void main(String[] args) {
    run(parse(args), System.out);
  }
}
