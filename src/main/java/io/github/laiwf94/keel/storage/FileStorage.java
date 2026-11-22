package io.github.laiwf94.keel.storage;

import io.github.laiwf94.keel.codec.ByteReader;
import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.codec.CodecException;
import io.github.laiwf94.keel.raft.Codec;
import io.github.laiwf94.keel.raft.Entry;
import io.github.laiwf94.keel.raft.HardState;
import io.github.laiwf94.keel.raft.Snapshot;
import io.github.laiwf94.keel.raft.SnapshotMeta;
import io.github.laiwf94.keel.raft.Storage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Durable storage in one directory.
 *
 * <ul>
 *   <li>{@code state} holds the term and vote. It is replaced atomically: write a temporary file,
 *       fsync it, rename it over the old one, fsync the directory.
 *   <li>{@code snapshot} holds the latest snapshot, replaced the same way.
 *   <li>{@code wal} holds the log as fixed-size segment files; see {@link SegmentedLog}. Appends
 *       are buffered by the OS until {@link #sync()}, which calls fdatasync. Truncating a
 *       conflicting suffix deletes the later segments and cuts the one holding the first entry to
 *       drop. A snapshot that the log agrees with compacts it by deleting the segments wholly at or
 *       below the snapshot index; live entries are never rewritten. Only a snapshot that conflicts
 *       with the log (one installed from the leader that replaces it) writes a new log into {@code
 *       wal.new}, and the two directories are swapped by renames.
 * </ul>
 *
 * <p>On startup a record that fails its length or CRC check is a torn final write if it is in the
 * last segment and nothing valid follows it, and the segment is cut there; that data was never
 * synced, so no peer was told about it. Anywhere else the damage is in data the node may have
 * acknowledged, and startup fails rather than silently dropping entries.
 *
 * <p>A directory written before the log was segmented has a single {@code log} file of the same
 * records; it is read with the same rules and moved into segments on first open.
 */
public final class FileStorage implements Storage, AutoCloseable {
  private static final int HEADER = SegmentedLog.RECORD_HEADER;
  private static final int MAX_STATE = 1024;

  private final Path dir;
  private final Path wal;
  private final Path walNew;
  private final Path walOld;
  private final long segmentBytes;
  private final FileChannel lockChannel;
  private final FileLock lock;
  private SegmentedLog log;
  private HardState pendingState;
  private long fsyncs;
  private long tornBytesDropped;
  private long segmentsCompacted;
  private long logRewrites;
  private long snapIndex;
  private long snapTerm;
  private State loaded;

  /** Opens (creating if needed) the storage directory and recovers its contents. */
  public FileStorage(Path dir) {
    this(dir, SegmentedLog.DEFAULT_SEGMENT_BYTES);
  }

  /**
   * Opens the storage directory with log segments of {@code segmentBytes}. The size applies to
   * segments started from now on; existing segments are read whatever their size.
   */
  public FileStorage(Path dir, long segmentBytes) {
    this.dir = dir;
    this.wal = dir.resolve("wal");
    this.walNew = dir.resolve("wal.new");
    this.walOld = dir.resolve("wal.old");
    this.segmentBytes = segmentBytes;
    FileChannel lc = null;
    FileLock fl = null;
    try {
      Files.createDirectories(dir);
      lc =
          FileChannel.open(
              dir.resolve("lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
      try {
        fl = lc.tryLock();
      } catch (OverlappingFileLockException e) {
        fl = null;
      }
      if (fl == null) {
        throw new StorageException(dir + " is in use by another process");
      }
      this.lockChannel = lc;
      this.lock = fl;
      recover();
    } catch (IOException | RuntimeException e) {
      closeQuietly(fl, lc);
      if (e instanceof StorageException se) {
        throw se;
      }
      throw new StorageException("cannot open " + dir + ": " + e.getMessage(), e);
    }
  }

  private void closeQuietly(FileLock fl, FileChannel lc) {
    try {
      if (log != null) {
        log.close();
      }
      if (fl != null) {
        fl.release();
      }
      if (lc != null) {
        lc.close();
      }
    } catch (IOException ignored) {
      // Already failing; the original error is the one worth reporting.
    }
  }

  private void recover() throws IOException {
    HardState hs = readState();
    Optional<Snapshot> snapshot = readSnapshot();
    snapIndex = snapshot.map(s -> s.meta().index()).orElse(0L);
    snapTerm = snapshot.map(s -> s.meta().term()).orElse(0L);
    finishRewrite();
    migrateLegacyLog();
    List<Entry> entries = new ArrayList<>();
    log = SegmentedLog.open(wal, segmentBytes, entries);
    tornBytesDropped += log.tornBytesDropped();

    // Keep only entries after the snapshot. The first segment may begin with entries the
    // snapshot covers, and a crash during compaction may leave whole segments of them; those are
    // deleted now. If the log does not hold the snapshot's last entry with the same term, its
    // entries conflict with the snapshot (we crashed between installing a snapshot from the
    // leader and replacing the log) and are dropped.
    List<Entry> kept = entries;
    if (entries.isEmpty()) {
      // A crash right after a segment was started can leave one with a header and no records.
      if (log.segmentCount() == 0) {
        log.startAt(snapIndex + 1);
      } else if (log.nextIndex() != snapIndex + 1) {
        rewriteLog(snapIndex + 1, kept);
      }
    } else if (snapshot.isPresent()) {
      int at = -1;
      for (int i = 0; i < entries.size(); i++) {
        if (entries.get(i).index() == snapIndex) {
          at = i;
        }
      }
      if (at >= 0 && entries.get(at).term() == snapTerm) {
        kept = entries.subList(at + 1, entries.size());
        segmentsCompacted += log.compactTo(snapIndex);
      } else if (entries.get(0).index() != snapIndex + 1) {
        kept = List.of();
        rewriteLog(snapIndex + 1, kept);
      }
    }
    loaded = new State(hs, snapshot, kept);
  }

  /**
   * Completes or abandons a log rewrite a crash interrupted. {@code wal.new} is renamed into place
   * only once it is complete and only after {@code wal} was renamed away, so {@code wal.new}
   * without {@code wal} is complete, and {@code wal.new} beside {@code wal} is not.
   */
  private void finishRewrite() throws IOException {
    if (!Files.exists(wal) && Files.isDirectory(walNew)) {
      Files.move(walNew, wal, StandardCopyOption.ATOMIC_MOVE);
      syncDirectory();
      fsyncs++;
    }
    boolean removed = deleteTree(walNew);
    removed |= deleteTree(walOld);
    if (removed) {
      syncDirectory();
      fsyncs++;
    }
  }

  /** Moves a single-file {@code log} from before segments into {@code wal}. */
  private void migrateLegacyLog() throws IOException {
    Path legacy = dir.resolve("log");
    if (!Files.isRegularFile(legacy)) {
      return;
    }
    if (!Files.exists(wal)) {
      List<Entry> entries = new ArrayList<>();
      try (FileChannel ch = FileChannel.open(legacy, StandardOpenOption.READ)) {
        long end = SegmentedLog.readLegacy(ch, legacy, entries);
        tornBytesDropped += ch.size() - end;
      }
      long first = entries.isEmpty() ? 1 : entries.get(0).index();
      try (SegmentedLog moved = SegmentedLog.create(walNew, segmentBytes, first, entries)) {
        fsyncs += moved.fsyncs();
      }
      Files.move(walNew, wal, StandardCopyOption.ATOMIC_MOVE);
      syncDirectory();
      fsyncs++;
    }
    // wal exists, so a migration before a crash completed.
    Files.delete(legacy);
    syncDirectory();
    fsyncs++;
  }

  /** Deletes a directory of segment files, if it exists; returns whether it did. */
  private static boolean deleteTree(Path d) throws IOException {
    if (!Files.isDirectory(d)) {
      return false;
    }
    try (DirectoryStream<Path> files = Files.newDirectoryStream(d)) {
      for (Path f : files) {
        Files.delete(f);
      }
    }
    Files.delete(d);
    return true;
  }

  private HardState readState() throws IOException {
    Path p = dir.resolve("state");
    if (!Files.exists(p)) {
      return HardState.INITIAL;
    }
    byte[] payload = readChecked(p, MAX_STATE);
    try {
      ByteReader r = new ByteReader(payload);
      HardState hs = new HardState(r.readNonNegativeLong(), r.readString(64));
      r.expectEnd();
      return hs;
    } catch (CodecException | IllegalArgumentException e) {
      throw new StorageException(p + " is corrupt: " + e.getMessage(), e);
    }
  }

  private Optional<Snapshot> readSnapshot() throws IOException {
    Path p = dir.resolve("snapshot");
    if (!Files.exists(p)) {
      return Optional.empty();
    }
    byte[] payload = readChecked(p, Codec.MAX_SNAPSHOT_BYTES + 64 * 1024);
    try {
      ByteReader r = new ByteReader(payload);
      Snapshot s = Codec.readSnapshot(r);
      r.expectEnd();
      return Optional.of(s);
    } catch (CodecException e) {
      throw new StorageException(p + " is corrupt: " + e.getMessage(), e);
    }
  }

  /** Reads a file written by {@link #replaceAtomically}, checking its size before reading it. */
  private static byte[] readChecked(Path p, int max) throws IOException {
    long size = Files.size(p);
    if (size < HEADER || size > (long) max + HEADER) {
      throw new StorageException(p + " has an impossible size " + size);
    }
    byte[] all = Files.readAllBytes(p);
    ByteBuffer b = ByteBuffer.wrap(all);
    int len = b.getInt();
    int crc = b.getInt();
    if (len != all.length - HEADER) {
      throw new StorageException(p + " is truncated");
    }
    byte[] payload = new byte[len];
    b.get(payload);
    if (SegmentedLog.crc32c(payload, 0, payload.length) != crc) {
      throw new StorageException(p + " fails its checksum");
    }
    return payload;
  }

  private void replaceAtomically(String name, byte[] payload) throws IOException {
    Path tmp = dir.resolve(name + ".tmp");
    try (FileChannel ch =
        FileChannel.open(
            tmp,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      ByteBuffer b = ByteBuffer.wrap(SegmentedLog.record(payload));
      while (b.hasRemaining()) {
        ch.write(b);
      }
      ch.force(true);
    }
    Files.move(tmp, dir.resolve(name), StandardCopyOption.ATOMIC_MOVE);
    syncDirectory();
    fsyncs += 2;
  }

  private void syncDirectory() throws IOException {
    try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
      d.force(true);
    }
  }

  /**
   * Replaces the log with {@code entries}, which start at {@code first}: they are written to a new
   * directory, made durable, and swapped in with two renames, each followed by a directory fsync.
   */
  private void rewriteLog(long first, List<Entry> entries) throws IOException {
    deleteTree(walNew);
    SegmentedLog fresh = SegmentedLog.create(walNew, segmentBytes, first, entries);
    log.close();
    fsyncs += log.fsyncs();
    log = fresh;
    Files.move(wal, walOld, StandardCopyOption.ATOMIC_MOVE);
    syncDirectory();
    Files.move(walNew, wal, StandardCopyOption.ATOMIC_MOVE);
    fresh.movedTo(wal);
    syncDirectory();
    fsyncs += 2;
    logRewrites++;
    // Recovery deletes wal.old if a crash leaves it, so this needs no fsync of its own.
    deleteTree(walOld);
  }

  /**
   * Whether the log already is {@code snapshot} followed by {@code remaining}: it holds the
   * snapshot's last entry with the same term (or an earlier compaction already dropped it at the
   * same point) and then exactly the entries of {@code remaining}. Within that range only the first
   * and last entries are read back and compared; by the Log Matching property two logs that agree
   * on an entry's index and term agree on every entry before it, and the node appends through this
   * storage, so the entries between agree too.
   */
  private boolean logIs(SnapshotMeta meta, List<Entry> remaining) throws IOException {
    long index = meta.index();
    long last = remaining.isEmpty() ? index : remaining.get(remaining.size() - 1).index();
    if (log.nextIndex() - 1 != last
        || (!remaining.isEmpty() && remaining.get(0).index() != index + 1)) {
      return false;
    }
    boolean base =
        index < log.firstIndex()
            ? index == snapIndex && meta.term() == snapTerm
            : log.read(index).map(e -> e.term() == meta.term()).orElse(false);
    if (!base || remaining.isEmpty()) {
      return base;
    }
    return log.read(index + 1).map(remaining.get(0)::equals).orElse(false)
        && log.read(last).map(remaining.get(remaining.size() - 1)::equals).orElse(false);
  }

  @Override
  public State load() {
    if (loaded == null) {
      throw new IllegalStateException("load may be called once, right after opening");
    }
    State s = loaded;
    loaded = null;
    return s;
  }

  @Override
  public void saveHardState(HardState state) {
    pendingState = state;
  }

  @Override
  public void append(List<Entry> entries) {
    try {
      log.append(entries);
    } catch (IOException e) {
      throw new StorageException("append failed: " + e.getMessage(), e);
    }
  }

  @Override
  public void saveSnapshot(Snapshot snapshot, List<Entry> remaining) {
    try {
      ByteWriter w = new ByteWriter(snapshot.data().length + 256);
      Codec.writeSnapshot(w, snapshot);
      replaceAtomically("snapshot", w.toByteArray());
      // The snapshot is durable before any segment goes, so a crash in between leaves segments
      // that recovery recognises as covered by it and deletes.
      SnapshotMeta meta = snapshot.meta();
      if (logIs(meta, remaining)) {
        segmentsCompacted += log.compactTo(meta.index());
      } else {
        rewriteLog(meta.index() + 1, remaining);
      }
      snapIndex = meta.index();
      snapTerm = meta.term();
    } catch (IOException e) {
      throw new StorageException("snapshot failed: " + e.getMessage(), e);
    }
  }

  @Override
  public void sync() {
    try {
      if (pendingState != null) {
        ByteWriter w = new ByteWriter(80);
        w.writeLong(pendingState.term()).writeString(pendingState.votedFor());
        replaceAtomically("state", w.toByteArray());
        pendingState = null;
      }
      log.sync();
    } catch (IOException e) {
      throw new StorageException("sync failed: " + e.getMessage(), e);
    }
  }

  /** Number of log segment files, for metrics and tests. */
  public int segmentCount() {
    return log.segmentCount();
  }

  /** Number of segment files deleted by compaction since opening, for metrics and tests. */
  public long segmentsCompacted() {
    return segmentsCompacted;
  }

  /** Number of times the log was replaced wholesale since opening, for metrics and tests. */
  public long logRewrites() {
    return logRewrites;
  }

  /** Number of fsync calls made so far, for metrics. */
  public long fsyncs() {
    return fsyncs + log.fsyncs();
  }

  /** Bytes cut from a torn log tail during recovery. */
  public long tornBytesDropped() {
    return tornBytesDropped;
  }

  @Override
  public void close() {
    try {
      log.close();
      lock.release();
      lockChannel.close();
    } catch (IOException e) {
      throw new StorageException("close failed: " + e.getMessage(), e);
    }
  }
}
