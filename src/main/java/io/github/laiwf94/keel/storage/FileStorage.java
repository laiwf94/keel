package io.github.laiwf94.keel.storage;

import io.github.laiwf94.keel.codec.ByteReader;
import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.codec.CodecException;
import io.github.laiwf94.keel.raft.Codec;
import io.github.laiwf94.keel.raft.Entry;
import io.github.laiwf94.keel.raft.HardState;
import io.github.laiwf94.keel.raft.Snapshot;
import io.github.laiwf94.keel.raft.Storage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32C;

/**
 * Durable storage in one directory.
 *
 * <ul>
 *   <li>{@code state} holds the term and vote. It is replaced atomically: write a temporary file,
 *       fsync it, rename it over the old one, fsync the directory.
 *   <li>{@code snapshot} holds the latest snapshot, replaced the same way.
 *   <li>{@code log} is append-only. Each record is {@code [length][crc32c][payload]}. Appends are
 *       buffered by the OS until {@link #sync()}, which calls fdatasync. Truncating a conflicting
 *       suffix truncates the file. After a snapshot the log is rewritten to hold only later
 *       entries.
 * </ul>
 *
 * <p>On startup a record that fails its length or CRC check is a torn final write if nothing valid
 * follows it, and the file is cut there; that data was never synced, so no peer was told about it.
 * If a valid record does follow, the damage is in the middle of the log and startup fails rather
 * than silently dropping entries the node may have acknowledged.
 */
public final class FileStorage implements Storage, AutoCloseable {
  private static final int HEADER = 8;
  // index, term, type and the data length prefix
  private static final int ENTRY_FIXED = 8 + 8 + 1 + 4;
  private static final int MAX_RECORD = Codec.MAX_ENTRY_BYTES + 64;
  private static final int MAX_STATE = 1024;

  private final Path dir;
  private final Path logPath;
  private final FileChannel lockChannel;
  private final FileLock lock;
  private FileChannel log;
  private long fileFirstIndex;
  private final List<Long> offsets = new ArrayList<>();
  private HardState pendingState;
  private boolean logDirty;
  private long fsyncs;
  private long tornBytesDropped;
  private State loaded;

  /** Opens (creating if needed) the storage directory and recovers its contents. */
  public FileStorage(Path dir) {
    this.dir = dir;
    this.logPath = dir.resolve("log");
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
    long snapIndex = snapshot.map(s -> s.meta().index()).orElse(0L);
    List<Entry> entries = new ArrayList<>();
    List<Long> recordOffsets = new ArrayList<>();
    long validEnd = scanLog(entries, recordOffsets);

    log =
        FileChannel.open(
            logPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    if (validEnd < log.size()) {
      tornBytesDropped = log.size() - validEnd;
      log.truncate(validEnd);
      log.force(true);
    }

    // Keep only entries after the snapshot. If the log does not hold the snapshot's last entry
    // with the same term, its entries conflict with the snapshot (we crashed between installing a
    // snapshot from the leader and rewriting the log) and are dropped.
    List<Entry> kept = entries;
    if (snapshot.isPresent()) {
      long snapTerm = snapshot.get().meta().term();
      int at = -1;
      for (int i = 0; i < entries.size(); i++) {
        if (entries.get(i).index() == snapIndex) {
          at = i;
        }
      }
      boolean firstAfter = !entries.isEmpty() && entries.get(0).index() == snapIndex + 1;
      if (at >= 0 && entries.get(at).term() == snapTerm) {
        kept = entries.subList(at + 1, entries.size());
      } else if (!firstAfter) {
        kept = List.of();
      }
    }
    if (kept.size() != entries.size()) {
      fileFirstIndex = snapIndex + 1;
      rewriteLog(kept);
    } else {
      offsets.addAll(recordOffsets);
      fileFirstIndex = entries.isEmpty() ? snapIndex + 1 : entries.get(0).index();
    }
    loaded = new State(hs, snapshot, kept);
  }

  /** Reads every valid record; returns the offset where valid data ends. */
  private long scanLog(List<Entry> out, List<Long> recordOffsets) throws IOException {
    if (!Files.exists(logPath)) {
      return 0;
    }
    try (FileChannel ch = FileChannel.open(logPath, StandardOpenOption.READ)) {
      long size = ch.size();
      long pos = 0;
      while (pos < size) {
        Optional<Entry> e = readRecord(ch, pos, size);
        if (e.isEmpty()) {
          long next = nextRecordAfterDamage(ch, pos, size);
          if (next >= 0) {
            throw new StorageException(
                "log record at byte "
                    + pos
                    + " is corrupt but a valid record follows at byte "
                    + next
                    + "; refusing to drop acknowledged entries");
          }
          return pos;
        }
        Entry entry = e.get();
        if (!out.isEmpty()) {
          Entry prev = out.get(out.size() - 1);
          if (entry.index() != prev.index() + 1 || entry.term() < prev.term()) {
            throw new StorageException(
                "log record at byte " + pos + " (" + entry + ") does not follow " + prev);
          }
        }
        out.add(entry);
        recordOffsets.add(pos);
        pos += HEADER + ENTRY_FIXED + entry.data().length;
      }
      return pos;
    }
  }

  /** Reads one record at {@code pos}, or empty if it is short or fails its checks. */
  private static Optional<Entry> readRecord(FileChannel ch, long pos, long size)
      throws IOException {
    if (size - pos < HEADER) {
      return Optional.empty();
    }
    ByteBuffer header = ByteBuffer.allocate(HEADER);
    readFully(ch, header, pos);
    int len = header.getInt(0);
    int crc = header.getInt(4);
    if (len < ENTRY_FIXED || len > MAX_RECORD || len > size - pos - HEADER) {
      return Optional.empty();
    }
    ByteBuffer body = ByteBuffer.allocate(len);
    readFully(ch, body, pos + HEADER);
    byte[] payload = body.array();
    if (crc32c(payload) != crc) {
      return Optional.empty();
    }
    try {
      ByteReader r = new ByteReader(payload);
      Entry e = Codec.readEntry(r);
      r.expectEnd();
      return Optional.of(e);
    } catch (CodecException ex) {
      return Optional.empty();
    }
  }

  /** If the damaged record's length is plausible and a valid record follows it, returns that. */
  private static long nextRecordAfterDamage(FileChannel ch, long pos, long size)
      throws IOException {
    if (size - pos < HEADER) {
      return -1;
    }
    ByteBuffer header = ByteBuffer.allocate(4);
    readFully(ch, header, pos);
    int len = header.getInt(0);
    if (len < ENTRY_FIXED || len > MAX_RECORD) {
      return -1;
    }
    long next = pos + HEADER + len;
    return next < size && readRecord(ch, next, size).isPresent() ? next : -1;
  }

  private static void readFully(FileChannel ch, ByteBuffer buf, long pos) throws IOException {
    while (buf.hasRemaining()) {
      int n = ch.read(buf, pos + buf.position());
      if (n < 0) {
        throw new IOException("unexpected end of file");
      }
    }
  }

  private static int crc32c(byte[] b) {
    CRC32C c = new CRC32C();
    c.update(b);
    return (int) c.getValue();
  }

  private static byte[] record(byte[] payload) {
    return new ByteWriter(HEADER + payload.length)
        .writeInt(payload.length)
        .writeInt(crc32c(payload))
        .writeRaw(payload)
        .toByteArray();
  }

  private static byte[] entryRecord(Entry e) {
    ByteWriter w = new ByteWriter(e.data().length + 32);
    Codec.writeEntry(w, e);
    return record(w.toByteArray());
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
    if (crc32c(payload) != crc) {
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
      ByteBuffer b = ByteBuffer.wrap(record(payload));
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

  private void rewriteLog(List<Entry> entries) throws IOException {
    Path tmp = dir.resolve("log.tmp");
    List<Long> newOffsets = new ArrayList<>();
    try (FileChannel ch =
        FileChannel.open(
            tmp,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      long pos = 0;
      for (Entry e : entries) {
        byte[] rec = entryRecord(e);
        newOffsets.add(pos);
        ByteBuffer b = ByteBuffer.wrap(rec);
        while (b.hasRemaining()) {
          ch.write(b);
        }
        pos += rec.length;
      }
      ch.force(true);
    }
    if (log != null) {
      log.close();
    }
    Files.move(tmp, logPath, StandardCopyOption.ATOMIC_MOVE);
    syncDirectory();
    fsyncs += 2;
    log = FileChannel.open(logPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
    offsets.clear();
    offsets.addAll(newOffsets);
    if (!entries.isEmpty()) {
      fileFirstIndex = entries.get(0).index();
    }
    logDirty = false;
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
    if (entries.isEmpty()) {
      return;
    }
    try {
      long first = entries.get(0).index();
      long lastInFile = fileFirstIndex + offsets.size() - 1;
      if (first <= lastInFile) {
        int keep = (int) (first - fileFirstIndex);
        if (keep < 0) {
          throw new IllegalStateException("cannot overwrite compacted entry " + first);
        }
        log.truncate(offsets.get(keep));
        offsets.subList(keep, offsets.size()).clear();
      } else if (first != lastInFile + 1) {
        throw new IllegalStateException("gap before entry " + first + " (last " + lastInFile + ")");
      }
      if (offsets.isEmpty()) {
        fileFirstIndex = first;
      }
      long pos = log.size();
      for (Entry e : entries) {
        byte[] rec = entryRecord(e);
        offsets.add(pos);
        ByteBuffer b = ByteBuffer.wrap(rec);
        while (b.hasRemaining()) {
          pos += log.write(b, pos);
        }
      }
      logDirty = true;
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
      fileFirstIndex = snapshot.meta().index() + 1;
      rewriteLog(remaining);
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
      if (logDirty) {
        log.force(false);
        fsyncs++;
        logDirty = false;
      }
    } catch (IOException e) {
      throw new StorageException("sync failed: " + e.getMessage(), e);
    }
  }

  /** Number of fsync calls made so far, for metrics. */
  public long fsyncs() {
    return fsyncs;
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
