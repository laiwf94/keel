package io.github.laiwf94.keel.storage;

import io.github.laiwf94.keel.codec.ByteReader;
import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.codec.CodecException;
import io.github.laiwf94.keel.raft.Codec;
import io.github.laiwf94.keel.raft.Entry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.zip.CRC32C;

/**
 * The Raft log as a sequence of segment files in one directory.
 *
 * <p>Each segment is named after the index of its first entry ({@code 00000000000000000001.seg})
 * and starts with a header: a magic number, a format version, the first index again and a CRC32C of
 * those 16 bytes. Records follow, each {@code [length][crc32c][payload]}, the same record as the
 * single-file log used. A segment takes records until the next one would take it past the segment
 * size; then a new segment is started. A segment always holds at least one record, so one larger
 * than the segment size gets a segment of its own.
 *
 * <p>Starting a segment makes the one before it durable first: it forces the old segment, creates
 * the new file with its header, forces that, and fsyncs the directory. So every segment but the
 * last is complete on disk, and only the last can end in a torn write. That is what lets recovery
 * keep the single-file guarantees across segment boundaries: a record that fails its checks in the
 * last segment, with no valid record after it, is a torn tail and is cut; one anywhere else, or one
 * followed by a valid record, is damage to data the node may have acknowledged, and opening fails.
 *
 * <p>Compaction deletes the segments that hold only entries at or below the snapshot index, from
 * the first one forward, with a directory fsync after each deletion, so a crash leaves a suffix of
 * the segments without a gap. Live data is never rewritten. The first segment left may still begin
 * with compacted entries; they stay on disk until the whole segment is below a later snapshot, and
 * {@link #firstIndex()} reports the first entry after the snapshot, not the first on disk.
 *
 * <p>Memory is one {@code int} per entry, the byte offset of its record within its segment, which
 * is what truncating a conflicting suffix and reading one entry back need.
 *
 * <p>Not thread-safe; the storage owner serializes calls.
 */
final class SegmentedLog implements AutoCloseable {
  /** Default size at which a segment is closed and the next one started. */
  static final long DEFAULT_SEGMENT_BYTES = 64L << 20;

  static final int RECORD_HEADER = 8;
  static final int SEGMENT_HEADER = 20;
  static final int MAGIC = 0x4B53_4547; // "KSEG"
  static final int VERSION = 1;
  static final String SUFFIX = ".seg";
  // index, term, type and the data length prefix
  private static final int ENTRY_FIXED = 8 + 8 + 1 + 4;
  private static final int MAX_RECORD = Codec.MAX_ENTRY_BYTES + 64;
  private static final Pattern NAME = Pattern.compile("\\d{20}\\.seg");

  /** One segment file: its first index and the offset of each record. */
  private static final class Segment {
    final long firstIndex;
    int count;
    int[] offsets = new int[16];
    long size = SEGMENT_HEADER;

    Segment(long firstIndex) {
      this.firstIndex = firstIndex;
    }

    long lastIndex() {
      return firstIndex + count - 1;
    }

    void add(long offset) {
      if (count == offsets.length) {
        offsets = Arrays.copyOf(offsets, count * 2);
      }
      offsets[count++] = (int) offset;
    }
  }

  private Path dir;
  private final long segmentBytes;
  private final List<Segment> segments = new ArrayList<>();

  /** Open on the last segment, or null when there is none. */
  private FileChannel active;

  private boolean dirty;
  private long nextIndex = 1;

  /** First entry not covered by a snapshot; entries below it in the first segment are dead. */
  private long floor;

  private long fsyncs;
  private long tornBytesDropped;

  private SegmentedLog(Path dir, long segmentBytes) {
    if (segmentBytes < SEGMENT_HEADER + RECORD_HEADER + ENTRY_FIXED || segmentBytes > 1L << 30) {
      throw new IllegalArgumentException("segment size out of range: " + segmentBytes);
    }
    this.dir = dir;
    this.segmentBytes = segmentBytes;
  }

  /**
   * Opens the log in {@code dir}, creating the directory if needed, and adds every entry it holds
   * to {@code out} in order. A torn tail of the last segment is cut.
   */
  static SegmentedLog open(Path dir, long segmentBytes, List<Entry> out) throws IOException {
    SegmentedLog log = new SegmentedLog(dir, segmentBytes);
    try {
      log.recover(out);
      return log;
    } catch (IOException | RuntimeException e) {
      log.closeQuietly();
      throw e;
    }
  }

  /**
   * Creates a log in the new directory {@code dir} holding {@code entries}, which start at {@code
   * firstIndex}, and makes it durable.
   */
  static SegmentedLog create(Path dir, long segmentBytes, long firstIndex, List<Entry> entries)
      throws IOException {
    Files.createDirectory(dir);
    SegmentedLog log = new SegmentedLog(dir, segmentBytes);
    try {
      log.nextIndex = firstIndex;
      log.append(entries);
      log.sync();
      return log;
    } catch (IOException | RuntimeException e) {
      log.closeQuietly();
      throw e;
    }
  }

  private void recover(List<Entry> out) throws IOException {
    if (!Files.isDirectory(dir)) {
      Files.createDirectories(dir);
      syncDirectory(dir.toAbsolutePath().getParent());
      fsyncs++;
    }
    List<Long> firsts = new ArrayList<>();
    try (DirectoryStream<Path> files = Files.newDirectoryStream(dir)) {
      for (Path p : files) {
        String name = p.getFileName().toString();
        if (NAME.matcher(name).matches()) {
          firsts.add(Long.parseLong(name.substring(0, 20)));
        }
      }
    }
    firsts.sort(null);
    for (int i = 0; i < firsts.size(); i++) {
      long first = firsts.get(i);
      boolean last = i == firsts.size() - 1;
      if (!segments.isEmpty()) {
        Segment prev = segments.get(segments.size() - 1);
        if (first != prev.lastIndex() + 1) {
          throw new StorageException(
              "segment "
                  + name(first)
                  + " does not follow segment "
                  + name(prev.firstIndex)
                  + ", which ends at entry "
                  + prev.lastIndex());
        }
      }
      recoverSegment(first, last, out);
    }
    if (!segments.isEmpty()) {
      Segment s = segments.get(segments.size() - 1);
      active =
          FileChannel.open(path(s.firstIndex), StandardOpenOption.READ, StandardOpenOption.WRITE);
      nextIndex = s.lastIndex() + 1;
    }
  }

  private void recoverSegment(long first, boolean last, List<Entry> out) throws IOException {
    Path p = path(first);
    Segment seg = new Segment(first);
    long size;
    long validEnd;
    try (FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {
      size = ch.size();
      if (!headerValid(ch, size, first)) {
        // A segment's header is forced before any record is written to it, so a bad header is
        // a torn creation only in the last segment and only with no valid record after it.
        if (!last) {
          throw new StorageException(p + " has a corrupt header and later segments follow");
        }
        if (readRecord(ch, SEGMENT_HEADER, size).isPresent()) {
          throw new StorageException(
              p + " has a corrupt header but a valid record follows it; refusing to drop entries");
        }
        validEnd = -1;
      } else {
        validEnd = readRecords(ch, SEGMENT_HEADER, size, p, last, first, out, seg);
      }
    }
    if (validEnd < 0) {
      tornBytesDropped += size;
      Files.delete(p);
      syncDirectory(dir);
      fsyncs++;
      return;
    }
    if (validEnd < size) {
      tornBytesDropped += size - validEnd;
      try (FileChannel ch = FileChannel.open(p, StandardOpenOption.WRITE)) {
        ch.truncate(validEnd);
        ch.force(true);
        fsyncs++;
      }
    }
    segments.add(seg);
  }

  private static boolean headerValid(FileChannel ch, long size, long first) throws IOException {
    if (size < SEGMENT_HEADER) {
      return false;
    }
    ByteBuffer h = ByteBuffer.allocate(SEGMENT_HEADER);
    readFully(ch, h, 0);
    return h.getInt(0) == MAGIC
        && h.getInt(4) == VERSION
        && h.getLong(8) == first
        && h.getInt(16) == crc32c(h.array(), 0, 16);
  }

  static byte[] header(long first) {
    ByteBuffer h = ByteBuffer.allocate(SEGMENT_HEADER);
    h.putInt(MAGIC).putInt(VERSION).putLong(first);
    h.putInt(crc32c(h.array(), 0, 16));
    return h.array();
  }

  /**
   * Reads valid records from {@code pos} into {@code out} and returns the offset where valid data
   * ends. A record that fails its checks ends the valid data if {@code last} is set and no valid
   * record follows it; otherwise the damage is in data that may have been acknowledged, and this
   * throws. {@code expect} is the index the first record must carry, or 0 for any.
   */
  private static long readRecords(
      FileChannel ch,
      long pos,
      long size,
      Path file,
      boolean last,
      long expect,
      List<Entry> out,
      Segment seg)
      throws IOException {
    while (pos < size) {
      Optional<Entry> e = readRecord(ch, pos, size);
      if (e.isEmpty()) {
        long next = nextRecordAfterDamage(ch, pos, size);
        if (next >= 0) {
          throw new StorageException(
              "log record at byte "
                  + pos
                  + " of "
                  + file
                  + " is corrupt but a valid record follows at byte "
                  + next
                  + "; refusing to drop acknowledged entries");
        }
        if (!last) {
          throw new StorageException(
              "log record at byte "
                  + pos
                  + " of "
                  + file
                  + " is corrupt and later segments follow; refusing to drop acknowledged entries");
        }
        break;
      }
      Entry entry = e.get();
      Entry prev = out.isEmpty() ? null : out.get(out.size() - 1);
      boolean follows =
          expect != 0 ? entry.index() == expect : prev == null || entry.index() == prev.index() + 1;
      if (!follows || (prev != null && entry.term() < prev.term())) {
        throw new StorageException(
            "log record at byte "
                + pos
                + " of "
                + file
                + " ("
                + entry
                + ") does not follow "
                + (prev != null ? prev : "the segment header"));
      }
      out.add(entry);
      if (seg != null) {
        seg.add(pos);
      }
      expect = entry.index() + 1;
      pos += RECORD_HEADER + ENTRY_FIXED + entry.data().length;
    }
    if (seg != null) {
      seg.size = pos;
    }
    return pos;
  }

  /**
   * Reads the single-file log written before segments, adding its entries to {@code out}, and
   * returns the offset where valid data ends. The same rules apply as to the last segment.
   */
  static long readLegacy(FileChannel ch, Path file, List<Entry> out) throws IOException {
    return readRecords(ch, 0, ch.size(), file, true, 0, out, null);
  }

  /** Reads one record at {@code pos}, or empty if it is short or fails its checks. */
  private static Optional<Entry> readRecord(FileChannel ch, long pos, long size)
      throws IOException {
    if (size - pos < RECORD_HEADER) {
      return Optional.empty();
    }
    ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER);
    readFully(ch, header, pos);
    int len = header.getInt(0);
    int crc = header.getInt(4);
    if (len < ENTRY_FIXED || len > MAX_RECORD || len > size - pos - RECORD_HEADER) {
      return Optional.empty();
    }
    ByteBuffer body = ByteBuffer.allocate(len);
    readFully(ch, body, pos + RECORD_HEADER);
    byte[] payload = body.array();
    if (crc32c(payload, 0, payload.length) != crc) {
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
    if (size - pos < RECORD_HEADER) {
      return -1;
    }
    ByteBuffer header = ByteBuffer.allocate(4);
    readFully(ch, header, pos);
    int len = header.getInt(0);
    if (len < ENTRY_FIXED || len > MAX_RECORD) {
      return -1;
    }
    long next = pos + RECORD_HEADER + len;
    return next < size && readRecord(ch, next, size).isPresent() ? next : -1;
  }

  static void readFully(FileChannel ch, ByteBuffer buf, long pos) throws IOException {
    while (buf.hasRemaining()) {
      int n = ch.read(buf, pos + buf.position());
      if (n < 0) {
        throw new IOException("unexpected end of file");
      }
    }
  }

  static int crc32c(byte[] b, int off, int len) {
    CRC32C c = new CRC32C();
    c.update(b, off, len);
    return (int) c.getValue();
  }

  static byte[] record(byte[] payload) {
    return new ByteWriter(RECORD_HEADER + payload.length)
        .writeInt(payload.length)
        .writeInt(crc32c(payload, 0, payload.length))
        .writeRaw(payload)
        .toByteArray();
  }

  static byte[] entryRecord(Entry e) {
    ByteWriter w = new ByteWriter(e.data().length + 32);
    Codec.writeEntry(w, e);
    return record(w.toByteArray());
  }

  static void syncDirectory(Path d) throws IOException {
    try (FileChannel c = FileChannel.open(d, StandardOpenOption.READ)) {
      c.force(true);
    }
  }

  static String name(long first) {
    return String.format("%020d%s", first, SUFFIX);
  }

  private Path path(long first) {
    return dir.resolve(name(first));
  }

  /**
   * Index of the first entry held after the last compaction, or of the next to append if the log is
   * empty.
   */
  long firstIndex() {
    return segments.isEmpty() ? nextIndex : Math.max(segments.get(0).firstIndex, floor);
  }

  /** Index the next appended entry must carry, unless it replaces a suffix. */
  long nextIndex() {
    return nextIndex;
  }

  /** Sets the index the first entry of an empty log must carry. */
  void startAt(long index) {
    if (!segments.isEmpty()) {
      throw new IllegalStateException("log is not empty");
    }
    nextIndex = index;
  }

  int segmentCount() {
    return segments.size();
  }

  /**
   * Appends entries. The first may start at or before the last index, in which case everything from
   * that index on is replaced.
   */
  void append(List<Entry> entries) throws IOException {
    if (entries.isEmpty()) {
      return;
    }
    long first = entries.get(0).index();
    if (first < nextIndex) {
      if (first < firstIndex()) {
        throw new IllegalStateException("cannot overwrite compacted entry " + first);
      }
      truncateFrom(first);
    } else if (first != nextIndex) {
      throw new IllegalStateException(
          "gap before entry " + first + " (last " + (nextIndex - 1) + ")");
    }
    for (Entry e : entries) {
      if (e.index() != nextIndex) {
        throw new IllegalStateException(
            "entry " + e.index() + " does not follow " + (nextIndex - 1));
      }
      byte[] rec = entryRecord(e);
      Segment s = segments.isEmpty() ? null : segments.get(segments.size() - 1);
      if (s == null || (s.count > 0 && s.size + rec.length > segmentBytes)) {
        s = roll(e.index());
      }
      ByteBuffer b = ByteBuffer.wrap(rec);
      long pos = s.size;
      while (b.hasRemaining()) {
        pos += active.write(b, pos);
      }
      s.add(s.size);
      s.size = pos;
      nextIndex++;
      dirty = true;
    }
  }

  /**
   * Reads entry {@code index} back from disk, or empty if the log does not hold it. Unsynced
   * entries are read from the page cache. A record that no longer reads back as the entry the index
   * says it is means the file changed under the node, and this throws.
   */
  Optional<Entry> read(long index) throws IOException {
    if (index < firstIndex() || index >= nextIndex) {
      return Optional.empty();
    }
    int k = segmentFor(index);
    Segment s = segments.get(k);
    long pos = s.offsets[(int) (index - s.firstIndex)];
    Optional<Entry> e;
    if (k == segments.size() - 1 && active != null) {
      e = readRecord(active, pos, s.size);
    } else {
      try (FileChannel ch = FileChannel.open(path(s.firstIndex), StandardOpenOption.READ)) {
        e = readRecord(ch, pos, s.size);
      }
    }
    if (e.isEmpty() || e.get().index() != index) {
      throw new StorageException(
          "entry " + index + " no longer reads back from " + path(s.firstIndex) + " at " + pos);
    }
    return e;
  }

  /** Position in {@link #segments} of the segment holding {@code index}, which the log holds. */
  private int segmentFor(long index) {
    int lo = 0;
    int hi = segments.size() - 1;
    while (lo < hi) {
      int mid = (lo + hi + 1) >>> 1;
      if (segments.get(mid).firstIndex <= index) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    return lo;
  }

  /**
   * Drops every entry up to and including {@code index}, which must not be past the last entry.
   * Segments holding nothing after {@code index} are deleted from the first one forward, each
   * deletion made durable by a directory fsync before the next, so a crash leaves the segments
   * after some point and never a gap. Returns the number of segments deleted.
   */
  int compactTo(long index) throws IOException {
    if (index >= nextIndex) {
      throw new IllegalStateException(
          "cannot compact to " + index + " past the last entry " + (nextIndex - 1));
    }
    int deleted = 0;
    while (!segments.isEmpty() && segments.get(0).lastIndex() <= index) {
      Segment s = segments.get(0);
      if (segments.size() == 1 && active != null) {
        // Nothing in it survives, so unsynced records in it need no force.
        dirty = false;
        active.close();
        active = null;
      }
      Files.delete(path(s.firstIndex));
      syncDirectory(dir);
      fsyncs++;
      segments.remove(0);
      deleted++;
    }
    floor = Math.max(floor, index + 1);
    return deleted;
  }

  /** Makes the active segment durable and starts a new one whose first entry is {@code first}. */
  private Segment roll(long first) throws IOException {
    if (active != null) {
      if (dirty) {
        active.force(false);
        fsyncs++;
        dirty = false;
      }
      active.close();
      active = null;
    }
    FileChannel ch =
        FileChannel.open(
            path(first),
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);
    try {
      ByteBuffer h = ByteBuffer.wrap(header(first));
      while (h.hasRemaining()) {
        ch.write(h, h.position());
      }
      ch.force(true);
      fsyncs++;
      syncDirectory(dir);
      fsyncs++;
    } catch (IOException | RuntimeException e) {
      ch.close();
      throw e;
    }
    active = ch;
    Segment s = new Segment(first);
    segments.add(s);
    return s;
  }

  /**
   * Drops every entry from {@code index} on. Later segments are deleted from the last one back,
   * each deletion made durable before the next, so a crash leaves a prefix of the old log; then the
   * segment holding {@code index} is cut.
   */
  private void truncateFrom(long index) throws IOException {
    int k = segments.size() - 1;
    while (segments.get(k).firstIndex > index) {
      k--;
    }
    for (int j = segments.size() - 1; j > k; j--) {
      if (active != null) {
        active.close();
        active = null;
      }
      Files.delete(path(segments.get(j).firstIndex));
      syncDirectory(dir);
      fsyncs++;
      segments.remove(j);
    }
    Segment s = segments.get(k);
    if (active == null) {
      active =
          FileChannel.open(path(s.firstIndex), StandardOpenOption.READ, StandardOpenOption.WRITE);
    }
    int keep = (int) (index - s.firstIndex);
    long cut = s.offsets[keep];
    active.truncate(cut);
    s.count = keep;
    s.size = cut;
    nextIndex = index;
    dirty = true;
  }

  /** Forces appended data to disk, if there is any. */
  void sync() throws IOException {
    if (dirty) {
      active.force(false);
      fsyncs++;
      dirty = false;
    }
  }

  /** Points the log at {@code newDir} after its directory was renamed there. */
  void movedTo(Path newDir) {
    this.dir = newDir;
  }

  long fsyncs() {
    return fsyncs;
  }

  long tornBytesDropped() {
    return tornBytesDropped;
  }

  private void closeQuietly() {
    try {
      close();
    } catch (IOException ignored) {
      // Already failing; the original error is the one worth reporting.
    }
  }

  @Override
  public void close() throws IOException {
    if (active != null) {
      active.close();
      active = null;
    }
  }
}
