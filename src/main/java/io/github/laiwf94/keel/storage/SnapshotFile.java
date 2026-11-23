package io.github.laiwf94.keel.storage;

import io.github.laiwf94.keel.codec.ByteReader;
import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.codec.CodecException;
import io.github.laiwf94.keel.raft.Codec;
import io.github.laiwf94.keel.raft.Snapshot;
import io.github.laiwf94.keel.raft.SnapshotData;
import io.github.laiwf94.keel.raft.SnapshotMeta;
import io.github.laiwf94.keel.raft.SnapshotSink;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

/**
 * The snapshot file, written as a stream so that a snapshot of any size is written, read, checked
 * and sent with a bounded buffer.
 *
 * <pre>
 *   head     magic "KSNP" (4 bytes), version (4 bytes)
 *   data     the state machine bytes
 *   meta     the snapshot metadata: index, term and configuration
 *   trailer  data length (8), meta length (4), CRC32C of data (4), CRC32C of meta (4),
 *            CRC32C of the 20 trailer bytes before it (4)
 * </pre>
 *
 * <p>The metadata goes after the data because a state machine serializes its bytes before the node
 * knows the configuration at the snapshot index, and a follower learns the metadata from the first
 * chunk but has the whole snapshot only with the last. The head magic cannot be the length field of
 * the single-record file written before snapshots were streamed, because that length is at most 256
 * MiB, so either layout is recognised from its first four bytes.
 */
final class SnapshotFile {
  static final int MAGIC = 0x4B53_4E50; // "KSNP"
  static final int VERSION = 1;
  static final int HEAD = 8;
  static final int TRAILER = 24;
  static final int MAX_META = 1 << 20;
  private static final int BUFFER = 256 << 10;

  private SnapshotFile() {}

  /** Whether {@code p} starts like a file in this layout rather than the older single record. */
  static boolean isStreamed(Path p) throws IOException {
    try (FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {
      ByteBuffer b = ByteBuffer.allocate(4);
      while (b.hasRemaining() && ch.read(b) >= 0) {
        // keep reading
      }
      return !b.hasRemaining() && b.getInt(0) == MAGIC;
    }
  }

  /**
   * Opens and checks a snapshot file: the head, the trailer, the metadata and the CRC of every data
   * byte, read a block at a time. The returned snapshot's data reads from the open file and holds
   * one reference, which the caller owns.
   *
   * @throws StorageException if any part fails its check
   */
  static Snapshot open(Path p) throws IOException {
    FileChannel ch = FileChannel.open(p, StandardOpenOption.READ);
    try {
      long size = ch.size();
      if (size < HEAD + TRAILER) {
        throw corrupt(p, "it is " + size + " bytes, too short to be a snapshot");
      }
      ByteBuffer head = ByteBuffer.wrap(readFully(ch, 0, HEAD));
      if (head.getInt() != MAGIC) {
        throw corrupt(p, "bad magic");
      }
      int version = head.getInt();
      if (version != VERSION) {
        throw corrupt(p, "unsupported version " + version);
      }
      byte[] trailer = readFully(ch, size - TRAILER, TRAILER);
      ByteBuffer t = ByteBuffer.wrap(trailer);
      if (crc(trailer, 0, TRAILER - 4) != t.getInt(TRAILER - 4)) {
        throw corrupt(p, "the trailer fails its checksum");
      }
      long dataLen = t.getLong();
      int metaLen = t.getInt();
      int dataCrc = t.getInt();
      int metaCrc = t.getInt();
      if (dataLen < 0
          || metaLen < 0
          || metaLen > MAX_META
          || dataLen != size - HEAD - TRAILER - metaLen) {
        throw corrupt(p, "the trailer's lengths do not match the file size " + size);
      }
      byte[] metaBytes = readFully(ch, HEAD + dataLen, metaLen);
      if (crc(metaBytes, 0, metaLen) != metaCrc) {
        throw corrupt(p, "the metadata fails its checksum");
      }
      SnapshotMeta meta;
      try {
        ByteReader r = new ByteReader(metaBytes);
        meta = Codec.readSnapshotMeta(r);
        r.expectEnd();
      } catch (CodecException e) {
        throw corrupt(p, e.getMessage());
      }
      CRC32C c = new CRC32C();
      ByteBuffer block = ByteBuffer.allocate(BUFFER);
      for (long at = HEAD; at < HEAD + dataLen; ) {
        block.clear().limit((int) Math.min(BUFFER, HEAD + dataLen - at));
        readFully(ch, at, block);
        block.flip();
        at += block.remaining();
        c.update(block);
      }
      if ((int) c.getValue() != dataCrc) {
        throw corrupt(p, "the data fails its checksum");
      }
      return new Snapshot(meta, new Data(ch, dataLen, p));
    } catch (IOException | RuntimeException e) {
      ch.close();
      throw e;
    }
  }

  private static StorageException corrupt(Path p, String why) {
    return new StorageException(p + " is corrupt: " + why);
  }

  static int crc(byte[] b, int off, int len) {
    CRC32C c = new CRC32C();
    c.update(b, off, len);
    return (int) c.getValue();
  }

  private static byte[] readFully(FileChannel ch, long at, int len) throws IOException {
    byte[] b = new byte[len];
    readFully(ch, at, ByteBuffer.wrap(b));
    return b;
  }

  private static void readFully(FileChannel ch, long at, ByteBuffer b) throws IOException {
    long pos = at;
    while (b.hasRemaining()) {
      int n = ch.read(b, pos);
      if (n < 0) {
        throw new IOException("unexpected end of snapshot file at " + pos);
      }
      pos += n;
    }
  }

  /**
   * Snapshot bytes read from an open snapshot file with positional reads, which may run on any
   * thread. The file stays open while references remain, so a leader can finish sending a snapshot
   * that storage has since replaced or a later rename has moved: on the file systems keel runs on,
   * an open file stays readable after it is renamed over or deleted.
   */
  static final class Data implements SnapshotData {
    private final FileChannel ch;
    private final long size;
    private final AtomicInteger refs = new AtomicInteger(1);
    private volatile Path path;

    Data(FileChannel ch, long size, Path path) {
      this.ch = ch;
      this.size = size;
      this.path = path;
    }

    Path path() {
      return path;
    }

    void movedTo(Path p) {
      path = p;
    }

    /** Whether the file is still open, for tests. */
    boolean isOpen() {
      return ch.isOpen();
    }

    @Override
    public long size() {
      return size;
    }

    @Override
    public byte[] read(long offset, int length) {
      SnapshotData.checkRange(offset, length, size);
      try {
        return readFully(ch, HEAD + offset, length);
      } catch (IOException e) {
        throw SnapshotData.failure("cannot read snapshot " + path, e);
      }
    }

    @Override
    public InputStream open() {
      return new InputStream() {
        private final ByteBuffer buf = ByteBuffer.allocate(64 << 10).limit(0);
        private long next;

        private boolean fill() throws IOException {
          if (buf.hasRemaining()) {
            return true;
          }
          if (next >= size) {
            return false;
          }
          buf.clear().limit((int) Math.min(buf.capacity(), size - next));
          readFully(ch, HEAD + next, buf);
          buf.flip();
          next += buf.remaining();
          return true;
        }

        @Override
        public int read() throws IOException {
          return fill() ? buf.get() & 0xff : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          Objects.checkFromIndexSize(off, len, b.length);
          if (len == 0) {
            return 0;
          }
          if (!fill()) {
            return -1;
          }
          int n = Math.min(len, buf.remaining());
          buf.get(b, off, n);
          return n;
        }

        @Override
        public int available() {
          return buf.remaining();
        }
      };
    }

    @Override
    public void retain() {
      if (refs.getAndUpdate(n -> n == 0 ? 0 : n + 1) == 0) {
        throw new IllegalStateException("snapshot " + path + " was already released");
      }
    }

    @Override
    public void release() {
      int left = refs.updateAndGet(n -> n == 0 ? -1 : n - 1);
      if (left < 0) {
        refs.set(0);
        throw new IllegalStateException("snapshot " + path + " released too often");
      }
      if (left == 0) {
        try {
          ch.close();
        } catch (IOException e) {
          throw new UncheckedIOException("cannot close snapshot " + path, e);
        }
      }
    }
  }

  /**
   * Writes a new snapshot file at a temporary path. The bytes are buffered and written in large
   * blocks with their CRC computed on the way; {@link #finish} appends the metadata and trailer and
   * fsyncs the file. The file becomes the node's snapshot only when {@link FileStorage} renames it
   * into place.
   */
  static final class Sink implements SnapshotSink {
    private final Path path;
    private final Runnable onFsync;
    private final Consumer<Data> onFinish;
    private final CRC32C crc = new CRC32C();
    private final ByteBuffer buf = ByteBuffer.allocate(BUFFER);
    private FileChannel ch;
    private long size;
    private Data finished;

    Sink(Path path, Runnable onFsync, Consumer<Data> onFinish) throws IOException {
      this.path = path;
      this.onFsync = onFsync;
      this.onFinish = onFinish;
      this.ch = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
      buf.putInt(MAGIC).putInt(VERSION);
    }

    Path path() {
      return path;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      Objects.checkFromIndexSize(off, len, b.length);
      if (ch == null) {
        throw new IllegalStateException("snapshot sink " + path + " is closed");
      }
      crc.update(b, off, len);
      size += len;
      try {
        while (len > 0) {
          int n = Math.min(len, buf.remaining());
          buf.put(b, off, n);
          off += n;
          len -= n;
          if (!buf.hasRemaining()) {
            flush();
          }
        }
      } catch (IOException e) {
        abort();
        throw new StorageException("cannot write snapshot " + path + ": " + e.getMessage(), e);
      }
    }

    private void flush() throws IOException {
      buf.flip();
      while (buf.hasRemaining()) {
        ch.write(buf);
      }
      buf.clear();
    }

    @Override
    public long size() {
      return size;
    }

    @Override
    public Snapshot finish(SnapshotMeta meta) {
      if (ch == null) {
        throw new IllegalStateException("snapshot sink " + path + " is closed");
      }
      ByteWriter mw = new ByteWriter(256);
      Codec.writeSnapshotMeta(mw, meta);
      // At most 64 members with bounded addresses, so far below MAX_META.
      byte[] metaBytes = mw.toByteArray();
      byte[] trailer = new byte[TRAILER];
      ByteBuffer t = ByteBuffer.wrap(trailer);
      t.putLong(size).putInt(metaBytes.length).putInt((int) crc.getValue());
      t.putInt(crc(metaBytes, 0, metaBytes.length));
      t.putInt(crc(trailer, 0, TRAILER - 4));
      try {
        writeAll(metaBytes);
        writeAll(trailer);
        flush();
        ch.force(true);
        onFsync.run();
        ch.close();
        ch = null;
        finished = new Data(FileChannel.open(path, StandardOpenOption.READ), size, path);
        onFinish.accept(finished);
      } catch (IOException e) {
        abort();
        throw new StorageException("cannot write snapshot " + path + ": " + e.getMessage(), e);
      }
      return new Snapshot(meta, finished);
    }

    private void writeAll(byte[] b) throws IOException {
      for (int off = 0; off < b.length; ) {
        int n = Math.min(b.length - off, buf.remaining());
        buf.put(b, off, n);
        off += n;
        if (!buf.hasRemaining()) {
          flush();
        }
      }
    }

    @Override
    public void abort() {
      if (finished != null) {
        return;
      }
      try {
        if (ch != null) {
          ch.close();
          ch = null;
        }
        Files.deleteIfExists(path);
      } catch (IOException e) {
        // Recovery deletes leftover temporary snapshot files, so nothing is lost by failing here.
      }
    }
  }
}
