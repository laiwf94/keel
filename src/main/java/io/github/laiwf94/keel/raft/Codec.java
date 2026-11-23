package io.github.laiwf94.keel.raft;

import io.github.laiwf94.keel.codec.ByteReader;
import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.codec.CodecException;

/** Binary encodings of entries and snapshots shared by the disk format and the wire format. */
public final class Codec {
  /** Largest entry payload accepted from disk or the network. */
  public static final int MAX_ENTRY_BYTES = 8 << 20;

  /**
   * Largest snapshot in the single-record snapshot file written before snapshots were streamed.
   * Snapshots in the current file format have no limit but the disk.
   */
  public static final int MAX_LEGACY_SNAPSHOT_BYTES = 256 << 20;

  /** Largest InstallSnapshot chunk accepted from the network. */
  public static final int MAX_SNAPSHOT_CHUNK_BYTES = 8 << 20;

  private Codec() {}

  public static void writeEntry(ByteWriter w, Entry e) {
    w.writeLong(e.index()).writeLong(e.term()).writeByte(e.type().code()).writeBytes(e.data());
  }

  public static Entry readEntry(ByteReader r) {
    long index = r.readNonNegativeLong();
    long term = r.readNonNegativeLong();
    EntryType type = EntryType.fromCode(r.readByte());
    byte[] data = r.readBytes(MAX_ENTRY_BYTES);
    try {
      return new Entry(index, term, type, data);
    } catch (IllegalArgumentException e) {
      throw new CodecException(e.getMessage());
    }
  }

  /** Writes a snapshot as one value, the layout of the snapshot file before it was streamed. */
  public static void writeLegacySnapshot(ByteWriter w, Snapshot s) {
    writeSnapshotMeta(w, s.meta());
    w.writeBytes(s.data().toByteArray());
  }

  /** Reads a snapshot written by {@link #writeLegacySnapshot}. */
  public static Snapshot readLegacySnapshot(ByteReader r) {
    SnapshotMeta meta = readSnapshotMeta(r);
    return new Snapshot(meta, r.readBytes(MAX_LEGACY_SNAPSHOT_BYTES));
  }

  public static void writeSnapshotMeta(ByteWriter w, SnapshotMeta meta) {
    w.writeLong(meta.index()).writeLong(meta.term());
    meta.config().writeTo(w);
  }

  public static SnapshotMeta readSnapshotMeta(ByteReader r) {
    long index = r.readNonNegativeLong();
    long term = r.readNonNegativeLong();
    Configuration config = Configuration.readFrom(r);
    try {
      return new SnapshotMeta(index, term, config);
    } catch (IllegalArgumentException e) {
      throw new CodecException(e.getMessage());
    }
  }
}
