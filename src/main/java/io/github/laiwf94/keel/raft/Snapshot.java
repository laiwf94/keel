package io.github.laiwf94.keel.raft;

import java.util.Objects;

/** A state machine snapshot: the position it covers and the state machine bytes at that point. */
public final class Snapshot {
  private final SnapshotMeta meta;
  private final SnapshotData data;

  public Snapshot(SnapshotMeta meta, SnapshotData data) {
    this.meta = Objects.requireNonNull(meta, "meta");
    this.data = Objects.requireNonNull(data, "data");
  }

  /** A snapshot of the bytes in {@code data}, which the caller must not modify afterwards. */
  public Snapshot(SnapshotMeta meta, byte[] data) {
    this(meta, SnapshotData.of(data));
  }

  public SnapshotMeta meta() {
    return meta;
  }

  /** The state machine bytes. */
  public SnapshotData data() {
    return data;
  }

  /** Equal when the metadata and every byte agree; compares the bytes a block at a time. */
  @Override
  public boolean equals(Object o) {
    return o instanceof Snapshot s && s.meta.equals(meta) && SnapshotData.sameBytes(s.data, data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(meta, data.size());
  }

  @Override
  public String toString() {
    return "Snapshot{" + meta.index() + "@" + meta.term() + " " + data.size() + "B}";
  }
}
