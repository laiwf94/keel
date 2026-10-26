package io.github.laiwf94.keel.raft;

import java.util.Arrays;
import java.util.Objects;

/** A state machine snapshot. The data array is owned by the snapshot and never modified. */
public final class Snapshot {
  private final SnapshotMeta meta;
  private final byte[] data;

  public Snapshot(SnapshotMeta meta, byte[] data) {
    this.meta = Objects.requireNonNull(meta, "meta");
    this.data = Objects.requireNonNull(data, "data");
  }

  public SnapshotMeta meta() {
    return meta;
  }

  /** The state machine bytes. Callers must not modify the returned array. */
  public byte[] data() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Snapshot s && s.meta.equals(meta) && Arrays.equals(s.data, data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(meta, Arrays.hashCode(data));
  }

  @Override
  public String toString() {
    return "Snapshot{" + meta.index() + "@" + meta.term() + " " + data.length + "B}";
  }
}
