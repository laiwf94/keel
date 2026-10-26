package io.github.laiwf94.keel.raft;

import java.util.Objects;

/** The last log position a snapshot covers and the configuration in force at that position. */
public record SnapshotMeta(long index, long term, Configuration config) {
  public SnapshotMeta {
    if (index < 0 || term < 0) {
      throw new IllegalArgumentException("negative index or term");
    }
    if (index == 0 && term != 0) {
      throw new IllegalArgumentException("index 0 must have term 0");
    }
    Objects.requireNonNull(config, "config");
  }
}
