package io.github.laiwf94.keel.raft;

import java.util.List;
import java.util.Optional;

/**
 * Durable state for one node. Writes may be buffered; nothing written is guaranteed to survive a
 * crash until {@link #sync()} returns. The driver calls {@code sync()} after every batch of node
 * activity and before sending any message the batch produced, because a message such as a granted
 * vote or an append acknowledgement is a promise about durable state.
 */
public interface Storage {
  /** What a node finds on startup. Entries follow the snapshot index without gaps. */
  record State(HardState hardState, Optional<Snapshot> snapshot, List<Entry> entries) {
    public State {
      entries = List.copyOf(entries);
    }
  }

  State load();

  void saveHardState(HardState state);

  /**
   * Appends entries. The first entry may start at or before the current last index, in which case
   * everything from that index on is replaced.
   */
  void append(List<Entry> entries);

  /**
   * Starts a new snapshot. The caller writes the state machine bytes to the sink, seals them with
   * {@link SnapshotSink#finish} and installs the result with {@link #saveSnapshot}, or throws them
   * away with {@link SnapshotSink#abort}. Storage that keeps snapshots on disk writes the bytes
   * straight to a file, so a snapshot never has to fit in memory.
   */
  SnapshotSink beginSnapshot();

  /**
   * Replaces the snapshot and the log with the snapshot plus the entries that follow it. A snapshot
   * from this storage's own {@link #beginSnapshot} is installed without copying its bytes; any
   * other is copied in.
   */
  void saveSnapshot(Snapshot snapshot, List<Entry> remaining);

  void sync();
}
