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

  /** Replaces the snapshot and the log with the snapshot plus the entries that follow it. */
  void saveSnapshot(Snapshot snapshot, List<Entry> remaining);

  void sync();
}
