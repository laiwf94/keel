package io.github.laiwf94.keel.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Storage in memory that models a disk: writes are pending until {@link #sync()}, and {@link
 * #crash()} throws pending writes away. The simulator uses it to crash nodes between a write and
 * its fsync.
 */
public final class MemStorage implements Storage {
  private HardState hardState = HardState.INITIAL;
  private Snapshot snapshot;
  private final List<Entry> entries = new ArrayList<>();
  private final List<Runnable> pending = new ArrayList<>();
  private long syncs;

  @Override
  public State load() {
    if (!pending.isEmpty()) {
      throw new IllegalStateException("load with unsynced writes");
    }
    return new State(hardState, Optional.ofNullable(snapshot), entries);
  }

  @Override
  public void saveHardState(HardState state) {
    pending.add(() -> hardState = state);
  }

  @Override
  public void append(List<Entry> batch) {
    if (batch.isEmpty()) {
      return;
    }
    List<Entry> copy = List.copyOf(batch);
    pending.add(
        () -> {
          long base = snapshot == null ? 0 : snapshot.meta().index();
          long first = copy.get(0).index();
          int keep = (int) Math.max(0, first - base - 1);
          if (keep > entries.size()) {
            throw new IllegalStateException("gap before entry " + first);
          }
          entries.subList(keep, entries.size()).clear();
          entries.addAll(copy);
        });
  }

  @Override
  public void saveSnapshot(Snapshot s, List<Entry> remaining) {
    List<Entry> copy = List.copyOf(remaining);
    pending.add(
        () -> {
          snapshot = s;
          entries.clear();
          entries.addAll(copy);
        });
  }

  @Override
  public void sync() {
    if (pending.isEmpty()) {
      return;
    }
    pending.forEach(Runnable::run);
    pending.clear();
    syncs++;
  }

  /** Drops every write that was not synced, as a power failure would. */
  public void crash() {
    pending.clear();
  }

  /** Number of syncs that had something to write. */
  public long syncs() {
    return syncs;
  }
}
