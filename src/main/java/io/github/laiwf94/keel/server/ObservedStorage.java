package io.github.laiwf94.keel.server;

import io.github.laiwf94.keel.obs.Metrics;
import io.github.laiwf94.keel.raft.Entry;
import io.github.laiwf94.keel.raft.HardState;
import io.github.laiwf94.keel.raft.Snapshot;
import io.github.laiwf94.keel.raft.Storage;
import java.util.List;

/** Wraps a {@link Storage} and records how long each sync takes. */
final class ObservedStorage implements Storage {
  private final Storage inner;
  private final Metrics.Histogram syncSeconds;
  private boolean dirty;

  ObservedStorage(Storage inner, Metrics metrics) {
    this.inner = inner;
    this.syncSeconds =
        metrics.histogram("keel_storage_sync_seconds", "Time to make a batch of writes durable.");
  }

  @Override
  public State load() {
    return inner.load();
  }

  @Override
  public void saveHardState(HardState state) {
    dirty = true;
    inner.saveHardState(state);
  }

  @Override
  public void append(List<Entry> entries) {
    dirty |= !entries.isEmpty();
    inner.append(entries);
  }

  @Override
  public void saveSnapshot(Snapshot snapshot, List<Entry> remaining) {
    inner.saveSnapshot(snapshot, remaining);
  }

  @Override
  public void sync() {
    if (!dirty) {
      return;
    }
    long start = System.nanoTime();
    inner.sync();
    syncSeconds.observeNanos(System.nanoTime() - start);
    dirty = false;
  }
}
