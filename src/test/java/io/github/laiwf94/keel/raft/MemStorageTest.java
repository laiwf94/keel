package io.github.laiwf94.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class MemStorageTest {
  static Entry e(long index, long term) {
    return RaftLogTest.e(index, term);
  }

  @Test
  void aSyncMakesWritesDurableAndACrashLosesTheRest() {
    MemStorage s = new MemStorage();
    s.append(List.of(e(1, 1)));
    s.sync();
    s.append(List.of(e(2, 1)));
    assertEquals(1, s.unsyncedWrites());
    s.crash();
    assertEquals(0, s.unsyncedWrites());
    assertEquals(List.of(1L), s.load().entries().stream().map(Entry::index).toList());
    assertEquals(1, s.syncs());
  }

  @Test
  void aTornSyncKeepsAPrefixOfTheWrites() {
    MemStorage s = new MemStorage();
    s.append(List.of(e(1, 1)));
    s.saveHardState(new HardState(2, "a"));
    s.append(List.of(e(2, 2)));
    s.append(List.of());
    assertEquals(3, s.unsyncedWrites(), "an empty append is not a write");
    s.crash(2);
    Storage.State st = s.load();
    assertEquals(List.of(1L), st.entries().stream().map(Entry::index).toList());
    assertEquals(new HardState(2, "a"), st.hardState());
    assertEquals(0, s.syncs(), "a torn sync is not a sync");

    s.append(List.of(e(2, 2)));
    s.crash(1);
    assertEquals(2, s.load().entries().size());
  }

  @Test
  void aTornSyncCannotKeepMoreThanWasWritten() {
    MemStorage s = new MemStorage();
    s.append(List.of(e(1, 1)));
    assertThrows(IllegalArgumentException.class, () -> s.crash(2));
    assertThrows(IllegalArgumentException.class, () -> s.crash(-1));
    assertEquals(1, s.unsyncedWrites());
  }

  @Test
  void snapshotsAreCollectedInMemoryAndInstalledOnSync() throws java.io.IOException {
    MemStorage s = new MemStorage();
    SnapshotSink sink = s.beginSnapshot();
    byte[] data = new byte[1000];
    new java.util.Random(3).nextBytes(data);
    sink.write(data, 0, 300);
    try (java.io.OutputStream out = sink.stream()) {
      out.write(data[300]);
      out.write(data, 301, data.length - 301);
    }
    assertEquals(data.length, sink.size());
    SnapshotMeta meta = new SnapshotMeta(2, 1, Configuration.EMPTY);
    Snapshot snap = sink.finish(meta);
    assertThrows(IllegalStateException.class, () -> sink.write(new byte[1], 0, 1));
    assertThrows(IllegalStateException.class, () -> sink.finish(meta));
    sink.abort();
    s.saveSnapshot(snap, List.of());
    assertEquals(1, s.unsyncedWrites());
    s.sync();
    SnapshotData d = s.load().snapshot().orElseThrow().data();
    org.junit.jupiter.api.Assertions.assertArrayEquals(data, d.toByteArray());
    org.junit.jupiter.api.Assertions.assertArrayEquals(
        java.util.Arrays.copyOfRange(data, 10, 20), d.read(10, 10));
    assertThrows(IllegalArgumentException.class, () -> d.read(995, 6));
    try (java.io.InputStream in = d.open()) {
      org.junit.jupiter.api.Assertions.assertArrayEquals(data, in.readAllBytes());
    }
    d.retain();
    d.release();
    d.release();
    org.junit.jupiter.api.Assertions.assertArrayEquals(data, d.toByteArray());

    SnapshotSink dropped = s.beginSnapshot();
    dropped.write(data, 0, 10);
    dropped.abort();
    assertThrows(IllegalStateException.class, () -> dropped.write(data, 0, 1));
    assertThrows(IndexOutOfBoundsException.class, () -> s.beginSnapshot().write(data, 999, 2));
  }

  @Test
  void snapshotDataComparesByContent() {
    SnapshotData a = SnapshotData.of(new byte[200_000]);
    byte[] other = new byte[200_000];
    assertTrue(SnapshotData.sameBytes(a, SnapshotData.of(other)));
    other[150_000] = 1;
    assertTrue(!SnapshotData.sameBytes(a, SnapshotData.of(other)));
    assertTrue(!SnapshotData.sameBytes(a, SnapshotData.of(new byte[1])));
    SnapshotData huge =
        new SnapshotData() {
          @Override
          public long size() {
            return 3L << 30;
          }

          @Override
          public byte[] read(long offset, int length) {
            return new byte[length];
          }

          @Override
          public java.io.InputStream open() {
            return java.io.InputStream.nullInputStream();
          }
        };
    assertThrows(IllegalStateException.class, huge::toByteArray);
    huge.retain();
    huge.release();
  }
}
