package io.github.laiwf94.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}
