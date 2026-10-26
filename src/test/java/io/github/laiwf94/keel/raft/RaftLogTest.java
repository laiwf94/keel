package io.github.laiwf94.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class RaftLogTest {
  static final Configuration C0 =
      Configuration.ofVoters(List.of(new Member("a", "a:1", "a:2", true)));

  static Entry e(long index, long term) {
    return new Entry(index, term, EntryType.COMMAND, new byte[] {(byte) index});
  }

  static Entry cfg(long index, long term, Configuration c) {
    return new Entry(index, term, EntryType.CONFIG, c.encode());
  }

  static RaftLog log(long... terms) {
    List<Entry> es = new ArrayList<>();
    for (int i = 0; i < terms.length; i++) {
      es.add(e(i + 1, terms[i]));
    }
    return new RaftLog(new SnapshotMeta(0, 0, C0), es);
  }

  @Test
  void termLookupsIncludeTheSnapshotBoundary() {
    RaftLog l = log(1, 1, 2);
    assertEquals(1, l.firstIndex());
    assertEquals(3, l.lastIndex());
    assertEquals(2, l.lastTerm());
    assertEquals(0, l.termAt(0));
    assertEquals(2, l.termAt(3));
    assertEquals(RaftLog.UNKNOWN, l.termAt(4));
    l.compactTo(2);
    assertEquals(1, l.termAt(2));
    assertEquals(RaftLog.UNKNOWN, l.termAt(1));
    assertTrue(l.matches(2, 1));
    assertFalse(l.matches(1, 1));
    assertThrows(IndexOutOfBoundsException.class, () -> l.entry(2));
    assertThrows(IndexOutOfBoundsException.class, () -> l.entry(4));
    assertEquals(3, l.entry(3).index());
  }

  @Test
  void slicesRespectCountAndBytesButAlwaysMakeProgress() {
    RaftLog l = log(1, 1, 1, 1, 1);
    assertEquals(2, l.slice(1, 2, 1 << 20).size());
    assertEquals(1, l.slice(1, 10, 1).size());
    assertEquals(2, l.slice(1, 10, 66).size());
    assertEquals(5, l.slice(1, 10, 1 << 20).size());
    assertTrue(l.slice(6, 10, 100).isEmpty());
    l.compactTo(2);
    assertThrows(IndexOutOfBoundsException.class, () -> l.slice(2, 1, 1));
  }

  @Test
  void appendChecksOrder() {
    RaftLog l = log(1, 2);
    assertThrows(IllegalArgumentException.class, () -> l.append(e(4, 2)));
    assertThrows(IllegalArgumentException.class, () -> l.append(e(3, 1)));
    l.append(e(3, 2));
    assertEquals(3, l.lastIndex());
  }

  @Test
  void leaderAppendKeepsMatchingEntriesAndCutsAtConflict() {
    RaftLog l = log(1, 1, 1, 1);
    // Duplicate delivery of entries we already have writes nothing.
    assertTrue(l.appendFromLeader(List.of(e(2, 1), e(3, 1)), 0).isEmpty());
    assertEquals(4, l.lastIndex());
    // A conflict at 3 cuts 3 and 4, then writes the new suffix.
    List<Entry> written = l.appendFromLeader(List.of(e(2, 1), e(3, 2), e(4, 2), e(5, 2)), 2);
    assertEquals(List.of(e(3, 2), e(4, 2), e(5, 2)), written);
    assertEquals(5, l.lastIndex());
    assertEquals(2, l.termAt(3));
    // A shorter, stale message does not truncate the matching suffix.
    assertTrue(l.appendFromLeader(List.of(e(3, 2)), 2).isEmpty());
    assertEquals(5, l.lastIndex());
  }

  @Test
  void conflictWithCommittedEntryIsABug() {
    RaftLog l = log(1, 1, 1);
    assertThrows(IllegalStateException.class, () -> l.appendFromLeader(List.of(e(2, 2)), 2));
  }

  @Test
  void entriesInsideTheSnapshotAreSkipped() {
    RaftLog l = log(1, 1, 1);
    l.compactTo(2);
    List<Entry> written = l.appendFromLeader(List.of(e(1, 1), e(2, 1), e(3, 1), e(4, 1)), 2);
    assertEquals(List.of(e(4, 1)), written);
  }

  @Test
  void truncation() {
    RaftLog l = log(1, 1, 1);
    l.truncateFrom(9);
    assertEquals(3, l.lastIndex());
    l.truncateFrom(2);
    assertEquals(1, l.lastIndex());
    l.compactTo(1);
    assertThrows(IllegalArgumentException.class, () -> l.truncateFrom(1));
  }

  @Test
  void termSearches() {
    RaftLog l = log(1, 1, 2, 2, 2, 4);
    assertEquals(3, l.firstIndexOfTermAt(5));
    assertEquals(1, l.firstIndexOfTermAt(2));
    assertEquals(5, l.lastIndexOfTerm(2));
    assertEquals(RaftLog.UNKNOWN, l.lastIndexOfTerm(3));
    assertEquals(0, l.lastIndexOfTerm(0));
    l.compactTo(4);
    assertEquals(4, l.firstIndexOfTermAt(4));
    assertEquals(5, l.firstIndexOfTermAt(5));
  }

  @Test
  void upToDateComparesTermThenLength() {
    RaftLog l = log(1, 2);
    assertTrue(l.isUpToDate(1, 3));
    assertTrue(l.isUpToDate(2, 2));
    assertTrue(l.isUpToDate(5, 2));
    assertFalse(l.isUpToDate(1, 2));
    assertFalse(l.isUpToDate(9, 1));
  }

  @Test
  void compaction() {
    RaftLog l = log(1, 1, 2);
    assertSame(l.snapshot(), l.compactTo(0));
    SnapshotMeta m = l.compactTo(2);
    assertEquals(2, m.index());
    assertEquals(1, m.term());
    assertEquals(3, l.firstIndex());
    assertEquals(List.of(e(3, 2)), l.entries());
    assertThrows(IllegalArgumentException.class, () -> l.compactTo(4));
  }

  @Test
  void installSnapshotKeepsMatchingSuffixOrDiscardsAll() {
    RaftLog l = log(1, 1, 2, 2);
    l.installSnapshot(new SnapshotMeta(2, 1, C0));
    assertEquals(4, l.lastIndex());
    assertEquals(3, l.firstIndex());

    RaftLog other = log(1, 1, 2, 2);
    other.installSnapshot(new SnapshotMeta(3, 3, C0));
    assertEquals(3, other.lastIndex());
    assertEquals(3, other.lastTerm());
    assertTrue(other.entries().isEmpty());

    RaftLog shortLog = log(1);
    shortLog.installSnapshot(new SnapshotMeta(10, 4, C0));
    assertEquals(10, shortLog.lastIndex());
  }

  @Test
  void configurationsRollBackOnTruncationAndSurviveCompaction() {
    Configuration c1 = C0.withLearner(new Member("b", "b:1", "b:2", false));
    Configuration c2 = c1.withPromoted("b");
    RaftLog l = new RaftLog(new SnapshotMeta(0, 0, C0), List.of());
    l.append(e(1, 1));
    l.append(cfg(2, 1, c1));
    l.append(e(3, 1));
    l.append(cfg(4, 1, c2));
    assertEquals(c2, l.latestConfig());
    assertEquals(4, l.latestConfigIndex());
    assertEquals(C0, l.configAt(1));
    assertEquals(c1, l.configAt(3));
    l.truncateFrom(4);
    assertEquals(c1, l.latestConfig());
    assertEquals(2, l.latestConfigIndex());
    SnapshotMeta m = l.compactTo(3);
    assertEquals(c1, m.config());
    assertEquals(c1, l.latestConfig());
    assertEquals(3, l.latestConfigIndex());
    l.append(cfg(4, 2, c2));
    l.installSnapshot(new SnapshotMeta(4, 2, c2));
    assertEquals(c2, l.latestConfig());
    l.append(cfg(5, 2, c1));
    l.installSnapshot(new SnapshotMeta(9, 3, c2));
    assertEquals(c2, l.latestConfig());
  }
}
