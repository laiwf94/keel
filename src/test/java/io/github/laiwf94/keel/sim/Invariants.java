package io.github.laiwf94.keel.sim;

import io.github.laiwf94.keel.raft.Entry;
import io.github.laiwf94.keel.raft.RaftLog;
import io.github.laiwf94.keel.raft.RaftNode;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Raft's safety properties (paper, Figure 3), checked while the simulation runs: one leader per
 * term, identical committed entries everywhere, logs that agree on a prefix whenever they agree on
 * an entry, and leaders that hold every entry committed in an earlier term.
 */
final class Invariants {
  private record Committed(Entry entry, long observedInTerm) {}

  private final Map<Long, String> leaderOfTerm = new HashMap<>();
  private final TreeMap<Long, Committed> committed = new TreeMap<>();
  private final Map<String, Long> checkedUpTo = new HashMap<>();
  private final Map<String, Long> completenessCheckedForTerm = new HashMap<>();

  void observe(RaftNode node) {
    RaftLog log = node.log();
    long from = checkedUpTo.getOrDefault(node.id(), 0L);
    if (from > node.commitIndex()) {
      from = 0; // the node restarted and has not relearned its commit index yet
    }
    for (long i = Math.max(from + 1, log.firstIndex()); i <= node.commitIndex(); i++) {
      Entry e = log.entry(i);
      Committed known = committed.putIfAbsent(i, new Committed(e, node.term()));
      if (known != null && !known.entry().equals(e)) {
        throw new AssertionError(
            "state machine safety: "
                + node.id()
                + " committed "
                + e
                + " but "
                + known.entry()
                + " was committed at the same index");
      }
    }
    checkedUpTo.put(node.id(), node.commitIndex());

    if (node.role() == RaftNode.Role.LEADER) {
      String prev = leaderOfTerm.putIfAbsent(node.term(), node.id());
      if (prev != null && !prev.equals(node.id())) {
        throw new AssertionError(
            "election safety: " + prev + " and " + node.id() + " both lead term " + node.term());
      }
      Long checked = completenessCheckedForTerm.put(node.id(), node.term());
      if (checked == null || checked != node.term()) {
        checkLeaderCompleteness(node);
      }
    }
  }

  private void checkLeaderCompleteness(RaftNode leader) {
    RaftLog log = leader.log();
    for (Map.Entry<Long, Committed> c :
        committed.tailMap(log.snapshot().index(), false).entrySet()) {
      if (c.getValue().observedInTerm() >= leader.term()) {
        continue;
      }
      long i = c.getKey();
      if (i > log.lastIndex() || !log.entry(i).equals(c.getValue().entry())) {
        throw new AssertionError(
            "leader completeness: leader "
                + leader.id()
                + " of term "
                + leader.term()
                + " lacks committed entry "
                + c.getValue().entry());
      }
    }
  }

  /** Log matching: if two logs hold an entry with the same index and term, they agree before it. */
  static void checkLogMatching(RaftNode a, RaftNode b) {
    RaftLog x = a.log();
    RaftLog y = b.log();
    long lo = Math.max(x.firstIndex(), y.firstIndex());
    long hi = Math.min(x.lastIndex(), y.lastIndex());
    long lastAgree = -1;
    for (long i = hi; i >= lo; i--) {
      if (x.termAt(i) == y.termAt(i)) {
        lastAgree = i;
        break;
      }
    }
    for (long i = lo; i <= lastAgree; i++) {
      if (!x.entry(i).equals(y.entry(i))) {
        throw new AssertionError(
            "log matching: "
                + a.id()
                + " and "
                + b.id()
                + " agree at "
                + lastAgree
                + " but differ at "
                + i
                + ": "
                + x.entry(i)
                + " vs "
                + y.entry(i));
      }
    }
  }

  int committedCount() {
    return committed.size();
  }
}
