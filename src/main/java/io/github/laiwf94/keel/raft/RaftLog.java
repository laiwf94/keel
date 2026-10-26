package io.github.laiwf94.keel.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The in-memory log: entries after the last snapshot, plus enough about the snapshot to answer term
 * lookups at its boundary. Storage holds a durable copy; this class is what the node reads.
 *
 * <p>It also tracks every configuration entry, because a configuration takes effect as soon as it
 * is appended and must roll back to the previous one if a new leader truncates it (thesis section
 * 4.1).
 */
public final class RaftLog {
  /** Returned by {@link #termAt} for an index that is compacted or past the end. */
  public static final long UNKNOWN = -1;

  private SnapshotMeta snapshot;
  private final ArrayList<Entry> entries = new ArrayList<>();
  private final TreeMap<Long, Configuration> configs = new TreeMap<>();

  public RaftLog(SnapshotMeta snapshot, List<Entry> initial) {
    this.snapshot = snapshot;
    for (Entry e : initial) {
      append(e);
    }
  }

  public SnapshotMeta snapshot() {
    return snapshot;
  }

  public long firstIndex() {
    return snapshot.index() + 1;
  }

  public long lastIndex() {
    return snapshot.index() + entries.size();
  }

  public long lastTerm() {
    return termAt(lastIndex());
  }

  /** The term of the entry at {@code index}, or {@link #UNKNOWN} if it is compacted or absent. */
  public long termAt(long index) {
    if (index == snapshot.index()) {
      return snapshot.term();
    }
    if (index < snapshot.index() || index > lastIndex()) {
      return UNKNOWN;
    }
    return entries.get((int) (index - firstIndex())).term();
  }

  public boolean matches(long index, long term) {
    return termAt(index) == term;
  }

  public Entry entry(long index) {
    if (index < firstIndex() || index > lastIndex()) {
      throw new IndexOutOfBoundsException(
          "index " + index + " outside [" + firstIndex() + ", " + lastIndex() + "]");
    }
    return entries.get((int) (index - firstIndex()));
  }

  /**
   * Entries from {@code from} onward, at most {@code maxEntries} and roughly {@code maxBytes}, but
   * always at least one entry when any exist so a large entry still makes progress.
   */
  public List<Entry> slice(long from, int maxEntries, long maxBytes) {
    if (from < firstIndex()) {
      throw new IndexOutOfBoundsException("index " + from + " is compacted");
    }
    List<Entry> out = new ArrayList<>();
    long bytes = 0;
    for (long i = from; i <= lastIndex() && out.size() < maxEntries; i++) {
      Entry e = entry(i);
      if (!out.isEmpty() && bytes + e.size() > maxBytes) {
        break;
      }
      bytes += e.size();
      out.add(e);
    }
    return out;
  }

  /** Appends one entry at the end; used by the leader and on load. */
  public void append(Entry e) {
    if (e.index() != lastIndex() + 1) {
      throw new IllegalArgumentException(
          "entry " + e.index() + " does not follow last index " + lastIndex());
    }
    if (e.term() < lastTerm()) {
      throw new IllegalArgumentException(
          "entry term " + e.term() + " is older than last term " + lastTerm());
    }
    entries.add(e);
    if (e.type() == EntryType.CONFIG) {
      configs.put(e.index(), Configuration.decode(e.data()));
    }
  }

  /**
   * Merges entries a leader sent after a matching {@code prevLogIndex} (caller checked the match).
   * Entries already present with the same term are kept; the log is cut at the first conflict.
   * Returns the entries that were actually written, which is what storage must persist.
   *
   * @throws IllegalStateException if a conflict would remove a committed entry, which a correct
   *     leader never causes
   */
  public List<Entry> appendFromLeader(List<Entry> incoming, long commitIndex) {
    int i = 0;
    for (; i < incoming.size(); i++) {
      Entry e = incoming.get(i);
      if (e.index() <= snapshot.index()) {
        continue;
      }
      long existing = termAt(e.index());
      if (existing == UNKNOWN) {
        break;
      }
      if (existing != e.term()) {
        if (e.index() <= commitIndex) {
          throw new IllegalStateException(
              "leader conflicts with committed entry "
                  + e.index()
                  + " (commit "
                  + commitIndex
                  + ")");
        }
        truncateFrom(e.index());
        break;
      }
    }
    List<Entry> written = incoming.subList(i, incoming.size());
    for (Entry e : written) {
      append(e);
    }
    return List.copyOf(written);
  }

  /** Removes {@code index} and everything after it. */
  public void truncateFrom(long index) {
    if (index <= snapshot.index()) {
      throw new IllegalArgumentException("cannot truncate into the snapshot at " + index);
    }
    if (index > lastIndex()) {
      return;
    }
    entries.subList((int) (index - firstIndex()), entries.size()).clear();
    configs.tailMap(index, true).clear();
  }

  /** First index whose term equals the term at {@code index}, scanning back to the snapshot. */
  public long firstIndexOfTermAt(long index) {
    long term = termAt(index);
    long i = index;
    while (i - 1 >= firstIndex() && termAt(i - 1) == term) {
      i--;
    }
    return i;
  }

  /** Last index that has {@code term}, or {@link #UNKNOWN} if the log has none. */
  public long lastIndexOfTerm(long term) {
    for (long i = lastIndex(); i >= snapshot.index(); i--) {
      long t = termAt(i);
      if (t == term) {
        return i;
      }
      if (t < term) {
        break;
      }
    }
    return UNKNOWN;
  }

  /** Raft's election restriction: is a log ending at (index, term) at least as up to date? */
  public boolean isUpToDate(long index, long term) {
    return term > lastTerm() || (term == lastTerm() && index >= lastIndex());
  }

  /** Drops entries up to and including {@code index}, which a local snapshot now covers. */
  public SnapshotMeta compactTo(long index) {
    if (index <= snapshot.index()) {
      return snapshot;
    }
    if (index > lastIndex()) {
      throw new IllegalArgumentException("cannot compact past last index " + lastIndex());
    }
    SnapshotMeta next = new SnapshotMeta(index, termAt(index), configAt(index));
    entries.subList(0, (int) (index - snapshot.index())).clear();
    configs.headMap(index, true).clear();
    snapshot = next;
    return next;
  }

  /**
   * Applies a snapshot received from the leader. If the log already holds the snapshot's last entry
   * with the same term, the suffix after it is kept (paper section 7); otherwise the whole log is
   * discarded.
   */
  public void installSnapshot(SnapshotMeta meta) {
    if (meta.index() <= lastIndex() && matches(meta.index(), meta.term())) {
      entries.subList(0, (int) (meta.index() - snapshot.index())).clear();
      configs.headMap(meta.index(), true).clear();
    } else {
      entries.clear();
      configs.clear();
    }
    snapshot = meta;
  }

  /** Entries currently held, after the snapshot. */
  public List<Entry> entries() {
    return List.copyOf(entries);
  }

  /** The configuration in force at {@code index}. */
  public Configuration configAt(long index) {
    Map.Entry<Long, Configuration> e = configs.floorEntry(index);
    return e != null ? e.getValue() : snapshot.config();
  }

  public Configuration latestConfig() {
    return configs.isEmpty() ? snapshot.config() : configs.lastEntry().getValue();
  }

  /** Index of the latest configuration entry, or the snapshot index if the log has none. */
  public long latestConfigIndex() {
    return configs.isEmpty() ? snapshot.index() : configs.lastKey();
  }
}
