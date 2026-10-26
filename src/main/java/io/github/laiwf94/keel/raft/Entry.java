package io.github.laiwf94.keel.raft;

import java.util.Arrays;
import java.util.Objects;

/**
 * One log entry. The data array is owned by the entry and treated as immutable; nothing in keel
 * writes to it after construction.
 */
public final class Entry {
  private final long index;
  private final long term;
  private final EntryType type;
  private final byte[] data;

  public Entry(long index, long term, EntryType type, byte[] data) {
    if (index < 1) {
      throw new IllegalArgumentException("index must be at least 1: " + index);
    }
    if (term < 0) {
      throw new IllegalArgumentException("negative term: " + term);
    }
    this.index = index;
    this.term = term;
    this.type = Objects.requireNonNull(type, "type");
    this.data = Objects.requireNonNull(data, "data");
  }

  public long index() {
    return index;
  }

  public long term() {
    return term;
  }

  public EntryType type() {
    return type;
  }

  /** The payload. Callers must not modify the returned array. */
  public byte[] data() {
    return data;
  }

  /** Approximate in-memory and on-wire size, used to bound batches. */
  public int size() {
    return data.length + 32;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Entry e
        && e.index == index
        && e.term == term
        && e.type == type
        && Arrays.equals(e.data, data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, term, type, Arrays.hashCode(data));
  }

  @Override
  public String toString() {
    return "Entry{" + index + "@" + term + " " + type + " " + data.length + "B}";
  }
}
