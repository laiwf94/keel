package io.github.laiwf94.keel.raft;

import io.github.laiwf94.keel.codec.CodecException;

/** What a log entry carries. */
public enum EntryType {
  /**
   * Appended by a new leader so entries from earlier terms can commit (Raft paper, section 5.4.2).
   */
  NOOP(0),
  /** An opaque command for the state machine. */
  COMMAND(1),
  /** A new cluster configuration, effective as soon as it is appended. */
  CONFIG(2);

  private final int code;

  EntryType(int code) {
    this.code = code;
  }

  /** The stable number written to disk and the wire. */
  public int code() {
    return code;
  }

  public static EntryType fromCode(int code) {
    for (EntryType t : values()) {
      if (t.code == code) {
        return t;
      }
    }
    throw new CodecException("unknown entry type " + code);
  }
}
