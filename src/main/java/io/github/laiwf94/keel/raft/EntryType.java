package io.github.laiwf94.keel.raft;

import io.github.laiwf94.keel.codec.CodecException;

/** What a log entry carries. */
public enum EntryType {
  /**
   * Appended by a new leader so entries from earlier terms can commit (Raft paper, section 5.4.2).
   */
  NOOP,
  /** An opaque command for the state machine. */
  COMMAND,
  /** A new cluster configuration, effective as soon as it is appended. */
  CONFIG;

  public static EntryType fromCode(int code) {
    EntryType[] all = values();
    if (code < 0 || code >= all.length) {
      throw new CodecException("unknown entry type " + code);
    }
    return all[code];
  }
}
