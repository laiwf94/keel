package io.github.laiwf94.keel.server;

import io.github.laiwf94.keel.raft.ReadMode;
import java.util.Locale;
import java.util.Optional;

/** Read consistency a client asks for. */
public enum Consistency {
  /** ReadIndex on the leader. The default. */
  LINEARIZABLE,
  /** Leader lease; see {@link ReadMode#LEASE} for the clock assumption. */
  LEASE,
  /** Whatever this node has applied, leader or not. May be arbitrarily stale. */
  STALE;

  public static Optional<Consistency> parse(String s) {
    for (Consistency c : values()) {
      if (c.name().toLowerCase(Locale.ROOT).equals(s)) {
        return Optional.of(c);
      }
    }
    return Optional.empty();
  }
}
