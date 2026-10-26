package io.github.laiwf94.keel.raft;

/**
 * Timing and batching settings. Times are in ticks; the driver decides how long a tick is.
 *
 * @param electionTicks minimum election timeout; each node draws its timeout from {@code
 *     [electionTicks, 2 * electionTicks)}
 * @param heartbeatTicks interval between leader heartbeats
 * @param maxAppendEntries most entries in one AppendEntries message
 * @param maxAppendBytes rough byte budget for one AppendEntries message
 * @param maxInflightEntries how far a leader may send ahead of a follower's acknowledged index
 * @param leaseDriftTicks how much shorter than {@code electionTicks} a leader lease is, to cover
 *     clock rate differences between nodes
 */
public record RaftOptions(
    int electionTicks,
    int heartbeatTicks,
    int maxAppendEntries,
    long maxAppendBytes,
    int maxInflightEntries,
    int leaseDriftTicks) {
  public static final RaftOptions DEFAULTS = new RaftOptions(10, 2, 256, 1 << 20, 4096, 2);

  public RaftOptions {
    if (heartbeatTicks < 1 || electionTicks <= heartbeatTicks) {
      throw new IllegalArgumentException("need 1 <= heartbeatTicks < electionTicks");
    }
    if (maxAppendEntries < 1 || maxAppendBytes < 1 || maxInflightEntries < 1) {
      throw new IllegalArgumentException("batch limits must be positive");
    }
    if (leaseDriftTicks < 1 || leaseDriftTicks >= electionTicks) {
      throw new IllegalArgumentException("need 1 <= leaseDriftTicks < electionTicks");
    }
  }

  /** How long a leader may serve reads after a quorum acknowledged a heartbeat it sent. */
  public int leaseTicks() {
    return electionTicks - leaseDriftTicks;
  }
}
