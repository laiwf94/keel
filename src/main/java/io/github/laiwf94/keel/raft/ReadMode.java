package io.github.laiwf94.keel.raft;

/** How a leader confirms it may serve a read. */
public enum ReadMode {
  /**
   * ReadIndex (thesis section 6.4): record the commit index, then confirm leadership with a round
   * of heartbeats acknowledged by a majority. Costs one round trip; needs no clock assumption.
   */
  LINEARIZABLE,
  /**
   * Serve immediately while the leader's lease holds (thesis section 6.4.1). The lease starts when
   * the leader sends a heartbeat that a majority later acknowledges and lasts {@link
   * RaftOptions#leaseTicks()}. It is safe only if no node's clock runs faster than the leader's by
   * more than {@code electionTicks / leaseTicks}; outside the lease it falls back to ReadIndex.
   */
  LEASE
}
