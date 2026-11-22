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
 * @param snapshotChunkBytes most snapshot bytes in one InstallSnapshot chunk
 * @param maxInflightSnapshotChunks how many InstallSnapshot chunks a leader may send to one
 *     follower before the first of them is acknowledged
 */
public record RaftOptions(
    int electionTicks,
    int heartbeatTicks,
    int maxAppendEntries,
    long maxAppendBytes,
    int maxInflightEntries,
    int leaseDriftTicks,
    int snapshotChunkBytes,
    int maxInflightSnapshotChunks) {
  /** Snapshot chunk size unless set otherwise: 1 MiB. */
  public static final int DEFAULT_SNAPSHOT_CHUNK_BYTES = 1 << 20;

  /** Snapshot chunks in flight to one follower unless set otherwise. */
  public static final int DEFAULT_INFLIGHT_SNAPSHOT_CHUNKS = 4;

  public static final RaftOptions DEFAULTS = new RaftOptions(10, 2, 256, 1 << 20, 4096, 2);

  /** Options with the default snapshot chunk size and window. */
  public RaftOptions(
      int electionTicks,
      int heartbeatTicks,
      int maxAppendEntries,
      long maxAppendBytes,
      int maxInflightEntries,
      int leaseDriftTicks) {
    this(
        electionTicks,
        heartbeatTicks,
        maxAppendEntries,
        maxAppendBytes,
        maxInflightEntries,
        leaseDriftTicks,
        DEFAULT_SNAPSHOT_CHUNK_BYTES,
        DEFAULT_INFLIGHT_SNAPSHOT_CHUNKS);
  }

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
    if (snapshotChunkBytes < 1 || snapshotChunkBytes > Codec.MAX_SNAPSHOT_CHUNK_BYTES) {
      throw new IllegalArgumentException(
          "need 1 <= snapshotChunkBytes <= " + Codec.MAX_SNAPSHOT_CHUNK_BYTES);
    }
    if (maxInflightSnapshotChunks < 1) {
      throw new IllegalArgumentException("need at least one snapshot chunk in flight");
    }
  }

  /** These options with another snapshot chunk size and window. */
  public RaftOptions withSnapshotChunks(int chunkBytes, int inflightChunks) {
    return new RaftOptions(
        electionTicks,
        heartbeatTicks,
        maxAppendEntries,
        maxAppendBytes,
        maxInflightEntries,
        leaseDriftTicks,
        chunkBytes,
        inflightChunks);
  }

  /** How long a leader may serve reads after a quorum acknowledged a heartbeat it sent. */
  public int leaseTicks() {
    return electionTicks - leaseDriftTicks;
  }
}
