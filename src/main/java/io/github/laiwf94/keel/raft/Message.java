package io.github.laiwf94.keel.raft;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32C;

/**
 * Every message Raft nodes exchange. Each carries the sender, the receiver and the sender's term.
 * The {@code seq} fields carry the leader's heartbeat round, echoed back so the leader can confirm
 * it still leads before serving a read (thesis section 6.4).
 */
public sealed interface Message {
  String from();

  String to();

  long term();

  private static void check(String from, String to, long term) {
    Member.validateId(from);
    Member.validateId(to);
    if (term < 0) {
      throw new IllegalArgumentException("negative term");
    }
  }

  private static void nonNegative(long... values) {
    for (long v : values) {
      if (v < 0) {
        throw new IllegalArgumentException("negative index or sequence: " + v);
      }
    }
  }

  /**
   * RequestVote, or a pre-vote probe when {@code preVote} is set (thesis section 9.6). A {@code
   * transfer} vote comes from a candidate the leader told to start an election with {@link
   * TimeoutNow}, so voters grant it even while they still hear from that leader (thesis section
   * 3.10).
   */
  record VoteRequest(
      String from,
      String to,
      long term,
      long lastLogIndex,
      long lastLogTerm,
      boolean preVote,
      boolean transfer)
      implements Message {
    public VoteRequest {
      check(from, to, term);
      nonNegative(lastLogIndex, lastLogTerm);
      if (preVote && transfer) {
        throw new IllegalArgumentException("a transfer election skips the pre-vote");
      }
    }

    public VoteRequest(
        String from, String to, long term, long lastLogIndex, long lastLogTerm, boolean preVote) {
      this(from, to, term, lastLogIndex, lastLogTerm, preVote, false);
    }
  }

  record VoteResponse(String from, String to, long term, boolean granted, boolean preVote)
      implements Message {
    public VoteResponse {
      check(from, to, term);
    }
  }

  /** AppendEntries. An empty entry list is a heartbeat. */
  record AppendRequest(
      String from,
      String to,
      long term,
      long prevLogIndex,
      long prevLogTerm,
      List<Entry> entries,
      long leaderCommit,
      long seq)
      implements Message {
    public AppendRequest {
      check(from, to, term);
      nonNegative(prevLogIndex, prevLogTerm, leaderCommit, seq);
      entries = List.copyOf(entries);
      long expect = prevLogIndex + 1;
      for (Entry e : entries) {
        if (e.index() != expect++) {
          throw new IllegalArgumentException("entries are not contiguous after " + prevLogIndex);
        }
      }
    }
  }

  /**
   * Reply to AppendEntries. On success {@code matchIndex} is the last index known to match the
   * leader. On failure {@code rejectedIndex} is the prevLogIndex that did not match, and {@code
   * conflictIndex}/{@code conflictTerm} let the leader skip a whole term at once.
   */
  record AppendResponse(
      String from,
      String to,
      long term,
      boolean success,
      long matchIndex,
      long rejectedIndex,
      long conflictIndex,
      long conflictTerm,
      long seq)
      implements Message {
    public AppendResponse {
      check(from, to, term);
      nonNegative(matchIndex, rejectedIndex, conflictIndex, conflictTerm, seq);
    }
  }

  /**
   * One chunk of an InstallSnapshot transfer (section 7). The leader cuts the snapshot described by
   * {@code meta} into chunks and sends {@code data} as the bytes at {@code offset}; {@code done}
   * marks the chunk that ends the snapshot. {@code checksum} is the CRC32C of {@code data}, checked
   * by the follower before it keeps the bytes. Every chunk carries the whole {@code meta}, so a
   * follower can tell chunks of different snapshots apart and resume a transfer from any offset.
   * The data array is owned by the message and never modified; use {@link SnapshotRequest#of} to
   * compute the checksum.
   */
  final class SnapshotRequest implements Message {
    private final String from;
    private final String to;
    private final long term;
    private final SnapshotMeta meta;
    private final long offset;
    private final byte[] data;
    private final boolean done;
    private final int checksum;
    private final long seq;

    public SnapshotRequest(
        String from,
        String to,
        long term,
        SnapshotMeta meta,
        long offset,
        byte[] data,
        boolean done,
        int checksum,
        long seq) {
      check(from, to, term);
      Objects.requireNonNull(meta, "meta");
      Objects.requireNonNull(data, "data");
      nonNegative(offset, seq);
      if (offset > Long.MAX_VALUE - data.length) {
        throw new IllegalArgumentException("chunk ends past the largest offset");
      }
      if (data.length == 0 && !done) {
        throw new IllegalArgumentException("only the last chunk may be empty");
      }
      this.from = from;
      this.to = to;
      this.term = term;
      this.meta = meta;
      this.offset = offset;
      this.data = data;
      this.done = done;
      this.checksum = checksum;
      this.seq = seq;
    }

    @Override
    public String from() {
      return from;
    }

    @Override
    public String to() {
      return to;
    }

    @Override
    public long term() {
      return term;
    }

    public SnapshotMeta meta() {
      return meta;
    }

    public long offset() {
      return offset;
    }

    /** The chunk's bytes. Callers must not modify the returned array. */
    public byte[] data() {
      return data;
    }

    public boolean done() {
      return done;
    }

    public int checksum() {
      return checksum;
    }

    public long seq() {
      return seq;
    }

    /** A chunk with its checksum computed from {@code data}. */
    public static SnapshotRequest of(
        String from,
        String to,
        long term,
        SnapshotMeta meta,
        long offset,
        byte[] data,
        boolean done,
        long seq) {
      return new SnapshotRequest(from, to, term, meta, offset, data, done, crc32c(data), seq);
    }

    /** The CRC32C a chunk carrying {@code data} must have. */
    public static int crc32c(byte[] data) {
      CRC32C crc = new CRC32C();
      crc.update(data);
      return (int) crc.getValue();
    }

    /** Whether {@link #checksum} matches {@link #data}. */
    public boolean intact() {
      return crc32c(data) == checksum;
    }

    /** The offset just past this chunk. */
    public long end() {
      return offset + data.length;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof SnapshotRequest r
          && r.from.equals(from)
          && r.to.equals(to)
          && r.term == term
          && r.meta.equals(meta)
          && r.offset == offset
          && Arrays.equals(r.data, data)
          && r.done == done
          && r.checksum == checksum
          && r.seq == seq;
    }

    @Override
    public int hashCode() {
      return Objects.hash(from, to, term, meta, offset, Arrays.hashCode(data), done, checksum, seq);
    }

    @Override
    public String toString() {
      return "SnapshotRequest[from="
          + from
          + ", to="
          + to
          + ", term="
          + term
          + ", snapshot="
          + meta.index()
          + "@"
          + meta.term()
          + ", offset="
          + offset
          + ", "
          + data.length
          + "B"
          + (done ? ", done" : "")
          + ", seq="
          + seq
          + "]";
    }
  }

  /**
   * Reply to one InstallSnapshot chunk. {@code snapshotIndex} names the snapshot the chunk belonged
   * to, so the leader can ignore replies about a transfer it no longer runs.
   *
   * <ul>
   *   <li>{@link SnapshotResult#ACCEPTED}: the follower holds the first {@code nextOffset} bytes of
   *       the snapshot. The leader may send on from there.
   *   <li>{@link SnapshotResult#REJECTED}: the chunk did not follow the bytes the follower holds,
   *       or failed its checksum. The leader resends from {@code nextOffset}.
   *   <li>{@link SnapshotResult#INSTALLED}: the follower's log matches the leader's up to {@code
   *       matchIndex}, either because it installed the snapshot or because it had already committed
   *       past it.
   * </ul>
   */
  record SnapshotResponse(
      String from,
      String to,
      long term,
      SnapshotResult result,
      long snapshotIndex,
      long nextOffset,
      long matchIndex,
      long seq)
      implements Message {
    public SnapshotResponse {
      check(from, to, term);
      Objects.requireNonNull(result, "result");
      nonNegative(snapshotIndex, nextOffset, matchIndex, seq);
    }

    /** The follower holds the snapshot or a log that covers it, matching up to {@code match}. */
    public static SnapshotResponse installed(
        String from, String to, long term, long snapshotIndex, long match, long seq) {
      return new SnapshotResponse(
          from, to, term, SnapshotResult.INSTALLED, snapshotIndex, 0, match, seq);
    }
  }

  /** What a follower did with an InstallSnapshot chunk. */
  enum SnapshotResult {
    ACCEPTED(0),
    REJECTED(1),
    INSTALLED(2);

    private final int code;

    SnapshotResult(int code) {
      this.code = code;
    }

    /** The stable number written to the wire. */
    public int code() {
      return code;
    }

    /** The result with this code, or empty for an unknown one. */
    public static Optional<SnapshotResult> fromCode(int code) {
      for (SnapshotResult r : values()) {
        if (r.code == code) {
          return Optional.of(r);
        }
      }
      return Optional.empty();
    }
  }

  /**
   * Sent by a leader handing over leadership to a follower whose log matches its own: start an
   * election now, without waiting for a timeout and without a pre-vote (thesis section 3.10).
   */
  record TimeoutNow(String from, String to, long term) implements Message {
    public TimeoutNow {
      check(from, to, term);
    }
  }
}
