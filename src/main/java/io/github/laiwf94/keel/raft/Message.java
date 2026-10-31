package io.github.laiwf94.keel.raft;

import java.util.List;
import java.util.Objects;

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

  /** InstallSnapshot, sent whole. */
  record SnapshotRequest(String from, String to, long term, Snapshot snapshot, long seq)
      implements Message {
    public SnapshotRequest {
      check(from, to, term);
      Objects.requireNonNull(snapshot, "snapshot");
      nonNegative(seq);
    }
  }

  record SnapshotResponse(String from, String to, long term, long matchIndex, long seq)
      implements Message {
    public SnapshotResponse {
      check(from, to, term);
      nonNegative(matchIndex, seq);
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
