package io.github.laiwf94.keel.raft;

import io.github.laiwf94.keel.raft.Message.AppendRequest;
import io.github.laiwf94.keel.raft.Message.AppendResponse;
import io.github.laiwf94.keel.raft.Message.SnapshotRequest;
import io.github.laiwf94.keel.raft.Message.SnapshotResponse;
import io.github.laiwf94.keel.raft.Message.VoteRequest;
import io.github.laiwf94.keel.raft.Message.VoteResponse;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

/**
 * One Raft participant as a deterministic state machine. It never touches a clock, a socket or a
 * thread: the driver calls {@link #tick()} at a fixed interval, feeds received messages to {@link
 * #step}, and collects the results from {@link #ready()}. That is what lets the simulator and the
 * server run exactly the same code.
 *
 * <p>Section numbers in comments refer to the extended Raft paper (Ongaro and Ousterhout, 2014).
 */
public final class RaftNode {
  public enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER
  }

  private static final String NONE = "";

  private final String id;
  private final RaftOptions opts;
  private final Storage storage;
  private final Random random;
  private final RaftLog log;

  private long term;
  private String votedFor;
  private Role role = Role.FOLLOWER;
  private String leaderId = NONE;
  private long commitIndex;
  private long handedOut;

  private int electionElapsed;
  private int randomizedElectionTicks;
  private int heartbeatElapsed;

  private final Set<String> votesGranted = new HashSet<>();
  private final Set<String> votesRejected = new HashSet<>();

  private final List<Message> outbox = new ArrayList<>();

  public RaftNode(
      String id, Configuration initial, RaftOptions opts, Storage storage, Random random) {
    this.id = Member.validateId(id);
    this.opts = opts;
    this.storage = storage;
    this.random = random;
    Storage.State st = storage.load();
    SnapshotMeta meta =
        st.snapshot().map(Snapshot::meta).orElseGet(() -> new SnapshotMeta(0, 0, initial));
    this.log = new RaftLog(meta, st.entries());
    this.term = st.hardState().term();
    this.votedFor = st.hardState().votedFor();
    this.commitIndex = meta.index();
    this.handedOut = meta.index();
    resetElectionTimer();
  }

  public String id() {
    return id;
  }

  public Role role() {
    return role;
  }

  public long term() {
    return term;
  }

  public Optional<String> leader() {
    return leaderId.isEmpty() ? Optional.empty() : Optional.of(leaderId);
  }

  public Configuration config() {
    return log.latestConfig();
  }

  public long commitIndex() {
    return commitIndex;
  }

  /** Read-only view of the log, for tests and status. */
  public RaftLog log() {
    return log;
  }

  /** Advances logical time by one tick. */
  public void tick() {
    if (role == Role.LEADER) {
      if (++heartbeatElapsed >= opts.heartbeatTicks()) {
        heartbeatElapsed = 0;
        broadcastHeartbeat();
      }
      return;
    }
    if (++electionElapsed >= randomizedElectionTicks) {
      electionElapsed = 0;
      if (config().isVoter(id)) {
        campaign();
      } else {
        resetElectionTimer();
      }
    }
  }

  /** Handles one message from another node. Messages addressed elsewhere are ignored. */
  public void step(Message m) {
    if (!m.to().equals(id) || m.from().equals(id)) {
      return;
    }
    if (m.term() > term) {
      String leader = m instanceof AppendRequest || m instanceof SnapshotRequest ? m.from() : NONE;
      becomeFollower(m.term(), leader);
    } else if (m.term() < term) {
      // A stale leader learns the new term from our reply and steps down.
      if (m instanceof AppendRequest || m instanceof SnapshotRequest) {
        send(new AppendResponse(id, m.from(), term, false, 0, 0, 0, 0, 0));
      }
      return;
    }
    switch (m) {
      case VoteRequest vr -> handleVoteRequest(vr);
      case VoteResponse vr -> handleVoteResponse(vr);
      case AppendRequest ar -> handleAppend(ar);
      case AppendResponse ar -> {}
      case SnapshotRequest sr -> {}
      case SnapshotResponse sr -> {}
    }
  }

  /** Collects what this node produced since the last call. */
  public Ready ready() {
    List<Entry> committed = new ArrayList<>();
    for (long i = handedOut + 1; i <= commitIndex; i++) {
      committed.add(log.entry(i));
    }
    handedOut = commitIndex;
    Ready r = new Ready(outbox, Optional.empty(), committed, List.of(), status());
    outbox.clear();
    return r;
  }

  public Ready.Status status() {
    return new Ready.Status(
        id, role, term, leaderId, commitIndex, log.lastIndex(), log.snapshot().index());
  }

  private void campaign() {
    term++;
    votedFor = id;
    persistHardState();
    role = Role.CANDIDATE;
    leaderId = NONE;
    votesGranted.clear();
    votesRejected.clear();
    votesGranted.add(id);
    resetElectionTimer();
    if (config().hasQuorum(votesGranted)) {
      becomeLeader();
      return;
    }
    for (String peer : config().voterIds()) {
      if (!peer.equals(id)) {
        send(new VoteRequest(id, peer, term, log.lastIndex(), log.lastTerm(), false));
      }
    }
  }

  private void handleVoteRequest(VoteRequest vr) {
    boolean canVote = votedFor.equals(vr.from()) || (votedFor.isEmpty() && leaderId.isEmpty());
    boolean grant = canVote && log.isUpToDate(vr.lastLogIndex(), vr.lastLogTerm());
    if (grant) {
      votedFor = vr.from();
      persistHardState();
      electionElapsed = 0;
    }
    send(new VoteResponse(id, vr.from(), term, grant, false));
  }

  private void handleVoteResponse(VoteResponse vr) {
    if (role != Role.CANDIDATE) {
      return;
    }
    (vr.granted() ? votesGranted : votesRejected).add(vr.from());
    if (config().hasQuorum(votesGranted)) {
      becomeLeader();
    } else if (config().hasQuorum(votesRejected)) {
      becomeFollower(term, NONE);
    }
  }

  private void handleAppend(AppendRequest ar) {
    if (role != Role.FOLLOWER) {
      becomeFollower(term, ar.from());
    }
    leaderId = ar.from();
    electionElapsed = 0;
    boolean ok = log.matches(ar.prevLogIndex(), ar.prevLogTerm());
    send(
        new AppendResponse(
            id, ar.from(), term, ok, ok ? ar.prevLogIndex() : 0, ar.prevLogIndex(), 0, 0, 0));
  }

  private void becomeFollower(long newTerm, String leader) {
    if (newTerm > term) {
      term = newTerm;
      votedFor = NONE;
      persistHardState();
    }
    role = Role.FOLLOWER;
    leaderId = leader;
    resetElectionTimer();
  }

  private void becomeLeader() {
    role = Role.LEADER;
    leaderId = id;
    heartbeatElapsed = 0;
    broadcastHeartbeat();
  }

  private void broadcastHeartbeat() {
    for (String peer : config().ids()) {
      if (!peer.equals(id)) {
        send(
            new AppendRequest(
                id, peer, term, log.lastIndex(), log.lastTerm(), List.of(), commitIndex, 0));
      }
    }
  }

  private void resetElectionTimer() {
    electionElapsed = 0;
    randomizedElectionTicks = opts.electionTicks() + random.nextInt(opts.electionTicks());
  }

  private void persistHardState() {
    storage.saveHardState(new HardState(term, votedFor));
  }

  private void send(Message m) {
    outbox.add(m);
  }
}
