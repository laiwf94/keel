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
 * <p>Section numbers in comments refer to the extended Raft paper (Ongaro and Ousterhout) unless
 * they say "thesis", which means Ongaro's PhD thesis, "Consensus: Bridging Theory and Practice".
 */
public final class RaftNode {
  public enum Role {
    FOLLOWER,
    PRE_CANDIDATE,
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
  private int checkQuorumElapsed;
  // A freshly started node behaves as if it had just heard from a leader until one election
  // timeout passes, so a restart cannot cut short another leader's lease (see inLease()).
  private boolean bootGuard = true;

  private final Set<String> votesGranted = new HashSet<>();
  private final Set<String> votesRejected = new HashSet<>();
  private final Set<String> recentlyActive = new HashSet<>();

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
      tickLeader();
      return;
    }
    electionElapsed++;
    if (electionElapsed >= opts.electionTicks()) {
      bootGuard = false;
    }
    if (electionElapsed >= randomizedElectionTicks) {
      if (config().isVoter(id)) {
        campaign(true);
      } else {
        resetElectionTimer();
      }
    }
  }

  private void tickLeader() {
    // Check quorum (thesis section 6.2): a leader that has not heard from a majority within an
    // election timeout steps down, so a leader stranded in a minority stops accepting work.
    if (++checkQuorumElapsed >= opts.electionTicks()) {
      checkQuorumElapsed = 0;
      recentlyActive.add(id);
      boolean ok = config().hasQuorum(recentlyActive);
      recentlyActive.clear();
      if (!ok) {
        becomeFollower(term, NONE);
        return;
      }
    }
    if (++heartbeatElapsed >= opts.heartbeatTicks()) {
      heartbeatElapsed = 0;
      broadcastHeartbeat();
    }
  }

  /**
   * Whether this node believes a leader is alive. While it does, it ignores votes for a higher term
   * (thesis section 4.2.3). That keeps removed or partitioned nodes from deposing a healthy leader,
   * and it is what makes lease reads safe.
   */
  private boolean inLease() {
    if (role == Role.LEADER) {
      return true;
    }
    return (!leaderId.isEmpty() || bootGuard) && electionElapsed < opts.electionTicks();
  }

  /** Handles one message from another node. Messages addressed elsewhere are ignored. */
  public void step(Message m) {
    if (!m.to().equals(id) || m.from().equals(id)) {
      return;
    }
    if (m.term() > term) {
      if (m instanceof VoteRequest vr) {
        if (inLease()) {
          return;
        }
        if (!vr.preVote()) {
          becomeFollower(m.term(), NONE);
        }
      } else if (!(m instanceof VoteResponse vr && vr.preVote() && vr.granted())) {
        // A granted pre-vote carries the term the pre-candidate would campaign in, so it is the
        // one higher-term message that does not move this node to that term.
        String leader =
            m instanceof AppendRequest || m instanceof SnapshotRequest ? m.from() : NONE;
        becomeFollower(m.term(), leader);
      }
    } else if (m.term() < term) {
      // A stale leader or pre-candidate learns the newer term from the reply.
      if (m instanceof AppendRequest || m instanceof SnapshotRequest) {
        send(new AppendResponse(id, m.from(), term, false, 0, 0, 0, 0, 0));
      } else if (m instanceof VoteRequest vr && vr.preVote()) {
        send(new VoteResponse(id, m.from(), term, false, true));
      }
      return;
    }
    switch (m) {
      case VoteRequest vr -> handleVoteRequest(vr);
      case VoteResponse vr -> handleVoteResponse(vr);
      case AppendRequest ar -> handleAppend(ar);
      case AppendResponse ar -> handleAppendResponse(ar);
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

  /**
   * Starts an election. With {@code preVote} the node first asks whether it could win in term+1
   * without changing anyone's term (thesis section 9.6), so a node returning from a partition
   * cannot force the cluster into a new term.
   */
  private void campaign(boolean preVote) {
    if (preVote) {
      role = Role.PRE_CANDIDATE;
    } else {
      term++;
      votedFor = id;
      persistHardState();
      role = Role.CANDIDATE;
    }
    leaderId = NONE;
    votesGranted.clear();
    votesRejected.clear();
    votesGranted.add(id);
    resetElectionTimer();
    if (config().hasQuorum(votesGranted)) {
      if (preVote) {
        campaign(false);
      } else {
        becomeLeader();
      }
      return;
    }
    long voteTerm = preVote ? term + 1 : term;
    for (String peer : config().voterIds()) {
      if (!peer.equals(id)) {
        send(new VoteRequest(id, peer, voteTerm, log.lastIndex(), log.lastTerm(), preVote));
      }
    }
  }

  private void handleVoteRequest(VoteRequest vr) {
    boolean canVote =
        votedFor.equals(vr.from())
            || (votedFor.isEmpty() && leaderId.isEmpty())
            || (vr.preVote() && vr.term() > term);
    boolean grant = canVote && log.isUpToDate(vr.lastLogIndex(), vr.lastLogTerm());
    if (grant && !vr.preVote()) {
      votedFor = vr.from();
      persistHardState();
      electionElapsed = 0;
    }
    long replyTerm = grant && vr.preVote() ? vr.term() : term;
    send(new VoteResponse(id, vr.from(), replyTerm, grant, vr.preVote()));
  }

  private void handleVoteResponse(VoteResponse vr) {
    Role expected = vr.preVote() ? Role.PRE_CANDIDATE : Role.CANDIDATE;
    if (role != expected) {
      return;
    }
    (vr.granted() ? votesGranted : votesRejected).add(vr.from());
    if (config().hasQuorum(votesGranted)) {
      if (vr.preVote()) {
        campaign(false);
      } else {
        becomeLeader();
      }
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

  private void handleAppendResponse(AppendResponse ar) {
    if (role != Role.LEADER) {
      return;
    }
    recentlyActive.add(ar.from());
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
    checkQuorumElapsed = 0;
    recentlyActive.clear();
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
