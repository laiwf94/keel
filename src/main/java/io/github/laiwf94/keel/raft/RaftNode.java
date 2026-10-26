package io.github.laiwf94.keel.raft;

import io.github.laiwf94.keel.raft.Message.AppendRequest;
import io.github.laiwf94.keel.raft.Message.AppendResponse;
import io.github.laiwf94.keel.raft.Message.SnapshotRequest;
import io.github.laiwf94.keel.raft.Message.SnapshotResponse;
import io.github.laiwf94.keel.raft.Message.VoteRequest;
import io.github.laiwf94.keel.raft.Message.VoteResponse;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

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

  /** What the leader knows about one follower's log (section 5.3). */
  static final class Progress {
    enum State {
      /** Send one batch at a time until the follower's match point is found. */
      PROBE,
      /** The follower is in step: stream batches without waiting for each reply. */
      REPLICATE,
      /** The follower needs entries we compacted; a snapshot is on its way. */
      SNAPSHOT
    }

    long match;
    long next;
    State state = State.PROBE;
    boolean paused;
    long ackedSeq;
    int snapshotElapsed;

    Progress(long next) {
      this.next = next;
    }
  }

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

  private final Map<String, Progress> progress = new LinkedHashMap<>();
  private long seq;
  private boolean broadcastPending;
  private long termStartIndex;

  // Reads (thesis section 6.4). Time is counted in ticks for the lease.
  private record PendingRead(long context, ReadMode mode, long index, long seq) {}

  private long now;
  private long leaseExpiry;
  private final TreeMap<Long, Long> seqSentAt = new TreeMap<>();
  private final List<PendingRead> readsAwaitingTermCommit = new ArrayList<>();
  private final ArrayDeque<PendingRead> readsAwaitingQuorum = new ArrayDeque<>();
  private final List<Ready.ReadState> readsReady = new ArrayList<>();
  private boolean readRoundPending;

  private final List<Message> outbox = new ArrayList<>();
  private Snapshot latestSnapshot;
  private Snapshot toRestore;

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
    this.latestSnapshot = st.snapshot().orElse(null);
    this.toRestore = latestSnapshot;
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
    now++;
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
    for (Map.Entry<String, Progress> e : progress.entrySet()) {
      Progress pr = e.getValue();
      if (pr.state == Progress.State.SNAPSHOT && ++pr.snapshotElapsed >= opts.electionTicks()) {
        sendSnapshot(e.getKey()); // the last one was probably lost
      }
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

  /**
   * Appends a command to the leader's log and returns its index. The command is committed once a
   * majority stores it; the driver learns that from {@link Ready#committed()}.
   *
   * @throws NotLeaderException if this node is not the leader
   */
  public long propose(byte[] command) {
    if (role != Role.LEADER) {
      throw new NotLeaderException(leaderId);
    }
    Entry e = new Entry(log.lastIndex() + 1, term, EntryType.COMMAND, command);
    appendLocal(e);
    return e.index();
  }

  /**
   * Replaces the log up to {@code index} with a snapshot of the state machine at that index. The
   * driver calls this after applying {@code index}; {@code data} is the state machine's snapshot.
   */
  public void compact(long index, byte[] data) {
    if (index > handedOut) {
      throw new IllegalArgumentException(
          "cannot snapshot at " + index + " before applying it (applied " + handedOut + ")");
    }
    if (index <= log.snapshot().index()) {
      return;
    }
    SnapshotMeta meta = log.compactTo(index);
    latestSnapshot = new Snapshot(meta, data);
    storage.saveSnapshot(latestSnapshot, log.entries());
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
      case SnapshotRequest sr -> handleSnapshot(sr);
      case SnapshotResponse sr -> handleSnapshotResponse(sr);
    }
  }

  /**
   * Asks to serve a read. Once it is safe, {@link Ready#reads()} carries the context with the index
   * the state machine must have applied before answering.
   *
   * @throws NotLeaderException if this node is not the leader
   */
  public void read(long context, ReadMode mode) {
    if (role != Role.LEADER) {
      throw new NotLeaderException(leaderId);
    }
    PendingRead r = new PendingRead(context, mode, 0, 0);
    // Until an entry of this term commits, the leader does not know the latest commit index.
    if (commitIndex < termStartIndex) {
      readsAwaitingTermCommit.add(r);
    } else {
      startRead(r);
    }
  }

  private void startRead(PendingRead r) {
    if (r.mode() == ReadMode.LEASE && now < leaseExpiry) {
      readsReady.add(new Ready.ReadState(r.context(), commitIndex));
      return;
    }
    // Every read that arrives before the next heartbeat round shares it.
    readsAwaitingQuorum.add(new PendingRead(r.context(), r.mode(), commitIndex, seq + 1));
    readRoundPending = true;
  }

  /** Releases reads and extends the lease once a majority has acknowledged a heartbeat round. */
  private void advanceReads() {
    List<Long> acked = new ArrayList<>();
    for (String v : config().voterIds()) {
      if (v.equals(id)) {
        acked.add(seq);
      } else {
        Progress pr = progress.get(v);
        acked.add(pr == null ? 0 : pr.ackedSeq);
      }
    }
    if (acked.isEmpty()) {
      return;
    }
    acked.sort((a, b) -> Long.compare(b, a));
    long quorumSeq = acked.get(config().quorum() - 1);
    while (!readsAwaitingQuorum.isEmpty() && readsAwaitingQuorum.peek().seq() <= quorumSeq) {
      PendingRead r = readsAwaitingQuorum.poll();
      readsReady.add(new Ready.ReadState(r.context(), r.index()));
    }
    Long sentAt = seqSentAt.get(quorumSeq);
    if (sentAt != null) {
      leaseExpiry = Math.max(leaseExpiry, sentAt + opts.leaseTicks());
    }
    seqSentAt.headMap(quorumSeq).clear();
  }

  /** Collects what this node produced since the last call. */
  public Ready ready() {
    if (broadcastPending && role == Role.LEADER) {
      for (String peer : progress.keySet()) {
        sendAppend(peer, false);
      }
    }
    broadcastPending = false;
    if (readRoundPending && role == Role.LEADER) {
      broadcastHeartbeat();
      advanceReads();
    }
    readRoundPending = false;
    List<Entry> committed = new ArrayList<>();
    for (long i = handedOut + 1; i <= commitIndex; i++) {
      committed.add(log.entry(i));
    }
    handedOut = commitIndex;
    Ready r = new Ready(outbox, Optional.ofNullable(toRestore), committed, readsReady, status());
    outbox.clear();
    readsReady.clear();
    toRestore = null;
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
    long prev = ar.prevLogIndex();
    if (prev < log.snapshot().index()) {
      // Everything up to our commit index is committed and therefore matches the leader.
      send(new AppendResponse(id, ar.from(), term, true, commitIndex, 0, 0, 0, ar.seq()));
      return;
    }
    if (log.matches(prev, ar.prevLogTerm())) {
      List<Entry> written = log.appendFromLeader(ar.entries(), commitIndex);
      if (!written.isEmpty()) {
        storage.append(written);
      }
      long lastNew = prev + ar.entries().size();
      commitIndex = Math.max(commitIndex, Math.min(ar.leaderCommit(), lastNew));
      send(new AppendResponse(id, ar.from(), term, true, lastNew, 0, 0, 0, ar.seq()));
      return;
    }
    // Tell the leader where to retry: past our end, or the start of the conflicting term.
    long conflictIndex;
    long conflictTerm;
    if (prev > log.lastIndex()) {
      conflictIndex = log.lastIndex() + 1;
      conflictTerm = 0;
    } else {
      conflictTerm = log.termAt(prev);
      conflictIndex = log.firstIndexOfTermAt(prev);
    }
    send(
        new AppendResponse(
            id, ar.from(), term, false, 0, prev, conflictIndex, conflictTerm, ar.seq()));
  }

  private void handleAppendResponse(AppendResponse ar) {
    Progress pr = progress.get(ar.from());
    if (role != Role.LEADER || pr == null) {
      return;
    }
    recentlyActive.add(ar.from());
    if (ar.seq() > pr.ackedSeq) {
      pr.ackedSeq = ar.seq();
      advanceReads();
    }
    if (ar.success()) {
      pr.match = Math.max(pr.match, ar.matchIndex());
      pr.paused = false;
      if (pr.state == Progress.State.PROBE) {
        pr.state = Progress.State.REPLICATE;
        pr.next = pr.match + 1;
      }
      pr.next = Math.max(pr.next, pr.match + 1);
      maybeCommit();
      maybePromote();
      if (role == Role.LEADER) {
        sendAppend(ar.from(), false);
      }
      return;
    }
    if (ar.rejectedIndex() <= pr.match || pr.state == Progress.State.SNAPSHOT) {
      return; // a reply to an older message, or a heartbeat while a snapshot is in flight
    }
    long next = ar.conflictIndex();
    if (ar.conflictTerm() > 0) {
      long last = log.lastIndexOfTerm(ar.conflictTerm());
      if (last != RaftLog.UNKNOWN) {
        next = last + 1;
      }
    }
    pr.next = Math.max(Math.min(next, ar.rejectedIndex()), pr.match + 1);
    pr.state = Progress.State.PROBE;
    pr.paused = false;
    sendAppend(ar.from(), false);
  }

  /**
   * Advances the commit index to the highest index stored on a majority, but only if that entry is
   * from the current term. Counting replicas of an older-term entry is unsafe (section 5.4.2,
   * Figure 8); such entries commit indirectly once a current-term entry after them does.
   */
  private void maybeCommit() {
    List<Long> matches = new ArrayList<>();
    for (String v : config().voterIds()) {
      if (v.equals(id)) {
        matches.add(log.lastIndex());
      } else {
        Progress pr = progress.get(v);
        matches.add(pr == null ? 0 : pr.match);
      }
    }
    if (matches.isEmpty()) {
      return;
    }
    matches.sort((a, b) -> Long.compare(b, a));
    long candidate = matches.get(config().quorum() - 1);
    if (candidate > commitIndex && log.termAt(candidate) == term) {
      commitIndex = candidate;
      onCommitAdvanced();
    }
  }

  private void onCommitAdvanced() {
    if (commitIndex >= termStartIndex && !readsAwaitingTermCommit.isEmpty()) {
      List<PendingRead> waiting = List.copyOf(readsAwaitingTermCommit);
      readsAwaitingTermCommit.clear();
      waiting.forEach(this::startRead);
    }
    if (log.latestConfigIndex() <= commitIndex) {
      // The latest configuration is committed: stop replicating to servers it removed, and if
      // it removed this leader, step down (thesis section 4.2.2).
      progress.keySet().removeIf(peer -> !config().contains(peer));
      if (!config().contains(id)) {
        becomeFollower(term, NONE);
        return;
      }
    }
    maybePromote();
  }

  private void appendLocal(Entry e) {
    log.append(e);
    storage.append(List.of(e));
    broadcastPending = true;
    if (e.type() == EntryType.CONFIG) {
      for (String peer : config().ids()) {
        if (!peer.equals(id)) {
          progress.putIfAbsent(peer, new Progress(log.lastIndex() + 1));
        }
      }
    }
    maybeCommit();
  }

  /**
   * Adds a server as a learner. It receives the log but does not vote; once it has caught up to the
   * commit index, the leader promotes it to a voter in a second, separate change.
   *
   * @return the index of the configuration entry
   * @throws NotLeaderException if this node is not the leader
   * @throws ConfigChangeException if a change is already in progress or the server is a member
   */
  public long addServer(Member m) {
    checkCanChangeConfig();
    if (config().contains(m.id())) {
      throw new ConfigChangeException(m.id() + " is already a member");
    }
    return appendConfig(config().withLearner(m));
  }

  /**
   * Removes a voter or learner.
   *
   * @return the index of the configuration entry
   * @throws NotLeaderException if this node is not the leader
   * @throws ConfigChangeException if a change is in progress, the server is not a member, or it is
   *     the last voter
   */
  public long removeServer(String serverId) {
    checkCanChangeConfig();
    if (!config().contains(serverId)) {
      throw new ConfigChangeException(serverId + " is not a member");
    }
    if (config().voterIds().equals(List.of(serverId))) {
      throw new ConfigChangeException("cannot remove the last voter");
    }
    return appendConfig(config().without(serverId));
  }

  /**
   * Single-server changes are safe one at a time (thesis section 4.1), and only after the leader
   * has committed an entry of its own term. Without that second rule a change from a previous
   * leader that never committed can combine with this one into two disjoint majorities (the bug
   * Ongaro described on raft-dev in 2015).
   */
  private boolean canChangeConfig() {
    return role == Role.LEADER
        && commitIndex >= termStartIndex
        && log.latestConfigIndex() <= commitIndex;
  }

  private void checkCanChangeConfig() {
    if (role != Role.LEADER) {
      throw new NotLeaderException(leaderId);
    }
    if (!canChangeConfig()) {
      throw new ConfigChangeException("another membership change is still in progress");
    }
  }

  private long appendConfig(Configuration next) {
    Entry e = new Entry(log.lastIndex() + 1, term, EntryType.CONFIG, next.encode());
    appendLocal(e);
    return e.index();
  }

  /** Promotes the first learner that has caught up, if a change may start now. */
  private void maybePromote() {
    if (!canChangeConfig()) {
      return;
    }
    for (Member m : config().members()) {
      Progress pr = progress.get(m.id());
      if (!m.voter() && pr != null && pr.match >= commitIndex) {
        appendConfig(config().withPromoted(m.id()));
        return;
      }
    }
  }

  /**
   * Sends the next batch to one follower. Heartbeats go out even with nothing to send, and double
   * as probes when the follower's position is unknown.
   */
  private void sendAppend(String peer, boolean heartbeat) {
    Progress pr = progress.get(peer);
    if (pr == null) {
      return; // removed from the configuration
    }
    if (pr.state == Progress.State.SNAPSHOT) {
      if (heartbeat) {
        long at = log.snapshot().index();
        send(new AppendRequest(id, peer, term, at, log.termAt(at), List.of(), commitIndex, seq));
      }
      return;
    }
    long prev = pr.next - 1;
    long prevTerm = log.termAt(prev);
    if (prevTerm == RaftLog.UNKNOWN) {
      sendSnapshot(peer);
      return;
    }
    List<Entry> entries = List.of();
    boolean windowFull =
        pr.state == Progress.State.REPLICATE && pr.next - pr.match - 1 >= opts.maxInflightEntries();
    boolean probing = pr.state == Progress.State.PROBE && pr.paused;
    if (pr.next <= log.lastIndex() && !windowFull && !probing) {
      entries = log.slice(pr.next, opts.maxAppendEntries(), opts.maxAppendBytes());
    }
    if (entries.isEmpty() && !heartbeat) {
      return;
    }
    if (!entries.isEmpty()) {
      if (pr.state == Progress.State.REPLICATE) {
        pr.next = entries.get(entries.size() - 1).index() + 1;
      } else {
        pr.paused = true;
      }
    }
    send(new AppendRequest(id, peer, term, prev, prevTerm, entries, commitIndex, seq));
  }

  private void sendSnapshot(String peer) {
    Progress pr = progress.get(peer);
    pr.state = Progress.State.SNAPSHOT;
    pr.snapshotElapsed = 0;
    send(new SnapshotRequest(id, peer, term, latestSnapshot, seq));
  }

  /** InstallSnapshot (section 7), received whole. */
  private void handleSnapshot(SnapshotRequest sr) {
    if (role != Role.FOLLOWER) {
      becomeFollower(term, sr.from());
    }
    leaderId = sr.from();
    electionElapsed = 0;
    SnapshotMeta meta = sr.snapshot().meta();
    if (meta.index() <= commitIndex) {
      send(new SnapshotResponse(id, sr.from(), term, commitIndex, sr.seq()));
      return;
    }
    log.installSnapshot(meta);
    storage.saveSnapshot(sr.snapshot(), log.entries());
    latestSnapshot = sr.snapshot();
    toRestore = sr.snapshot();
    commitIndex = meta.index();
    handedOut = meta.index();
    send(new SnapshotResponse(id, sr.from(), term, meta.index(), sr.seq()));
  }

  private void handleSnapshotResponse(SnapshotResponse sr) {
    Progress pr = progress.get(sr.from());
    if (role != Role.LEADER || pr == null) {
      return;
    }
    recentlyActive.add(sr.from());
    if (sr.seq() > pr.ackedSeq) {
      pr.ackedSeq = sr.seq();
      advanceReads();
    }
    pr.match = Math.max(pr.match, sr.matchIndex());
    pr.next = pr.match + 1;
    pr.state = Progress.State.PROBE;
    pr.paused = false;
    maybeCommit();
    maybePromote();
    if (role == Role.LEADER) {
      sendAppend(sr.from(), false);
    }
  }

  private void becomeFollower(long newTerm, String leader) {
    if (newTerm > term) {
      term = newTerm;
      votedFor = NONE;
      persistHardState();
    }
    role = Role.FOLLOWER;
    leaderId = leader;
    progress.clear();
    broadcastPending = false;
    clearReads();
    resetElectionTimer();
  }

  private void becomeLeader() {
    role = Role.LEADER;
    leaderId = id;
    heartbeatElapsed = 0;
    checkQuorumElapsed = 0;
    recentlyActive.clear();
    progress.clear();
    for (String peer : config().ids()) {
      if (!peer.equals(id)) {
        progress.put(peer, new Progress(log.lastIndex() + 1));
      }
    }
    clearReads();
    // A no-op in the new term lets entries from earlier terms commit (section 5.4.2) and tells
    // the leader which of its entries are committed.
    termStartIndex = log.lastIndex() + 1;
    appendLocal(new Entry(termStartIndex, term, EntryType.NOOP, new byte[0]));
    broadcastHeartbeat();
  }

  /** Drops reads that were not served. The driver fails them when it sees the role change. */
  private void clearReads() {
    readsAwaitingTermCommit.clear();
    readsAwaitingQuorum.clear();
    readRoundPending = false;
    leaseExpiry = 0;
    seqSentAt.clear();
  }

  private void broadcastHeartbeat() {
    seq++;
    seqSentAt.put(seq, now);
    for (String peer : progress.keySet()) {
      sendAppend(peer, true);
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
