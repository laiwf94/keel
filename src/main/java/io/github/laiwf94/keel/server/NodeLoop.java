package io.github.laiwf94.keel.server;

import io.github.laiwf94.keel.obs.Log;
import io.github.laiwf94.keel.obs.Metrics;
import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.Message;
import io.github.laiwf94.keel.raft.RaftNode;
import io.github.laiwf94.keel.raft.RaftOptions;
import io.github.laiwf94.keel.raft.ReadMode;
import io.github.laiwf94.keel.raft.Storage;
import io.github.laiwf94.keel.replica.Outcome;
import io.github.laiwf94.keel.replica.ReadOutcome;
import io.github.laiwf94.keel.replica.Replica;
import io.github.laiwf94.keel.replica.StateMachine;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The single thread that owns a {@link Replica}. Everything that touches Raft state, from HTTP
 * handlers and from the transport, is a task in one bounded queue. Each round runs every queued
 * task (up to a batch limit), ticks if a tick is due, then calls {@link Replica#process()} once, so
 * a burst of proposals costs one fsync and one AppendEntries per peer. Before each task the replica
 * learns how much real time has passed, which keeps lease reads safe across pauses.
 *
 * <p>If storage fails the loop stops and calls the fatal handler: a node that cannot persist must
 * not keep talking to its peers.
 */
public final class NodeLoop implements AutoCloseable {
  /** Loop settings: tick length, queue capacity and the most tasks run per round. */
  public record Settings(int tickMs, int inboxCapacity, int maxBatch) {
    public static final Settings DEFAULTS = new Settings(25, 100_000, 4096);
  }

  private final String id;
  private final Replica replica;
  private final StateMachine machine;
  private final Settings settings;
  private final Log log;
  private final Runnable onFatal;
  private final BlockingQueue<Runnable> inbox;
  private final Metrics.Counter proposals;
  private final Metrics.Counter reads;
  private final Metrics.Counter overloaded;
  private final Metrics.Histogram proposalSeconds;
  private final Metrics.Histogram readSeconds;
  private final Thread thread;
  // Leadership transfers waiting for an outcome. Touched only on the loop thread.
  private final List<TransferWatch> transfers = new ArrayList<>();
  private volatile NodeStatus status;
  private volatile boolean running = true;

  public NodeLoop(
      String id,
      Configuration initial,
      RaftOptions raftOptions,
      Storage storage,
      StateMachine machine,
      Consumer<Message> transport,
      long snapshotEvery,
      Settings settings,
      Log log,
      Metrics metrics,
      Runnable onFatal) {
    this.id = id;
    this.machine = machine;
    this.settings = settings;
    this.log = log;
    this.onFatal = onFatal;
    this.inbox = new ArrayBlockingQueue<>(settings.inboxCapacity());
    this.replica =
        new Replica(
            id,
            initial,
            raftOptions,
            new ObservedStorage(storage, metrics),
            machine,
            new SecureRandom(),
            transport,
            snapshotEvery);
    this.proposals = metrics.counter("keel_proposals_total", "Writes proposed on this node.");
    this.reads = metrics.counter("keel_reads_total", "Reads submitted on this node.");
    this.overloaded =
        metrics.counter("keel_overloaded_total", "Requests refused because the queue was full.");
    this.proposalSeconds =
        metrics.histogram("keel_proposal_seconds", "Time from proposal to commit and apply.");
    this.readSeconds =
        metrics.histogram("keel_read_seconds", "Time from read request to a safe answer.");
    metrics.gauge("keel_raft_term", "Current term.", () -> status.term());
    metrics.gauge(
        "keel_raft_leader", "1 if this node is the leader.", () -> isLeader(status) ? 1 : 0);
    metrics.gauge("keel_raft_commit_index", "Highest committed index.", () -> status.commitIndex());
    metrics.gauge("keel_raft_applied_index", "Highest applied index.", () -> status.appliedIndex());
    metrics.gauge("keel_raft_last_index", "Last index in the log.", () -> status.lastIndex());
    metrics.gauge(
        "keel_raft_snapshot_index", "Index of the latest snapshot.", () -> status.snapshotIndex());
    metrics.gauge("keel_inbox_depth", "Tasks waiting for the event loop.", inbox::size);
    publishStatus();
    this.thread = Thread.ofPlatform().name("keel-loop-" + id).unstarted(this::run);
  }

  private static boolean isLeader(NodeStatus s) {
    return s.role() == RaftNode.Role.LEADER;
  }

  public void start() {
    thread.start();
  }

  public NodeStatus status() {
    return status;
  }

  /** Hands a peer message to the loop; dropped if the loop is overloaded. */
  public void receive(Message m) {
    if (!submit(() -> replica.receive(m))) {
      overloaded.inc();
    }
  }

  /** Proposes a command. Completes when it is applied, or with why it was not. */
  public CompletableFuture<Outcome> propose(byte[] command) {
    proposals.inc();
    return change(cb -> replica.propose(command, cb));
  }

  public CompletableFuture<Outcome> addServer(Member m) {
    return change(cb -> replica.addServer(m, cb));
  }

  public CompletableFuture<Outcome> removeServer(String serverId) {
    return change(cb -> replica.removeServer(serverId, cb));
  }

  private CompletableFuture<Outcome> change(Consumer<Consumer<Outcome>> action) {
    CompletableFuture<Outcome> f = new CompletableFuture<>();
    long start = System.nanoTime();
    Consumer<Outcome> done =
        o -> {
          proposalSeconds.observeNanos(System.nanoTime() - start);
          f.complete(o);
        };
    if (!submit(() -> action.accept(done))) {
      overloaded.inc();
      f.complete(new Outcome.Rejected("node is overloaded"));
    }
    return f;
  }

  /**
   * Runs {@code query} against the state machine once it is safe for {@code consistency}. The query
   * runs on the loop thread, so it may read the state machine directly.
   */
  public <T> CompletableFuture<ReadResult<T>> read(Consistency consistency, Supplier<T> query) {
    reads.inc();
    CompletableFuture<ReadResult<T>> f = new CompletableFuture<>();
    long start = System.nanoTime();
    Runnable task;
    if (consistency == Consistency.STALE) {
      task = () -> f.complete(new ReadResult.Value<>(query.get(), replica.appliedIndex()));
    } else {
      ReadMode mode = consistency == Consistency.LEASE ? ReadMode.LEASE : ReadMode.LINEARIZABLE;
      task =
          () ->
              replica.read(
                  mode,
                  o -> {
                    readSeconds.observeNanos(System.nanoTime() - start);
                    f.complete(
                        switch (o) {
                          case ReadOutcome.Served s ->
                              new ReadResult.Value<>(query.get(), s.index());
                          case ReadOutcome.NotLeader n ->
                              new ReadResult.NotLeader<>(n.leaderHint());
                          case ReadOutcome.Failed x -> new ReadResult.Failed<>(x.reason());
                        });
                  });
    }
    if (!submit(task)) {
      overloaded.inc();
      f.complete(new ReadResult.Failed<>("node is overloaded"));
    }
    return f;
  }

  private record TransferWatch(
      long term, long deadlineNanos, CompletableFuture<TransferOutcome> future) {}

  /**
   * Hands leadership to {@code target}, or to the most caught-up voter if it is empty (see {@link
   * RaftNode#transferLeadership}). Completes once another node is known to lead, or with why not:
   * the target did not take over within an election timeout, or {@code timeout} passed first.
   */
  public CompletableFuture<TransferOutcome> transferLeadership(
      Optional<String> target, Duration timeout) {
    CompletableFuture<TransferOutcome> f = new CompletableFuture<>();
    long deadline = System.nanoTime() + timeout.toNanos();
    Runnable task =
        () -> {
          RaftNode node = replica.node();
          if (node.role() != RaftNode.Role.LEADER) {
            f.complete(new TransferOutcome.NotLeader(node.leader()));
            return;
          }
          Optional<String> to = target.isPresent() ? target : node.bestTransferTarget();
          if (to.isEmpty()) {
            f.complete(new TransferOutcome.Rejected("there is no other voter to hand over to"));
            return;
          }
          if (to.get().equals(id)) {
            f.complete(new TransferOutcome.Transferred(id, node.term()));
            return;
          }
          try {
            node.transferLeadership(to.get());
          } catch (IllegalArgumentException e) {
            f.complete(new TransferOutcome.Rejected(e.getMessage()));
            return;
          }
          log.info("transfer_start", "term", node.term(), "to", to.get());
          transfers.add(new TransferWatch(node.term(), deadline, f));
        };
    if (!submit(task)) {
      overloaded.inc();
      f.complete(new TransferOutcome.Failed("node is overloaded or stopping"));
    }
    return f;
  }

  /** Completes the transfers whose outcome is now known. Runs on the loop thread every round. */
  private void checkTransfers() {
    if (transfers.isEmpty()) {
      return;
    }
    RaftNode node = replica.node();
    long now = System.nanoTime();
    for (Iterator<TransferWatch> it = transfers.iterator(); it.hasNext(); ) {
      TransferWatch w = it.next();
      TransferOutcome o = null;
      boolean stillLeads = node.role() == RaftNode.Role.LEADER && node.term() == w.term();
      if (!stillLeads && node.leader().isPresent()) {
        o = new TransferOutcome.Transferred(node.leader().get(), node.term());
      } else if (stillLeads && node.transferee().isEmpty()) {
        o =
            new TransferOutcome.Failed(
                "the target did not take over within an election timeout; " + id + " still leads");
      } else if (now - w.deadlineNanos() >= 0) {
        o =
            new TransferOutcome.Failed(
                stillLeads
                    ? "timed out while the target caught up; " + id + " still leads"
                    : "timed out before a new leader was known");
      }
      if (o != null) {
        it.remove();
        log.info("transfer_done", "outcome", o);
        w.future().complete(o);
      }
    }
  }

  private void failTransfers() {
    for (TransferWatch w : transfers) {
      w.future().complete(new TransferOutcome.Failed("node is stopping"));
    }
    transfers.clear();
  }

  private boolean submit(Runnable task) {
    return running && inbox.offer(task);
  }

  /** Whole tick periods from {@code origin} to {@code now}, both from {@link System#nanoTime}. */
  static long ticksSince(long origin, long now, long tickNanos) {
    return Math.max(0, now - origin) / tickNanos;
  }

  private void run() {
    long tickNanos = TimeUnit.MILLISECONDS.toNanos(settings.tickMs());
    long origin = System.nanoTime();
    long nextTick = origin + tickNanos;
    try {
      while (running) {
        long wait = nextTick - System.nanoTime();
        Runnable task = wait > 0 ? inbox.poll(wait, TimeUnit.NANOSECONDS) : inbox.poll();
        int ran = 0;
        while (task != null) {
          // Before every task the lease clock catches up with real time, so a read never meets a
          // lease that ran out while the thread was paused in an fsync or a garbage collection.
          replica.observeClock(ticksSince(origin, System.nanoTime(), tickNanos));
          task.run();
          if (++ran >= settings.maxBatch()) {
            break;
          }
          task = inbox.poll();
        }
        long now = System.nanoTime();
        replica.observeClock(ticksSince(origin, now, tickNanos));
        if (now >= nextTick) {
          replica.tick();
          nextTick += tickNanos;
          if (now - nextTick > 10 * tickNanos) {
            // We fell far behind (a long GC pause or a stalled disk). Ticking to catch up would
            // fire timers in a burst, so skip ahead instead; the lease clock has already caught
            // up through observeClock.
            nextTick = now + tickNanos;
          }
        }
        replica.process();
        publishStatus();
        checkTransfers();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (RuntimeException | Error e) {
      running = false;
      log.error("fatal", "error", e.toString());
      onFatal.run();
    } finally {
      failTransfers();
    }
  }

  private void publishStatus() {
    NodeStatus prev = status;
    var s = replica.status();
    var node = replica.node();
    NodeStatus next =
        new NodeStatus(
            id,
            s.role(),
            s.term(),
            s.leader(),
            s.commitIndex(),
            replica.appliedIndex(),
            s.lastIndex(),
            s.snapshotIndex(),
            node.config());
    status = next;
    if (prev == null) {
      log.info(
          "started",
          "term",
          next.term(),
          "last_index",
          next.lastIndex(),
          "snapshot_index",
          next.snapshotIndex());
      return;
    }
    if (prev.role() != next.role()
        || prev.term() != next.term()
        || !prev.leader().equals(next.leader())) {
      log.info("role", "role", next.role(), "term", next.term(), "leader", next.leader());
    }
    if (prev.snapshotIndex() != next.snapshotIndex()) {
      log.info("snapshot", "index", next.snapshotIndex());
    }
    if (!prev.config().equals(next.config())) {
      log.info("configuration", "voters", next.config().voterIds(), "members", next.config().ids());
    }
  }

  /** The state machine, for reads that run on the loop thread. */
  public StateMachine machine() {
    return machine;
  }

  /** Lets a task run on the loop thread; false if the loop is full or stopped. */
  public boolean execute(Runnable task) {
    return submit(task);
  }

  public boolean isRunning() {
    return running && thread.isAlive();
  }

  public Optional<Member> member(String memberId) {
    return status.config().member(memberId);
  }

  @Override
  public void close() {
    running = false;
    thread.interrupt();
    try {
      thread.join(5_000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
