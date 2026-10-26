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
 * a burst of proposals costs one fsync and one AppendEntries per peer.
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

  private boolean submit(Runnable task) {
    return running && inbox.offer(task);
  }

  private void run() {
    long tickNanos = TimeUnit.MILLISECONDS.toNanos(settings.tickMs());
    long nextTick = System.nanoTime() + tickNanos;
    try {
      while (running) {
        long wait = nextTick - System.nanoTime();
        Runnable task = wait > 0 ? inbox.poll(wait, TimeUnit.NANOSECONDS) : inbox.poll();
        int ran = 0;
        while (task != null) {
          task.run();
          if (++ran >= settings.maxBatch()) {
            break;
          }
          task = inbox.poll();
        }
        long now = System.nanoTime();
        if (now >= nextTick) {
          replica.tick();
          nextTick += tickNanos;
          if (now - nextTick > 10 * tickNanos) {
            // We fell far behind (a long GC pause or a stalled disk). Ticking to catch up would
            // fire timers in a burst, so skip ahead instead.
            nextTick = now + tickNanos;
          }
        }
        replica.process();
        publishStatus();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (RuntimeException | Error e) {
      running = false;
      log.error("fatal", "error", e.toString());
      onFatal.run();
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
