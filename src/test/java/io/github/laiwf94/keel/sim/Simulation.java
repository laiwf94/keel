package io.github.laiwf94.keel.sim;

import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.Entry;
import io.github.laiwf94.keel.raft.HardState;
import io.github.laiwf94.keel.raft.MemStorage;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.Message;
import io.github.laiwf94.keel.raft.RaftNode;
import io.github.laiwf94.keel.raft.RaftOptions;
import io.github.laiwf94.keel.raft.ReadMode;
import io.github.laiwf94.keel.raft.Snapshot;
import io.github.laiwf94.keel.raft.Storage;
import io.github.laiwf94.keel.replica.Outcome;
import io.github.laiwf94.keel.replica.ReadOutcome;
import io.github.laiwf94.keel.replica.Replica;
import io.github.laiwf94.keel.replica.StateMachine;
import io.github.laiwf94.keel.server.NodeLoop;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A deterministic, single-threaded cluster. Time is virtual and every random choice (timeouts,
 * delays, drops, faults, client behaviour) comes from one {@link Random} seeded by the caller, so a
 * seed replays the exact same run.
 */
final class Simulation {
  record Options(
      int nodes,
      long durationMs,
      int tickMs,
      int minDelayMs,
      int maxDelayMs,
      double dropRate,
      double duplicateRate,
      boolean faults,
      int meanFaultGapMs,
      long snapshotEvery,
      double maxDrift,
      boolean oneWayPartitions,
      boolean diskFaults,
      RaftOptions raft) {
    /**
     * Every fault on. {@code oneWayPartitions} adds a second fault schedule that cuts links in one
     * direction only; {@code diskFaults} gives every node a disk with its own latency, stall and
     * fsync-failure rates. Both only act while {@code faults} is on.
     */
    static Options defaults() {
      return new Options(
          5, 6_000, 10, 1, 15, 0.02, 0.02, true, 400, 40, 0.03, true, true, RaftOptions.DEFAULTS);
    }

    /** Only the faults the simulator had before one-way partitions and disk faults. */
    Options withClassicFaults() {
      return withOneWayPartitions(false).withDiskFaults(false);
    }

    Options withOneWayPartitions(boolean on) {
      return new Options(
          nodes,
          durationMs,
          tickMs,
          minDelayMs,
          maxDelayMs,
          dropRate,
          duplicateRate,
          faults,
          meanFaultGapMs,
          snapshotEvery,
          maxDrift,
          on,
          diskFaults,
          raft);
    }

    Options withDiskFaults(boolean on) {
      return new Options(
          nodes,
          durationMs,
          tickMs,
          minDelayMs,
          maxDelayMs,
          dropRate,
          duplicateRate,
          faults,
          meanFaultGapMs,
          snapshotEvery,
          maxDrift,
          oneWayPartitions,
          on,
          raft);
    }

    Options withNodes(int n) {
      return new Options(
          n,
          durationMs,
          tickMs,
          minDelayMs,
          maxDelayMs,
          dropRate,
          duplicateRate,
          faults,
          meanFaultGapMs,
          snapshotEvery,
          maxDrift,
          oneWayPartitions,
          diskFaults,
          raft);
    }

    Options withFaults(boolean f) {
      return new Options(
          nodes,
          durationMs,
          tickMs,
          minDelayMs,
          maxDelayMs,
          dropRate,
          duplicateRate,
          f,
          meanFaultGapMs,
          snapshotEvery,
          maxDrift,
          oneWayPartitions,
          diskFaults,
          raft);
    }

    /** Clock rates differ by up to {@code drift} either way. */
    Options withDrift(double drift) {
      return new Options(
          nodes,
          durationMs,
          tickMs,
          minDelayMs,
          maxDelayMs,
          dropRate,
          duplicateRate,
          faults,
          meanFaultGapMs,
          snapshotEvery,
          drift,
          oneWayPartitions,
          diskFaults,
          raft);
    }

    Options withSnapshotEvery(long every) {
      return new Options(
          nodes,
          durationMs,
          tickMs,
          minDelayMs,
          maxDelayMs,
          dropRate,
          duplicateRate,
          faults,
          meanFaultGapMs,
          every,
          maxDrift,
          oneWayPartitions,
          diskFaults,
          raft);
    }
  }

  private record Event(long at, long seq, Runnable action) {}

  /**
   * How one node's disk behaves while disk faults are on. A sync that has something to write takes
   * an exponentially distributed time with mean {@code meanSyncMs}; with probability {@code
   * stallRate} it stalls instead, for 100 ms to 1 s; with probability {@code failRate} the fsync
   * fails, the node dies as the server does on a storage error, and only a prefix of the batch
   * reaches the disk.
   */
  record DiskProfile(double meanSyncMs, double stallRate, double failRate) {
    static final DiskProfile PERFECT = new DiskProfile(0, 0, 0);
    static final int MIN_STALL_MS = 100;
    static final int MAX_STALL_MS = 1_000;

    /** Most disks are fast, some are slower, one in ten is slow; stalls and failures vary too. */
    static DiskProfile random(Random r) {
      double p = r.nextDouble();
      double mean =
          p < 0.6
              ? 0.2 + r.nextDouble()
              : p < 0.9 ? 2 + 3 * r.nextDouble() : 10 + 20 * r.nextDouble();
      double q = r.nextDouble();
      double stall = q < 0.4 ? 0 : q < 0.75 ? 0.003 : 0.01;
      double f = r.nextDouble();
      double fail = f < 0.6 ? 0 : f < 0.85 ? 0.0005 : 0.002;
      return new DiskProfile(mean, stall, fail);
    }

    /** How long one sync takes, in whole milliseconds. */
    long syncMs(Random r) {
      if (r.nextDouble() < stallRate) {
        return (long) MIN_STALL_MS + r.nextInt(MAX_STALL_MS - MIN_STALL_MS + 1);
      }
      return Math.round(-meanSyncMs * Math.log(1 - r.nextDouble()));
    }
  }

  static final class SimNode {
    final String id;
    final MemStorage storage = new MemStorage();
    Configuration initial;
    final double drift;
    final DiskProfile disk;
    Replica replica;
    StateMachine machine;
    boolean up;
    int incarnation;

    /**
     * Until this time the node's event loop is blocked in an fsync: it takes no ticks, messages or
     * client calls, and what it sends after the fsync leaves only then.
     */
    long busyUntil;

    /** When the next tick is due. */
    long nextTickAt;

    /** If positive, the next sync with something to write takes this long. */
    long stallNext;

    SimNode(String id, double drift, DiskProfile disk) {
      this.id = id;
      this.drift = drift;
      this.disk = disk;
    }

    boolean busy(long now) {
      return now < busyUntil;
    }
  }

  final long seed;
  final Random rnd;

  /**
   * The random source for one-way partitions and disk faults. It is separate from {@link #rnd} so
   * that with those faults off, a seed replays exactly the run it replayed before they existed.
   */
  final Random faultRnd;

  final Options opts;
  final Invariants invariants = new Invariants();
  final Map<String, SimNode> nodes = new TreeMap<>();
  final Configuration config;
  private final Supplier<StateMachine> machines;
  private final PriorityQueue<Event> events =
      new PriorityQueue<>(
          (a, b) -> a.at != b.at ? Long.compare(a.at, b.at) : Long.compare(a.seq, b.seq));
  private final Set<String> blocked = new HashSet<>();
  private final Set<String> blockedOneWay = new HashSet<>();
  private long now;
  private long eventSeq;
  private long trace = 0xcbf29ce484222325L;
  long delivered;
  long snapshotsDelivered;
  final List<String> faultLog = new ArrayList<>();

  Simulation(long seed, Options opts, Supplier<StateMachine> machines) {
    this(seed, opts, machines, opts.nodes());
  }

  /** The first {@code voters} nodes form the cluster; the rest start empty, waiting to join. */
  Simulation(long seed, Options opts, Supplier<StateMachine> machines, int voters) {
    this.seed = seed;
    this.rnd = new Random(seed);
    this.faultRnd = new Random(faultSeed(seed));
    this.opts = opts;
    this.machines = machines;
    List<Member> ms = new ArrayList<>();
    for (int i = 1; i <= opts.nodes(); i++) {
      ms.add(member("n" + i));
    }
    this.config = new Configuration(ms.subList(0, voters));
    for (Member m : ms) {
      // By default clock rates differ by up to 3%, well inside what lease reads allow for.
      double drift = 1 - opts.maxDrift() + rnd.nextDouble() * 2 * opts.maxDrift();
      SimNode n = new SimNode(m.id(), drift, newDisk());
      n.initial = config.contains(m.id()) ? config : Configuration.EMPTY;
      nodes.put(m.id(), n);
    }
  }

  static long faultSeed(long seed) {
    return Long.rotateLeft(seed, 32) ^ 0x9E3779B97F4A7C15L;
  }

  private DiskProfile newDisk() {
    return opts.faults() && opts.diskFaults() ? DiskProfile.random(faultRnd) : DiskProfile.PERFECT;
  }

  static Member member(String id) {
    return new Member(id, id + ":7000", id + ":8080", true);
  }

  long now() {
    return now;
  }

  long trace() {
    return trace;
  }

  void schedule(long delayMs, Runnable action) {
    events.add(new Event(now + Math.max(0, delayMs), eventSeq++, action));
  }

  private void mix(long v) {
    trace = (trace ^ v) * 0x100000001b3L;
  }

  void start(String id) {
    SimNode n = nodes.get(id);
    n.machine = machines.get();
    n.replica =
        new Replica(
            id,
            n.initial,
            opts.raft(),
            disk(n),
            n.machine,
            new Random(rnd.nextLong()),
            this::send,
            opts.snapshotEvery());
    n.up = true;
    n.busyUntil = 0;
    n.stallNext = 0;
    int inc = ++n.incarnation;
    mix(id.hashCode() * 31L + inc);
    // The first tick lands at a random phase so nodes are not in lockstep.
    scheduleTick(n, inc, rnd.nextInt(opts.tickMs()) + 1);
  }

  private void scheduleTick(SimNode n, int inc, long delay) {
    long at = now + delay;
    n.nextTickAt = at;
    schedule(delay, () -> tickLoop(n, inc, at));
  }

  private void tickLoop(SimNode n, int inc, long at) {
    if (!n.up || n.incarnation != inc || n.nextTickAt != at) {
      return; // a stale incarnation, or a catch-up already ran this tick
    }
    if (n.busy(now)) {
      schedule(n.busyUntil - now, () -> tickLoop(n, inc, at));
      return;
    }
    tickNow(n);
    afterStep(n);
  }

  /**
   * Runs the tick that was due at {@code nextTickAt}, and accounts for every later one that was
   * missed while the node was blocked, as {@code NodeLoop} does: a short delay is ticked through, a
   * long one is one tick plus {@link Replica#elapse}.
   */
  private void tickNow(SimNode n) {
    long period = Math.round(opts.tickMs() * n.drift);
    long due = (now - n.nextTickAt) / period + 1;
    if (due > NodeLoop.MAX_TICK_BURST) {
      n.replica.elapse(due - 1);
      n.replica.tick();
    } else {
      for (long i = 0; i < due; i++) {
        n.replica.tick();
      }
    }
    lateTicks += due - 1;
    scheduleTick(n, n.incarnation, period);
  }

  /**
   * Before a node that was blocked handles anything, it catches up on the ticks it missed, as
   * {@code NodeLoop} does before every task. Without a block no tick is ever overdue here.
   */
  private void catchUp(SimNode n) {
    if (now > n.nextTickAt) {
      tickNow(n);
    }
  }

  long lateTicks;

  void crash(String id) {
    if (stop(id, 0)) {
      faultLog.add(now + " crash " + id);
    }
  }

  /** Stops the node, keeping the first {@code keep} of its unsynced writes. */
  private boolean stop(String id, int keep) {
    SimNode n = nodes.get(id);
    if (!n.up) {
      return false;
    }
    n.storage.crash(keep);
    n.up = false;
    n.replica = null;
    n.machine = null;
    return true;
  }

  void restart(String id) {
    if (!nodes.get(id).up) {
      start(id);
      faultLog.add(now + " restart " + id);
    }
  }

  /** Cuts every link between {@code side} and the other nodes, in both directions. */
  void partition(Set<String> side) {
    for (String a : nodes.keySet()) {
      for (String b : nodes.keySet()) {
        if (side.contains(a) != side.contains(b)) {
          blocked.add(a + ">" + b);
        }
      }
    }
    faultLog.add(now + " partition " + side);
  }

  /**
   * Cuts every link from a node in {@code from} to a node outside it, in that direction only: the
   * nodes in {@code from} still hear the others but cannot reach them. Replaces any earlier one-way
   * cut and leaves two-way partitions alone.
   */
  void partitionOneWay(Set<String> from) {
    blockedOneWay.clear();
    for (String a : from) {
      for (String b : nodes.keySet()) {
        if (!from.contains(b)) {
          blockedOneWay.add(a + ">" + b);
        }
      }
    }
    oneWayFaults++;
    faultLog.add(now + " one-way " + from + " cannot reach the rest");
  }

  /** Cuts each directed link independently with probability {@code p}. */
  private void cutRandomLinks(double p) {
    blockedOneWay.clear();
    List<String> cut = new ArrayList<>();
    for (String a : nodes.keySet()) {
      for (String b : nodes.keySet()) {
        if (!a.equals(b) && faultRnd.nextDouble() < p) {
          blockedOneWay.add(a + ">" + b);
          cut.add(a + ">" + b);
        }
      }
    }
    oneWayFaults++;
    faultLog.add(now + " one-way links cut " + cut);
  }

  long oneWayFaults;

  void heal() {
    blocked.clear();
    blockedOneWay.clear();
    faultLog.add(now + " heal");
  }

  private boolean linkDown(Message m) {
    String link = m.from() + ">" + m.to();
    return blocked.contains(link) || blockedOneWay.contains(link);
  }

  private void send(Message m) {
    if (linkDown(m) || rnd.nextDouble() < opts.dropRate()) {
      return;
    }
    // A message produced after a slow fsync leaves only when the fsync returns.
    SimNode sender = nodes.get(m.from());
    long hold = sender == null ? 0 : Math.max(0, sender.busyUntil - now);
    deliverLater(m, hold);
    if (rnd.nextDouble() < opts.duplicateRate()) {
      deliverLater(m, hold);
    }
  }

  private void deliverLater(Message m, long hold) {
    int span = opts.maxDelayMs() - opts.minDelayMs() + 1;
    schedule(hold + opts.minDelayMs() + rnd.nextInt(span), () -> deliver(m));
  }

  private void deliver(Message m) {
    SimNode n = nodes.get(m.to());
    if (n == null || !n.up || linkDown(m)) {
      return;
    }
    if (n.busy(now)) {
      // Queued behind the fsync, in arrival order.
      schedule(n.busyUntil - now, () -> deliver(m));
      return;
    }
    catchUp(n);
    delivered++;
    if (m instanceof Message.SnapshotRequest) {
      snapshotsDelivered++;
    }
    mix(now);
    mix(m.hashCode());
    n.replica.receive(m);
    afterStep(n);
  }

  /** Thrown from a sync to model losing power in the middle of an fsync. */
  private static final class PowerLoss extends RuntimeException {
    private static final long serialVersionUID = 1L;

    PowerLoss() {
      super(null, null, false, false);
    }
  }

  /** Thrown from a sync to model an fsync that returns an error after writing part of a batch. */
  private static final class FsyncFailure extends RuntimeException {
    private static final long serialVersionUID = 1L;
    final int kept;

    FsyncFailure(int kept) {
      super(null, null, false, false);
      this.kept = kept;
    }
  }

  long powerLosses;
  long fsyncFailures;
  long slowSyncs;
  long diskStalls;

  /**
   * Makes the next sync on {@code id} that has something to write take {@code ms}, whether or not
   * disk faults are on.
   */
  void stallDisk(String id, long ms) {
    nodes.get(id).stallNext = ms;
    faultLog.add(now + " disk stall of " + ms + " ms armed on " + id);
  }

  /**
   * The node's disk. While faults are on, a sync occasionally fails as if power were lost during
   * the fsync: the node dies with that batch unwritten, after a leader has already sent the batch's
   * entries to its followers. With disk faults on, each sync also takes time drawn from the node's
   * {@link DiskProfile}, during which the node is blocked, and may stall or fail.
   */
  private Storage disk(SimNode n) {
    MemStorage m = n.storage;
    return new Storage() {
      @Override
      public State load() {
        return m.load();
      }

      @Override
      public void saveHardState(HardState state) {
        m.saveHardState(state);
      }

      @Override
      public void append(List<Entry> entries) {
        m.append(entries);
      }

      @Override
      public void saveSnapshot(Snapshot snapshot, List<Entry> remaining) {
        m.saveSnapshot(snapshot, remaining);
      }

      @Override
      public void sync() {
        if (opts.faults() && now < opts.durationMs() && rnd.nextInt(2000) == 0) {
          throw new PowerLoss();
        }
        int writes = m.unsyncedWrites();
        if (writes > 0) {
          long ms = n.stallNext;
          n.stallNext = 0;
          if (ms == 0 && opts.faults() && opts.diskFaults() && now < opts.durationMs()) {
            if (faultRnd.nextDouble() < n.disk.failRate()) {
              throw new FsyncFailure(faultRnd.nextInt(writes + 1));
            }
            ms = n.disk.syncMs(faultRnd);
          }
          if (ms > 0) {
            slowSyncs++;
            if (ms >= DiskProfile.MIN_STALL_MS) {
              diskStalls++;
              faultLog.add(now + " disk on " + n.id + " stalls for " + ms + " ms");
            }
            n.busyUntil = Math.max(n.busyUntil, now) + ms;
          }
        }
        m.sync();
      }
    };
  }

  void afterStep(SimNode n) {
    try {
      n.replica.process();
    } catch (PowerLoss e) {
      powerLosses++;
      faultLog.add(now + " power loss during fsync on " + n.id);
      crash(n.id);
      return;
    } catch (FsyncFailure e) {
      fsyncFailures++;
      faultLog.add(now + " fsync failed on " + n.id + ", " + e.kept + " writes reached the disk");
      stop(n.id, e.kept);
      return;
    }
    invariants.observe(n.replica.node());
  }

  /** Proposes a command at whichever node is reached first, following leader hints once. */
  void propose(String target, byte[] command, Consumer<Outcome> done) {
    SimNode n = nodes.get(target);
    if (!n.up) {
      done.accept(new Outcome.NotLeader(Optional.empty()));
      return;
    }
    if (n.busy(now)) {
      schedule(n.busyUntil - now, () -> propose(target, command, done));
      return;
    }
    catchUp(n);
    n.replica.propose(command, o -> reply(n, () -> done.accept(o)));
    afterStep(n);
  }

  /**
   * Answers a client. An answer produced after a slow fsync reaches the client when the fsync
   * returns, and not at all if the node dies first.
   */
  private void reply(SimNode n, Runnable answer) {
    if (!n.busy(now)) {
      answer.run();
      return;
    }
    int inc = n.incarnation;
    schedule(
        n.busyUntil - now,
        () -> {
          if (n.up && n.incarnation == inc) {
            answer.run();
          }
        });
  }

  /** Asks {@code target} to serve a read; the callback gets the node so it can read its state. */
  void read(String target, ReadMode mode, BiConsumer<SimNode, ReadOutcome> done) {
    SimNode n = nodes.get(target);
    if (!n.up) {
      done.accept(n, new ReadOutcome.NotLeader(Optional.empty()));
      return;
    }
    if (n.busy(now)) {
      schedule(n.busyUntil - now, () -> read(target, mode, done));
      return;
    }
    catchUp(n);
    n.replica.read(mode, o -> reply(n, () -> done.accept(n, o)));
    afterStep(n);
  }

  Optional<SimNode> leader() {
    SimNode best = null;
    for (SimNode n : nodes.values()) {
      if (n.up && n.replica.node().role() == RaftNode.Role.LEADER) {
        if (best == null || n.replica.node().term() > best.replica.node().term()) {
          best = n;
        }
      }
    }
    return Optional.ofNullable(best);
  }

  /** Runs events until virtual time reaches {@code until}. */
  void runUntil(long until) {
    while (!events.isEmpty() && events.peek().at <= until) {
      Event e = events.poll();
      now = e.at;
      e.action.run();
    }
    now = until;
  }

  /** Starts every node and, if enabled, the random fault schedule. */
  void startAll() {
    for (String id : nodes.keySet()) {
      start(id);
    }
    if (opts.faults()) {
      schedule(nextFaultGap(), this::fault);
      if (opts.oneWayPartitions()) {
        schedule(
            opts.meanFaultGapMs() / 2 + faultRnd.nextInt(opts.meanFaultGapMs()), this::oneWayFault);
      }
    }
  }

  /**
   * The one-way fault schedule, which runs beside {@link #fault()}: a leader that can send but not
   * hear, a leader that can hear but not send, a random group that cannot reach the rest, random
   * directed links, or a heal of the one-way cuts.
   */
  private void oneWayFault() {
    if (now >= opts.durationMs()) {
      return;
    }
    Optional<SimNode> leader = leader();
    switch (faultRnd.nextInt(5)) {
      case 0 ->
          leader.ifPresent(
              l -> {
                Set<String> others = new HashSet<>(nodes.keySet());
                others.remove(l.id);
                partitionOneWay(others);
              });
      case 1 -> leader.ifPresent(l -> partitionOneWay(Set.of(l.id)));
      case 2 -> {
        Set<String> side = new HashSet<>();
        for (String id : nodes.keySet()) {
          if (faultRnd.nextBoolean()) {
            side.add(id);
          }
        }
        partitionOneWay(side);
      }
      case 3 -> cutRandomLinks(0.25);
      default -> {
        blockedOneWay.clear();
        faultLog.add(now + " heal one-way cuts");
      }
    }
    schedule(
        opts.meanFaultGapMs() / 2 + faultRnd.nextInt(opts.meanFaultGapMs()), this::oneWayFault);
  }

  private long nextFaultGap() {
    return (long) opts.meanFaultGapMs() / 2 + rnd.nextInt(opts.meanFaultGapMs());
  }

  private void fault() {
    if (now >= opts.durationMs()) {
      return;
    }
    List<String> ids = new ArrayList<>(nodes.keySet());
    int choice = rnd.nextInt(6);
    switch (choice) {
      case 0 -> crash(ids.get(rnd.nextInt(ids.size())));
      case 1 -> {
        for (String id : ids) {
          if (!retired.contains(id)) {
            restart(id);
          }
        }
      }
      case 2 -> {
        Set<String> side = new HashSet<>();
        for (String id : ids) {
          if (rnd.nextBoolean()) {
            side.add(id);
          }
        }
        heal();
        partition(side);
      }
      case 3 ->
          leader()
              .ifPresent(
                  l -> {
                    heal();
                    partition(Set.of(l.id));
                  });
      case 4 -> leader().ifPresent(l -> crash(l.id));
      default -> heal();
    }
    schedule(nextFaultGap(), this::fault);
  }

  /**
   * Adds and removes servers at random through the leader, one change at a time. A new server
   * always gets a new id and an empty disk. A removed server is stopped shortly after its removal
   * commits; it comes back with its old disk when the run settles, which checks that a server that
   * missed its own removal cannot disrupt the cluster.
   *
   * <p>Reusing an id with a wiped disk is not allowed: the server would forget votes it cast and
   * could vote twice in one term. The simulator found exactly that when it reused ids.
   */
  void enableMembershipChanges(int minVoters) {
    schedule(300 + rnd.nextInt(300), () -> membershipChange(minVoters));
  }

  final List<String> membershipLog = new ArrayList<>();
  final Set<String> retired = new HashSet<>();

  private void membershipChange(int minVoters) {
    if (now >= opts.durationMs()) {
      return;
    }
    schedule(300 + rnd.nextInt(400), () -> membershipChange(minVoters));
    Optional<SimNode> leader = leader();
    if (leader.isEmpty() || leader.get().busy(now)) {
      return;
    }
    catchUp(leader.get());
    Replica r = leader.get().replica;
    Configuration current = r.node().config();
    boolean add = current.voterIds().size() <= minVoters || rnd.nextBoolean();
    if (add && current.members().size() < 7) {
      String id = "n" + (nodes.size() + 1);
      SimNode fresh =
          new SimNode(id, 1 - opts.maxDrift() + rnd.nextDouble() * 2 * opts.maxDrift(), newDisk());
      fresh.initial = Configuration.EMPTY;
      nodes.put(id, fresh);
      start(id);
      membershipLog.add(now + " add " + id);
      r.addServer(member(id), o -> membershipLog.add(now + " add " + id + " -> " + o));
    } else if (current.voterIds().size() > minVoters) {
      List<String> ids = current.ids();
      String id = ids.get(rnd.nextInt(ids.size()));
      membershipLog.add(now + " remove " + id);
      r.removeServer(
          id,
          o -> {
            membershipLog.add(now + " remove " + id + " -> " + o);
            if (o instanceof Outcome.Applied) {
              schedule(200, () -> retireIfRemoved(id));
            }
          });
    }
    afterStep(leader.get());
  }

  private void retireIfRemoved(String id) {
    boolean member = leader().map(l -> l.replica.node().config().contains(id)).orElse(true);
    if (!member) {
      crash(id);
      retired.add(id);
    }
  }

  /** Heals the network, restarts every node and runs until things settle. */
  void settle(long ms) {
    heal();
    for (String id : nodes.keySet()) {
      restart(id);
    }
    runUntil(now + ms);
  }

  void checkLogMatching() {
    List<SimNode> up = nodes.values().stream().filter(n -> n.up).toList();
    for (int i = 0; i < up.size(); i++) {
      for (int j = i + 1; j < up.size(); j++) {
        Invariants.checkLogMatching(up.get(i).replica.node(), up.get(j).replica.node());
      }
    }
  }

  static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}
