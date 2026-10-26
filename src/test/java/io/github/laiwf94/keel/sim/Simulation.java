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
      RaftOptions raft) {
    static Options defaults() {
      return new Options(
          5, 6_000, 10, 1, 15, 0.02, 0.02, true, 400, 40, 0.03, RaftOptions.DEFAULTS);
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
          raft);
    }
  }

  private record Event(long at, long seq, Runnable action) {}

  static final class SimNode {
    final String id;
    final MemStorage storage = new MemStorage();
    Configuration initial;
    final double drift;
    Replica replica;
    StateMachine machine;
    boolean up;
    int incarnation;

    SimNode(String id, double drift) {
      this.id = id;
      this.drift = drift;
    }
  }

  final long seed;
  final Random rnd;
  final Options opts;
  final Invariants invariants = new Invariants();
  final Map<String, SimNode> nodes = new TreeMap<>();
  final Configuration config;
  private final Supplier<StateMachine> machines;
  private final PriorityQueue<Event> events =
      new PriorityQueue<>(
          (a, b) -> a.at != b.at ? Long.compare(a.at, b.at) : Long.compare(a.seq, b.seq));
  private final Set<String> blocked = new HashSet<>();
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
      SimNode n = new SimNode(m.id(), drift);
      n.initial = config.contains(m.id()) ? config : Configuration.EMPTY;
      nodes.put(m.id(), n);
    }
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
    int inc = ++n.incarnation;
    mix(id.hashCode() * 31L + inc);
    // The first tick lands at a random phase so nodes are not in lockstep.
    schedule(rnd.nextInt(opts.tickMs()) + 1, () -> tickLoop(n, inc));
  }

  private void tickLoop(SimNode n, int inc) {
    if (!n.up || n.incarnation != inc) {
      return;
    }
    n.replica.tick();
    afterStep(n);
    schedule(Math.round(opts.tickMs() * n.drift), () -> tickLoop(n, inc));
  }

  void crash(String id) {
    SimNode n = nodes.get(id);
    if (!n.up) {
      return;
    }
    n.storage.crash();
    n.up = false;
    n.replica = null;
    n.machine = null;
    faultLog.add(now + " crash " + id);
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

  void heal() {
    blocked.clear();
    faultLog.add(now + " heal");
  }

  private void send(Message m) {
    if (blocked.contains(m.from() + ">" + m.to()) || rnd.nextDouble() < opts.dropRate()) {
      return;
    }
    deliverLater(m);
    if (rnd.nextDouble() < opts.duplicateRate()) {
      deliverLater(m);
    }
  }

  private void deliverLater(Message m) {
    int span = opts.maxDelayMs() - opts.minDelayMs() + 1;
    schedule(opts.minDelayMs() + rnd.nextInt(span), () -> deliver(m));
  }

  private void deliver(Message m) {
    SimNode n = nodes.get(m.to());
    if (n == null || !n.up || blocked.contains(m.from() + ">" + m.to())) {
      return;
    }
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

  long powerLosses;

  /**
   * The node's disk. While faults are on, a sync occasionally fails as if power were lost during
   * the fsync: the node dies with that batch unwritten, after a leader has already sent the batch's
   * entries to its followers.
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
    n.replica.propose(command, done);
    afterStep(n);
  }

  /** Asks {@code target} to serve a read; the callback gets the node so it can read its state. */
  void read(String target, ReadMode mode, BiConsumer<SimNode, ReadOutcome> done) {
    SimNode n = nodes.get(target);
    if (!n.up) {
      done.accept(n, new ReadOutcome.NotLeader(Optional.empty()));
      return;
    }
    n.replica.read(mode, o -> done.accept(n, o));
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
    }
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
    if (leader.isEmpty()) {
      return;
    }
    Replica r = leader.get().replica;
    Configuration current = r.node().config();
    boolean add = current.voterIds().size() <= minVoters || rnd.nextBoolean();
    if (add && current.members().size() < 7) {
      String id = "n" + (nodes.size() + 1);
      SimNode fresh = new SimNode(id, 1 - opts.maxDrift() + rnd.nextDouble() * 2 * opts.maxDrift());
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
