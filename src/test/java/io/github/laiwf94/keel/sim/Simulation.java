package io.github.laiwf94.keel.sim;

import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.MemStorage;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.Message;
import io.github.laiwf94.keel.raft.RaftNode;
import io.github.laiwf94.keel.raft.RaftOptions;
import io.github.laiwf94.keel.replica.Outcome;
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
      RaftOptions raft) {
    static Options defaults() {
      return new Options(5, 6_000, 10, 1, 15, 0.02, 0.02, true, 400, 40, RaftOptions.DEFAULTS);
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
          raft);
    }
  }

  private record Event(long at, long seq, Runnable action) {}

  static final class SimNode {
    final String id;
    final MemStorage storage = new MemStorage();
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
    this.seed = seed;
    this.rnd = new Random(seed);
    this.opts = opts;
    this.machines = machines;
    List<Member> ms = new ArrayList<>();
    for (int i = 1; i <= opts.nodes(); i++) {
      ms.add(new Member("n" + i, "n" + i + ":7000", "n" + i + ":8080", true));
    }
    this.config = new Configuration(ms);
    for (Member m : ms) {
      // Clock rates differ by up to 3%, inside the drift the lease reads allow for.
      nodes.put(m.id(), new SimNode(m.id(), 0.97 + rnd.nextDouble() * 0.06));
    }
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
            config,
            opts.raft(),
            n.storage,
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

  void afterStep(SimNode n) {
    n.replica.process();
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
          restart(id);
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
