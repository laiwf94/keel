package io.github.laiwf94.keel.sim;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.kv.Command;
import io.github.laiwf94.keel.kv.KvMachine;
import io.github.laiwf94.keel.raft.ReadMode;
import io.github.laiwf94.keel.replica.StateMachine;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

/** Simulated clients against a faulty cluster, with every history checked for linearizability. */
class KvSimulationTest {
  record Run(Simulation sim, History history, int retries) {}

  static Run run(long seed, Simulation.Options opts, Supplier<StateMachine> machines) {
    return run(new Simulation(seed, opts, machines), opts);
  }

  static Run run(Simulation sim, Simulation.Options opts) {
    History history = new History();
    List<String> keys = List.of("a", "b", "c");
    List<SimKvClient> clients =
        java.util.stream.IntStream.range(0, 4)
            .mapToObj(
                i ->
                    new SimKvClient(
                        i,
                        sim,
                        history,
                        keys,
                        opts.durationMs(),
                        i % 2 == 0 ? ReadMode.LINEARIZABLE : ReadMode.LEASE))
            .toList();
    sim.startAll();
    clients.forEach(SimKvClient::start);
    sim.runUntil(opts.durationMs());
    sim.settle(3_000);
    return new Run(sim, history, clients.stream().mapToInt(c -> c.retries).sum());
  }

  static void checkSeed(long seed, Simulation.Options opts) {
    Run r = run(seed, opts, () -> new KvMachine(60_000, 1_000));
    Optional<String> violation = Linearizability.check(r.history().ops);
    assertTrue(violation.isEmpty(), () -> violation.get());
    assertTrue(r.history().completed() > 0, "no operation completed");
    r.sim().checkLogMatching();
    // Once settled, replicas at the same applied index hold identical state.
    var nodes = r.sim().nodes.values().stream().toList();
    long applied = nodes.get(0).replica.appliedIndex();
    for (var n : nodes) {
      if (n.replica.appliedIndex() == applied) {
        assertArrayEquals(nodes.get(0).machine.snapshot(), n.machine.snapshot());
      }
    }
  }

  @Test
  void historiesAreLinearizableUnderFaults() {
    Seeds.run("KvSimulationTest", 300, seed -> checkSeed(seed, Simulation.Options.defaults()));
  }

  @Test
  void historiesAreLinearizableOnThreeNodes() {
    Seeds.run(
        "KvSimulationTest",
        100,
        seed -> checkSeed(seed, Simulation.Options.defaults().withNodes(3)));
  }

  @Test
  void leaseReadsStayLinearizableAtTheEdgeOfTheDriftBound() {
    // Rates within 10% either way: at worst 1.1 / 0.9 = 1.22, under electionTicks / leaseTicks.
    Seeds.run(
        "KvSimulationTest",
        200,
        seed -> checkSeed(seed, Simulation.Options.defaults().withDrift(0.1)));
  }

  @Test
  void historiesAreLinearizableWhileServersJoinAndLeave() {
    Seeds.run(
        "KvSimulationTest",
        300,
        seed -> {
          Simulation.Options opts = Simulation.Options.defaults();
          Simulation sim =
              new Simulation(seed, opts.withNodes(3), () -> new KvMachine(60_000, 1_000));
          sim.enableMembershipChanges(2);
          Run r = run(sim, opts);
          Optional<String> violation = Linearizability.check(r.history().ops);
          assertTrue(violation.isEmpty(), () -> violation.get() + "\n" + sim.membershipLog);
          sim.checkLogMatching();
          // Every current member converges on the same state.
          var leader = sim.leader().orElseThrow(() -> new AssertionError("no leader"));
          var members = leader.replica.node().config();
          sim.runUntil(sim.now() + 1_000);
          for (String id : members.ids()) {
            var n = sim.nodes.get(id);
            assertEquals(leader.replica.appliedIndex(), n.replica.appliedIndex(), id);
            assertArrayEquals(leader.machine.snapshot(), n.machine.snapshot(), id);
          }
        });
  }

  @Test
  void membershipChangesHappen() {
    Simulation.Options opts = Simulation.Options.defaults().withFaults(false);
    Simulation sim = new Simulation(5, opts.withNodes(3), () -> new KvMachine(60_000, 1_000));
    sim.enableMembershipChanges(2);
    run(sim, opts);
    long applied = sim.membershipLog.stream().filter(l -> l.contains("Applied")).count();
    assertTrue(applied >= 5, "only " + applied + " changes applied: " + sim.membershipLog);
    assertTrue(
        sim.membershipLog.stream().anyMatch(l -> l.contains("remove") && l.contains("Applied")));
  }

  /** A replica that ignores sessions, so a retried write can apply twice. */
  static final class ForgetfulSessions implements StateMachine {
    final KvMachine inner = new KvMachine(60_000, 1_000);

    @Override
    public byte[] apply(long index, byte[] command) {
      Command c = Command.decode(command);
      if (c.op() == Command.Op.PUT) {
        c = Command.put(c.timestamp(), 0, 0, c.key(), c.value(), c.expectedVersion());
      } else if (c.op() == Command.Op.DELETE) {
        c = Command.delete(c.timestamp(), 0, 0, c.key(), c.expectedVersion());
      }
      return inner.apply(index, c.encode());
    }

    @Override
    public byte[] snapshot() {
      return inner.snapshot();
    }

    @Override
    public void restore(byte[] snapshot) {
      inner.restore(snapshot);
    }
  }

  @Test
  void checkerCatchesAReplicaThatAppliesRetriesTwice() {
    AtomicInteger caught = new AtomicInteger();
    LongStream.range(0, 200)
        .parallel()
        .forEach(
            seed -> {
              Run r = run(seed, Simulation.Options.defaults(), ForgetfulSessions::new);
              if (Linearizability.check(r.history().ops).isPresent()) {
                caught.incrementAndGet();
              }
            });
    assertTrue(caught.get() > 0, "the checker never noticed duplicated writes");
  }

  @Test
  void snapshotsAreInstalledInMostSeeds() {
    AtomicInteger withInstall = new AtomicInteger();
    LongStream.range(0, 40)
        .parallel()
        .forEach(
            seed -> {
              Run r = run(seed, Simulation.Options.defaults(), () -> new KvMachine(60_000, 100));
              if (r.sim().snapshotsDelivered > 0) {
                withInstall.incrementAndGet();
              }
            });
    assertTrue(withInstall.get() >= 20, "InstallSnapshot ran in only " + withInstall + " of 40");
  }

  @Test
  void clientsMakeProgress() {
    Run r =
        run(3, Simulation.Options.defaults().withFaults(false), () -> new KvMachine(60_000, 10));
    assertTrue(r.history().completed() > 300, "completed " + r.history().completed());
    assertEquals(0, r.retries() > 50 ? 1 : 0, "too many retries without faults: " + r.retries());
  }
}
