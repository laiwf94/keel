package io.github.laiwf94.keel.sim;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.kv.Command;
import io.github.laiwf94.keel.kv.KvMachine;
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
    Simulation sim = new Simulation(seed, opts, machines);
    History history = new History();
    List<String> keys = List.of("a", "b", "c");
    List<SimKvClient> clients =
        java.util.stream.IntStream.range(0, 4)
            .mapToObj(i -> new SimKvClient(i, sim, history, keys, opts.durationMs()))
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
  void clientsMakeProgress() {
    Run r =
        run(3, Simulation.Options.defaults().withFaults(false), () -> new KvMachine(60_000, 10));
    assertTrue(r.history().completed() > 300, "completed " + r.history().completed());
    assertEquals(0, r.retries() > 50 ? 1 : 0, "too many retries without faults: " + r.retries());
  }
}
