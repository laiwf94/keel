package io.github.laiwf94.keel.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.replica.Outcome;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * Random crashes, restarts, partitions, drops, delays, reordering and duplication against the Raft
 * core, checking the safety invariants on every step and durability of acknowledged writes at the
 * end.
 */
class RaftSimulationTest {
  record Result(long trace, Map<String, List<String>> states, Set<String> acked) {}

  static Result run(long seed, Simulation.Options opts) {
    Simulation sim = new Simulation(seed, opts, ListMachine::new);
    Set<String> acked = new HashSet<>();
    int[] next = {0};
    Runnable[] writer = new Runnable[1];
    writer[0] =
        () -> {
          if (sim.now() < opts.durationMs()) {
            String cmd = "w" + next[0]++;
            List<String> ids = new ArrayList<>(sim.nodes.keySet());
            String target = ids.get(sim.rnd.nextInt(ids.size()));
            sim.propose(
                target,
                Simulation.bytes(cmd),
                o -> {
                  if (o instanceof Outcome.Applied) {
                    acked.add(cmd);
                  } else if (o instanceof Outcome.NotLeader nl && nl.leaderHint().isPresent()) {
                    String hint = nl.leaderHint().get();
                    sim.schedule(
                        1,
                        () ->
                            sim.propose(
                                hint,
                                Simulation.bytes(cmd),
                                o2 -> {
                                  if (o2 instanceof Outcome.Applied) {
                                    acked.add(cmd);
                                  }
                                }));
                  }
                });
            sim.schedule(5 + sim.rnd.nextInt(20), writer[0]);
          }
        };
    sim.startAll();
    sim.schedule(50, writer[0]);
    sim.runUntil(opts.durationMs());
    sim.checkLogMatching();
    sim.settle(3_000);

    // Liveness once faults stop: a leader exists and a new write commits.
    boolean[] finalWrite = {false};
    Simulation.SimNode leader = sim.leader().orElseThrow(() -> new AssertionError("no leader"));
    sim.propose(
        leader.id, Simulation.bytes("final"), o -> finalWrite[0] = o instanceof Outcome.Applied);
    sim.runUntil(sim.now() + 1_000);
    assertTrue(finalWrite[0], "final write did not commit after the network healed");
    sim.checkLogMatching();

    Map<String, List<String>> states = new TreeMap<>();
    for (Simulation.SimNode n : sim.nodes.values()) {
      states.put(n.id, List.copyOf(((ListMachine) n.machine).applied));
    }
    List<String> first = states.values().iterator().next();
    for (var e : states.entrySet()) {
      assertEquals(first, e.getValue(), "replica " + e.getKey() + " diverged");
    }
    Set<String> seen = new HashSet<>();
    for (String c : first) {
      assertTrue(seen.add(c), "command applied twice: " + c);
    }
    for (String c : acked) {
      assertTrue(seen.contains(c), "acknowledged write lost: " + c);
    }
    return new Result(sim.trace(), states, acked);
  }

  @Test
  void safetyAndDurabilityUnderRandomFaults() {
    Seeds.run("RaftSimulationTest", 300, seed -> run(seed, Simulation.Options.defaults()));
  }

  @Test
  void threeNodeClusters() {
    Seeds.run(
        "RaftSimulationTest", 100, seed -> run(seed, Simulation.Options.defaults().withNodes(3)));
  }

  @Test
  void sameSeedSameRun() {
    Result a = run(42, Simulation.Options.defaults());
    Result b = run(42, Simulation.Options.defaults());
    assertEquals(a.trace(), b.trace());
    assertEquals(a.states(), b.states());
    assertEquals(a.acked(), b.acked());
    assertNotEquals(a.trace(), run(43, Simulation.Options.defaults()).trace());
  }

  @Test
  void makesProgressWithoutFaults() {
    Result r = run(7, Simulation.Options.defaults().withFaults(false));
    assertTrue(r.acked().size() > 100, "only " + r.acked().size() + " writes acknowledged");
  }
}
