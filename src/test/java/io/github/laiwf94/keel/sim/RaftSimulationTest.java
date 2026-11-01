package io.github.laiwf94.keel.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.raft.RaftNode;
import io.github.laiwf94.keel.replica.Outcome;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
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

  /** A quiet five-node cluster with a leader and one committed write, and no random faults. */
  static Simulation quietCluster(long seed) {
    Simulation sim =
        new Simulation(seed, Simulation.Options.defaults().withFaults(false), ListMachine::new);
    sim.startAll();
    sim.runUntil(1_000);
    assertTrue(sim.leader().isPresent(), "no leader");
    assertTrue(commits(sim, "first"), "first write did not commit");
    return sim;
  }

  /** Proposes at the current leader and runs for a second; true if the write applied. */
  static boolean commits(Simulation sim, String cmd) {
    boolean[] applied = {false};
    Simulation.SimNode leader = sim.leader().orElseThrow(() -> new AssertionError("no leader"));
    sim.propose(leader.id, Simulation.bytes(cmd), o -> applied[0] = o instanceof Outcome.Applied);
    sim.runUntil(sim.now() + 1_000);
    return applied[0];
  }

  /**
   * The leader can still send but hears nothing: its heartbeats keep the followers from timing out,
   * so only check-quorum gets the cluster moving again. It must step down and a new leader must
   * take over.
   */
  @Test
  void aLeaderThatCanSendButNotHearStepsDown() {
    Simulation sim = quietCluster(11);
    Simulation.SimNode old = sim.leader().orElseThrow();
    long term = old.replica.node().term();
    Set<String> others = new HashSet<>(sim.nodes.keySet());
    others.remove(old.id);
    sim.partitionOneWay(others);
    sim.runUntil(sim.now() + 2_000);
    Simulation.SimNode next = sim.leader().orElseThrow(() -> new AssertionError("no leader"));
    assertNotEquals(old.id, next.id);
    assertTrue(next.replica.node().term() > term);
    assertNotEquals(RaftNode.Role.LEADER, old.replica.node().role());
    assertTrue(commits(sim, "after"), "the new leader cannot commit");
    sim.checkLogMatching();
    assertEquals(1, sim.oneWayFaults);
  }

  /** The leader hears everything but cannot send: the others replace it and it learns of that. */
  @Test
  void aLeaderThatCanHearButNotSendIsReplaced() {
    Simulation sim = quietCluster(12);
    Simulation.SimNode old = sim.leader().orElseThrow();
    sim.partitionOneWay(Set.of(old.id));
    sim.runUntil(sim.now() + 2_000);
    Simulation.SimNode next = sim.leader().orElseThrow(() -> new AssertionError("no leader"));
    assertNotEquals(old.id, next.id);
    assertEquals(RaftNode.Role.FOLLOWER, old.replica.node().role());
    assertEquals(next.replica.node().term(), old.replica.node().term());
    assertTrue(commits(sim, "after"));
    sim.heal();
    sim.runUntil(sim.now() + 1_000);
    sim.checkLogMatching();
    assertEquals(
        ((ListMachine) next.machine).applied, ((ListMachine) old.machine).applied, "old leader");
  }

  /**
   * The leader's disk hangs for longer than an election timeout in the middle of a write. The rest
   * elect a new leader; when the disk returns the old leader catches up on the time it lost, steps
   * down, and the write it was persisting is either committed everywhere or nowhere.
   */
  @Test
  void aStalledLeaderDiskCostsLeadershipButNoWrite() {
    Simulation sim = quietCluster(13);
    Simulation.SimNode old = sim.leader().orElseThrow();
    sim.stallDisk(old.id, 800);
    Outcome[] stuck = {null};
    sim.propose(old.id, Simulation.bytes("stuck"), o -> stuck[0] = o);
    sim.runUntil(sim.now() + 400);
    Simulation.SimNode next = sim.leader().orElseThrow(() -> new AssertionError("no leader"));
    assertNotEquals(old.id, next.id, "the others did not replace a leader stuck in an fsync");
    assertTrue(old.busy(sim.now()), "the old leader should still be inside its fsync");
    sim.runUntil(sim.now() + 1_000);
    assertNotEquals(RaftNode.Role.LEADER, old.replica.node().role());
    assertTrue(sim.lateTicks > 0, "the old leader did not account for the ticks it missed");
    assertEquals(1, sim.diskStalls);
    assertTrue(commits(sim, "after"));
    sim.checkLogMatching();
    List<String> applied = ((ListMachine) sim.leader().orElseThrow().machine).applied;
    if (stuck[0] instanceof Outcome.Applied) {
      assertTrue(applied.contains("stuck"), "an acknowledged write was lost");
    }
    for (Simulation.SimNode n : sim.nodes.values()) {
      assertEquals(applied, ((ListMachine) n.machine).applied, n.id);
    }
  }

  /**
   * With the new faults off nothing is drawn from their random source, so a seed replays exactly
   * the run it replayed before they existed.
   */
  @Test
  void classicFaultsLeaveTheNewFaultSourceUntouched() {
    Simulation.Options classic = Simulation.Options.defaults().withClassicFaults();
    Simulation sim = new Simulation(42, classic, ListMachine::new);
    sim.startAll();
    sim.runUntil(classic.durationMs());
    assertTrue(!sim.faultLog.isEmpty(), "the classic faults did not run");
    assertEquals(new Random(Simulation.faultSeed(42)).nextLong(), sim.faultRnd.nextLong());
    assertEquals(0, sim.oneWayFaults + sim.slowSyncs + sim.fsyncFailures + sim.lateTicks);
    Result a = run(42, classic);
    assertEquals(a.trace(), run(42, classic).trace());
    assertNotEquals(a.trace(), run(42, Simulation.Options.defaults()).trace());
  }
}
