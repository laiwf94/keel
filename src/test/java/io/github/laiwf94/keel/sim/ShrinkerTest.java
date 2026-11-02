package io.github.laiwf94.keel.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

/** Plans, delta debugging, and shrinking real simulation failures down to their causes. */
class ShrinkerTest {
  private static BitSet bits(int... members) {
    BitSet b = new BitSet();
    for (int m : members) {
      b.set(m);
    }
    return b;
  }

  private static BitSet range(int n) {
    BitSet b = new BitSet();
    b.set(0, n);
    return b;
  }

  @Test
  void ordinalSetsRoundTripThroughTheirCommandLineForm() {
    assertEquals("none", Plan.format(new BitSet()));
    assertEquals("0-4,7,9-10", Plan.format(bits(0, 1, 2, 3, 4, 7, 9, 10)));
    assertEquals("3", Plan.format(bits(3)));
    for (BitSet b : List.of(new BitSet(), bits(0), bits(1, 2), bits(0, 2, 4, 5, 6, 100))) {
      assertEquals(b, Plan.parse(Plan.format(b)));
    }
    assertEquals(bits(1, 2, 3, 8), Plan.parse(" 1-3 , 8 "));
    assertEquals(new BitSet(), Plan.parse(""));
    assertThrows(IllegalArgumentException.class, () -> Plan.parse("x"));
    assertThrows(IllegalArgumentException.class, () -> Plan.parse("5-2"));
    assertThrows(IllegalArgumentException.class, () -> Plan.parse("-3"));
    assertThrows(IllegalArgumentException.class, () -> Plan.parse("1,,2"));
  }

  @Test
  void aPlanNumbersFaultsAndStepsSeparatelyAndLogsOnlyWhatItAllows() {
    Plan p = Plan.only(bits(1), bits(0, 2));
    assertFalse(p.fault(10, "crash n1"));
    assertTrue(p.step(11, "c0 get a"));
    assertTrue(p.fault(12, "heal"));
    assertFalse(p.step(13, "c1 put b"));
    assertTrue(p.step(14, "c2 delete c"));
    assertFalse(p.fault(15, "crash n2"));
    assertEquals(3, p.faultsAsked());
    assertEquals(3, p.stepsAsked());
    assertEquals(
        List.of("s0 at 11 ms: c0 get a", "f1 at 12 ms: heal", "s2 at 14 ms: c2 delete c"), p.log());

    Plan all = Plan.all();
    assertTrue(all.fault(0, "x") && all.step(0, "y") && all.fault(1, "z"));
    assertEquals(2, all.faultsAsked());
  }

  @Test
  void theCurrentPlanIsPerThreadAndRestoredAfterwards() {
    Plan outer = Plan.only(new BitSet(), null);
    Plan inner = Plan.all();
    Plan.runUnder(
        outer,
        () -> {
          assertTrue(Plan.current() == outer);
          Plan.runUnder(inner, () -> assertTrue(Plan.current() == inner));
          assertTrue(Plan.current() == outer);
        });
    assertTrue(Plan.current() != outer && Plan.current() != inner);
  }

  /**
   * A plan that allows every ordinal it was asked for replays the run with no plan at all, so the
   * shrinker starts from exactly the failure the seed produced. A plan without faults changes it.
   */
  @Test
  void aPlanThatAllowsEverythingReplaysTheSeedExactly() {
    Plan recorded = Plan.all();
    RaftSimulationTest.Result[] free = {null};
    Plan.runUnder(
        recorded, () -> free[0] = RaftSimulationTest.run(42, Simulation.Options.defaults()));
    assertTrue(recorded.faultsAsked() > 10, "faults asked: " + recorded.faultsAsked());
    assertTrue(recorded.stepsAsked() > 100, "steps asked: " + recorded.stepsAsked());

    RaftSimulationTest.Result[] explicit = {null};
    Plan.runUnder(
        Plan.only(range(recorded.faultsAsked()), range(recorded.stepsAsked())),
        () -> explicit[0] = RaftSimulationTest.run(42, Simulation.Options.defaults()));
    assertEquals(free[0].trace(), explicit[0].trace());
    assertEquals(free[0].states(), explicit[0].states());

    Plan none = Plan.only(new BitSet(), null);
    RaftSimulationTest.Result[] quiet = {null};
    Plan.runUnder(none, () -> quiet[0] = RaftSimulationTest.run(42, Simulation.Options.defaults()));
    assertNotEquals(free[0].trace(), quiet[0].trace());
    assertTrue(none.log().stream().noneMatch(l -> l.startsWith("f")), "a fault happened");
  }

  @Test
  void deltaDebuggingFindsTheOneMinimalSet() {
    AtomicInteger calls = new AtomicInteger();
    Predicate<BitSet> needs3and17and40 =
        b -> {
          calls.incrementAndGet();
          return b.get(3) && b.get(17) && b.get(40);
        };
    Shrinker.Search search = new Shrinker.Search(10_000);
    assertEquals(bits(3, 17, 40), search.ddmin(range(64), needs3and17and40));
    assertFalse(search.exhausted());
    assertEquals(calls.get(), search.replays());
    assertTrue(search.replays() < 200, "replays: " + search.replays());

    // Any one of two members is enough: a single member is 1-minimal.
    BitSet one = new Shrinker.Search(10_000).ddmin(range(30), b -> b.get(7) || b.get(22));
    assertEquals(1, one.cardinality());

    // Nothing needed at all.
    assertEquals(new BitSet(), new Shrinker.Search(10_000).ddmin(range(30), b -> true));
    assertEquals(new BitSet(), new Shrinker.Search(10_000).ddmin(new BitSet(), b -> true));
  }

  /** A search that runs out of budget still returns a set that fails, but says so. */
  @Test
  void aSearchOutOfBudgetReturnsAFailingSetAndSaysItMayNotBeMinimal() {
    Predicate<BitSet> needs = b -> b.get(3) && b.get(17) && b.get(40) && b.get(41) && b.get(90);
    Shrinker.Search search = new Shrinker.Search(6);
    BitSet r = search.ddmin(range(128), needs);
    assertTrue(search.exhausted());
    assertTrue(needs.test(r));
    assertTrue(r.cardinality() > 5);
    assertTrue(
        search.replays() <= 6 + Runtime.getRuntime().availableProcessors(),
        "replays: " + search.replays());
  }

  @Test
  void splitCutsIntoNearlyEqualContiguousParts() {
    List<Integer> c = List.of(1, 2, 3, 4, 5, 6, 7);
    assertEquals(List.of(List.of(1, 2), List.of(3, 4), List.of(5, 6, 7)), Shrinker.split(c, 3));
    assertEquals(List.of(List.of(1, 2, 3), List.of(4, 5, 6, 7)), Shrinker.split(c, 2));
    assertEquals(7, Shrinker.split(c, 7).size());
  }

  /** Fails when two or more nodes are down as the fault period ends: two crashes are the cause. */
  static void twoNodesDownAtTheEnd(long seed) {
    Simulation.Options opts = Simulation.Options.defaults().withClassicFaults();
    Simulation sim = new Simulation(seed, opts, ListMachine::new);
    sim.startAll();
    sim.runUntil(opts.durationMs());
    long down = sim.nodes.values().stream().filter(n -> !n.up).count();
    if (down >= 2) {
      throw new AssertionError(down + " nodes are down when the faults stop");
    }
  }

  private static long firstFailingSeed(LongConsumer body, long limit) {
    return LongStream.range(0, limit)
        .filter(
            seed -> {
              try {
                body.accept(seed);
                return false;
              } catch (AssertionError e) {
                return true;
              }
            })
        .findFirst()
        .orElseThrow(() -> new AssertionError("no seed below " + limit + " fails"));
  }

  private static Optional<Throwable> replay(
      LongConsumer body, long seed, BitSet faults, BitSet steps) {
    Throwable[] t = {null};
    Plan.runUnder(
        Plan.only(faults, steps),
        () -> {
          try {
            body.accept(seed);
          } catch (AssertionError | RuntimeException e) {
            t[0] = e;
          }
        });
    return Optional.ofNullable(t[0]);
  }

  /**
   * Two nodes must go down, so at least two crashes stay. Taking a fault out changes the path the
   * cluster takes after it, so a later ordinal can come to mean a different fault, and a 1-minimal
   * schedule may keep an extra fault that steers the run to the crashes; it is still minimal in the
   * sense that matters: without any one of its faults the run passes.
   */
  @Test
  void aSimulatedFailureShrinksToTheCrashesThatCauseIt() {
    LongConsumer body = ShrinkerTest::twoNodesDownAtTheEnd;
    long seed = firstFailingSeed(body, 200);
    Shrinker.Result r = Shrinker.shrink("ShrinkerTest.example", body, seed, 2_000);
    assertTrue(r.minimal(), r.report());
    assertTrue(r.originalFaults() > 5, r.report());
    assertTrue(r.faults().cardinality() <= r.originalFaults() / 2, r.report());
    assertEquals(0, r.steps().cardinality());
    assertEquals(r.faults().cardinality(), r.schedule().size(), r.report());
    long crashes =
        r.schedule().stream()
            .filter(l -> l.matches("f\\d+ at \\d+ ms: (crash|power loss).*"))
            .count();
    assertTrue(crashes >= 2, r.report());
    // The shrunk plan fails the same way, and without any one of its faults it passes.
    String signature = Shrinker.signature(r.failure());
    assertEquals(
        signature, Shrinker.signature(replay(body, seed, r.faults(), r.steps()).orElseThrow()));
    r.faults().stream()
        .forEach(
            f -> {
              BitSet fewer = (BitSet) r.faults().clone();
              fewer.clear(f);
              assertTrue(
                  replay(body, seed, fewer, r.steps()).isEmpty(), "still fails without f" + f);
            });
    String report = r.report();
    assertTrue(report.contains("removing any one of them makes the failure go away"), report);
    assertTrue(
        report.endsWith(
            "replay with ./gradlew test --tests '*ShrinkerTest.example' -Dkeel.sim.seed="
                + seed
                + " -Dkeel.sim.faults="
                + Plan.format(r.faults())
                + " -Dkeel.sim.steps=none"),
        report);
  }

  /**
   * A real bug: a replica that ignores client sessions applies a retried write twice. The
   * linearizability checker catches it under faults; the shrinker reduces the run to the few faults
   * and client operations that still show it.
   */
  @Test
  void aDuplicatedRetryShrinksToAFewFaultsAndOperations() {
    LongConsumer body =
        seed ->
            assertTrue(
                Linearizability.check(
                        KvSimulationTest.run(
                                seed,
                                Simulation.Options.defaults(),
                                KvSimulationTest.ForgetfulSessions::new)
                            .history()
                            .ops)
                    .isEmpty(),
                "not linearizable");
    long seed = firstFailingSeed(body, 200);
    Shrinker.Result r = Shrinker.shrink("KvSimulationTest.forgetful", body, seed, 600);
    String report = r.report();
    System.out.println(report);
    // Seed 0 goes from 30 faults and 654 steps to 2 faults and 4 steps in 136 replays.
    assertTrue(r.minimal(), report);
    assertTrue(r.faults().cardinality() * 4 < r.originalFaults(), report);
    assertTrue(r.steps().cardinality() * 20 < r.originalSteps(), report);
    assertEquals(r.faults().cardinality() + r.steps().cardinality(), r.schedule().size(), report);
    assertEquals(
        Shrinker.signature(r.failure()),
        Shrinker.signature(replay(body, seed, r.faults(), r.steps()).orElseThrow()));
  }

  @Test
  void aPassingSeedHasNothingToShrink() {
    assertThrows(IllegalArgumentException.class, () -> Shrinker.shrink("t", seed -> {}, 1, 100));
  }

  @Test
  void failuresFromDifferentLinesAreDifferentFailures() {
    AssertionError a = new AssertionError("x");
    AssertionError b = new AssertionError("x");
    IllegalStateException c = new IllegalStateException("x");
    assertNotEquals(Shrinker.signature(a), Shrinker.signature(b));
    assertNotEquals(Shrinker.signature(a), Shrinker.signature(c));
    assertTrue(Shrinker.signature(a).contains("ShrinkerTest"));
    Error bare = new Error("no frames");
    bare.setStackTrace(new StackTraceElement[0]);
    assertEquals("java.lang.Error", Shrinker.signature(bare));
  }

  private static String captureOut(Runnable r) {
    PrintStream old = System.out;
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    synchronized (ShrinkerTest.class) {
      System.setOut(new PrintStream(buf, true, StandardCharsets.UTF_8));
      try {
        r.run();
      } finally {
        System.setOut(old);
      }
    }
    return buf.toString(StandardCharsets.UTF_8);
  }

  /**
   * The whole loop a person goes through: a seed fails, the message says how to shrink it, the
   * shrink prints a schedule and a replay command, and that command fails the same way.
   */
  @Test
  void seedsPointToTheShrinkerAndItsReplayCommandReproducesTheFailure() {
    LongConsumer body = ShrinkerTest::twoNodesDownAtTheEnd;
    long seed = firstFailingSeed(body, 200);
    AssertionError failed =
        assertThrows(
            AssertionError.class,
            () -> Seeds.run("ShrinkerTest.example", Seeds.Settings.seeds((int) seed + 1), body));
    assertTrue(
        failed
            .getMessage()
            .endsWith(
                "; shrink it with ./gradlew test --tests '*ShrinkerTest.example' -Dkeel.sim.seed="
                    + seed
                    + " -Dkeel.sim.shrink=true"),
        failed.getMessage());

    Seeds.Settings shrink = new Seeds.Settings(1, seed, null, null, true, 2_000);
    AssertionError[] shrunk = {null};
    String out =
        captureOut(
            () ->
                shrunk[0] =
                    assertThrows(
                        AssertionError.class,
                        () -> Seeds.run("ShrinkerTest.example", shrink, body)));
    assertTrue(out.contains("schedule:\n  f"), out);
    Matcher m =
        Pattern.compile(
                "-Dkeel\\.sim\\.seed=(\\d+) -Dkeel\\.sim\\.faults=(\\S+) -Dkeel\\.sim\\.steps=(\\S+)")
            .matcher(shrunk[0].getMessage());
    assertTrue(m.find(), shrunk[0].getMessage());
    Seeds.Settings replay =
        new Seeds.Settings(
            1,
            Long.parseLong(m.group(1)),
            Plan.parse(m.group(2)),
            Plan.parse(m.group(3)),
            false,
            Shrinker.DEFAULT_BUDGET);
    AssertionError again =
        assertThrows(AssertionError.class, () -> Seeds.run("ShrinkerTest.example", replay, body));
    assertTrue(again.getMessage().contains("-Dkeel.sim.faults=" + m.group(2)), again.getMessage());
    assertEquals(Shrinker.signature(shrunk[0].getCause()), Shrinker.signature(again.getCause()));

    // A seed that passes has nothing to shrink and does not fail the test.
    long passing = seed == 0 ? 1 : 0;
    String note =
        captureOut(
            () ->
                Seeds.run(
                    "ShrinkerTest.example",
                    new Seeds.Settings(1, passing, null, null, true, 100),
                    s -> {}));
    assertTrue(note.contains("passes; nothing to shrink"), note);
  }

  @Test
  void settingsThatCannotWorkAreRejected() {
    LongConsumer body = s -> {};
    assertThrows(
        IllegalArgumentException.class,
        () -> Seeds.run("t", new Seeds.Settings(5, null, null, null, true, 10), body));
    assertThrows(
        IllegalArgumentException.class,
        () -> Seeds.run("t", new Seeds.Settings(5, null, new BitSet(), null, false, 10), body));
    Seeds.Settings defaults = Seeds.Settings.fromSystemProperties(7);
    assertFalse(defaults.shrink());
  }
}
