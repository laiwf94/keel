package io.github.laiwf94.keel.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class LinearizabilityTest {
  private final History h = new History();

  private History.Op put(int client, String v, long expected) {
    return h.call(client, History.Kind.PUT, "k", v, expected);
  }

  private History.Op del(int client, long expected) {
    return h.call(client, History.Kind.DELETE, "k", null, expected);
  }

  private History.Op get(int client) {
    return h.call(client, History.Kind.GET, "k", null, -1);
  }

  private boolean ok() {
    return Linearizability.check(h.ops).isEmpty();
  }

  @Test
  void sequentialHistoryIsLinearizable() {
    h.write(put(0, "a", -1), true, 5);
    h.read(get(0), "a", 5);
    h.write(put(0, "b", 5), true, 7);
    h.write(del(0, 7), true, 7);
    h.read(get(0), null, 0);
    assertTrue(ok());
  }

  @Test
  void staleReadIsCaught() {
    h.write(put(0, "a", -1), true, 5);
    h.write(put(0, "b", -1), true, 7);
    h.read(get(1), "a", 5);
    assertFalse(ok());
    assertTrue(Linearizability.check(h.ops).orElseThrow().contains("not linearizable on key k"));
  }

  @Test
  void concurrentOperationsMayLandEitherWay() {
    History.Op w = put(0, "a", -1);
    h.read(get(1), null, 0);
    History.Op r = get(2);
    h.write(w, true, 5);
    h.read(r, "a", 5);
    assertTrue(ok());
  }

  @Test
  void lostWriteIsCaught() {
    h.write(put(0, "a", -1), true, 5);
    h.read(get(1), null, 0);
    assertFalse(ok());
  }

  @Test
  void versionsMustIncrease() {
    h.write(put(0, "a", -1), true, 7);
    h.write(put(0, "b", -1), true, 5);
    assertFalse(ok());
  }

  @Test
  void conflictMustReportTheCurrentVersion() {
    h.write(put(0, "a", -1), true, 5);
    h.write(put(1, "b", 3), false, 4);
    assertFalse(ok());
  }

  @Test
  void conditionalWriteMustMatch() {
    h.write(put(0, "a", -1), true, 5);
    h.write(put(1, "b", 4), true, 9);
    assertFalse(ok());
  }

  @Test
  void deleteMustReportTheRemovedVersion() {
    h.write(put(0, "a", -1), true, 5);
    h.write(del(1, -1), true, 4);
    assertFalse(ok());
  }

  @Test
  void duplicatedWriteIsCaught() {
    // "a" was written once, overwritten, then reappears with a new version no operation made.
    h.write(put(0, "a", -1), true, 5);
    h.write(put(1, "b", -1), true, 7);
    h.read(get(2), "a", 9);
    assertFalse(ok());
  }

  @Test
  void openWritesMayTakeEffectLaterOrNeverAndBindTheirVersion() {
    put(0, "a", -1); // never answered
    h.write(put(1, "b", 0), false, 9); // finds a's version, 9
    h.write(put(1, "c", 9), true, 12);
    h.read(get(2), "c", 12);
    assertTrue(ok());

    History h2 = new History();
    h2.call(0, History.Kind.PUT, "k", "a", -1);
    h2.read(h2.call(1, History.Kind.GET, "k", null, -1), null, 0);
    assertTrue(Linearizability.check(h2.ops).isEmpty(), "an open write may never happen");
  }

  @Test
  void openConditionalWritesBranch() {
    put(0, "a", -1); // open, unknown version
    put(1, "b", 4); // open, may or may not match
    del(2, 4); // open delete, may or may not match
    h.read(get(3), "b", 20);
    assertTrue(ok());
  }

  @Test
  void openWritesOnKnownStatesApplyOnlyWhenTheConditionHolds() {
    h.write(put(0, "a", -1), true, 5);
    put(1, "b", 3); // condition fails against version 5
    h.read(get(2), "b", 6);
    assertFalse(ok());
  }

  @Test
  void openReadsAreIgnored() {
    h.write(put(0, "a", -1), true, 5);
    get(1);
    h.write(del(0, 5), true, 5);
    h.write(del(0, 3), false, 0);
    assertTrue(ok());
  }

  /**
   * A small history with up to two random corruptions: a version moved, zeroed or taken from
   * another operation, a read value taken from another write, an outcome flipped, or an answer
   * lost.
   */
  private static List<History.Op> randomHistory(long seed) {
    SplittableRandom rnd = new SplittableRandom(seed);
    int n = 3 + rnd.nextInt(40);
    double openRate = rnd.nextInt(4) * 0.07;
    List<History.Op> ops =
        HistoryGenerator.linearizable(
            seed, new HistoryGenerator.Shape(n, 2 + rnd.nextInt(4), 1 + rnd.nextInt(2), openRate));
    for (int m = rnd.nextInt(3); m > 0; m--) {
      History.Op o = ops.get(rnd.nextInt(ops.size()));
      if (o.ret == History.OPEN) {
        continue;
      }
      switch (rnd.nextInt(6)) {
        case 0 -> o.version += rnd.nextInt(5) - 2;
        case 1 -> o.version = ops.get(rnd.nextInt(ops.size())).version;
        case 2 -> o.version = 0;
        case 3 -> o.readValue = ops.get(rnd.nextInt(ops.size())).value;
        case 4 -> o.ok = !o.ok;
        default -> o.ret = History.OPEN;
      }
    }
    return ops;
  }

  @Test
  void agreesWithTheUnoptimizedSearchOnRandomHistories() {
    int violations = 0;
    for (long seed = 0; seed < 20_000; seed++) {
      List<History.Op> ops = randomHistory(seed);
      boolean expected = LinearizabilityReference.check(ops);
      long s = seed;
      assertEquals(
          expected,
          Linearizability.check(ops).isEmpty(),
          () -> "seed " + s + ":\n" + ops.stream().map(History.Op::toString).toList());
      violations += expected ? 0 : 1;
    }
    // Both outcomes are well represented.
    assertTrue(violations > 4_000 && violations < 12_000, "violations: " + violations);
  }

  /**
   * The checker stays fast on histories far longer than the simulator's: a generous bound that the
   * search it replaced, quadratic in the length of a key's history, does not meet at this size.
   * LinearizabilityBenchmark has the measured times.
   */
  @Test
  void hundredThousandOperationHistoriesCheckQuickly() {
    var shape = HistoryGenerator.Shape.simulatorLike(100_000);
    List<History.Op> good = HistoryGenerator.linearizable(1, shape);
    assertTimeout(Duration.ofSeconds(3), () -> assertTrue(Linearizability.check(good).isEmpty()));

    List<History.Op> bad =
        HistoryGenerator.linearizable(2, new HistoryGenerator.Shape(100_000, 4, 3, 0.0005));
    History.Op stale = HistoryGenerator.staleRead(bad, 2);
    String violation =
        assertTimeout(Duration.ofSeconds(3), () -> Linearizability.check(bad).orElseThrow());
    assertTrue(violation.startsWith("not linearizable on key " + stale.key), violation);
    // The report shows the neighbourhood of the violation, not the whole key.
    assertTrue(violation.contains(stale.toString()), violation);
    assertTrue(violation.lines().count() < 60, violation);
  }
}
