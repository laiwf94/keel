package io.github.laiwf94.keel.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.sim.LinearizabilityBenchmark.Spread;
import org.junit.jupiter.api.Test;

/** How the benchmark sums up its JVMs and decides whether a row is within its time budget. */
class LinearizabilityBenchmarkTest {
  @Test
  void medianOfTheFirstRunsOnly() {
    assertEquals(3, LinearizabilityBenchmark.median(new long[] {5, 1, 3, 0, 0}, 3));
    // An even count takes the upper of the two middle runs.
    assertEquals(9, LinearizabilityBenchmark.median(new long[] {4, 9, 0}, 2));
    assertEquals(7, LinearizabilityBenchmark.median(new long[] {7}, 1));
  }

  @Test
  void spreadKeepsTheMedianAndTheExtremesAndLeavesItsInputAlone() {
    long[] jvms = {6, 2, 9};
    assertEquals(new Spread(6, 2, 9), Spread.of(jvms));
    assertEquals(6, jvms[0]);
    assertEquals(new Spread(4, 4, 4), Spread.of(new long[] {4}));
  }

  @Test
  void withinOnlyWhenEveryJvmOfTheNewOneIsNoSlowerThanEveryJvmOfTheOld() {
    Spread old = new Spread(4_000_000, 3_800_000, 4_400_000);
    String within =
        LinearizabilityBenchmark.budget(new Spread(3_500_000, 3_300_000, 3_800_000), old);
    assertTrue(within.startsWith("within the budget"), within);
    assertTrue(within.contains("new at 100,000 3.50 ms, old at 10,000 4.00 ms"), within);
    assertTrue(within.contains("0.88x the budget"), within);

    String over = LinearizabilityBenchmark.budget(new Spread(5_000_000, 4_500_000, 5_600_000), old);
    assertTrue(over.startsWith("over the budget"), over);
    assertTrue(over.contains("1.25x"), over);

    String open = LinearizabilityBenchmark.budget(new Spread(4_100_000, 3_900_000, 4_300_000), old);
    assertTrue(open.startsWith("not settled"), open);
  }

  @Test
  void millisecondsKeepTwoDecimalsBelowTen() {
    assertEquals("0.53 ms", LinearizabilityBenchmark.ms(530_000));
    assertEquals("9.99 ms", LinearizabilityBenchmark.ms(9_990_000));
    assertEquals("1,234 ms", LinearizabilityBenchmark.ms(1_234_000_000));
  }
}
