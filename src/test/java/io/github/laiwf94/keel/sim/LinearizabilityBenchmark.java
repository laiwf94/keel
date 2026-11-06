package io.github.laiwf94.keel.sim;

import io.github.laiwf94.keel.kv.KvMachine;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Times {@link Linearizability} against {@link LinearizabilityReference}, the search it replaced,
 * on generated histories of 10,000 and 100,000 operations and on histories from the simulator. Run
 * it with {@code ./gradlew linearizabilityBenchmark}, or with {@code --args=--simulator} for only
 * the simulator's histories; it prints a Markdown table. Each case is run after a warm-up and the
 * median of several runs is reported. The test JVM's 1 GiB heap applies.
 */
final class LinearizabilityBenchmark {
  private LinearizabilityBenchmark() {}

  private static boolean fast(List<History.Op> ops) {
    return Linearizability.check(ops).isEmpty();
  }

  /** One row: a set of histories, what they should check as, and which checkers can run them. */
  private record Case(
      String name, Supplier<List<List<History.Op>>> histories, boolean linearizable, int runs) {}

  public static void main(String[] args) {
    List<Case> cases = new ArrayList<>();
    boolean simulatorOnly = Arrays.asList(args).contains("--simulator");
    for (int n : simulatorOnly ? new int[0] : new int[] {10_000, 100_000}) {
      cases.add(synthetic(n, 0.002, false, 5));
      cases.add(synthetic(n, 0, true, 5));
      cases.add(synthetic(n, 0.0005, true, 5));
    }
    if (!simulatorOnly) {
      cases.add(synthetic(10_000, 0.002, true, 5));
      cases.add(synthetic(30_000, 0.002, true, 1));
    }
    cases.add(simulated(6_000, 40));
    cases.add(simulated(60_000, 8));
    System.out.println("| histories | operations | answer | reference | new |");
    System.out.println("|---|---:|---|---:|---:|");
    for (Case c : cases) {
      List<List<History.Op>> hs = c.histories().get();
      long ops = hs.stream().mapToLong(List::size).sum();
      String[] times = time(c, hs);
      System.out.printf(
          "| %s | %,d | %s | %s | %s |%n",
          c.name(),
          ops / hs.size(),
          c.linearizable() ? "linearizable" : "one stale read",
          times[0],
          times[1]);
    }
  }

  private static Case synthetic(int n, double openRate, boolean stale, int runs) {
    String name =
        String.format(
            "generated, %s unanswered",
            openRate == 0 ? "none" : String.format("%.2f%%", openRate * 100));
    return new Case(
        name,
        () -> {
          List<History.Op> ops =
              HistoryGenerator.linearizable(n, new HistoryGenerator.Shape(n, 4, 3, openRate));
          if (stale) {
            HistoryGenerator.staleRead(ops, n);
          }
          return List.of(ops);
        },
        !stale,
        runs);
  }

  private static Case simulated(long durationMs, int seeds) {
    return new Case(
        String.format("simulator, %d s runs, %d seeds", durationMs / 1000, seeds),
        () -> {
          List<List<History.Op>> out = new ArrayList<>();
          for (long seed = 0; seed < seeds; seed++) {
            Simulation.Options opts = Simulation.Options.defaults().withDuration(durationMs);
            out.add(
                KvSimulationTest.run(seed, opts, () -> new KvMachine(60_000, 1_000)).history().ops);
          }
          return out;
        },
        true,
        25);
  }

  /**
   * Median wall time per history for the reference and the new checker, or why one could not
   * finish. The two take turns, after three warm-up rounds, so that both see the same JIT and
   * garbage collector state.
   */
  private static String[] time(Case c, List<List<History.Op>> hs) {
    List<Predicate<List<History.Op>>> checkers =
        List.of(LinearizabilityReference::check, LinearizabilityBenchmark::fast);
    long[][] runs = new long[2][c.runs()];
    String[] out = new String[2];
    for (int r = -3; r < c.runs(); r++) {
      for (int k = 0; k < 2; k++) {
        if (out[k] != null) {
          continue;
        }
        long start = System.nanoTime();
        if (!finishes(checkers.get(k), c, hs)) {
          out[k] = "out of memory";
          continue;
        }
        if (r >= 0) {
          runs[k][r] = (System.nanoTime() - start) / hs.size();
        }
      }
    }
    for (int k = 0; k < 2; k++) {
      if (out[k] == null) {
        Arrays.sort(runs[k]);
        double ms = runs[k][runs[k].length / 2] / 1e6;
        out[k] = ms < 10 ? String.format("%.2f ms", ms) : String.format("%,.0f ms", ms);
      }
    }
    return out;
  }

  /**
   * Runs {@code checker} on every history and checks its answers; false if it ran out of memory.
   *
   * <p>The error is caught here, in a method that holds no objects of its own while the checker
   * runs, and the heap is collected before it returns (#80). A compiled frame that reaches a path
   * it never took while it was compiled, such as a handler, or a branch on an answer it had never
   * seen, deoptimizes, which re-allocates the objects the compiler had eliminated, such as a
   * for-each loop's iterator. Right after the failure the heap is still full of what the failed
   * search left, and that allocation throws a second OutOfMemoryError, out of the benchmark. The
   * collection also keeps G1 from still being short of space when the other checker starts, which
   * would charge the failure to it.
   */
  private static boolean finishes(
      Predicate<List<History.Op>> checker, Case c, List<List<History.Op>> hs) {
    try {
      for (int i = 0; i < hs.size(); i++) {
        if (checker.test(hs.get(i)) != c.linearizable()) {
          throw new AssertionError(c.name() + ": wrong answer");
        }
      }
      return true;
    } catch (OutOfMemoryError e) {
      System.gc();
      return false;
    }
  }
}
