package io.github.laiwf94.keel.sim;

import io.github.laiwf94.keel.kv.KvMachine;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.management.CompilationMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
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
 * median of several runs is reported. The test JVM's 1 GiB heap applies, allocated in full from the
 * start.
 *
 * <p>Each checker is timed on each case in a JVM of its own, started with the same options and
 * classpath. The two used to take turns in one JVM, and each was then timed in the other's wake: on
 * the 100,000-operation histories the old search keeps several hundred megabytes live for a fifth
 * of a second, and G1 ran 206 concurrent marking cycles in 28 seconds of taking turns, so the new
 * checker ran beside marking threads, with the barriers marking turns on, and took a young
 * collection over the old search's heap in a third of its runs. That made its rows read twice its
 * time on its own and swing by as much from run to run, and the old search's rows were slowed the
 * same way by the runs before them.
 *
 * <p>The warm-up lasts until a round passes without the JIT compiler spending any time, at least
 * {@link #MIN_WARMUP} rounds and at most {@link #MAX_WARMUP}. A fixed three rounds was not enough:
 * on the first long histories the compiler was still at work for several of the timed runs, and the
 * new checker's median at 100,000 operations was mostly compilation.
 */
final class LinearizabilityBenchmark {
  private LinearizabilityBenchmark() {}

  private static final int MIN_WARMUP = 3;
  private static final int MAX_WARMUP = 20;

  /** The least time a warm-up takes, in a JVM that has run nothing else; see {@link #time}. */
  private static final long WARMUP_NANOS = 2_000_000_000L;

  private static boolean fast(List<History.Op> ops) {
    return Linearizability.check(ops).isEmpty();
  }

  /** One row: a set of histories, what they should check as, and which checkers can run them. */
  private record Case(
      String name, Supplier<List<List<History.Op>>> histories, boolean linearizable, int runs) {}

  /** The checkers, by the number a child JVM is given: the reference, then the new one. */
  private static final List<Predicate<List<History.Op>>> CHECKERS =
      List.of(LinearizabilityReference::check, LinearizabilityBenchmark::fast);

  /** What a child JVM prints before its result, so that nothing else it prints is taken for it. */
  private static final String RESULT = "result: ";

  public static void main(String[] args) {
    List<String> flags = Arrays.asList(args);
    List<Case> cases = cases(flags.contains("--simulator"));
    int one = flags.indexOf("--one");
    if (one >= 0) {
      // A child: time one checker on one case.
      Case c = cases.get(Integer.parseInt(args[one + 1]));
      int checker = Integer.parseInt(args[one + 2]);
      List<List<History.Op>> hs = c.histories().get();
      long ops = hs.stream().mapToLong(List::size).sum() / hs.size();
      System.out.println(RESULT + ops + " " + time(c, hs, CHECKERS.get(checker)));
      return;
    }
    System.out.println("| histories | operations | answer | reference | new |");
    System.out.println("|---|---:|---|---:|---:|");
    for (int i = 0; i < cases.size(); i++) {
      Case c = cases.get(i);
      String[] times = new String[2];
      long ops = 0;
      for (int k = 0; k < 2; k++) {
        String[] result = inChild(args, i, k).split(" ", 2);
        ops = Long.parseLong(result[0]);
        times[k] = result[1];
      }
      System.out.printf(
          "| %s | %,d | %s | %s | %s |%n",
          c.name(), ops, c.linearizable() ? "linearizable" : "one stale read", times[0], times[1]);
    }
  }

  /**
   * Runs case {@code i} with checker {@code k} in a new JVM, with this JVM's options and classpath,
   * and returns what it printed: the operations per history and the time.
   */
  private static String inChild(String[] args, int i, int k) {
    List<String> command = new ArrayList<>();
    command.add(ProcessHandle.current().info().command().orElse("java"));
    command.addAll(ManagementFactory.getRuntimeMXBean().getInputArguments());
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LinearizabilityBenchmark.class.getName());
    command.addAll(Arrays.asList(args));
    command.addAll(List.of("--one", Integer.toString(i), Integer.toString(k)));
    try {
      Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
      String result = null;
      List<String> output = new ArrayList<>();
      try (BufferedReader in =
          new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
        for (String line = in.readLine(); line != null; line = in.readLine()) {
          output.add(line);
          if (line.startsWith(RESULT)) {
            result = line.substring(RESULT.length());
          }
        }
      }
      int exit = p.waitFor();
      if (exit != 0 || result == null) {
        throw new IllegalStateException(
            "case "
                + i
                + ", checker "
                + k
                + " exited with "
                + exit
                + ":\n"
                + String.join("\n", output));
      }
      return result;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  /** The cases, in the order of the table; only the simulator's if {@code simulatorOnly}. */
  private static List<Case> cases(boolean simulatorOnly) {
    List<Case> cases = new ArrayList<>();
    for (int n : simulatorOnly ? new int[0] : new int[] {10_000, 100_000}) {
      cases.add(synthetic(n, 0.002, false, 11));
      cases.add(synthetic(n, 0, true, 11));
      cases.add(synthetic(n, 0.0005, true, 11));
    }
    if (!simulatorOnly) {
      cases.add(synthetic(10_000, 0.002, true, 11));
      cases.add(synthetic(30_000, 0.002, true, 1));
    }
    cases.add(simulated(6_000, 40));
    cases.add(simulated(60_000, 8));
    return cases;
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
   * Median wall time per history for {@code checker}, after the warm-up, or why it could not
   * finish.
   *
   * <p>The warm-up also lasts at least {@link #WARMUP_NANOS}. Taking turns, each case was warmed by
   * the cases before it; in a JVM of its own, three rounds of a few milliseconds each passed with
   * no compilation before the compiler had got to the hot loops, and the old search's
   * 10,000-operation stale read read 9.4 ms against 3.6 to 4.4 ms after a long warm-up.
   */
  private static String time(
      Case c, List<List<History.Op>> hs, Predicate<List<History.Op>> checker) {
    CompilationMXBean jit = ManagementFactory.getCompilationMXBean();
    long[] runs = new long[c.runs()];
    boolean warming = true;
    int warmups = 0;
    int r = 0;
    long began = System.nanoTime();
    while (r < c.runs()) {
      long compiling = jit.getTotalCompilationTime();
      long start = System.nanoTime();
      if (!finishes(checker, c, hs)) {
        return "out of memory";
      }
      if (warming) {
        warmups++;
        boolean settled = jit.getTotalCompilationTime() == compiling;
        boolean longEnough = System.nanoTime() - began >= WARMUP_NANOS;
        warming = !longEnough || (!(warmups >= MIN_WARMUP && settled) && warmups < MAX_WARMUP);
      } else {
        runs[r++] = (System.nanoTime() - start) / hs.size();
      }
    }
    Arrays.sort(runs);
    double ms = runs[runs.length / 2] / 1e6;
    return ms < 10 ? String.format("%.2f ms", ms) : String.format("%,.0f ms", ms);
  }

  /**
   * Runs {@code checker} on every history and checks its answers; false if it ran out of memory.
   *
   * <p>The error is caught here, in a method that holds no objects of its own while the checker
   * runs, and the heap is collected before it returns (#80). A compiled frame that reaches a path
   * it never took while it was compiled, such as a handler, or a branch on an answer it had never
   * seen, deoptimizes, which re-allocates the objects the compiler had eliminated, such as a
   * for-each loop's iterator. Right after the failure the heap is still full of what the failed
   * search left, and that allocation throws a second OutOfMemoryError, out of the benchmark.
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
