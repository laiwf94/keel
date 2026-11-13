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
 * the simulator's histories; it prints a Markdown table. Each case is run after a warm-up and timed
 * for at least {@link #TIMED_NANOS} and {@link Case#runs} runs; the median wall time and the mean
 * process CPU time per history are reported. The test JVM's 1 GiB heap applies, allocated in full
 * from the start.
 *
 * <p>Each checker is timed on each case in {@link #DEFAULT_JVMS} JVMs, the two checkers taking
 * turns, or as many as {@code --jvms N} asks for. A cell gives the median of the JVMs' medians and,
 * in brackets, the lowest and highest. Eleven runs in one JVM swung by up to a quarter from one
 * benchmark to the next on a shared 4-core machine, which was more than the margin the checker's
 * time budget is judged by; most of the swing is between JVMs (where the JIT puts its code, which
 * cores the threads land on), not between runs in one, so more runs alone do not settle it. Below
 * the table, {@link #budget} compares each 100,000-operation row with the old search's
 * 10,000-operation row of the same shape and says whether it is within that time: only when every
 * JVM agrees, and otherwise that the ranges overlap.
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

  /** The least time the timed runs of a case take in one JVM, and the most runs they take. */
  private static final long TIMED_NANOS = 2_000_000_000L;

  private static final int MAX_RUNS = 1_000;

  /** The JVMs each checker is timed in on each case, unless {@code --jvms} says otherwise. */
  static final int DEFAULT_JVMS = 3;

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
      Timing t = time(c, hs, CHECKERS.get(checker));
      System.out.println(
          RESULT + ops + " " + (t == null ? "oom" : t.wallNanos() + " " + t.cpuNanos()));
      return;
    }
    int at = flags.indexOf("--jvms");
    int jvms = at >= 0 ? Integer.parseInt(args[at + 1]) : DEFAULT_JVMS;
    if (jvms < 1) {
      throw new IllegalArgumentException("--jvms must be at least 1");
    }
    String[] childArgs = withoutJvms(args);
    System.out.printf(
        "Median wall time and mean process CPU time per history over at least %d s of timed runs"
            + " in each JVM; %d JVMs per checker, median [lowest, highest].%n%n",
        TIMED_NANOS / 1_000_000_000L, jvms);
    System.out.println(
        "| histories | operations | answer | reference | reference CPU | new | new CPU |");
    System.out.println("|---|---:|---|---:|---:|---:|---:|");
    List<Spread[]> rows = new ArrayList<>();
    for (int i = 0; i < cases.size(); i++) {
      Case c = cases.get(i);
      long[][][] t = new long[2][2][jvms];
      boolean[] oom = new boolean[2];
      long ops = 0;
      for (int j = 0; j < jvms; j++) {
        for (int k = 0; k < 2; k++) {
          if (oom[k]) {
            continue;
          }
          String[] result = inChild(childArgs, i, k).split(" ", 3);
          ops = Long.parseLong(result[0]);
          if (result[1].equals("oom")) {
            oom[k] = true;
          } else {
            t[k][0][j] = Long.parseLong(result[1]);
            t[k][1][j] = Long.parseLong(result[2]);
          }
        }
      }
      Spread[] row = new Spread[4];
      for (int k = 0; k < 2; k++) {
        for (int m = 0; m < 2; m++) {
          row[2 * k + m] = oom[k] ? null : Spread.of(t[k][m]);
        }
      }
      rows.add(row);
      System.out.printf(
          "| %s | %,d | %s | %s | %s | %s | %s |%n",
          c.name(),
          ops,
          c.linearizable() ? "linearizable" : "one stale read",
          cell(row[0]),
          cell(row[1]),
          cell(row[2]),
          cell(row[3]));
    }
    if (!flags.contains("--simulator")) {
      System.out.println();
      // Rows 3 and 4 are rows 0 and 1 with ten times the operations.
      System.out.println("- linearizable: " + budget(rows.get(3)[2], rows.get(0)[0]));
      System.out.println("- one stale read: " + budget(rows.get(4)[2], rows.get(1)[0]));
    }
  }

  private static String[] withoutJvms(String[] args) {
    List<String> out = new ArrayList<>(Arrays.asList(args));
    int at = out.indexOf("--jvms");
    if (at >= 0) {
      out.subList(at, Math.min(at + 2, out.size())).clear();
    }
    return out.toArray(new String[0]);
  }

  /** The median, lowest and highest of a checker's per-JVM medians on one case, in nanoseconds. */
  record Spread(long median, long low, long high) {
    static Spread of(long[] values) {
      long[] v = values.clone();
      Arrays.sort(v);
      return new Spread(v[v.length / 2], v[0], v[v.length - 1]);
    }
  }

  private static String cell(Spread s) {
    if (s == null) {
      return "out of memory";
    }
    if (s.low() == s.high()) {
      return ms(s.median());
    }
    return ms(s.median()) + " [" + ms(s.low()) + ", " + ms(s.high()) + "]";
  }

  static String ms(long nanos) {
    double ms = nanos / 1e6;
    return ms < 10 ? String.format("%.2f ms", ms) : String.format("%,.0f ms", ms);
  }

  /**
   * Whether the new checker's wall time on a 100,000-operation row, {@code longer}, is within the
   * old search's on the 10,000-operation row of the same shape, {@code budget}: within if the
   * slowest JVM of the one was no slower than the fastest of the other, over if the fastest was
   * slower than the slowest, and otherwise not settled.
   */
  static String budget(Spread longer, Spread budget) {
    String ratio = String.format("%.2fx the budget", (double) longer.median() / budget.median());
    String times =
        "new at 100,000 " + ms(longer.median()) + ", old at 10,000 " + ms(budget.median());
    if (longer.high() <= budget.low()) {
      return "within the budget (" + times + ", " + ratio + ", every JVM)";
    }
    if (longer.low() > budget.high()) {
      return "over the budget (" + times + ", " + ratio + ", every JVM)";
    }
    return "not settled, the ranges overlap (" + times + ", " + ratio + ")";
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

  /** A checker's median wall time and mean process CPU time per history in one JVM. */
  record Timing(long wallNanos, long cpuNanos) {}

  /**
   * Median wall and mean process CPU time per history for {@code checker}, after the warm-up, or
   * null if it ran out of memory. The timed runs last at least {@link #TIMED_NANOS} and {@link
   * Case#runs} runs, and at most {@link #MAX_RUNS} runs.
   *
   * <p>The warm-up also lasts at least {@link #WARMUP_NANOS}. Taking turns, each case was warmed by
   * the cases before it; in a JVM of its own, three rounds of a few milliseconds each passed with
   * no compilation before the compiler had got to the hot loops, and the old search's
   * 10,000-operation stale read read 9.4 ms against 3.6 to 4.4 ms after a long warm-up.
   *
   * <p>CPU time is the whole process's, so it includes the checker's worker threads, and also the
   * collector's and, if it wakes, the compiler's. It is read once around all the timed runs, not
   * around each: on Linux the JDK counts it in clock ticks of 10 ms, longer than most runs.
   */
  private static Timing time(
      Case c, List<List<History.Op>> hs, Predicate<List<History.Op>> checker) {
    CompilationMXBean jit = ManagementFactory.getCompilationMXBean();
    com.sun.management.OperatingSystemMXBean os =
        (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    long[] wall = new long[MAX_RUNS];
    boolean warming = true;
    int warmups = 0;
    int r = 0;
    long began = System.nanoTime();
    long timedFrom = 0;
    long cpuFrom = 0;
    while (r < MAX_RUNS
        && (warming || r < c.runs() || System.nanoTime() - timedFrom < TIMED_NANOS)) {
      long compiling = jit.getTotalCompilationTime();
      long start = System.nanoTime();
      if (!finishes(checker, c, hs)) {
        return null;
      }
      long end = System.nanoTime();
      if (warming) {
        warmups++;
        boolean settled = jit.getTotalCompilationTime() == compiling;
        boolean longEnough = end - began >= WARMUP_NANOS;
        warming = !longEnough || (!(warmups >= MIN_WARMUP && settled) && warmups < MAX_WARMUP);
        timedFrom = System.nanoTime();
        cpuFrom = os.getProcessCpuTime();
      } else {
        wall[r] = (end - start) / hs.size();
        r++;
      }
    }
    long cpu = (os.getProcessCpuTime() - cpuFrom) / ((long) r * hs.size());
    return new Timing(median(wall, r), cpu);
  }

  static long median(long[] values, int n) {
    long[] v = Arrays.copyOf(values, n);
    Arrays.sort(v);
    return v[n / 2];
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
