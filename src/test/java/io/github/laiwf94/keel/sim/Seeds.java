package io.github.laiwf94.keel.sim;

import java.util.BitSet;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;

/**
 * Runs a simulation over many seeds. {@code -Dkeel.sim.seeds=N} sets how many; {@code
 * -Dkeel.sim.seed=S} replays one. A failure names the seed, the command that replays it and the
 * command that shrinks it.
 *
 * <p>With {@code -Dkeel.sim.seed=S}, {@code -Dkeel.sim.faults=...} and {@code -Dkeel.sim.steps=...}
 * replay the seed with only those faults and client steps (see {@link Plan}), and {@code
 * -Dkeel.sim.shrink=true} runs the {@link Shrinker} on it instead, which prints the fewest faults
 * and steps that still fail and the command that replays exactly that. {@code
 * -Dkeel.sim.shrink.budget=N} caps the replays a shrink may spend.
 */
final class Seeds {
  private Seeds() {}

  /** What to run, normally read from system properties. A null plan set allows everything. */
  record Settings(int count, Long seed, BitSet faults, BitSet steps, boolean shrink, int budget) {
    static Settings fromSystemProperties(int defaultCount) {
      String faults = System.getProperty("keel.sim.faults");
      String steps = System.getProperty("keel.sim.steps");
      return new Settings(
          Integer.getInteger("keel.sim.seeds", defaultCount),
          Long.getLong("keel.sim.seed"),
          faults == null ? null : Plan.parse(faults),
          steps == null ? null : Plan.parse(steps),
          Boolean.getBoolean("keel.sim.shrink"),
          Integer.getInteger("keel.sim.shrink.budget", Shrinker.DEFAULT_BUDGET));
    }

    /** {@code count} seeds from 0, every fault and step. */
    static Settings seeds(int count) {
      return new Settings(count, null, null, null, false, Shrinker.DEFAULT_BUDGET);
    }
  }

  /** Runs {@code body} for each seed, as the system properties say. */
  static void run(int defaultCount, LongConsumer body) {
    run(caller(), Settings.fromSystemProperties(defaultCount), body);
  }

  /** Runs {@code body} for each seed; {@code test} names it in messages and commands. */
  static void run(String test, Settings settings, LongConsumer body) {
    if (settings.shrink()) {
      if (settings.seed() == null) {
        throw new IllegalArgumentException("-Dkeel.sim.shrink needs -Dkeel.sim.seed");
      }
      shrink(test, settings.seed(), settings.budget(), body);
      return;
    }
    if (settings.seed() == null && (settings.faults() != null || settings.steps() != null)) {
      throw new IllegalArgumentException(
          "-Dkeel.sim.faults and -Dkeel.sim.steps need -Dkeel.sim.seed");
    }
    LongStream seeds =
        settings.seed() != null
            ? LongStream.of(settings.seed())
            : LongStream.range(0, settings.count());
    String planned = planArguments(settings);
    seeds
        .parallel()
        .forEach(
            seed -> {
              try {
                Plan.runUnder(
                    Plan.only(settings.faults(), settings.steps()), () -> body.accept(seed));
              } catch (RuntimeException | AssertionError e) {
                throw new AssertionError(
                    test
                        + " failed for seed "
                        + seed
                        + "; replay with "
                        + replayCommand(test, seed)
                        + planned
                        + "; shrink it with "
                        + replayCommand(test, seed)
                        + " -Dkeel.sim.shrink=true",
                    e);
              }
            });
  }

  private static String planArguments(Settings s) {
    return (s.faults() == null ? "" : " -Dkeel.sim.faults=" + Plan.format(s.faults()))
        + (s.steps() == null ? "" : " -Dkeel.sim.steps=" + Plan.format(s.steps()));
  }

  private static void shrink(String test, long seed, int budget, LongConsumer body) {
    Shrinker.Result r;
    try {
      r = Shrinker.shrink(test, body, seed, budget);
    } catch (IllegalArgumentException e) {
      System.out.println(test + ": " + e.getMessage());
      return;
    }
    String report = r.report();
    System.out.println(report);
    throw new AssertionError(report, r.failure());
  }

  /** The Gradle command that runs {@code test} on {@code seed} alone. */
  static String replayCommand(String test, long seed) {
    return "./gradlew test --tests '*" + test + "' -Dkeel.sim.seed=" + seed;
  }

  /** {@code Class.method} of the test that called {@link #run(int, LongConsumer)}. */
  private static String caller() {
    StackWalker.StackFrame f =
        StackWalker.getInstance()
            .walk(
                frames ->
                    frames
                        .filter(x -> !x.getClassName().equals(Seeds.class.getName()))
                        .findFirst()
                        .orElseThrow());
    String cls = f.getClassName();
    return cls.substring(cls.lastIndexOf('.') + 1) + "." + f.getMethodName();
  }
}
