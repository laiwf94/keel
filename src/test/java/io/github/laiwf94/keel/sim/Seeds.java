package io.github.laiwf94.keel.sim;

import java.util.function.LongConsumer;
import java.util.stream.LongStream;

/**
 * Runs a simulation over many seeds. {@code -Dkeel.sim.seeds=N} sets how many; {@code
 * -Dkeel.sim.seed=S} replays one. A failure names the seed and the command that replays it.
 */
final class Seeds {
  private Seeds() {}

  static void run(String testName, int defaultCount, LongConsumer body) {
    Long only = Long.getLong("keel.sim.seed");
    LongStream seeds =
        only != null
            ? LongStream.of(only)
            : LongStream.range(0, Integer.getInteger("keel.sim.seeds", defaultCount));
    seeds
        .parallel()
        .forEach(
            seed -> {
              try {
                body.accept(seed);
              } catch (RuntimeException | AssertionError e) {
                throw new AssertionError(
                    testName
                        + " failed for seed "
                        + seed
                        + "; replay with ./gradlew test --tests '*"
                        + testName
                        + "*' -Dkeel.sim.seed="
                        + seed,
                    e);
              }
            });
  }
}
