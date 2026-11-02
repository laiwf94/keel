package io.github.laiwf94.keel.sim;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;

/**
 * Reduces a failing seed to the fewest faults and client steps that still fail the same way.
 *
 * <p>The seed is first run under a {@link Plan} that allows everything, which records how many
 * faults and steps it asked for. The fault ordinals are first cut to the shortest prefix that still
 * fails. Then delta debugging (Zeller and Hildebrandt, "Simplifying and isolating failure-inducing
 * input", 2002) replays the seed with halves, quarters and so on of the remaining faults, and with
 * their complements, and keeps any smaller set that still fails. Then it does the same over the
 * client steps, and it alternates the two passes until neither removes anything. When it finishes
 * inside its replay budget the result is 1-minimal: taking out any single remaining fault or step
 * makes the failure go away.
 *
 * <p>"The same way" means the same exception type thrown from the same line of keel code (the first
 * stack frame in keel outside the simulator harness), so the shrinker does not trade a lost write
 * for an unrelated liveness failure. Every replay is deterministic, so the candidates of one round
 * are run in parallel and the first failing one in order wins: the result does not depend on the
 * number of cores.
 */
final class Shrinker {
  /** Replays one shrink may spend unless {@code -Dkeel.sim.shrink.budget} says otherwise. */
  static final int DEFAULT_BUDGET = 3_000;

  /** Schedule lines the report prints before it summarizes the rest. */
  static final int REPORT_LINES = 200;

  private final LongConsumer body;
  private final long seed;
  private final Search search;
  private String signature;

  private Shrinker(LongConsumer body, long seed, int budget) {
    this.body = body;
    this.seed = seed;
    this.search = new Search(budget);
  }

  /**
   * What a shrink found. {@code faults} and {@code steps} are the ordinals that still have to
   * happen; {@code schedule} is what happened in the shrunk run.
   */
  record Result(
      String test,
      long seed,
      int originalFaults,
      int originalSteps,
      BitSet faults,
      BitSet steps,
      List<String> schedule,
      Throwable failure,
      int replays,
      boolean minimal) {

    /** The command that replays the shrunk run. */
    String replayCommand() {
      return Seeds.replayCommand(test, seed)
          + " -Dkeel.sim.faults="
          + Plan.format(faults)
          + " -Dkeel.sim.steps="
          + Plan.format(steps);
    }

    /** A report a person can read: sizes before and after, the schedule, the failure and replay. */
    String report() {
      StringBuilder b = new StringBuilder();
      b.append(test)
          .append(" seed ")
          .append(seed)
          .append(": shrunk from ")
          .append(originalFaults)
          .append(" faults and ")
          .append(originalSteps)
          .append(" steps to ")
          .append(faults.cardinality())
          .append(" faults and ")
          .append(steps.cardinality())
          .append(" steps in ")
          .append(replays)
          .append(" replays")
          .append(
              minimal
                  ? "; removing any one of them makes the failure go away"
                  : "; the replay budget ran out, so this may not be minimal")
          .append('\n');
      b.append("failure: ").append(firstLine(failure)).append('\n');
      b.append("schedule:\n");
      int shown = Math.min(schedule.size(), REPORT_LINES);
      for (String line : schedule.subList(0, shown)) {
        b.append("  ").append(line).append('\n');
      }
      if (schedule.size() > shown) {
        b.append("  ... and ").append(schedule.size() - shown).append(" more\n");
      }
      if (schedule.isEmpty()) {
        b.append("  (no faults and no client steps)\n");
      }
      b.append("replay with ").append(replayCommand());
      return b.toString();
    }
  }

  /**
   * Shrinks {@code seed} of {@code body}. A new round of candidates starts only while fewer than
   * {@code budget} replays have run, so a shrink may overshoot by up to one per core.
   *
   * @throws IllegalArgumentException if the seed does not fail
   */
  static Result shrink(String test, LongConsumer body, long seed, int budget) {
    return new Shrinker(body, seed, budget).run(test);
  }

  private Result run(String test) {
    Plan full = Plan.all();
    Throwable failure =
        attempt(full)
            .orElseThrow(
                () -> new IllegalArgumentException("seed " + seed + " passes; nothing to shrink"));
    signature = signature(failure);
    BitSet faults = new BitSet();
    faults.set(0, full.faultsAsked());
    BitSet steps = new BitSet();
    steps.set(0, full.stepsAsked());
    boolean changed = true;
    while (changed && !search.exhausted()) {
      BitSet s = steps;
      BitSet fewerFaults = search.ddmin(faults, c -> fails(c, s));
      BitSet fewerSteps = search.ddmin(steps, c -> fails(fewerFaults, c));
      changed = !fewerFaults.equals(faults) || !fewerSteps.equals(steps);
      faults = fewerFaults;
      steps = fewerSteps;
    }
    Plan last = Plan.only(faults, steps);
    Throwable lastFailure =
        attempt(last)
            .orElseThrow(() -> new IllegalStateException("the shrunk run no longer fails"));
    return new Result(
        test,
        seed,
        full.faultsAsked(),
        full.stepsAsked(),
        faults,
        steps,
        last.log(),
        lastFailure,
        search.replays(),
        !search.exhausted());
  }

  /** Runs the seed under {@code plan}; the failure, if it failed. */
  private Optional<Throwable> attempt(Plan plan) {
    Throwable[] failure = {null};
    Plan.runUnder(
        plan,
        () -> {
          try {
            body.accept(seed);
          } catch (RuntimeException | AssertionError e) {
            failure[0] = e;
          }
        });
    return Optional.ofNullable(failure[0]);
  }

  /** Whether the seed fails the same way with only these faults and steps. */
  private boolean fails(BitSet faults, BitSet steps) {
    return attempt(Plan.only(faults, steps)).map(t -> signature(t).equals(signature)).orElse(false);
  }

  /**
   * Delta debugging with a replay budget. Each {@link #ddmin} call remembers the answers it has
   * had, so only new candidates cost a replay.
   */
  static final class Search {
    private final int budget;
    private final AtomicInteger replays = new AtomicInteger();
    private boolean exhausted;

    Search(int budget) {
      this.budget = budget;
    }

    int replays() {
      return replays.get();
    }

    /** Whether the budget ran out before a search finished. */
    boolean exhausted() {
      return exhausted;
    }

    /**
     * The smallest subset of {@code set} found for which {@code fails} holds, assuming it holds for
     * {@code set} itself. If the budget lasts, the result is 1-minimal: {@code fails} is false for
     * the result with any one member taken out. Candidates of one round are tested in parallel, in
     * batches of one per core, and the first failing one in order is taken.
     */
    BitSet ddmin(BitSet set, Predicate<BitSet> fails) {
      Map<BitSet, Boolean> known = new ConcurrentHashMap<>();
      known.put((BitSet) set.clone(), true);
      Predicate<BitSet> cached =
          b -> {
            Boolean answer = known.get(b);
            if (answer == null) {
              replays.incrementAndGet();
              answer = fails.test(b);
              known.put(b, answer);
            }
            return answer;
          };
      List<Integer> c = set.stream().boxed().toList();
      if (c.isEmpty()) {
        return new BitSet();
      }
      // The cheapest answer first: maybe nothing of this kind is needed at all.
      if (firstFailing(List.of(List.of()), cached) >= 0) {
        return new BitSet();
      }
      c = shortestPrefix(c, cached);
      int n = 2;
      while (c.size() >= 2 && !exhausted) {
        List<List<Integer>> parts = split(c, n);
        List<List<Integer>> candidates = new ArrayList<>(parts);
        if (n > 2) {
          // With two parts each complement is the other part.
          for (int i = 0; i < parts.size(); i++) {
            candidates.add(complement(parts, i));
          }
        }
        int found = firstFailing(candidates, cached);
        if (found >= 0) {
          c = candidates.get(found);
          n = found < parts.size() ? 2 : Math.max(n - 1, 2);
        } else if (n >= c.size()) {
          break;
        } else {
          n = Math.min(c.size(), 2 * n);
        }
      }
      return bits(c);
    }

    /** Prefixes {@link #shortestPrefix} tries in each round. */
    static final int PREFIX_FANOUT = 7;

    /**
     * The shortest failing prefix of {@code c} found by a search that narrows the length down to
     * one of {@link #PREFIX_FANOUT} + 1 intervals per round. A failure usually needs only the start
     * of a run, and a whole tail goes in a few replays where delta debugging would take it apart
     * piece by piece. Failing is not monotone in the length, so this is a heuristic, but every
     * prefix it keeps has been seen to fail.
     */
    private List<Integer> shortestPrefix(List<Integer> c, Predicate<BitSet> fails) {
      int lo = 0; // the empty set passed
      int hi = c.size(); // the whole list fails
      while (hi - lo > 1 && !exhausted) {
        List<Integer> lengths = new ArrayList<>();
        for (int i = 1; i <= PREFIX_FANOUT; i++) {
          int len = lo + (int) ((long) (hi - lo) * i / (PREFIX_FANOUT + 1));
          if (len > lo && (lengths.isEmpty() || len > lengths.get(lengths.size() - 1))) {
            lengths.add(len);
          }
        }
        if (lengths.isEmpty()) {
          break;
        }
        List<List<Integer>> prefixes = new ArrayList<>();
        for (int len : lengths) {
          prefixes.add(c.subList(0, len));
        }
        int found = firstFailing(prefixes, fails);
        if (found >= 0) {
          hi = lengths.get(found);
          lo = found == 0 ? lo : lengths.get(found - 1);
        } else {
          lo = lengths.get(lengths.size() - 1);
        }
      }
      return List.copyOf(c.subList(0, hi));
    }

    /** The index of the first candidate, in order, for which {@code fails} holds, or -1. */
    private int firstFailing(List<List<Integer>> candidates, Predicate<BitSet> fails) {
      int batch = Math.max(1, Runtime.getRuntime().availableProcessors());
      for (int from = 0; from < candidates.size(); from += batch) {
        if (replays.get() >= budget) {
          exhausted = true;
          return -1;
        }
        int to = Math.min(candidates.size(), from + batch);
        boolean[] result = new boolean[to - from];
        int base = from;
        IntStream.range(from, to)
            .parallel()
            .forEach(i -> result[i - base] = fails.test(bits(candidates.get(i))));
        for (int i = 0; i < result.length; i++) {
          if (result[i]) {
            return from + i;
          }
        }
      }
      return -1;
    }
  }

  private static BitSet bits(List<Integer> members) {
    BitSet b = new BitSet();
    members.forEach(b::set);
    return b;
  }

  /** {@code c} cut into {@code n} contiguous parts whose sizes differ by at most one. */
  static List<List<Integer>> split(List<Integer> c, int n) {
    List<List<Integer>> parts = new ArrayList<>();
    int start = 0;
    for (int i = 0; i < n; i++) {
      int end = start + (c.size() - start) / (n - i);
      parts.add(List.copyOf(c.subList(start, end)));
      start = end;
    }
    return parts;
  }

  private static List<Integer> complement(List<List<Integer>> parts, int skip) {
    List<Integer> out = new ArrayList<>();
    for (int i = 0; i < parts.size(); i++) {
      if (i != skip) {
        out.addAll(parts.get(i));
      }
    }
    return out;
  }

  /**
   * What makes two failures the same: the exception type and the first stack frame in keel code
   * outside the harness that replays seeds.
   */
  static String signature(Throwable t) {
    for (StackTraceElement e : t.getStackTrace()) {
      String cls = e.getClassName();
      if (cls.startsWith("io.github.laiwf94.keel.") && !harness(cls)) {
        return t.getClass().getName() + " at " + e;
      }
    }
    return t.getClass().getName();
  }

  private static boolean harness(String cls) {
    for (Class<?> c : List.of(Shrinker.class, Plan.class, Seeds.class)) {
      if (cls.equals(c.getName()) || cls.startsWith(c.getName() + "$")) {
        return true;
      }
    }
    return false;
  }

  static String firstLine(Throwable t) {
    String m = String.valueOf(t.getMessage());
    int nl = m.indexOf('\n');
    return t.getClass().getSimpleName() + ": " + (nl < 0 ? m : m.substring(0, nl));
  }
}
