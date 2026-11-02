package io.github.laiwf94.keel.sim;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Which of a run's faults and client steps are allowed to happen.
 *
 * <p>Before the simulator injects a fault (a crash, a restart, a two-way or one-way partition, a
 * heal, a power loss, a failed fsync or a disk stall) it asks the plan, and so does a simulated
 * client before it issues an operation. Each question gets the next ordinal of its kind: {@code f0,
 * f1, ...} for faults and {@code s0, s1, ...} for steps. The plan answers from its sets of allowed
 * ordinals; a refused fault is skipped and a refused step leaves the client idle for a moment. The
 * random draws behind the decision are made either way, so a plan that allows everything replays a
 * seed exactly as it runs without a plan, and the {@link Shrinker} can take faults and steps out of
 * a failing run one set at a time.
 *
 * <p>Ambient behaviour is not part of the plan: message delays, drops and duplicates, ordinary sync
 * latency, election timeouts and membership changes stay as the seed draws them.
 *
 * <p>The simulator picks up the plan of the current thread ({@link #current()}), which {@link
 * Seeds} and the shrinker set around each run, so test bodies need no extra parameter.
 */
final class Plan {
  private static final ThreadLocal<Plan> CURRENT = new ThreadLocal<>();

  /** Allowed ordinals, or null for all of them. */
  private final BitSet faults;

  private final BitSet steps;
  private int faultCount;
  private int stepCount;
  private final List<String> log = new ArrayList<>();

  private Plan(BitSet faults, BitSet steps) {
    this.faults = faults == null ? null : (BitSet) faults.clone();
    this.steps = steps == null ? null : (BitSet) steps.clone();
  }

  /** Every fault and step happens: the run the seed alone describes. */
  static Plan all() {
    return new Plan(null, null);
  }

  /** Only these faults and steps happen; a null set allows every ordinal of its kind. */
  static Plan only(BitSet faults, BitSet steps) {
    return new Plan(faults, steps);
  }

  /** The plan of the run on this thread, or a fresh plan that allows everything. */
  static Plan current() {
    Plan p = CURRENT.get();
    return p != null ? p : all();
  }

  /** Runs {@code body} with {@code plan} as the current plan of this thread. */
  static void runUnder(Plan plan, Runnable body) {
    Plan previous = CURRENT.get();
    CURRENT.set(plan);
    try {
      body.run();
    } finally {
      if (previous == null) {
        CURRENT.remove();
      } else {
        CURRENT.set(previous);
      }
    }
  }

  /** Asks to inject the fault {@code what} at virtual time {@code at}; true if it may happen. */
  boolean fault(long at, String what) {
    int ordinal = faultCount++;
    return admit(faults, "f" + ordinal, ordinal, at, what);
  }

  /** Asks to issue the client operation {@code what} at virtual time {@code at}. */
  boolean step(long at, String what) {
    int ordinal = stepCount++;
    return admit(steps, "s" + ordinal, ordinal, at, what);
  }

  private boolean admit(BitSet allowed, String name, int ordinal, long at, String what) {
    if (allowed != null && !allowed.get(ordinal)) {
      return false;
    }
    log.add(name + " at " + at + " ms: " + what);
    return true;
  }

  /** How many faults the run asked for, allowed or not. */
  int faultsAsked() {
    return faultCount;
  }

  /** How many steps the run asked for, allowed or not. */
  int stepsAsked() {
    return stepCount;
  }

  /** The faults and steps that happened, in the order they happened. */
  List<String> log() {
    return List.copyOf(log);
  }

  /**
   * Writes a set of ordinals the way {@code -Dkeel.sim.faults} and {@code -Dkeel.sim.steps} take
   * it: comma-separated numbers and ranges such as {@code 0-4,7,9-12}, or {@code none}.
   */
  static String format(BitSet set) {
    if (set.isEmpty()) {
      return "none";
    }
    StringBuilder b = new StringBuilder();
    for (int from = set.nextSetBit(0); from >= 0; from = set.nextSetBit(from)) {
      int to = set.nextClearBit(from) - 1;
      if (b.length() > 0) {
        b.append(',');
      }
      b.append(from);
      if (to > from) {
        b.append('-').append(to);
      }
      from = to + 1;
    }
    return b.toString();
  }

  /** Reads what {@link #format} writes. */
  static BitSet parse(String text) {
    BitSet set = new BitSet();
    String t = text.strip();
    if (t.equals("none") || t.isEmpty()) {
      return set;
    }
    for (String part : t.split(",", -1)) {
      String p = part.strip();
      int dash = p.indexOf('-');
      try {
        if (dash < 0) {
          set.set(nonNegative(p, text));
        } else {
          int from = nonNegative(p.substring(0, dash), text);
          int to = nonNegative(p.substring(dash + 1), text);
          if (to < from) {
            throw new IllegalArgumentException("backwards range " + p + " in " + text);
          }
          set.set(from, to + 1);
        }
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("not a list of ordinals: " + text, e);
      }
    }
    return set;
  }

  private static int nonNegative(String s, String text) {
    int v = Integer.parseInt(s.strip());
    if (v < 0) {
      throw new IllegalArgumentException("negative ordinal in " + text);
    }
    return v;
  }
}
