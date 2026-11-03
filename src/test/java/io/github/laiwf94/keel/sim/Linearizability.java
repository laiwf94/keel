package io.github.laiwf94.keel.sim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Checks that a key-value history is linearizable: that every operation can be placed at one point
 * between its call and return so that the sequence obeys the store's sequential specification.
 *
 * <p>This is the algorithm of Wing and Gong as improved by Lowe ("Testing for linearizability",
 * 2017): a depth-first search over the call/return list that linearizes pending calls one at a time
 * and backtracks when a return is reached before its call was linearized, with a cache of visited
 * configurations. Porcupine and Knossos use the same approach. Three things keep it fast on long
 * histories:
 *
 * <ul>
 *   <li>P-compositionality (Horn and Kroening, "Faster linearizability checking via
 *       P-compositionality", 2015): a history of a map is linearizable exactly when the history of
 *       each key is, so each key is a partition, checked on its own and in parallel.
 *   <li>Compact, hashed configurations: a visited configuration is the return-order prefix of
 *       answered operations that is linearized, a window of at most one operation per concurrent
 *       client beyond it, and the state set, hashed once. The cost of a step no longer grows with
 *       the length of the history, as copying a bit set of every operation did.
 *   <li>An early cut-off on unanswered operations: unanswered reads and writes nobody could observe
 *       are dropped, the search stops once every answered operation is placed, unanswered writes
 *       are tried last, and a configuration reached with more of them applied than before is
 *       pruned, so they no longer multiply the work of a failing search.
 * </ul>
 *
 * <p>{@link LinearizabilityReference} keeps the search as it was before, as an oracle.
 *
 * <p>The model is a register that also tracks versions. Versions are log indexes, so a put must
 * produce a version above the current one and a conditional write must see exactly the version it
 * expected. An open operation (no reply) has a known effect but an unknown version, written as
 * {@link History#ANY}; the first later operation that reports a concrete version binds it. Because
 * an open conditional write on a state with an unknown version may or may not have matched, the
 * model is nondeterministic, and the search runs over sets of states (the powerset construction
 * Porcupine also uses).
 */
final class Linearizability {
  private Linearizability() {}

  record State(String value, long version) {
    static final State ABSENT = new State(null, 0);

    boolean known() {
      return version != History.ANY;
    }

    /** Whether the stored version could be {@code v}. */
    boolean couldBe(long v) {
      return !known() || version == v;
    }
  }

  /** Every state {@code op} can lead to from {@code s} given what it observed; empty if none. */
  static List<State> step(State s, History.Op op) {
    boolean open = op.ret == History.OPEN;
    return switch (op.kind) {
      case GET -> {
        if (open) {
          yield List.of(s);
        }
        boolean ok = Objects.equals(s.value(), op.readValue) && s.couldBe(op.version);
        yield ok ? List.of(new State(s.value(), op.version)) : List.of();
      }
      case PUT, DELETE -> open ? stepOpenWrite(s, op) : stepWrite(s, op);
    };
  }

  private static State effect(History.Op op, long version) {
    return op.kind == History.Kind.PUT ? new State(op.value, version) : State.ABSENT;
  }

  private static List<State> stepOpenWrite(State s, History.Op op) {
    State applied = effect(op, History.ANY);
    if (op.expected == -1 || (s.known() && s.version() == op.expected)) {
      return List.of(applied);
    }
    if (s.known()) {
      return List.of(s); // the condition failed: no effect
    }
    return List.of(applied, s);
  }

  private static List<State> stepWrite(State s, History.Op op) {
    if (!op.ok) {
      // A conflict reports the version it found, which differs from the one expected.
      boolean ok = s.couldBe(op.version) && op.expected != op.version;
      return ok ? List.of(new State(s.value(), op.version)) : List.of();
    }
    if (op.expected != -1 && !s.couldBe(op.expected)) {
      return List.of();
    }
    if (op.kind == History.Kind.DELETE) {
      // A delete reports the version it removed.
      boolean ok = s.couldBe(op.version) && (op.expected == -1 || op.expected == op.version);
      return ok ? List.of(State.ABSENT) : List.of();
    }
    long before = s.known() ? s.version() : op.expected;
    boolean newer = before == -1 || op.version > before;
    return newer ? List.of(effect(op, op.version)) : List.of();
  }

  /** Applies {@code op} to every state in {@code states}; empty if it fits none of them. */
  static Set<State> stepAll(Set<State> states, History.Op op) {
    if (states.size() == 1) {
      List<State> next = step(states.iterator().next(), op);
      return switch (next.size()) {
        case 0 -> Set.of();
        case 1 -> Set.of(next.get(0));
        default -> new HashSet<>(next);
      };
    }
    Set<State> out = new HashSet<>();
    for (State s : states) {
      out.addAll(step(s, op));
    }
    return out;
  }

  /**
   * Returns a description of the first key, in key order, whose history is not linearizable. Keys
   * of a long history are checked in parallel, since each is a partition of its own.
   */
  static Optional<String> check(List<History.Op> ops) {
    Map<String, List<History.Op>> byKey = new TreeMap<>();
    for (History.Op op : ops) {
      byKey.computeIfAbsent(op.key, k -> new ArrayList<>()).add(op);
    }
    var keys = byKey.entrySet().stream();
    return (ops.size() >= PARALLEL_FROM ? keys.parallel() : keys)
        .map(e -> new Search(e.getValue()).verdict(e.getKey()))
        .flatMap(Optional::stream)
        .findFirst();
  }

  /** Below this many operations, checking keys in parallel costs more than it saves. */
  private static final int PARALLEL_FROM = 10_000;

  /** A node in the doubly linked list of the call and return events of answered operations. */
  private static final class Event {
    final History.Op op;
    final int rank;
    final boolean isCall;
    final long time;
    Event match;
    Event prev;
    Event next;

    Event(History.Op op, int rank, boolean isCall, long time) {
      this.op = op;
      this.rank = rank;
      this.isCall = isCall;
      this.time = time;
    }
  }

  /**
   * Where the search stands, apart from which unanswered operations it has applied: every answered
   * operation of return rank below {@code prefix} is linearized, so are those in {@code window}
   * (ranks above it, at most one per concurrent client), and no other.
   */
  private static final class Config {
    final int prefix;
    final int[] window;
    final Set<State> states;
    final int hash;

    Config(int prefix, int[] window, Set<State> states) {
      this.prefix = prefix;
      this.window = window;
      this.states = states;
      this.hash = (prefix * 31 + Arrays.hashCode(window)) * 31 + states.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Config c
          && c.hash == hash
          && c.prefix == prefix
          && Arrays.equals(c.window, window)
          && c.states.equals(states);
    }

    @Override
    public int hashCode() {
      return hash;
    }
  }

  /**
   * What to restore on backtracking. {@code call} is the answered operation linearized at this
   * step, or null when it was the unanswered operation {@code open}.
   */
  private static final class Frame {
    final Event call;
    final int open;
    final Set<State> states;
    final int prefix;
    final int[] window;
    final Seen seen;

    Frame(Event call, int open, Set<State> states, int prefix, int[] window, Seen seen) {
      this.call = call;
      this.open = open;
      this.states = states;
      this.prefix = prefix;
      this.window = window;
      this.seen = seen;
    }
  }

  /**
   * One visit of a {@link Config}: the unanswered operations applied at the time, and, once the
   * search below it has finished, the classes of interchangeable unanswered operations it ever
   * found itself without an available one of ({@code shortOf}).
   */
  private static final class Seen {
    final long[] applied;
    final long[] shortOf;
    final Seen next; // an earlier visit of the same configuration
    boolean done;

    Seen(long[] applied, long[] shortOf, Seen next) {
      this.applied = applied;
      this.shortOf = shortOf;
      this.next = next;
    }
  }

  private static final long[] NO_BITS = new long[0];

  private static final int[] NONE = new int[0];

  /**
   * The value of an unanswered put that no answered read returned. Only reads look at values, so
   * such puts are interchangeable, and sharing one value lets states that differ only in which of
   * them was applied compare equal. Simulated clients never write a NUL.
   */
  private static final String UNREAD = "\0unread";

  /**
   * The search over one key's history.
   *
   * <p>Unanswered reads have no effect and are dropped, and so are unanswered writes called after
   * the last answer, since nothing could observe them. Only answered operations go into the event
   * list; unanswered writes sit in a list of their own, by call, and may be linearized at any point
   * after their call or never. The search succeeds as soon as every answered operation is placed.
   * At each step it tries the answered operations first and the unanswered ones last, and only
   * those that could help there ({@link #helps}), so it leaves them out unless it needs them.
   * Unanswered deletes, and puts whose value no answered read returns, are interchangeable with the
   * others of the same kind and condition; such a class is applied in call order, so only its
   * earliest unapplied member is ever tried.
   *
   * <p>A configuration is identified by its {@link Config}, which is exact and small: because the
   * search can only linearize an operation called before the first return still in the list, the
   * linearized set is a prefix of the answered operations in return order plus a window no larger
   * than the number of concurrent clients. Each configuration is hashed once. The cache keeps, for
   * each one, the unanswered operations that were applied when it was reached ({@link Seen}), and
   * prunes a later visit that an earlier one covers ({@link #visit}). Without that, k unanswered
   * writes would multiply the configurations of a failing search by up to 2^k.
   */
  private static final class Search {
    private final History.Op[] answered; // by return
    private final History.Op[] open; // by call, with unread values replaced by UNREAD
    private final History.Op[] openAsCalled;
    private final int[] classOf;
    private final int[] firstOfClass;
    private final int[][] members; // of each class, by call
    private final boolean[] alone; // a class of one: a put whose value an answered read returns
    private final boolean[] loose; // unconditional and interchangeable: counts may be relaxed
    private final int[] appliedOfClass;
    private final long[] rootShortOf;
    private final List<History.Op> failing = new ArrayList<>();
    // The states after each candidate read or conflict that fits.
    private final List<Set<State>> observed = new ArrayList<>();
    private final List<History.Op> blocked = new ArrayList<>();
    private final Event head = new Event(null, -1, false, Long.MIN_VALUE);
    private final long[] applied;
    private final Map<Config, Seen> cache = new HashMap<>();
    private final List<Frame> stack = new ArrayList<>();
    private Set<State> states = Set.of(State.ABSENT);
    private int prefix;
    private int[] window = NONE;
    private int deepest;

    Search(List<History.Op> ops) {
      long lastReturn = Long.MIN_VALUE;
      for (History.Op op : ops) {
        if (op.ret != History.OPEN) {
          lastReturn = Math.max(lastReturn, op.ret);
        }
      }
      List<History.Op> done = new ArrayList<>();
      List<History.Op> pending = new ArrayList<>();
      for (History.Op op : ops) {
        if (op.ret != History.OPEN) {
          done.add(op);
        } else if (op.kind != History.Kind.GET && op.call < lastReturn) {
          pending.add(op);
        }
      }
      answered =
          done.stream().sorted(Comparator.comparingLong(o -> o.ret)).toArray(History.Op[]::new);
      openAsCalled =
          pending.stream().sorted(Comparator.comparingLong(o -> o.call)).toArray(History.Op[]::new);
      Set<String> read = new HashSet<>();
      for (int i = 0; i < done.size() && !pending.isEmpty(); i++) {
        History.Op op = done.get(i);
        if (op.kind == History.Kind.GET && op.readValue != null) {
          read.add(op.readValue);
        }
      }
      open = new History.Op[openAsCalled.length];
      classOf = new int[open.length];
      Map<List<Object>, Integer> classes = new HashMap<>();
      List<Integer> firsts = new ArrayList<>();
      for (int i = 0; i < open.length; i++) {
        History.Op o = openAsCalled[i];
        boolean unread = o.kind == History.Kind.PUT && !read.contains(o.value);
        String value = unread ? UNREAD : o.value;
        open[i] = new History.Op(o.id, o.client, o.kind, o.key, value, o.expected, o.call);
        // Deletes and unread puts with the same condition are interchangeable; others are not.
        List<Object> cls =
            unread || o.kind == History.Kind.DELETE ? List.of(o.kind, o.expected) : List.of(i);
        int id = i;
        classOf[i] =
            classes.computeIfAbsent(
                cls,
                k -> {
                  firsts.add(id);
                  return firsts.size() - 1;
                });
      }
      firstOfClass = firsts.stream().mapToInt(Integer::intValue).toArray();
      alone = new boolean[firstOfClass.length];
      loose = new boolean[firstOfClass.length];
      for (int c = 0; c < alone.length; c++) {
        History.Op o = open[firstOfClass[c]];
        alone[c] = o.kind == History.Kind.PUT && !UNREAD.equals(o.value);
        loose[c] = !alone[c] && o.expected == -1;
      }
      int[] sizes = new int[firstOfClass.length];
      for (int c : classOf) {
        sizes[c]++;
      }
      members = new int[firstOfClass.length][];
      for (int c = 0; c < members.length; c++) {
        members[c] = new int[sizes[c]];
        sizes[c] = 0;
      }
      for (int o = 0; o < open.length; o++) {
        members[classOf[o]][sizes[classOf[o]]++] = o;
      }
      appliedOfClass = new int[firstOfClass.length];
      rootShortOf = new long[(firstOfClass.length + 63) / 64];
      applied = new long[(open.length + 63) / 64];
      List<Event> events = new ArrayList<>(2 * answered.length);
      for (int r = 0; r < answered.length; r++) {
        Event call = new Event(answered[r], r, true, answered[r].call);
        Event ret = new Event(answered[r], r, false, answered[r].ret);
        call.match = ret;
        events.add(call);
        events.add(ret);
      }
      events.sort(Comparator.comparingLong(e -> e.time));
      Event tail = head;
      for (Event ev : events) {
        tail.next = ev;
        ev.prev = tail;
        tail = ev;
      }
    }

    boolean run() {
      Event entry = head.next;
      int nextOpen = 0;
      while (true) {
        if (prefix == answered.length) {
          return true;
        }
        if (entry.isCall) {
          Set<State> next = stepAll(states, entry.op);
          if (!next.isEmpty()) {
            int savedPrefix = prefix;
            int[] savedWindow = window;
            place(entry.rank);
            Seen seen = visit(new Config(prefix, window, next));
            if (seen != null) {
              stack.add(new Frame(entry, -1, states, savedPrefix, savedWindow, seen));
              states = next;
              lift(entry);
              entry = head.next;
              nextOpen = 0;
              continue;
            }
            prefix = savedPrefix;
            window = savedWindow;
          }
          entry = entry.next;
          continue;
        }
        // entry is the first return still in the list; unanswered writes called before it may go
        // here.
        boolean moved = false;
        if (nextOpen < open.length && open[nextOpen].call < entry.time) {
          needs();
        }
        for (; nextOpen < open.length && open[nextOpen].call < entry.time; nextOpen++) {
          int o = nextOpen;
          if (o != head(classOf[o])) {
            continue; // applied, or a member of its class called earlier is not
          }
          Set<State> next = stepAll(states, open[o]);
          if (next.isEmpty() || !helps(open[o], next)) {
            continue;
          }
          flip(o);
          Seen seen = visit(new Config(prefix, window, next));
          if (seen != null) {
            stack.add(new Frame(null, o, states, prefix, window, seen));
            states = next;
            moved = true;
            break;
          }
          flip(o);
        }
        if (moved) {
          entry = head.next;
          nextOpen = 0;
          continue;
        }
        if (stack.isEmpty()) {
          return false;
        }
        Frame f = stack.remove(stack.size() - 1);
        f.seen.done = true;
        or(shortOf(), f.seen.shortOf);
        states = f.states;
        prefix = f.prefix;
        window = f.window;
        if (f.call != null) {
          unlift(f.call);
          entry = f.call.next;
          nextOpen = 0;
        } else {
          flip(f.open);
          entry = firstReturn();
          needs();
          nextOpen = f.open + 1;
        }
      }
    }

    /**
     * Collects what an unanswered write could help with here: the candidates that fit no state, and
     * the waiting unanswered conditional writes that no state lets match.
     */
    private void needs() {
      failing.clear();
      blocked.clear();
      observed.clear();
      for (Event e = head.next; e.isCall; e = e.next) {
        Set<State> after = stepAll(states, e.op);
        if (after.isEmpty()) {
          failing.add(e.op);
        } else if (e.op.kind == History.Kind.GET || !e.op.ok) {
          observed.add(after);
        }
      }
      long limit = firstReturn().time;
      for (int o = 0; o < open.length && open[o].call < limit; o++) {
        if (!isApplied(o) && open[o].expected != -1 && !couldMatch(states, open[o].expected)) {
          blocked.add(open[o]);
        }
      }
      // A class runs short here if all of it that has been called is applied, yet one more would
      // be worth trying.
      long[] mark = shortOf();
      for (int c = 0; c < firstOfClass.length; c++) {
        if (appliedOfClass[c] > 0 && !alone[c]) {
          int h = head(c);
          if (h < 0 || open[h].call >= limit) {
            Set<State> after = stepAll(states, open[members[c][0]]);
            if (!after.isEmpty() && helps(open[members[c][0]], after)) {
              mark[c >>> 6] |= 1L << c;
            }
          }
        }
      }
    }

    /**
     * Whether the unanswered write {@code w}, which leads to {@code after}, is worth trying here:
     * whether some candidate fits no state now but fits one of {@code after}, or a waiting
     * conditional write with a different effect could match only after it.
     *
     * <p>That is enough. Take a linearization that applies an unanswered write w here, and let b be
     * the next operation. If b fits the states without w, then either b overwrites the value and w
     * can be dropped, or b is a conflict or a read that leaves the value alone, and w can move
     * after b: what b observed did not need w, and a state whose version w left unknown allows
     * whatever the same state with b's version does. That fails only if w is conditional and b pins
     * the version to one w does not expect, so that case is tried too. An unanswered conditional
     * write in between can be moved the same way unless it can only match after w. Repeating this
     * moves w to a point that passes this test, or out of the linearization.
     */
    private boolean helps(History.Op w, Set<State> after) {
      if (w.expected != -1 && couldMatch(states, w.expected)) {
        // A read or a conflict may pin the version so that w no longer matches after it.
        for (Set<State> o : observed) {
          if (!couldMatch(o, w.expected)) {
            return true;
          }
        }
      }
      for (History.Op op : failing) {
        if (!stepAll(after, op).isEmpty()) {
          return true;
        }
      }
      for (History.Op c : blocked) {
        // If c has the same effect as w, applying c after w changes nothing w did not.
        boolean same = c.kind == w.kind && Objects.equals(c.value, w.value);
        if (!same && couldMatch(after, c.expected)) {
          return true;
        }
      }
      return false;
    }

    private static boolean couldMatch(Set<State> states, long expected) {
      for (State st : states) {
        if (st.couldBe(expected)) {
          return true;
        }
      }
      return false;
    }

    private Event firstReturn() {
      Event e = head.next;
      while (e.isCall) {
        e = e.next;
      }
      return e;
    }

    /** Adds the answered operation of return rank {@code rank} to the linearized set. */
    private void place(int rank) {
      if (rank != prefix) {
        int at = -Arrays.binarySearch(window, rank) - 1;
        int[] w = new int[window.length + 1];
        System.arraycopy(window, 0, w, 0, at);
        w[at] = rank;
        System.arraycopy(window, at, w, at + 1, window.length - at);
        window = w;
        return;
      }
      prefix++;
      int skip = 0;
      while (skip < window.length && window[skip] == prefix) {
        prefix++;
        skip++;
      }
      if (skip > 0) {
        window = skip == window.length ? NONE : Arrays.copyOfRange(window, skip, window.length);
      }
      deepest = Math.max(deepest, prefix);
    }

    /**
     * Records a visit of {@code c} with the unanswered operations now applied and returns it, or
     * returns null if an earlier visit already covers it.
     *
     * <p>An earlier visit covers this one if it had applied no unanswered operation that this one
     * has not, since those are optional and having more of them left can only allow more. It also
     * covers this one if it had applied more interchangeable ones of some classes, provided its
     * search has finished and never ran short of any of those classes: this search would make the
     * same moves with earlier operations of the same classes, so the extra ones left could not
     * help.
     */
    private Seen visit(Config c) {
      Seen first =
          new Seen(
              applied.length == 0 ? NO_BITS : applied.clone(),
              rootShortOf.length == 0 ? NO_BITS : new long[rootShortOf.length],
              null);
      Seen last = cache.putIfAbsent(c, first);
      if (last == null) {
        return first; // the usual case: a configuration not seen before
      }
      for (Seen e = last; e != null; e = e.next) {
        if (covers(e)) {
          if (e.done) {
            or(shortOf(), e.shortOf);
          } else {
            Arrays.fill(shortOf(), -1L);
          }
          return null;
        }
      }
      Seen v = new Seen(first.applied, first.shortOf, last);
      cache.put(c, v);
      return v;
    }

    private boolean covers(Seen e) {
      for (int i = 0; i < applied.length; i++) {
        long extra = e.applied[i] & ~applied[i];
        while (extra != 0) {
          int o = i * 64 + Long.numberOfTrailingZeros(extra);
          extra &= extra - 1;
          int c = classOf[o];
          if (!e.done || !loose[c] || (e.shortOf[c >>> 6] & (1L << c)) != 0) {
            return false;
          }
        }
      }
      return true;
    }

    /** Where shortages found now are recorded: the visit being searched. */
    private long[] shortOf() {
      return stack.isEmpty() ? rootShortOf : stack.get(stack.size() - 1).seen.shortOf;
    }

    private static void or(long[] into, long[] from) {
      for (int i = 0; i < into.length; i++) {
        into[i] |= from[i];
      }
    }

    /** The first operation of class {@code c} not applied yet, or -1. */
    private int head(int c) {
      int n = appliedOfClass[c];
      return n < members[c].length ? members[c][n] : -1;
    }

    private boolean isApplied(int o) {
      return (applied[o >>> 6] & (1L << o)) != 0;
    }

    private void flip(int o) {
      applied[o >>> 6] ^= 1L << o;
      appliedOfClass[classOf[o]] += isApplied(o) ? 1 : -1;
    }

    private static void lift(Event call) {
      call.prev.next = call.next;
      if (call.next != null) {
        call.next.prev = call.prev;
      }
      Event ret = call.match;
      ret.prev.next = ret.next;
      if (ret.next != null) {
        ret.next.prev = ret.prev;
      }
    }

    private static void unlift(Event call) {
      Event ret = call.match;
      ret.prev.next = ret;
      if (ret.next != null) {
        ret.next.prev = ret;
      }
      call.prev.next = call;
      if (call.next != null) {
        call.next.prev = call;
      }
    }

    /** Empty if linearizable, else what the search could not get past. */
    Optional<String> verdict(String key) {
      if (run()) {
        return Optional.empty();
      }
      History.Op stuck = answered[deepest];
      StringBuilder sb =
          new StringBuilder("not linearizable on key ")
              .append(key)
              .append(": no order of the operations explains ")
              .append(stuck)
              .append("\nthe operations up to it, with those running at the same time:");
      long from = answered[Math.max(0, deepest - 8)].call;
      for (History.Op o : answered) {
        from = o.ret > stuck.call ? Math.min(from, o.call) : from;
      }
      long since = from;
      Stream.concat(Arrays.stream(answered), Arrays.stream(openAsCalled))
          .filter(o -> o.call <= stuck.ret && (o.ret >= since || o.ret == History.OPEN))
          .sorted(Comparator.comparingLong(o -> o.call))
          .forEach(o -> sb.append("\n  ").append(o));
      return Optional.of(sb.toString());
    }
  }
}
