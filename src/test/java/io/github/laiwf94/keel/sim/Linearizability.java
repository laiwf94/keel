package io.github.laiwf94.keel.sim;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Checks that a key-value history is linearizable: that every operation can be placed at one point
 * between its call and return so that the sequence obeys the store's sequential specification.
 *
 * <p>This is the algorithm of Wing and Gong as improved by Lowe ("Testing for linearizability",
 * 2017): a depth-first search over the call/return list that linearizes pending calls one at a time
 * and backtracks when a return is reached before its call was linearized, with a cache of visited
 * (linearized set, state) pairs. Porcupine and Knossos use the same approach. Linearizability is
 * compositional (Herlihy and Wing, 1990), so each key is checked on its own.
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
    Set<State> out = new HashSet<>();
    for (State s : states) {
      out.addAll(step(s, op));
    }
    return out;
  }

  /** Returns the first key whose history is not linearizable, with its operations. */
  static Optional<String> check(List<History.Op> ops) {
    Map<String, List<History.Op>> byKey = new HashMap<>();
    for (History.Op op : ops) {
      byKey.computeIfAbsent(op.key, k -> new ArrayList<>()).add(op);
    }
    for (var e : byKey.entrySet()) {
      if (!checkOne(e.getValue())) {
        StringBuilder sb = new StringBuilder("not linearizable on key " + e.getKey() + ":");
        e.getValue().stream()
            .sorted((a, b) -> Long.compare(a.call, b.call))
            .forEach(o -> sb.append("\n  ").append(o));
        return Optional.of(sb.toString());
      }
    }
    return Optional.empty();
  }

  /** A node in the doubly linked list of call and return events. */
  private static final class Event {
    final History.Op op;
    final int local;
    final boolean isCall;
    final long time;
    Event match;
    Event prev;
    Event next;

    Event(History.Op op, int local, boolean isCall, long time) {
      this.op = op;
      this.local = local;
      this.isCall = isCall;
      this.time = time;
    }
  }

  private record Visited(BitSet linearized, Set<State> states) {}

  private record Frame(Event call, Set<State> states) {}

  static boolean checkOne(List<History.Op> ops) {
    List<Event> events = new ArrayList<>();
    for (int i = 0; i < ops.size(); i++) {
      History.Op op = ops.get(i);
      Event call = new Event(op, i, true, op.call);
      Event ret = new Event(op, i, false, op.ret);
      call.match = ret;
      events.add(call);
      events.add(ret);
    }
    events.sort((a, b) -> Long.compare(a.time, b.time));
    Event head = new Event(null, -1, false, Long.MIN_VALUE);
    Event tail = head;
    for (Event ev : events) {
      tail.next = ev;
      ev.prev = tail;
      tail = ev;
    }

    BitSet linearized = new BitSet(ops.size());
    Set<Visited> cache = new HashSet<>();
    List<Frame> stack = new ArrayList<>();
    Set<State> state = Set.of(State.ABSENT);
    Event entry = head.next;
    while (head.next != null) {
      if (entry.isCall) {
        Set<State> next = stepAll(state, entry.op);
        if (!next.isEmpty()) {
          BitSet after = (BitSet) linearized.clone();
          after.set(entry.local);
          if (cache.add(new Visited(after, next))) {
            stack.add(new Frame(entry, state));
            state = next;
            linearized.set(entry.local);
            lift(entry);
            entry = head.next;
            continue;
          }
        }
        entry = entry.next;
      } else {
        if (stack.isEmpty()) {
          return false;
        }
        Frame f = stack.remove(stack.size() - 1);
        state = f.states();
        linearized.clear(f.call().local);
        unlift(f.call());
        entry = f.call().next;
      }
    }
    return true;
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
}
