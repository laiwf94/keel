package io.github.laiwf94.keel.sim;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The linearizability search as it was before {@link Linearizability} was optimized: Wing and Gong
 * with Lowe's memoization over one key's whole history, caching a copy of the linearized set and
 * the state set at every step, and linearizing unanswered operations too. It is kept as an oracle:
 * {@code LinearizabilityTest} checks that both agree on random histories, and {@code
 * LinearizabilityBenchmark} measures the speed-up against it.
 */
final class LinearizabilityReference {
  private LinearizabilityReference() {}

  /** Whether every key's history in {@code ops} is linearizable. */
  static boolean check(List<History.Op> ops) {
    Map<String, List<History.Op>> byKey = new HashMap<>();
    for (History.Op op : ops) {
      byKey.computeIfAbsent(op.key, k -> new ArrayList<>()).add(op);
    }
    return byKey.values().stream().allMatch(LinearizabilityReference::checkOne);
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

  private record Visited(BitSet linearized, Set<Linearizability.State> states) {}

  private record Frame(Event call, Set<Linearizability.State> states) {}

  /** Whether the history of one key is linearizable. */
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
    Set<Linearizability.State> state = Set.of(Linearizability.State.ABSENT);
    Event entry = head.next;
    while (head.next != null) {
      if (entry.isCall) {
        Set<Linearizability.State> next = stepAll(state, entry.op);
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

  /** {@link Linearizability#stepAll} as it was, so that the benchmark keeps its baseline. */
  private static Set<Linearizability.State> stepAll(
      Set<Linearizability.State> states, History.Op op) {
    Set<Linearizability.State> out = new HashSet<>();
    for (Linearizability.State s : states) {
      out.addAll(Linearizability.step(s, op));
    }
    return out;
  }
}
