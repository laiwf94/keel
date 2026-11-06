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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Checks that a key-value history is linearizable: that every operation can be placed at one point
 * between its call and return so that the sequence obeys the store's sequential specification.
 *
 * <p>This is the algorithm of Wing and Gong as improved by Lowe ("Testing for linearizability",
 * 2017): a depth-first search over the call/return list that linearizes pending calls one at a time
 * and backtracks when a return is reached before its call was linearized, with a cache of visited
 * configurations. Porcupine and Knossos use the same approach. Five things keep it fast on long
 * histories:
 *
 * <ul>
 *   <li>P-compositionality (Horn and Kroening, "Faster linearizability checking via
 *       P-compositionality", 2015): a history of a map is linearizable exactly when the history of
 *       each key is, so each key is a partition, checked on its own and in parallel.
 *   <li>Compact, hashed configurations: a visited configuration is the return-order prefix of
 *       answered operations that is linearized, a window of at most one operation per concurrent
 *       client beyond it, and the state set. The cost of a step does not grow with the length of
 *       the history, as copying a bit set of every operation did.
 *   <li>Only finished configurations are cached. One still being searched is an ancestor on the
 *       stack, and the only ancestors a move can lead back to are found by looking down the stack.
 *       A search that never backtracks hashes nothing, and one that fails hashes each configuration
 *       once, when it is done.
 *   <li>An early cut-off on unanswered operations: unanswered reads and writes nobody could observe
 *       are dropped, the search stops once every answered operation is placed, unanswered writes
 *       are tried last, and a configuration reached with more of them applied than before is
 *       pruned, so they no longer multiply the work of a failing search.
 *   <li>Pins: an answered read or applied write that runs alone, called after every operation
 *       before it returned, leaves one configuration that every linearization goes through. A
 *       search that fails below a pin it reached with nothing unanswered applied fails everywhere,
 *       so it stops there instead of backtracking through the whole history, and a long key is cut
 *       at pins into stretches that are searched in parallel.
 * </ul>
 *
 * <p>The rest is the cost of a step. The operations of a key are copied into arrays in the order
 * the search reads them, the operations it may try at each point are found once for every return by
 * looking back a few ranks, the cache and the stack are arrays, and a set of states is a small
 * sorted array, so that a step is a few array reads and allocates at most the state it produces. A
 * state's hash is computed only once a search caches it, and unanswered writes are compared by
 * numbers rather than by their values: for a long history each value is a string somewhere else in
 * memory, and reading it is usually a cache miss. For the same reason a report of a violation reads
 * only the operations it shows.
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

  /**
   * A value and its version. The hash is computed on first use and kept, since the search hashes
   * states a lot once it backtracks, but a search that never does should not pay for it: hashing
   * the value reads the string, which for a long history is usually a cache miss.
   */
  static final class State {
    static final State ABSENT = new State(null, 0);

    private final String value;
    private final long version;
    private int hash; // 0 until computed, like String's

    State(String value, long version) {
      this.value = value;
      this.version = version;
    }

    String value() {
      return value;
    }

    long version() {
      return version;
    }

    boolean known() {
      return version != History.ANY;
    }

    /** Whether the stored version could be {@code v}. */
    boolean couldBe(long v) {
      return !known() || version == v;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof State s && s.version == version && Objects.equals(s.value, value);
    }

    @Override
    public int hashCode() {
      int h = hash;
      if (h == 0) {
        h = Objects.hashCode(value) * 31 + Long.hashCode(version);
        hash = h; // a benign race: every thread computes the same value
      }
      return h;
    }

    @Override
    public String toString() {
      return value + "@" + (known() ? Long.toString(version) : "?");
    }

    /** A total order, used to keep a set of states as a sorted array. */
    static int compare(State a, State b) {
      int ha = a.hashCode();
      int hb = b.hashCode();
      if (ha != hb) {
        return Integer.compare(ha, hb);
      }
      if (a.version != b.version) {
        return Long.compare(a.version, b.version);
      }
      if (a.value == null || b.value == null) {
        return a.value == null ? (b.value == null ? 0 : -1) : 1;
      }
      return a.value.compareTo(b.value);
    }
  }

  /** Every state {@code op} can lead to from {@code s} given what it observed; empty if none. */
  static List<State> step(State s, History.Op op) {
    boolean open = op.ret == History.OPEN;
    String value = op.kind == History.Kind.GET ? op.readValue : op.value;
    if (forks(s, op.kind, open, op.expected)) {
      return List.of(effect(op.kind, value, History.ANY), s);
    }
    State next = next(s, op.kind, open, op.ok, op.expected, op.version, value);
    return next == null ? List.of() : List.of(next);
  }

  /**
   * Whether an operation leads from {@code s} to two states: an open conditional write on a state
   * whose version is unknown may or may not have matched.
   */
  private static boolean forks(State s, History.Kind kind, boolean open, long expected) {
    return open && kind != History.Kind.GET && expected != -1 && !s.known();
  }

  /**
   * The state an operation leads to from {@code s} when it does not {@link #forks fork}, or null if
   * it does not fit. The operation is given by its fields: {@code value} is what a put wrote or a
   * read returned.
   */
  private static State next(
      State s,
      History.Kind kind,
      boolean open,
      boolean ok,
      long expected,
      long version,
      String value) {
    if (kind == History.Kind.GET) {
      if (open) {
        return s;
      }
      boolean fits = Objects.equals(s.value(), value) && s.couldBe(version);
      return fits ? withVersion(s, version) : null;
    }
    if (open) {
      // A conditional write on a known state applies only if the condition holds.
      boolean applies = expected == -1 || s.version() == expected;
      return applies ? effect(kind, value, History.ANY) : s;
    }
    if (!ok) {
      // A conflict reports the version it found, which differs from the one expected.
      boolean fits = s.couldBe(version) && expected != version;
      return fits ? withVersion(s, version) : null;
    }
    if (expected != -1 && !s.couldBe(expected)) {
      return null;
    }
    if (kind == History.Kind.DELETE) {
      // A delete reports the version it removed.
      boolean fits = s.couldBe(version) && (expected == -1 || expected == version);
      return fits ? State.ABSENT : null;
    }
    long before = s.known() ? s.version() : expected;
    boolean newer = before == -1 || version > before;
    return newer ? effect(kind, value, version) : null;
  }

  private static State withVersion(State s, long version) {
    return s.version() == version ? s : new State(s.value(), version);
  }

  private static State effect(History.Kind kind, String value, long version) {
    return kind == History.Kind.PUT ? new State(value, version) : State.ABSENT;
  }

  private static final State[] NO_STATES = new State[0];

  /**
   * Applies an operation to every state in {@code states}; empty if it fits none of them. A set of
   * states is a sorted array without duplicates ({@link State#compare}), so that equal sets are
   * equal arrays. The usual case, one state in and one out, allocates at most the new state.
   */
  private static State[] stepAll(
      State[] states,
      History.Kind kind,
      boolean open,
      boolean ok,
      long expected,
      long version,
      String value) {
    if (states.length == 1) {
      State s = states[0];
      if (forks(s, kind, open, expected)) {
        return sorted(new State[] {effect(kind, value, History.ANY), s}, 2);
      }
      State t = next(s, kind, open, ok, expected, version, value);
      return t == null ? NO_STATES : t.equals(s) ? states : new State[] {t};
    }
    State[] out = new State[2 * states.length];
    int n = 0;
    for (State s : states) {
      if (forks(s, kind, open, expected)) {
        out[n++] = effect(kind, value, History.ANY);
        out[n++] = s;
      } else {
        State t = next(s, kind, open, ok, expected, version, value);
        if (t != null) {
          out[n++] = t;
        }
      }
    }
    return sorted(out, n);
  }

  /** The first {@code n} states of {@code out}, sorted and without duplicates. */
  private static State[] sorted(State[] out, int n) {
    int m = 0;
    for (int i = 0; i < n; i++) {
      State v = out[i];
      int j = m - 1;
      while (j >= 0 && State.compare(out[j], v) > 0) {
        j--;
      }
      if (j >= 0 && State.compare(out[j], v) == 0) {
        continue; // a duplicate
      }
      System.arraycopy(out, j + 1, out, j + 2, m - j - 1);
      out[j + 1] = v;
      m++;
    }
    return m == 0 ? NO_STATES : m == out.length ? out : Arrays.copyOf(out, m);
  }

  /**
   * Returns a description of the first key, in key order, whose history is not linearizable. Keys
   * of a long history are checked in parallel, since each is a partition of its own.
   */
  static Optional<String> check(List<History.Op> ops) {
    Fields f = new Fields(ops.toArray(new History.Op[0]));
    int keys = f.names.size();
    int[] start = new int[keys + 1];
    for (int[] chunk : f.keyOf) {
      for (int k : chunk) {
        start[k + 1]++;
      }
    }
    for (int k = 0; k < keys; k++) {
      start[k + 1] += start[k];
    }
    int[] members = new int[f.ops.length]; // the operations of each key, in input order
    int[] at = Arrays.copyOf(start, keys);
    for (int c = 0; c < f.keyOf.length; c++) {
      int[] chunk = f.keyOf[c];
      for (int j = 0; j < chunk.length; j++) {
        members[at[chunk[j]]++] = (c << Fields.BITS) + j;
      }
    }
    Integer[] order = new Integer[keys];
    for (int k = 0; k < keys; k++) {
      order[k] = k;
    }
    Arrays.sort(order, Comparator.comparing(f.names::get));
    // An array splits evenly between threads; a TreeMap of a few keys may not split at all.
    Stream<Integer> stream = Arrays.stream(order);
    if (f.parallel && keys > 1) {
      stream = stream.parallel();
    }
    return stream
        .map(
            k ->
                new Search(f, Arrays.copyOfRange(members, start[k], start[k + 1]))
                    .verdict(f.names.get(k)))
        .toList()
        .stream()
        .flatMap(Optional::stream)
        .findFirst();
  }

  /**
   * The fields of every operation, read once into arrays by position in the input, and the key each
   * belongs to. Reading the operations themselves is most of the cost of a long history, since each
   * is an object of its own, so a long history is read in parallel. The arrays come in chunks of
   * {@code 2^BITS}, each allocated and filled by the thread that reads it, which keeps every array
   * small enough for the young generation. Each chunk numbers the keys it meets on its own,
   * starting from the keys of the first few operations, and its numbers are mapped to the global
   * ones afterwards if they differ.
   */
  private static final class Fields {
    static final int BITS = 13;
    static final int MASK = (1 << BITS) - 1;

    final History.Op[] ops;
    final byte[][] kind; // codes, see KINDS
    final boolean[][] ok;
    final long[][] expected;
    final long[][] version;
    final String[][] value; // what a put wrote or a read returned
    final long[][] call;
    final long[][] ret;
    final int[][] keyOf;
    final List<String> names;
    final boolean parallel;

    Fields(History.Op[] ops) {
      this.ops = ops;
      int n = ops.length;
      int chunks = (n + MASK) >>> BITS;
      kind = new byte[chunks][];
      ok = new boolean[chunks][];
      expected = new long[chunks][];
      version = new long[chunks][];
      value = new String[chunks][];
      call = new long[chunks][];
      ret = new long[chunks][];
      keyOf = new int[chunks][];
      parallel = n >= PARALLEL_FROM;
      Keys seed = new Keys();
      for (int i = 0; i < Math.min(n, 256); i++) {
        seed.id(ops[i].key);
      }
      Keys[] local = new Keys[chunks];
      IntStream range = IntStream.range(0, chunks);
      (parallel ? range.parallel() : range).forEach(c -> local[c] = read(c, seed.copy()));
      for (int c = 0; c < chunks; c++) {
        int[] ids = new int[local[c].names.size()];
        boolean same = true;
        for (int k = 0; k < ids.length; k++) {
          ids[k] = seed.id(local[c].names.get(k));
          same &= ids[k] == k;
        }
        for (int j = 0; j < keyOf[c].length && !same; j++) {
          keyOf[c][j] = ids[keyOf[c][j]];
        }
      }
      names = seed.names;
    }

    private Keys read(int c, Keys keys) {
      int from = c << BITS;
      int size = Math.min(ops.length - from, 1 << BITS);
      byte[] kinds = new byte[size];
      boolean[] oks = new boolean[size];
      long[] expects = new long[size];
      long[] versions = new long[size];
      String[] values = new String[size];
      long[] calls = new long[size];
      long[] rets = new long[size];
      int[] ids = new int[size];
      kind[c] = kinds;
      ok[c] = oks;
      expected[c] = expects;
      version[c] = versions;
      value[c] = values;
      call[c] = calls;
      ret[c] = rets;
      keyOf[c] = ids;
      for (int j = 0; j < size; j++) {
        History.Op op = ops[from + j];
        kinds[j] = code(op.kind);
        oks[j] = op.ok;
        expects[j] = op.expected;
        versions[j] = op.version;
        values[j] = op.kind == History.Kind.GET ? op.readValue : op.value;
        calls[j] = op.call;
        rets[j] = op.ret;
        ids[j] = keys.id(op.key);
      }
      return keys;
    }

    History.Kind kind(int i) {
      return KINDS[kind[i >>> BITS][i & MASK]];
    }

    boolean ok(int i) {
      return ok[i >>> BITS][i & MASK];
    }

    long expected(int i) {
      return expected[i >>> BITS][i & MASK];
    }

    long version(int i) {
      return version[i >>> BITS][i & MASK];
    }

    String value(int i) {
      return value[i >>> BITS][i & MASK];
    }

    long call(int i) {
      return call[i >>> BITS][i & MASK];
    }

    long ret(int i) {
      return ret[i >>> BITS][i & MASK];
    }
  }

  /** Below this many operations, checking keys in parallel costs more than it saves. */
  private static final int PARALLEL_FROM = 10_000;

  /** Numbers keys in the order they first appear: an open-addressing table of names. */
  private static final class Keys {
    final List<String> names = new ArrayList<>();
    private String[] slots = new String[16];
    private int[] ids = new int[16];

    int id(String key) {
      int mask = slots.length - 1;
      int i = (key.hashCode() * 0x9E3779B9) >>> Integer.numberOfLeadingZeros(mask);
      while (slots[i] != null) {
        if (slots[i].equals(key)) {
          return ids[i];
        }
        i = (i + 1) & mask;
      }
      slots[i] = key;
      ids[i] = names.size();
      names.add(key);
      if (names.size() * 2 > slots.length) {
        String[] old = slots;
        slots = new String[old.length * 2];
        ids = new int[old.length * 2];
        for (int k = 0; k < names.size(); k++) {
          put(names.get(k), k);
        }
      }
      return names.size() - 1;
    }

    Keys copy() {
      Keys k = new Keys();
      k.names.addAll(names);
      k.slots = slots.clone();
      k.ids = ids.clone();
      return k;
    }

    private void put(String key, int id) {
      int mask = slots.length - 1;
      int i = (key.hashCode() * 0x9E3779B9) >>> Integer.numberOfLeadingZeros(mask);
      while (slots[i] != null) {
        i = (i + 1) & mask;
      }
      slots[i] = key;
      ids[i] = id;
    }
  }

  /**
   * One visit of a configuration: the unanswered operations applied at the time, and the classes of
   * interchangeable unanswered operations its search ever found itself without an available one of
   * ({@code shortOf}).
   */
  private static final class Seen {
    final long[] applied; // shared with other visits, never changed
    long[] shortOf; // null while empty
    Seen next; // an earlier finished visit of the same configuration

    Seen(long[] applied) {
      this.applied = applied;
    }

    boolean isShortOf(int c) {
      return shortOf != null && (shortOf[c >>> 6] & (1L << c)) != 0;
    }

    void markShortOf(int c, int words) {
      if (shortOf == null) {
        shortOf = new long[words];
      }
      shortOf[c >>> 6] |= 1L << c;
    }

    void addShortOf(Seen from) {
      if (from.shortOf == null) {
        return;
      }
      if (shortOf == null) {
        shortOf = from.shortOf.clone();
        return;
      }
      for (int i = 0; i < shortOf.length; i++) {
        shortOf[i] |= from.shortOf[i];
      }
    }

    /** Marks every class, when a visit this one depends on has not finished yet. */
    void markAll(int words) {
      if (words > 0) {
        shortOf = new long[words];
        Arrays.fill(shortOf, -1L);
      }
    }
  }

  private static final long[] NO_BITS = new long[0];

  /** Kinds by {@link #code}: stored as bytes, which cost less to store than references. */
  private static final History.Kind[] KINDS = {
    History.Kind.GET, History.Kind.PUT, History.Kind.DELETE
  };

  private static byte code(History.Kind kind) {
    return switch (kind) {
      case GET -> 0;
      case PUT -> 1;
      case DELETE -> 2;
    };
  }

  private static final int[] NONE = new int[0];

  /**
   * The value of an unanswered put that no answered read returned. Only reads look at values, so
   * such puts are interchangeable, and sharing one value lets states that differ only in which of
   * them was applied compare equal. Simulated clients never write a NUL.
   */
  private static final String UNREAD = "\0unread";

  /**
   * Sorts {@code idx} by {@code key[idx[i]]}, keeping equal keys in their order. It returns at once
   * if the input is already sorted, as calls usually are, and otherwise runs a least significant
   * digit radix sort over the range the keys span, in as few passes of at most 13 bits as it takes.
   */
  static void sortBy(int[] idx, long[] key) {
    int n = idx.length;
    boolean sorted = true;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int i = 0; i < n; i++) {
      long k = key[idx[i]];
      sorted &= i == 0 || key[idx[i - 1]] <= k;
      min = Math.min(min, k);
      max = Math.max(max, k);
    }
    if (sorted) {
      return;
    }
    if (n <= 32) {
      for (int i = 1; i < n; i++) {
        int v = idx[i];
        int j = i - 1;
        while (j >= 0 && key[idx[j]] > key[v]) {
          idx[j + 1] = idx[j];
          j--;
        }
        idx[j + 1] = v;
      }
      return;
    }
    int bits = 64 - Long.numberOfLeadingZeros(max - min); // the range, read as unsigned
    int passes = (bits + 12) / 13;
    int digit = (bits + passes - 1) / passes;
    int radix = 1 << digit;
    int[] from = idx;
    int[] to = new int[n];
    int[] count = new int[radix + 1];
    for (int p = 0; p < passes; p++) {
      int shift = p * digit;
      Arrays.fill(count, 0);
      for (int v : from) {
        count[(int) (((key[v] - min) >>> shift) & (radix - 1)) + 1]++;
      }
      for (int d = 0; d < radix; d++) {
        count[d + 1] += count[d];
      }
      for (int v : from) {
        to[count[(int) (((key[v] - min) >>> shift) & (radix - 1))]++] = v;
      }
      int[] t = from;
      from = to;
      to = t;
    }
    if (passes % 2 == 1) {
      System.arraycopy(from, 0, idx, 0, n); // the result is in the scratch array
    }
  }

  /** A growable list of ints. */
  private static final class Ints {
    int[] items = new int[8];
    int size;

    void add(int v) {
      if (size == items.length) {
        items = Arrays.copyOf(items, 2 * size);
      }
      items[size++] = v;
    }
  }

  /**
   * The search over one key's history.
   *
   * <p>Unanswered reads have no effect and are dropped, and so are unanswered writes called after
   * the last answer, since nothing could observe them. Answered operations are ranked by return;
   * unanswered writes sit in a list of their own, by call, and may be linearized at any point after
   * their call or never. The search succeeds as soon as every answered operation is placed. At each
   * step it tries the answered operations first and the unanswered ones last, and only those that
   * could help there ({@link Walk#helps}), so it leaves them out unless it needs them. Unanswered
   * deletes, and puts whose value no answered read returns, are interchangeable with the others of
   * the same kind and condition; such a class is applied in call order, so only its earliest
   * unapplied member is ever tried.
   *
   * <p>A configuration is exact and small: the linearized set is a prefix of the answered
   * operations in return order plus a window no larger than the number of concurrent clients,
   * because the search can only linearize an operation called before the first return it has not
   * passed. That return is the one of rank {@code prefix}, so the operations it may try next are
   * those running at that return, which are computed once for every rank ({@link #cand}) instead of
   * being found by walking a list of events.
   *
   * <p>Finished configurations go into an open-addressing table, with the unanswered operations
   * that were applied when each was reached ({@link Seen}), and a later visit that an earlier one
   * covers is pruned ({@link Walk#visit}). Without that, k unanswered writes would multiply the
   * configurations of a failing search by up to 2^k.
   */
  private static final class Search {
    private final int n; // answered operations
    private final Fields fields; // for the report
    private final int[] answeredAt; // the input position of each answered operation, by rank
    private final History.Op[] openAsCalled; // by call, for the report

    // Every operation the search may linearize, by slot: the answered ones by rank, then the
    // unanswered writes by call. value is what a put wrote or a read returned, and UNREAD for an
    // unanswered put whose value no answered read returned.
    private final byte[] kind; // codes, see KINDS
    private final boolean[] ok;
    private final long[] expected;
    private final long[] version;
    private final String[] value;
    private final long[] call;
    private final long[] ret; // of the answered ones

    // The ranks of the answered operations running at the return of rank p, in call order, are
    // cand[candStart[p]] to cand[candStart[p + 1] - 1]. They include p, and every rank is >= p.
    private final int[] candStart;
    private final int[] cand;

    // Unanswered write o, from 0, is in slot n + o.
    private final int opens;
    private final int[] classOf;
    private final int[] firstOfClass;
    private final int[][] members; // of each class, by call
    private final boolean[] alone; // a class of one: a put whose value an answered read returns
    private final boolean[] loose; // unconditional and interchangeable: counts may be relaxed
    // What unanswered write o does to the value: equal numbers for the same kind and value, so
    // that helps() compares numbers instead of strings.
    private final int[] effectOf;
    private final int classWords; // longs in a bit set of classes
    // pin[p]: every linearization reaches one configuration right after rank p, see pins().
    private final boolean[] pin;
    private final int pins;

    private static final int INITIAL = 256;

    /** The fewest ranks in a stretch searched on its own; see {@link #search}. */
    private static final int STRETCH = 2048;

    /**
     * Steps per operation a stretch other than the first may take before it gives up. A search that
     * passes takes a few; one that fails can take far more, and when it is not the first to fail,
     * the single search would never have gone there.
     */
    private static final long STEPS = 64;

    private final AtomicInteger stopAt = new AtomicInteger(); // the first stretch that failed

    /** The search over the operations {@code mine} of {@code f}, given in input order. */
    Search(Fields f, int[] mine) {
      // Positions in mine, with their return and call times, so that the sorts read small arrays.
      int m = mine.length;
      long[] rets = new long[m];
      long[] calls = new long[m];
      int[] done = new int[m];
      Ints writes = new Ints(); // unanswered
      long lastReturn = Long.MIN_VALUE;
      int answers = 0;
      for (int j = 0; j < m; j++) {
        int i = mine[j];
        rets[j] = f.ret(i);
        calls[j] = f.call(i);
        if (rets[j] != History.OPEN) {
          lastReturn = Math.max(lastReturn, rets[j]);
          done[answers++] = j;
        } else if (f.kind(i) != History.Kind.GET) {
          writes.add(j);
        }
      }
      n = answers;
      int[] byRet = Arrays.copyOf(done, n);
      sortBy(byRet, rets);
      Ints pending = new Ints();
      for (int k = 0; k < writes.size; k++) {
        if (calls[writes.items[k]] < lastReturn) {
          pending.add(writes.items[k]);
        }
      }
      int[] byCall = Arrays.copyOf(pending.items, pending.size);
      sortBy(byCall, calls);
      opens = byCall.length;
      openAsCalled = new History.Op[opens];
      for (int o = 0; o < opens; o++) {
        openAsCalled[o] = f.ops[mine[byCall[o]]];
      }

      int slots = n + opens;
      kind = new byte[slots];
      ok = new boolean[slots];
      expected = new long[slots];
      version = new long[slots];
      value = new String[slots];
      call = new long[slots];
      ret = new long[n];
      fields = f;
      answeredAt = new int[n];
      for (int r = 0; r < n; r++) {
        int j = byRet[r];
        int i = mine[j];
        answeredAt[r] = i;
        kind[r] = f.kind[i >>> Fields.BITS][i & Fields.MASK];
        ok[r] = f.ok(i);
        expected[r] = f.expected(i);
        version[r] = f.version(i);
        value[r] = f.value(i);
        call[r] = calls[j];
        ret[r] = rets[j];
      }
      // The values of unanswered puts that an answered read returned. A read's value is compared
      // with theirs only if its hash is one of theirs, found in a sorted array.
      Ints openPuts = new Ints();
      for (int o = 0; o < opens; o++) {
        if (openAsCalled[o].kind == History.Kind.PUT && openAsCalled[o].value != null) {
          openPuts.add(o);
        }
      }
      long[] openHashes = new long[openPuts.size]; // hash in the high half, index in the low
      for (int i = 0; i < openPuts.size; i++) {
        int o = openPuts.items[i];
        openHashes[i] = ((long) openAsCalled[o].value.hashCode() << 32) | o;
      }
      Arrays.sort(openHashes);
      Set<String> read = new HashSet<>();
      for (int r = 0; r < n && openHashes.length > 0; r++) {
        String v = value[r];
        if (kind[r] != code(History.Kind.GET) || v == null) {
          continue;
        }
        long h = (long) v.hashCode() << 32;
        int at = Arrays.binarySearch(openHashes, h);
        for (int i = at < 0 ? -at - 1 : at;
            i < openHashes.length && (openHashes[i] & 0xFFFF_FFFF_0000_0000L) == h;
            i++) {
          if (v.equals(openAsCalled[(int) openHashes[i]].value)) {
            read.add(v);
            break;
          }
        }
      }

      classOf = new int[opens];
      Map<List<Object>, Integer> classes = new HashMap<>();
      List<Integer> firsts = new ArrayList<>();
      for (int i = 0; i < opens; i++) {
        History.Op o = openAsCalled[i];
        boolean unread = o.kind == History.Kind.PUT && !read.contains(o.value);
        int s = n + i;
        kind[s] = code(o.kind);
        expected[s] = o.expected;
        value[s] = unread ? UNREAD : o.value;
        call[s] = o.call;
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
        int s = n + firstOfClass[c];
        alone[c] = kindOf(s) == History.Kind.PUT && !UNREAD.equals(value[s]);
        loose[c] = !alone[c] && expected[s] == -1;
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
      for (int o = 0; o < opens; o++) {
        members[classOf[o]][sizes[classOf[o]]++] = o;
      }
      effectOf = new int[opens];
      Map<List<Object>, Integer> effects = new HashMap<>();
      for (int o = 0; o < opens; o++) {
        int s = n + o;
        effectOf[o] =
            effects.computeIfAbsent(Arrays.asList(kindOf(s), value[s]), k -> effects.size());
      }
      classWords = (firstOfClass.length + 63) / 64;

      candStart = new int[n + 1];
      cand = candidates();
      pin = new boolean[n];
      pins = pins();
    }

    /**
     * Fills {@link #candStart} and returns {@link #cand}. The operations running at the return of
     * rank p are p and the higher ranks called before it; since returns grow with rank, the ranks p
     * whose return a rank r is called before are the few just below r, found by looking back from
     * r. Each list is then put in call order, ties by rank, which is the order of a sweep over the
     * calls and returns in time.
     */
    private int[] candidates() {
      int[] first = new int[n]; // the lowest rank whose return rank r is called before
      int[] count = new int[n + 1];
      for (int r = 0; r < n; r++) {
        int p = r;
        while (p > 0 && call[r] < ret[p - 1]) {
          p--;
        }
        first[r] = p;
        count[p]++; // r is a candidate from p to r
        count[r + 1]--;
      }
      int size = 0;
      int live = 0;
      for (int p = 0; p < n; p++) {
        live += count[p];
        candStart[p] = size;
        size += live;
      }
      candStart[n] = size;
      int[] out = new int[size];
      int[] at = Arrays.copyOf(candStart, n);
      for (int r = 0; r < n; r++) {
        for (int p = first[r]; p <= r; p++) {
          out[at[p]++] = r;
        }
      }
      // Into call order, by insertion: there are only as many as there are concurrent clients.
      for (int p = 0; p < n; p++) {
        for (int i = candStart[p] + 1; i < candStart[p + 1]; i++) {
          int v = out[i];
          int j = i - 1;
          while (j >= candStart[p] && call[out[j]] > call[v]) {
            out[j + 1] = out[j];
            j--;
          }
          out[j + 1] = v;
        }
      }
      return out;
    }

    private State[] stepAll(State[] from, int s) {
      return Linearizability.stepAll(
          from, KINDS[kind[s]], s >= n, ok[s], expected[s], version[s], value[s]);
    }

    private History.Kind kindOf(int s) {
      return KINDS[kind[s]];
    }

    private static boolean couldMatch(State[] states, long expected) {
      for (State st : states) {
        if (st.couldBe(expected)) {
          return true;
        }
      }
      return false;
    }

    /**
     * The search from one configuration with nothing applied, ({@code from}, no window, {@code
     * start}), until it places every rank below {@code end}: the whole history, or the stretch
     * between two pins.
     */
    final class Walk {
      private final int[] appliedOfClass;
      private final long[] applied;
      private long[] appliedNow; // a copy of applied, or null after a change
      private final Seen root = new Seen(NO_BITS); // where shortages before any move go
      // Every visit when there are no unanswered operations, which leaves nothing to compare.
      private final Seen plain = new Seen(NO_BITS);
      private final Ints failing = new Ints();
      private final List<State[]> observed = new ArrayList<>(); // after candidate reads, conflicts
      private final Ints blocked = new Ints();

      // The table of finished configurations, by linear probing. A slot's key is its hash in the
      // high half and its prefix + 1 in the low half, so 0 marks a free slot.
      private long[] slotKey = new long[INITIAL];
      private int[][] slotWindow = new int[INITIAL][];
      private State[][] slotStates = new State[INITIAL][];
      private Seen[] slotSeen = new Seen[INITIAL];
      private int used;
      private long probed; // the key slot() last looked for
      // The prefixes of the configurations in the table, so that most visits need not hash.
      private final long[] finishedAt;

      // The stack: what to restore on backtracking. A frame that linearized an answered operation
      // holds its rank and its position among the candidates; one that applied an unanswered
      // operation holds -1 and the index of that operation.
      private int depth;
      private final int[] frameRank;
      private final int[] framePos;
      private final State[][] frameStates;
      private final int[] framePrefix;
      private final int[][] frameWindow;
      private final Seen[] frameSeen;

      private State[] states;
      private int prefix;
      private int[] window = NONE;
      private int deepest;
      private final int from;
      private final int end;
      private int appliedCount; // of the unanswered operations
      private final int index; // of the stretch
      private final long budget; // of steps
      private final int firstOpen; // the unanswered operations before it are left out
      boolean aborted;

      /**
       * A search of the stretch {@code index} from ({@code from}, no window, {@code start}) to
       * {@code end}, giving up after {@code budget} steps, that may apply the unanswered operations
       * from {@code firstOpen} on. Those before it are left out by marking them applied.
       */
      Walk(int index, int from, State start, int end, long budget, int firstOpen) {
        this.index = index;
        this.budget = budget;
        this.from = from;
        this.end = end;
        prefix = from;
        deepest = from;
        states = new State[] {start};
        appliedOfClass = new int[firstOfClass.length];
        applied = new long[(opens + 63) / 64];
        finishedAt = new long[(end - from + 64) / 64];
        int maxDepth = end - from + opens + 1;
        frameRank = new int[maxDepth];
        framePos = new int[maxDepth];
        frameStates = new State[maxDepth][];
        framePrefix = new int[maxDepth];
        frameWindow = new int[maxDepth][];
        frameSeen = opens == 0 ? null : new Seen[maxDepth]; // every visit is plain without them
        this.firstOpen = firstOpen;
        for (int o = 0; o < firstOpen; o++) {
          applied[o >>> 6] |= 1L << o;
          appliedOfClass[classOf[o]]++; // the earliest of each class, so heads stay right
        }
      }

      /** The unanswered operations this search applied, as a bit set. */
      long[] ownApplied() {
        long[] out = applied.clone();
        for (int o = 0; o < firstOpen; o++) {
          out[o >>> 6] &= ~(1L << o);
        }
        return out;
      }

      /** Whether nothing is applied: after a search that passed, whether it needed nothing. */
      boolean clean() {
        return appliedCount == 0;
      }

      /**
       * Whether the search passed. False also if it was {@link #aborted}: it ran out of {@link
       * #budget}, or another stretch made its answer useless.
       */
      boolean run() {
        int pos = 0; // among the candidates of the current prefix
        int nextOpen = 0;
        long steps = 0;
        while (true) {
          if (prefix >= end) {
            return true;
          }
          if ((++steps & 1023) == 0 && (steps > budget || stopAt.get() < index)) {
            aborted = true;
            return false;
          }
          int at = candStart[prefix] + pos;
          if (at < candStart[prefix + 1]) {
            int r = cand[at];
            if (r != prefix && inWindow(r)) {
              pos++;
              continue;
            }
            State[] next = stepAll(states, r);
            if (next.length > 0) {
              int savedPrefix = prefix;
              int[] savedWindow = window;
              place(r);
              Seen seen = visit(next);
              if (seen != null) {
                push(r, pos, savedPrefix, savedWindow, seen);
                states = next;
                pos = 0;
                nextOpen = 0;
                continue;
              }
              prefix = savedPrefix;
              window = savedWindow;
            }
            pos++;
            continue;
          }
          // Every candidate has been tried: unanswered writes called before the return of rank
          // prefix may go here.
          long limit = ret[prefix];
          boolean moved = false;
          if (nextOpen < opens && call[n + nextOpen] < limit) {
            needs();
            if (failing.size == 0 && blocked.size == 0 && observed.isEmpty()) {
              nextOpen = opens; // helps() would turn every one of them down
            }
          }
          for (; nextOpen < opens && call[n + nextOpen] < limit; nextOpen++) {
            int o = nextOpen;
            if (o != head(classOf[o])) {
              continue; // applied, or a member of its class called earlier is not
            }
            State[] next = stepAll(states, n + o);
            if (next.length == 0 || !helps(n + o, next)) {
              continue;
            }
            flip(o);
            Seen seen = onStack(next) ? null : visit(next);
            if (seen != null) {
              push(-1, o, prefix, window, seen);
              states = next;
              moved = true;
              break;
            }
            flip(o);
          }
          if (moved) {
            pos = 0;
            nextOpen = 0;
            continue;
          }
          if (depth == 0) {
            return false;
          }
          depth--;
          if (frameRank[depth] >= 0 && pin[frameRank[depth]] && appliedCount == 0) {
            // Every linearization passes the configuration this frame reached, and none of the
            // others can do better than this one, which had nothing applied (see pins()).
            return false;
          }
          Seen seen = frameSeen == null ? plain : frameSeen[depth];
          finish(seen);
          current().addShortOf(seen);
          states = frameStates[depth];
          prefix = framePrefix[depth];
          window = frameWindow[depth];
          if (frameRank[depth] >= 0) {
            pos = framePos[depth] + 1;
            nextOpen = 0;
          } else {
            int o = framePos[depth];
            flip(o);
            pos = candStart[prefix + 1] - candStart[prefix];
            needs();
            nextOpen = o + 1;
          }
        }
      }

      private void push(int rank, int pos, int savedPrefix, int[] savedWindow, Seen seen) {
        frameRank[depth] = rank;
        framePos[depth] = pos;
        frameStates[depth] = states;
        framePrefix[depth] = savedPrefix;
        frameWindow[depth] = savedWindow;
        if (frameSeen != null) {
          frameSeen[depth] = seen;
        }
        depth++;
      }

      private boolean inWindow(int rank) {
        for (int w : window) {
          if (w >= rank) {
            return w == rank;
          }
        }
        return false;
      }

      /**
       * Collects what an unanswered write could help with here: the candidates that fit no state,
       * and the waiting unanswered conditional writes that no state lets match.
       */
      private void needs() {
        failing.size = 0;
        blocked.size = 0;
        observed.clear();
        for (int at = candStart[prefix]; at < candStart[prefix + 1]; at++) {
          int r = cand[at];
          if (r != prefix && inWindow(r)) {
            continue;
          }
          State[] after = stepAll(states, r);
          if (after.length == 0) {
            failing.add(r);
          } else if (kindOf(r) == History.Kind.GET || !ok[r]) {
            observed.add(after);
          }
        }
        long limit = ret[prefix];
        for (int o = 0; o < opens && call[n + o] < limit; o++) {
          int s = n + o;
          if (!isApplied(o) && expected[s] != -1 && !couldMatch(states, expected[s])) {
            blocked.add(s);
          }
        }
        // A class runs short here if all of it that has been called is applied, yet one more would
        // be worth trying.
        for (int c = 0; c < firstOfClass.length; c++) {
          if (appliedOfClass[c] > 0 && !alone[c]) {
            int h = head(c);
            if (h < 0 || call[n + h] >= limit) {
              int s = n + members[c][0];
              State[] after = stepAll(states, s);
              if (after.length > 0 && helps(s, after)) {
                current().markShortOf(c, classWords);
              }
            }
          }
        }
      }

      /**
       * Whether the unanswered write in slot {@code w}, which leads to {@code after}, is worth
       * trying here: whether some candidate fits no state now but fits one of {@code after}, or a
       * waiting conditional write with a different effect could match only after it.
       *
       * <p>That is enough. Take a linearization that applies an unanswered write w here, and let b
       * be the next operation. If b fits the states without w, then either b overwrites the value
       * and w can be dropped, or b is a conflict or a read that leaves the value alone, and w can
       * move after b: what b observed did not need w, and a state whose version w left unknown
       * allows whatever the same state with b's version does. That fails only if w is conditional
       * and b pins the version to one w does not expect, so that case is tried too. An unanswered
       * conditional write in between can be moved the same way unless it can only match after w.
       * Repeating this moves w to a point that passes this test, or out of the linearization.
       */
      private boolean helps(int w, State[] after) {
        if (expected[w] != -1 && couldMatch(states, expected[w])) {
          // A read or a conflict may pin the version so that w no longer matches after it.
          for (State[] o : observed) {
            if (!couldMatch(o, expected[w])) {
              return true;
            }
          }
        }
        for (int i = 0; i < failing.size; i++) {
          if (stepAll(after, failing.items[i]).length > 0) {
            return true;
          }
        }
        for (int i = 0; i < blocked.size; i++) {
          int c = blocked.items[i];
          // If c has the same effect as w, applying c after w changes nothing w did not.
          boolean same = effectOf[c - n] == effectOf[w - n];
          if (!same && couldMatch(after, expected[c])) {
            return true;
          }
        }
        return false;
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
       * Whether a configuration still being searched, an ancestor of this one, is the same as
       * ({@link #prefix}, {@link #window}, {@code next}); if so, it covers this visit. Only moves
       * that apply unanswered operations keep the prefix and window, so only the frames at the top
       * of the stack that made such moves can lead from it to here. An ancestor's search has not
       * finished, so every class counts as short of operations.
       */
      private boolean onStack(State[] next) {
        for (int d = depth - 1; d >= 0; d--) {
          State[] at = d == depth - 1 ? states : frameStates[d + 1];
          if (Arrays.equals(at, next)) {
            current().markAll(classWords);
            return true;
          }
          if (frameRank[d] >= 0) {
            return false; // this frame's move changed the prefix or window
          }
        }
        return false;
      }

      private int hash(State[] next) {
        return (prefix * 31 + Arrays.hashCode(window)) * 31 + Arrays.hashCode(next);
      }

      /**
       * The slot of the configuration ({@link #prefix}, {@link #window}, {@code next}), or a free
       * one.
       */
      private int slot(State[] next) {
        int hash = hash(next);
        long key = ((long) hash << 32) | (prefix + 1L);
        probed = key;
        int mask = slotKey.length - 1;
        int i = (hash * 0x9E3779B9) >>> Integer.numberOfLeadingZeros(mask);
        while (true) {
          long k = slotKey[i];
          if (k == 0
              || (k == key
                  && Arrays.equals(slotWindow[i], window)
                  && Arrays.equals(slotStates[i], next))) {
            return i;
          }
          i = (i + 1) & mask;
        }
      }

      /**
       * Returns a new visit of the configuration ({@link #prefix}, {@link #window}, {@code next})
       * with the unanswered operations now applied, or null if an earlier visit whose search has
       * finished covers it.
       *
       * <p>An earlier visit covers this one if it had applied no unanswered operation that this one
       * has not, since those are optional and having more of them left can only allow more. It also
       * covers this one if it had applied more interchangeable ones of some classes, provided it
       * never ran short of any of those classes: this search would make the same moves with earlier
       * operations of the same classes, so the extra ones left could not help. A visit still being
       * searched is an ancestor, which has applied no more than this one, so it covers this one
       * too; {@link #onStack} finds it.
       */
      private Seen visit(State[] next) {
        if ((finishedAt[(prefix - from) >>> 6] & (1L << (prefix - from))) != 0) {
          for (Seen e = slotSeen[slot(next)]; e != null; e = e.next) {
            if (covers(e)) {
              current().addShortOf(e);
              return null;
            }
          }
        }
        return opens == 0 ? plain : new Seen(appliedNow());
      }

      /**
       * Enters the configuration of the frame being popped, ({@link #prefix}, {@link #window},
       * {@link #states}), into the table, now that the search below it has finished.
       */
      private void finish(Seen seen) {
        int i = slot(states);
        if (slotKey[i] != 0) {
          if (seen != plain) {
            seen.next = slotSeen[i];
            slotSeen[i] = seen;
          }
          return;
        }
        slotKey[i] = probed;
        finishedAt[(prefix - from) >>> 6] |= 1L << (prefix - from);
        slotWindow[i] = window;
        slotStates[i] = states;
        slotSeen[i] = seen;
        if (++used * 2 > slotKey.length) {
          grow();
        }
      }

      /** Makes the table four times larger, so that a long failing search rehashes little. */
      private void grow() {
        long[] keys = slotKey;
        int[][] windows = slotWindow;
        State[][] stateSets = slotStates;
        Seen[] seens = slotSeen;
        int capacity = keys.length * 4;
        slotKey = new long[capacity];
        slotWindow = new int[capacity][];
        slotStates = new State[capacity][];
        slotSeen = new Seen[capacity];
        int mask = capacity - 1;
        int shift = Integer.numberOfLeadingZeros(mask);
        for (int j = 0; j < keys.length; j++) {
          if (keys[j] != 0) {
            int i = ((int) (keys[j] >>> 32) * 0x9E3779B9) >>> shift;
            while (slotKey[i] != 0) {
              i = (i + 1) & mask;
            }
            slotKey[i] = keys[j];
            slotWindow[i] = windows[j];
            slotStates[i] = stateSets[j];
            slotSeen[i] = seens[j];
          }
        }
      }

      private boolean covers(Seen e) {
        for (int i = 0; i < applied.length; i++) {
          long extra = e.applied[i] & ~applied[i];
          while (extra != 0) {
            int o = i * 64 + Long.numberOfTrailingZeros(extra);
            extra &= extra - 1;
            int c = classOf[o];
            if (!loose[c] || e.isShortOf(c)) {
              return false;
            }
          }
        }
        return true;
      }

      /** The visit being searched, where the shortages found now are recorded. */
      private Seen current() {
        return depth == 0 || frameSeen == null ? root : frameSeen[depth - 1];
      }

      /** The unanswered operations applied now, as an array no one changes. */
      private long[] appliedNow() {
        if (appliedNow == null) {
          appliedNow = applied.clone();
        }
        return appliedNow;
      }

      /** The first operation of class {@code c} not applied yet, or -1. */
      private int head(int c) {
        int k = appliedOfClass[c];
        return k < members[c].length ? members[c][k] : -1;
      }

      private boolean isApplied(int o) {
        return (applied[o >>> 6] & (1L << o)) != 0;
      }

      private void flip(int o) {
        applied[o >>> 6] ^= 1L << o;
        appliedNow = null;
        int d = isApplied(o) ? 1 : -1;
        appliedOfClass[classOf[o]] += d;
        appliedCount += d;
      }
    }

    /** Empty if linearizable, else what the search could not get past. */
    Optional<String> verdict(String key) {
      int stuck = search();
      return stuck < 0 ? Optional.empty() : Optional.of(report(key, stuck));
    }

    /**
     * Returns -1 if the history is linearizable, else the deepest prefix the search reached.
     *
     * <p>A long history with pins is cut at some of them into stretches that are searched in
     * parallel, each from the configuration its pin leads to with nothing applied (see pins()).
     * Each stretch first leaves out the unanswered operations called before its pin returned, so no
     * two stretches can apply the same one; only if that fails is it searched again with all of
     * them.
     *
     * <ul>
     *   <li>If every stretch passes and no two applied the same unanswered operation, their
     *       linearizations join into one: each starts from the state the one before it ends in, and
     *       every unanswered operation it applies is still unapplied there.
     *   <li>If a stretch fails with every unanswered operation available, and the ones before it
     *       passed with nothing applied, those are the pieces of the single search, which reaches
     *       the failing stretch the same way, so it is not linearizable and the deepest prefix is
     *       the same.
     *   <li>Otherwise the single search is run instead. A stretch that fails from nothing applied
     *       fails from anything more, so the answer would still be no, but the single search may
     *       get stuck earlier, and its report is the one to give.
     * </ul>
     */
    private int search() {
      int[] bounds = stretches();
      int k = bounds.length - 1;
      if (k > 1) {
        Walk[] walks = new Walk[k];
        boolean[] passed = new boolean[k];
        boolean[] restricted = new boolean[k]; // passed without the earlier unanswered operations
        stopAt.set(Integer.MAX_VALUE);
        IntStream.range(0, k)
            .parallel()
            .forEach(
                i -> {
                  State start = i == 0 ? State.ABSENT : afterPin(bounds[i] - 1);
                  // The first stretch is the start of the single search, so its work is never
                  // wasted; the others give up if their search grows far beyond a pass.
                  long budget =
                      i == 0 ? Long.MAX_VALUE : STEPS * (bounds[i + 1] - bounds[i] + opens);
                  int fresh = i == 0 ? 0 : firstCalledAfter(ret[bounds[i] - 1]);
                  Walk w = new Walk(i, bounds[i], start, bounds[i + 1], budget, fresh);
                  boolean pass = w.run();
                  restricted[i] = pass && fresh > 0;
                  if (!pass && fresh > 0 && !w.aborted) {
                    w = new Walk(i, bounds[i], start, bounds[i + 1], budget, 0);
                    pass = w.run();
                  }
                  walks[i] = w;
                  passed[i] = pass;
                  if (!pass) {
                    stopAt.accumulateAndGet(i, Math::min); // the stretches after it are moot
                  }
                });
        long[] used = new long[(opens + 63) / 64];
        boolean clean = true;
        boolean joins = true;
        for (int i = 0; i < k && joins; i++) {
          if (!passed[i]) {
            if (clean && !walks[i].aborted) {
              return walks[i].deepest;
            }
            joins = false;
            break;
          }
          long[] mine = walks[i].ownApplied();
          for (int j = 0; j < used.length; j++) {
            joins &= restricted[i] || (used[j] & mine[j]) == 0;
            used[j] |= mine[j];
          }
          clean &= walks[i].clean();
        }
        if (joins) {
          return -1;
        }
      }
      Walk w = new Walk(0, 0, State.ABSENT, n, Long.MAX_VALUE, 0);
      return w.run() ? -1 : w.deepest;
    }

    /** The lowest rank that returns after {@code t}, or {@link #n} if none does. */
    private int firstReturnAfter(long t) {
      int lo = 0;
      int hi = n;
      while (lo < hi) {
        int mid = (lo + hi) >>> 1;
        if (ret[mid] > t) {
          hi = mid;
        } else {
          lo = mid + 1;
        }
      }
      return lo;
    }

    /** The one state a pin leaves. */
    private State afterPin(int p) {
      return kindOf(p) == History.Kind.DELETE ? State.ABSENT : new State(value[p], version[p]);
    }

    /** The first unanswered operation called after {@code t}, or {@link #opens} if none is. */
    private int firstCalledAfter(long t) {
      int lo = 0;
      int hi = opens;
      while (lo < hi) {
        int mid = (lo + hi) >>> 1;
        if (call[n + mid] > t) {
          hi = mid;
        } else {
          lo = mid + 1;
        }
      }
      return lo;
    }

    /**
     * Where the stretches searched in parallel begin, then {@link #n}: rank 0 and the rank after
     * each chosen pin, at least {@link #STRETCH} ranks apart. One stretch unless the history is
     * long enough to be worth splitting.
     */
    private int[] stretches() {
      if (!fields.parallel || n < 2 * STRETCH || pins == 0) {
        return new int[] {0, n};
      }
      Ints bounds = new Ints();
      bounds.add(0);
      for (int p = 0; p < n - STRETCH; p++) {
        if (pin[p] && p + 1 - bounds.items[bounds.size - 1] >= STRETCH) {
          bounds.add(p + 1);
        }
      }
      bounds.add(n);
      return Arrays.copyOf(bounds.items, bounds.size);
    }

    /**
     * Marks the pins and returns how many there are. Rank p is a pin if it is called after every
     * lower rank returned and no other answered operation is called before it returns, and if it
     * leaves one state whatever it is applied to: a read, or a write that applied. Then every
     * linearization places it on its own, at prefix p with an empty window, and goes on from the
     * same configuration, (p + 1, no window, that state), with some set of unanswered operations
     * applied. A visit with none applied covers every other: each operation the others have left it
     * has left too. So once the search below a pin it reached with nothing applied is over without
     * success, the history is not linearizable, and the search stops instead of trying every
     * alternative below the pin, which could only lead back to it.
     */
    private int pins() {
      int count = 0;
      for (int p = 0; p < n; p++) {
        boolean lone = candStart[p + 1] - candStart[p] == 1 && (p == 0 || call[p] > ret[p - 1]);
        History.Kind k = kindOf(p);
        if (lone && (k == History.Kind.GET || ok[p])) {
          pin[p] = true;
          count++;
        }
      }
      return count;
    }

    /**
     * Describes where the search got stuck: the operation of rank {@code deepest} and the
     * operations around it. Only the ranks that can appear are read, since on a long history each
     * is an object somewhere else in memory: those that return after the stuck one is called, which
     * are the ranks from a point found by binary search up to it, and the ranks running at its
     * return.
     */
    private String report(String key, int deepest) {
      History.Op stuck = fields.ops[answeredAt[deepest]];
      StringBuilder sb =
          new StringBuilder("not linearizable on key ")
              .append(key)
              .append(": no order of the operations explains ")
              .append(stuck)
              .append("\nthe operations up to it, with those running at the same time:");
      // The higher ranks called before the stuck one returns are its candidates. Any other higher
      // rank is called after that return, so it is not shown, and its call is later than the
      // return of rank deepest - 8, which bounds the earliest call shown from below.
      Ints later = new Ints();
      for (int at = candStart[deepest]; at < candStart[deepest + 1]; at++) {
        if (cand[at] > deepest) {
          later.add(cand[at]);
        }
      }
      Arrays.sort(later.items, 0, later.size); // rank order, as before the sort by call below
      long from = call[Math.max(0, deepest - 8)];
      for (int r = firstReturnAfter(call[deepest]); r <= deepest; r++) {
        from = Math.min(from, call[r]);
      }
      for (int i = 0; i < later.size; i++) {
        from = Math.min(from, call[later.items[i]]);
      }
      List<History.Op> shown = new ArrayList<>();
      for (int r = firstReturnAfter(from - 1); r <= deepest; r++) {
        shown.add(fields.ops[answeredAt[r]]);
      }
      for (int i = 0; i < later.size; i++) {
        shown.add(fields.ops[answeredAt[later.items[i]]]);
      }
      for (History.Op o : openAsCalled) {
        if (o.call <= stuck.ret) {
          shown.add(o);
        }
      }
      shown.sort(Comparator.comparingLong(o -> o.call)); // stable: ties stay in that order
      for (History.Op o : shown) {
        sb.append("\n  ").append(o);
      }
      return sb.toString();
    }
  }
}
