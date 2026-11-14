package io.github.laiwf94.keel.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class LinearizabilityTest {
  private final History h = new History();

  private History.Op put(int client, String v, long expected) {
    return h.call(client, History.Kind.PUT, "k", v, expected);
  }

  private History.Op del(int client, long expected) {
    return h.call(client, History.Kind.DELETE, "k", null, expected);
  }

  private History.Op get(int client) {
    return h.call(client, History.Kind.GET, "k", null, -1);
  }

  private boolean ok() {
    return Linearizability.check(h.ops).isEmpty();
  }

  @Test
  void sequentialHistoryIsLinearizable() {
    h.write(put(0, "a", -1), true, 5);
    h.read(get(0), "a", 5);
    h.write(put(0, "b", 5), true, 7);
    h.write(del(0, 7), true, 7);
    h.read(get(0), null, 0);
    assertTrue(ok());
  }

  @Test
  void staleReadIsCaught() {
    h.write(put(0, "a", -1), true, 5);
    h.write(put(0, "b", -1), true, 7);
    h.read(get(1), "a", 5);
    assertFalse(ok());
    assertTrue(Linearizability.check(h.ops).orElseThrow().contains("not linearizable on key k"));
  }

  @Test
  void concurrentOperationsMayLandEitherWay() {
    History.Op w = put(0, "a", -1);
    h.read(get(1), null, 0);
    History.Op r = get(2);
    h.write(w, true, 5);
    h.read(r, "a", 5);
    assertTrue(ok());
  }

  @Test
  void lostWriteIsCaught() {
    h.write(put(0, "a", -1), true, 5);
    h.read(get(1), null, 0);
    assertFalse(ok());
  }

  @Test
  void versionsMustIncrease() {
    h.write(put(0, "a", -1), true, 7);
    h.write(put(0, "b", -1), true, 5);
    assertFalse(ok());
  }

  @Test
  void conflictMustReportTheCurrentVersion() {
    h.write(put(0, "a", -1), true, 5);
    h.write(put(1, "b", 3), false, 4);
    assertFalse(ok());
  }

  @Test
  void conditionalWriteMustMatch() {
    h.write(put(0, "a", -1), true, 5);
    h.write(put(1, "b", 4), true, 9);
    assertFalse(ok());
  }

  @Test
  void deleteMustReportTheRemovedVersion() {
    h.write(put(0, "a", -1), true, 5);
    h.write(del(1, -1), true, 4);
    assertFalse(ok());
  }

  @Test
  void duplicatedWriteIsCaught() {
    // "a" was written once, overwritten, then reappears with a new version no operation made.
    h.write(put(0, "a", -1), true, 5);
    h.write(put(1, "b", -1), true, 7);
    h.read(get(2), "a", 9);
    assertFalse(ok());
  }

  @Test
  void openWritesMayTakeEffectLaterOrNeverAndBindTheirVersion() {
    put(0, "a", -1); // never answered
    h.write(put(1, "b", 0), false, 9); // finds a's version, 9
    h.write(put(1, "c", 9), true, 12);
    h.read(get(2), "c", 12);
    assertTrue(ok());

    History h2 = new History();
    h2.call(0, History.Kind.PUT, "k", "a", -1);
    h2.read(h2.call(1, History.Kind.GET, "k", null, -1), null, 0);
    assertTrue(Linearizability.check(h2.ops).isEmpty(), "an open write may never happen");
  }

  @Test
  void openConditionalWritesBranch() {
    put(0, "a", -1); // open, unknown version
    put(1, "b", 4); // open, may or may not match
    del(2, 4); // open delete, may or may not match
    h.read(get(3), "b", 20);
    assertTrue(ok());
  }

  @Test
  void openWritesOnKnownStatesApplyOnlyWhenTheConditionHolds() {
    h.write(put(0, "a", -1), true, 5);
    put(1, "b", 3); // condition fails against version 5
    h.read(get(2), "b", 6);
    assertFalse(ok());
  }

  @Test
  void openReadsAreIgnored() {
    h.write(put(0, "a", -1), true, 5);
    get(1);
    h.write(del(0, 5), true, 5);
    h.write(del(0, 3), false, 0);
    assertTrue(ok());
  }

  /**
   * An operation that runs across many returns, as a slow one can while other clients keep going,
   * is a candidate at every one of them. The answer must be the unoptimized search's, and a
   * violation must be reported where it is, with the slow operation among those shown.
   */
  @Test
  void anOperationRunningAcrossManyReturnsChecksTheSameWay() {
    for (int writes : new int[] {10, 62, 63, 64, 200}) {
      for (boolean stale : new boolean[] {false, true}) {
        History hist = new History();
        History.Op slow = hist.call(0, History.Kind.GET, "k", null, -1); // sees the empty key
        for (int i = 0; i < writes; i++) {
          hist.write(hist.call(1 + i % 3, History.Kind.PUT, "k", "v" + i, -1), true, 2L * i + 2);
        }
        History.Op last = hist.call(4, History.Kind.GET, "k", null, -1);
        // A read of the first value, long overwritten, is stale.
        hist.read(last, stale ? "v0" : "v" + (writes - 1), stale ? 2 : 2L * writes);
        hist.read(slow, null, 0);
        String what = writes + " writes" + (stale ? ", stale" : "");
        Optional<String> violation = Linearizability.check(hist.ops);
        assertEquals(LinearizabilityReference.check(hist.ops), violation.isEmpty(), what);
        assertEquals(!stale, violation.isEmpty(), what);
        violation.ifPresent(v -> assertTrue(v.contains(last.toString()), what + ": " + v));
        violation.ifPresent(v -> assertTrue(v.contains(slow.toString()), what + ": " + v));
      }
    }
  }

  /**
   * A small history with up to two random corruptions: a version moved, zeroed or taken from
   * another operation, a read value taken from another write, an outcome flipped, or an answer
   * lost.
   */
  private static List<History.Op> randomHistory(long seed) {
    SplittableRandom rnd = new SplittableRandom(seed);
    int n = 3 + rnd.nextInt(40);
    double openRate = rnd.nextInt(4) * 0.07;
    List<History.Op> ops =
        HistoryGenerator.linearizable(
            seed, new HistoryGenerator.Shape(n, 2 + rnd.nextInt(4), 1 + rnd.nextInt(2), openRate));
    for (int m = rnd.nextInt(3); m > 0; m--) {
      History.Op o = ops.get(rnd.nextInt(ops.size()));
      if (o.ret == History.OPEN) {
        continue;
      }
      switch (rnd.nextInt(6)) {
        case 0 -> o.version += rnd.nextInt(5) - 2;
        case 1 -> o.version = ops.get(rnd.nextInt(ops.size())).version;
        case 2 -> o.version = 0;
        case 3 -> o.readValue = ops.get(rnd.nextInt(ops.size())).value;
        case 4 -> o.ok = !o.ok;
        default -> o.ret = History.OPEN;
      }
    }
    return ops;
  }

  @Test
  void agreesWithTheUnoptimizedSearchOnRandomHistories() {
    int violations = 0;
    for (long seed = 0; seed < 20_000; seed++) {
      List<History.Op> ops = randomHistory(seed);
      boolean expected = LinearizabilityReference.check(ops);
      long s = seed;
      assertEquals(
          expected,
          Linearizability.check(ops).isEmpty(),
          () -> "seed " + s + ":\n" + ops.stream().map(History.Op::toString).toList());
      violations += expected ? 0 : 1;
    }
    // Both outcomes are well represented.
    assertTrue(violations > 4_000 && violations < 12_000, "violations: " + violations);
  }

  /**
   * The checker stays fast on histories far longer than the simulator's: a generous bound that the
   * search it replaced, quadratic in the length of a key's history, does not meet at this size.
   * LinearizabilityBenchmark has the measured times.
   */
  @Test
  void hundredThousandOperationHistoriesCheckQuickly() {
    var shape = HistoryGenerator.Shape.simulatorLike(100_000);
    List<History.Op> good = HistoryGenerator.linearizable(1, shape);
    assertTimeout(Duration.ofSeconds(3), () -> assertTrue(Linearizability.check(good).isEmpty()));

    List<History.Op> bad =
        HistoryGenerator.linearizable(2, new HistoryGenerator.Shape(100_000, 4, 3, 0.0005));
    History.Op stale = HistoryGenerator.staleRead(bad, 2);
    String violation =
        assertTimeout(Duration.ofSeconds(3), () -> Linearizability.check(bad).orElseThrow());
    assertTrue(violation.startsWith("not linearizable on key " + stale.key), violation);
    // The report shows the neighbourhood of the violation, not the whole key.
    assertTrue(violation.contains(stale.toString()), violation);
    assertTrue(violation.lines().count() < 60, violation);
  }

  /**
   * A long history on many keys takes the parallel path, where keys are numbered in chunks and most
   * first appear after the operations used to seed the numbering, so the chunks' numbers have to be
   * mapped. Each key's verdict must still match the unoptimized search.
   */
  @Test
  void longHistoriesOnManyKeysAgreeWithTheUnoptimizedSearch() {
    for (long seed = 0; seed < 4; seed++) {
      List<History.Op> ops =
          HistoryGenerator.linearizable(
              seed, new HistoryGenerator.Shape(40_000, 8, 2_000, seed % 2 == 0 ? 0.002 : 0));
      assertTrue(LinearizabilityReference.check(ops));
      assertTrue(Linearizability.check(ops).isEmpty(), "seed " + seed);
      History.Op stale = HistoryGenerator.staleRead(ops, seed);
      assertFalse(LinearizabilityReference.check(ops));
      String violation = Linearizability.check(ops).orElseThrow();
      assertTrue(violation.startsWith("not linearizable on key " + stale.key + ":"), violation);
    }
  }

  /**
   * A failing search over a history with one unanswered write in 500 used to run out of the 1 GiB
   * test heap at 30,000 operations; the search it replaced still does.
   */
  @Test
  void aFailingSearchWithUnansweredWritesFitsTheTestHeap() {
    List<History.Op> ops =
        HistoryGenerator.linearizable(30_000, new HistoryGenerator.Shape(30_000, 4, 3, 0.002));
    History.Op stale = HistoryGenerator.staleRead(ops, 30_000);
    String violation =
        assertTimeout(Duration.ofSeconds(30), () -> Linearizability.check(ops).orElseThrow());
    assertTrue(violation.startsWith("not linearizable on key " + stale.key + ":"), violation);
  }

  /**
   * Long histories of a few keys are cut at pins into stretches searched in parallel. Whether they
   * are linearizable must not change, with or without unanswered writes, which the stretches may
   * each apply, and whether the stretches line up or the single search has to take over.
   */
  @Test
  void stretchesSearchedInParallelAgreeWithTheUnoptimizedSearch() {
    for (long seed = 0; seed < 6; seed++) {
      double openRate = seed % 3 == 0 ? 0 : seed % 3 == 1 ? 0.0005 : 0.002;
      List<History.Op> ops =
          HistoryGenerator.linearizable(
              seed, new HistoryGenerator.Shape(20_000, 3 + (int) (seed % 3), 2, openRate));
      assertTrue(LinearizabilityReference.check(ops), "seed " + seed);
      assertTrue(Linearizability.check(ops).isEmpty(), "seed " + seed);
      // A stale read is not linearizable by construction; the old search cannot fit the failing
      // searches with unanswered writes in the test heap, so it is not asked.
      History.Op stale = HistoryGenerator.staleRead(ops, seed);
      String violation = Linearizability.check(ops).orElseThrow();
      assertTrue(violation.startsWith("not linearizable on key " + stale.key + ":"), violation);
      assertTrue(violation.contains(stale.toString()), violation);
      // The same history checks the same way every time, whichever stretch finishes first.
      assertEquals(violation, Linearizability.check(ops).orElseThrow());
    }
  }

  /**
   * The forward pass makes a walk's first moves and hands over to a walk where it gets stuck, then
   * takes over again past a pin; a walk that handed back with unanswered writes applied and then
   * fails sends the search back to the last pin passed without any. Whatever the path, the answer
   * and the report must be those of walks alone: on small random histories, which take every path,
   * and on long ones of a few keys, searched in stretches, with and without unanswered writes.
   */
  @Test
  void theForwardPassGivesTheAnswersAndReportsOfWalksAlone() {
    int violations = 0;
    for (long seed = 0; seed < 10_000; seed++) {
      List<History.Op> ops = randomHistory(seed);
      Optional<String> walks = Linearizability.check(ops, false);
      assertEquals(walks, Linearizability.check(ops, true), "seed " + seed);
      violations += walks.isPresent() ? 1 : 0;
    }
    assertTrue(violations > 2_000, "violations: " + violations);
    for (long seed = 0; seed < 6; seed++) {
      double openRate = seed % 3 == 0 ? 0 : seed % 3 == 1 ? 0.0005 : 0.002;
      List<History.Op> ops =
          HistoryGenerator.linearizable(
              seed, new HistoryGenerator.Shape(20_000, 3 + (int) (seed % 3), 2, openRate));
      assertEquals(Optional.empty(), Linearizability.check(ops, true), "seed " + seed);
      assertEquals(Optional.empty(), Linearizability.check(ops, false), "seed " + seed);
      HistoryGenerator.staleRead(ops, seed);
      assertEquals(
          Linearizability.check(ops, false), Linearizability.check(ops, true), "seed " + seed);
    }
  }

  /**
   * The forward pass reads each candidate's fields by position, and a stretch's fields are copied
   * into rank order only as far as a walk makes candidate lists, a little at a time. An answered
   * operation whose version, outcome or read value is wrong, anywhere in a long history, makes a
   * walk take over in its stretch, after walks in the stretches before it took over where the pass
   * got stuck; with unanswered writes, the single search may then take over across all of them.
   * Whichever ranks the walks reach, the answer and the report must be those of walks alone, which
   * copy every rank they search.
   */
  @Test
  void walksThatTakeOverHereAndThereGiveTheReportsOfWalksAlone() {
    int violations = 0;
    for (long seed = 0; seed < 24; seed++) {
      double openRate = seed % 3 == 0 ? 0 : seed % 3 == 1 ? 0.0005 : 0.002;
      List<History.Op> ops =
          HistoryGenerator.linearizable(
              seed, new HistoryGenerator.Shape(20_000, 3 + (int) (seed % 3), 2, openRate));
      SplittableRandom rnd = new SplittableRandom(seed);
      History.Op op = ops.get(rnd.nextInt(ops.size()));
      while (op.ret == History.OPEN || (op.kind != History.Kind.GET && seed % 3 == 2)) {
        op = ops.get(rnd.nextInt(ops.size()));
      }
      switch ((int) (seed % 3)) {
        case 0 -> op.version += 1;
        case 1 -> op.ok = !op.ok;
        default -> op.readValue = op.readValue == null ? "absent" : null;
      }
      Optional<String> walks = Linearizability.check(ops, false);
      assertEquals(walks, Linearizability.check(ops, true), "seed " + seed);
      violations += walks.isPresent() ? 1 : 0;
    }
    assertTrue(violations >= 16, "violations: " + violations);
  }

  /**
   * The sort by return packs each return time above the operation's position in a long, and falls
   * back to sorting positions by time where a time does not fit. Moving every time by the same
   * amount must not change the answer or the report, apart from the times it shows: by 2^40, which
   * still fits above the positions of a long history, and by 2^58, which fits above none. The long
   * histories are also read in two halves at once.
   */
  @Test
  void largeTimesCheckTheSameWay() {
    for (long shift : new long[] {1L << 40, 1L << 58}) {
      for (long seed = 0; seed < 2_000; seed++) {
        List<History.Op> ops = randomHistory(seed);
        assertEquals(
            withoutTimes(Linearizability.check(ops)),
            withoutTimes(Linearizability.check(shifted(ops, shift))),
            "seed " + seed + ", shift " + shift);
      }
      for (long seed = 0; seed < 4; seed++) {
        List<History.Op> ops =
            HistoryGenerator.linearizable(
                seed, new HistoryGenerator.Shape(20_000, 4, 2, seed % 2 == 0 ? 0 : 0.002));
        assertEquals(Optional.empty(), Linearizability.check(shifted(ops, shift)), "seed " + seed);
        HistoryGenerator.staleRead(ops, seed);
        Optional<String> violation = Linearizability.check(ops);
        assertTrue(violation.isPresent(), "seed " + seed);
        assertEquals(
            withoutTimes(violation),
            withoutTimes(Linearizability.check(shifted(ops, shift))),
            "seed " + seed + ", shift " + shift);
      }
    }
  }

  /** A copy of {@code ops} with every call and return time moved {@code by} later. */
  private static List<History.Op> shifted(List<History.Op> ops, long by) {
    List<History.Op> out = new ArrayList<>(ops.size());
    for (History.Op o : ops) {
      History.Op c =
          new History.Op(o.id, o.client, o.kind, o.key, o.value, o.expected, o.call + by);
      c.ret = o.ret == History.OPEN ? History.OPEN : o.ret + by;
      c.ok = o.ok;
      c.version = o.version;
      c.readValue = o.readValue;
      out.add(c);
    }
    return out;
  }

  /** A report with the times of the operations it shows left out. */
  private static Optional<String> withoutTimes(Optional<String> report) {
    return report.map(r -> r.replaceAll("\\[\\d+,(\\d+|inf)\\]", "[]"));
  }

  /**
   * The candidate lists are, for each return, the operations running at it in call order, ties by
   * rank, as a direct look at every pair finds them: with a few clients, with many, and with some
   * operations running across hundreds of returns, which the lists reach by a slower path. The
   * reach of each rank is the highest in its list.
   */
  @Test
  void candidateListsAreTheOperationsRunningAtEachReturn() {
    SplittableRandom rnd = new SplittableRandom(11);
    for (int round = 0; round < 300; round++) {
      int n = 1 + rnd.nextInt(round % 10 == 0 ? 8 : 600);
      int clients = 1 + rnd.nextInt(round % 3 == 0 ? 40 : 6);
      // Each client runs one operation at a time; a few run for a long time.
      long[][] spans = new long[n][];
      long[] free = new long[clients];
      for (int i = 0; i < n; i++) {
        int c = rnd.nextInt(clients);
        long call = free[c] + rnd.nextInt(5);
        long length = 1L + (rnd.nextInt(50) == 0 ? rnd.nextInt(2_000) : rnd.nextInt(20));
        spans[i] = new long[] {call, call + length};
        free[c] = call + length + 1;
      }
      Arrays.sort(spans, Comparator.comparingLong((long[] x) -> x[1]));
      long[] call = new long[n];
      long[] ret = new long[n];
      for (int r = 0; r < n; r++) {
        call[r] = spans[r][0];
        ret[r] = spans[r][1];
      }
      int[] candStart = new int[n + 1];
      int[] cand = Linearizability.candidates(call, ret, n, candStart);
      for (int p = 0; p < n; p++) {
        Integer[] expected = new Integer[n - p];
        int size = 0;
        for (int r = p; r < n; r++) {
          if (r == p || call[r] < ret[p]) {
            expected[size++] = r;
          }
        }
        Arrays.sort(expected, 0, size, Comparator.comparingLong(r -> call[r])); // stable
        int[] got = Arrays.copyOfRange(cand, candStart[p], candStart[p + 1]);
        int[] want = Arrays.stream(expected, 0, size).mapToInt(Integer::intValue).toArray();
        assertEquals(Arrays.toString(want), Arrays.toString(got), "round " + round + " at " + p);
      }
      // A range that no operation runs across, as between pins, has the same lists on its own.
      List<Integer> cuts = new ArrayList<>(); // 0, n, and every rank called after all below return
      long earliest = Long.MAX_VALUE;
      for (int b = n; b >= 0; b--) {
        if (b == 0 || b == n || earliest > ret[b - 1]) {
          cuts.add(0, b);
        }
        if (b > 0) {
          earliest = Math.min(earliest, call[b - 1]);
        }
      }
      for (int t = 0; t < 20; t++) {
        int x = rnd.nextInt(cuts.size());
        int y = rnd.nextInt(cuts.size());
        int from = cuts.get(Math.min(x, y));
        int to = cuts.get(Math.max(x, y));
        int[] start = new int[to - from + 1];
        int[] part = Linearizability.candidates(call, ret, from, to, start);
        // The forward pass finds the candidates of p among the ranks up to its reach.
        int[] reach = Linearizability.reach(Linearizability.lows(call, ret, from, to), to - from);
        for (int p = from; p < to; p++) {
          assertEquals(
              Arrays.toString(Arrays.copyOfRange(cand, candStart[p], candStart[p + 1])),
              Arrays.toString(Arrays.copyOfRange(part, start[p - from], start[p - from + 1])),
              "round " + round + ", ranks " + from + " to " + to + ", at " + p);
          int highest =
              Arrays.stream(part, start[p - from], start[p - from + 1]).max().orElseThrow();
          assertEquals(highest - from, reach[p - from], "round " + round + ", reach of " + p);
        }
      }
    }
  }

  /**
   * sortBy and sortPacked are stable sorts, on sorted, nearly sorted and scrambled input, with and
   * without ties.
   */
  @Test
  void sortByIsAStableSort() {
    SplittableRandom rnd = new SplittableRandom(7);
    for (int round = 0; round < 500; round++) {
      int n = round % 10 == 0 ? rnd.nextInt(33) : rnd.nextInt(3_000);
      long[] key = new long[n];
      int shape = round % 5;
      for (int i = 0; i < n; i++) {
        key[i] =
            switch (shape) {
              case 0 -> rnd.nextLong(1L << rnd.nextInt(1, 63)); // any range
              case 1 -> i * 10L + rnd.nextInt(40); // nearly sorted
              case 2 -> n - i; // reversed
              case 3 -> i; // sorted
              default -> rnd.nextInt(5); // few distinct keys
            };
      }
      Integer[] boxed = new Integer[n];
      int[] idx = new int[n];
      for (int i = 0; i < n; i++) {
        int v = shape == 0 || shape == 4 ? rnd.nextInt(n) : i;
        boxed[i] = v;
        idx[i] = v;
      }
      Arrays.sort(boxed, Comparator.comparingLong(v -> key[v])); // stable
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      for (int v : idx) {
        min = Math.min(min, key[v]);
        max = Math.max(max, key[v]);
      }
      // sortPacked sorts each key packed above its position, where they fit in a long, as a key's
      // setup packs return times above positions.
      int low = 32 - Integer.numberOfLeadingZeros(Math.max(1, n - 1));
      boolean fits = n == 0 || max < 1L << (63 - low);
      long[] packed = new long[n];
      for (int i = 0; i < n && fits; i++) {
        packed[i] = (key[idx[i]] << low) | i;
      }
      int bits = n == 0 ? 0 : 64 - Long.numberOfLeadingZeros(max - min);
      long[] sorted = fits ? Linearizability.sortPacked(packed, n, low, bits, min << low) : packed;
      int[] unsorted = idx.clone();
      Linearizability.sortBy(idx, key);
      for (int i = 0; i < n; i++) {
        assertEquals((int) boxed[i], idx[i], "round " + round + " at " + i);
        if (fits) {
          assertEquals(
              (int) boxed[i],
              unsorted[(int) sorted[i] & ((1 << low) - 1)],
              "sortPacked, round " + round + " at " + i);
        }
      }
    }
  }
}
