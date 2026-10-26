package io.github.laiwf94.keel.sim;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
