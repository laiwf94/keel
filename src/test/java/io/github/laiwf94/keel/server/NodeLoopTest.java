package io.github.laiwf94.keel.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.kv.Command;
import io.github.laiwf94.keel.kv.KvMachine;
import io.github.laiwf94.keel.kv.Result;
import io.github.laiwf94.keel.obs.Log;
import io.github.laiwf94.keel.obs.Metrics;
import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.Entry;
import io.github.laiwf94.keel.raft.HardState;
import io.github.laiwf94.keel.raft.MemStorage;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.Message;
import io.github.laiwf94.keel.raft.RaftNode;
import io.github.laiwf94.keel.raft.RaftOptions;
import io.github.laiwf94.keel.raft.Snapshot;
import io.github.laiwf94.keel.raft.Storage;
import io.github.laiwf94.keel.replica.Outcome;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NodeLoopTest {
  final List<NodeLoop> loops = new ArrayList<>();
  final ByteArrayOutputStream logs = new ByteArrayOutputStream();
  final Map<String, NodeLoop> byId = new ConcurrentHashMap<>();
  final Set<String> cut = ConcurrentHashMap.newKeySet();

  @AfterEach
  void stop() {
    loops.forEach(NodeLoop::close);
  }

  static Configuration config(int n) {
    List<Member> ms = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      ms.add(new Member("n" + i, "h:1", "h:2", true));
    }
    return new Configuration(ms);
  }

  NodeLoop loop(String id, Configuration c, Storage storage, Runnable onFatal) {
    Log log =
        new Log(
            id,
            new PrintStream(logs, true, StandardCharsets.UTF_8),
            Clock.systemUTC(),
            Log.Level.INFO);
    NodeLoop l =
        new NodeLoop(
            id,
            c,
            RaftOptions.DEFAULTS,
            storage,
            new KvMachine(60_000, 100),
            m -> {
              NodeLoop to = byId.get(m.to());
              if (to != null && !cut.contains(m.from()) && !cut.contains(m.to())) {
                to.receive(m);
              }
            },
            1_000,
            new NodeLoop.Settings(5, 1_000, 64),
            log,
            new Metrics(),
            onFatal);
    byId.put(id, l);
    loops.add(l);
    l.start();
    return l;
  }

  static void await(BooleanSupplier cond) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (!cond.getAsBoolean()) {
      assertTrue(System.nanoTime() < deadline, "timed out");
      Thread.sleep(2);
    }
  }

  NodeLoop leader() {
    return loops.stream()
        .filter(l -> l.status().role() == RaftNode.Role.LEADER && !cut.contains(l.status().id()))
        .findFirst()
        .orElse(null);
  }

  static byte[] put(String k, String v) {
    return Command.put(0, 0, 0, k, v.getBytes(StandardCharsets.UTF_8), -1).encode();
  }

  @Test
  void writesAreAcknowledgedAfterApplyAndReadableInEveryMode() throws Exception {
    for (int i = 1; i <= 3; i++) {
      loop("n" + i, config(3), new MemStorage(), () -> {});
    }
    await(() -> leader() != null);
    NodeLoop leader = leader();
    Outcome o = leader.propose(put("k", "v")).get(5, TimeUnit.SECONDS);
    Outcome.Applied a = assertInstanceOf(Outcome.Applied.class, o);
    assertEquals(Result.Status.OK, Result.decode(a.result()).status());
    assertTrue(leader.status().appliedIndex() >= a.index());
    KvMachine kv = (KvMachine) leader.machine();
    for (Consistency c : Consistency.values()) {
      ReadResult<String> r =
          leader
              .read(c, () -> new String(kv.get("k").orElseThrow().bytes(), StandardCharsets.UTF_8))
              .get(5, TimeUnit.SECONDS);
      assertEquals("v", ((ReadResult.Value<String>) r).value(), c.name());
    }
    NodeLoop follower = loops.stream().filter(l -> l != leader).findFirst().orElseThrow();
    assertInstanceOf(
        ReadResult.NotLeader.class,
        follower.read(Consistency.LINEARIZABLE, () -> 1).get(5, TimeUnit.SECONDS));
    assertInstanceOf(
        Outcome.NotLeader.class, follower.propose(put("a", "b")).get(5, TimeUnit.SECONDS));
    assertTrue(logs.toString(StandardCharsets.UTF_8).contains("\"event\":\"role\""));
    assertTrue(leader.isRunning());
    assertTrue(leader.execute(() -> {}));
    assertEquals(leader.status().id(), leader.member(leader.status().id()).orElseThrow().id());
  }

  @Test
  void aWriteWithoutAMajorityIsNeverAcknowledged() throws Exception {
    for (int i = 1; i <= 3; i++) {
      loop("n" + i, config(3), new MemStorage(), () -> {});
    }
    await(() -> leader() != null);
    NodeLoop leader = leader();
    for (NodeLoop l : loops) {
      if (l != leader) {
        cut.add(l.status().id());
      }
    }
    var pending = leader.propose(put("k", "v"));
    Thread.sleep(300);
    // The isolated leader steps down; the write is reported as unknown, never as applied.
    Outcome o = pending.get(5, TimeUnit.SECONDS);
    assertInstanceOf(Outcome.Unknown.class, o);
  }

  @Test
  void membershipCallsGoThroughTheLoop() throws Exception {
    NodeLoop n1 = loop("n1", config(1), new MemStorage(), () -> {});
    await(() -> n1.status().role() == RaftNode.Role.LEADER);
    assertInstanceOf(
        Outcome.Applied.class,
        n1.addServer(new Member("n2", "h:1", "h:2", true)).get(5, TimeUnit.SECONDS));
    assertInstanceOf(Outcome.Applied.class, n1.removeServer("n2").get(5, TimeUnit.SECONDS));
    await(() -> logs.toString(StandardCharsets.UTF_8).contains("\"event\":\"configuration\""));
  }

  @Test
  void storageFailureStopsTheNode() throws Exception {
    CountDownLatch fatal = new CountDownLatch(1);
    Storage failing =
        new Storage() {
          final MemStorage inner = new MemStorage();

          @Override
          public State load() {
            return inner.load();
          }

          @Override
          public void saveHardState(HardState state) {
            inner.saveHardState(state);
          }

          @Override
          public void append(List<Entry> entries) {
            inner.append(entries);
          }

          @Override
          public void saveSnapshot(Snapshot snapshot, List<Entry> remaining) {
            inner.saveSnapshot(snapshot, remaining);
          }

          @Override
          public void sync() {
            throw new io.github.laiwf94.keel.storage.StorageException("disk on fire");
          }
        };
    NodeLoop n = loop("n1", config(1), failing, fatal::countDown);
    assertTrue(fatal.await(5, TimeUnit.SECONDS));
    await(() -> !n.isRunning());
    assertTrue(logs.toString(StandardCharsets.UTF_8).contains("disk on fire"));
    Outcome o = n.propose(put("k", "v")).get(1, TimeUnit.SECONDS);
    assertInstanceOf(Outcome.Rejected.class, o);
    assertInstanceOf(
        ReadResult.Failed.class, n.read(Consistency.STALE, () -> 1).get(1, TimeUnit.SECONDS));
    n.receive(new Message.VoteRequest("n2", "n1", 1, 0, 0, false));
  }

  /** Memory storage whose next sync with something to write can be made to hang. */
  static final class StallingStorage implements Storage {
    final MemStorage inner = new MemStorage();
    volatile long stallMs;
    final CountDownLatch stalled = new CountDownLatch(1);
    private boolean dirty;

    @Override
    public State load() {
      return inner.load();
    }

    @Override
    public void saveHardState(HardState state) {
      dirty = true;
      inner.saveHardState(state);
    }

    @Override
    public void append(List<Entry> entries) {
      dirty |= !entries.isEmpty();
      inner.append(entries);
    }

    @Override
    public void saveSnapshot(Snapshot snapshot, List<Entry> remaining) {
      dirty = true;
      inner.saveSnapshot(snapshot, remaining);
    }

    @Override
    public void sync() {
      long ms = stallMs;
      boolean writes = dirty;
      dirty = false;
      if (ms > 0 && writes) {
        stallMs = 0;
        stalled.countDown();
        try {
          Thread.sleep(ms);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      inner.sync();
    }
  }

  /**
   * A leader whose fsync hangs for a second is replaced, and a lease read that was queued during
   * the hang must not be answered from the old leader's lease when the fsync returns: the loop has
   * to count the time it spent blocked before it runs the read.
   */
  @Test
  void aLeaderPausedInAnFsyncDoesNotServeAStaleLeaseRead() throws Exception {
    Map<String, StallingStorage> disks = new java.util.HashMap<>();
    for (int i = 1; i <= 3; i++) {
      StallingStorage d = new StallingStorage();
      disks.put("n" + i, d);
      loop("n" + i, config(3), d, () -> {});
    }
    await(() -> leader() != null);
    NodeLoop old = leader();
    String oldId = old.status().id();
    assertInstanceOf(Outcome.Applied.class, old.propose(put("k", "v1")).get(5, TimeUnit.SECONDS));
    // Let the lease renew so that it is valid when the fsync hangs.
    Thread.sleep(50);
    disks.get(oldId).stallMs = 1_000;
    var stuck = old.propose(put("k", "v-stuck"));
    assertTrue(disks.get(oldId).stalled.await(5, TimeUnit.SECONDS));
    cut.add(oldId);

    await(() -> leader() != null);
    NodeLoop next = leader();
    assertInstanceOf(Outcome.Applied.class, next.propose(put("k", "v2")).get(5, TimeUnit.SECONDS));

    KvMachine kv = (KvMachine) old.machine();
    ReadResult<String> r =
        old.read(
                Consistency.LEASE,
                () -> new String(kv.get("k").orElseThrow().bytes(), StandardCharsets.UTF_8))
            .get(5, TimeUnit.SECONDS);
    assertTrue(
        !(r instanceof ReadResult.Value<String> v) || v.value().equals("v2"),
        "the deposed leader answered a lease read with a stale value: " + r);
    // Followers may have acknowledged the write before the cut, so it can commit; it cannot be lost
    // silently.
    Outcome o = stuck.get(5, TimeUnit.SECONDS);
    assertTrue(o instanceof Outcome.Applied || o instanceof Outcome.Unknown, o.toString());
  }

  @Test
  void ticksDueCountsEveryTickThatHasComeDue() {
    assertEquals(0, NodeLoop.ticksDue(99, 100, 10));
    assertEquals(1, NodeLoop.ticksDue(100, 100, 10));
    assertEquals(1, NodeLoop.ticksDue(109, 100, 10));
    assertEquals(2, NodeLoop.ticksDue(110, 100, 10));
    assertEquals(101, NodeLoop.ticksDue(1_100, 100, 10));
    // nanoTime may be negative and may wrap; only differences count.
    assertEquals(2, NodeLoop.ticksDue(Long.MIN_VALUE + 5, Long.MAX_VALUE - 4, 10));
  }

  @Test
  void consistencyNamesParse() {
    assertEquals(Consistency.LEASE, Consistency.parse("lease").orElseThrow());
    assertTrue(Consistency.parse("LEASE").isEmpty());
    assertThrows(Exception.class, () -> Consistency.parse("bogus").orElseThrow());
  }
}
