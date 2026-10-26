package io.github.laiwf94.keel.raft;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.raft.Message.SnapshotRequest;
import io.github.laiwf94.keel.raft.Message.SnapshotResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SnapshotTest {
  static String follower(TestCluster c, RaftNode leader) {
    return c.nodes.keySet().stream().filter(k -> !k.equals(leader.id())).findFirst().orElseThrow();
  }

  @Test
  void laggingFollowerCatchesUpThroughASnapshot() {
    TestCluster c = new TestCluster(3, 31);
    RaftNode leader = c.awaitLeader();
    String f = follower(c, leader);
    c.isolated.add(f);
    for (int i = 0; i < 30; i++) {
      leader.propose(ReplicationTest.cmd("c" + i));
    }
    c.flush();
    long applied = leader.commitIndex();
    leader.compact(applied, new byte[] {42});
    assertEquals(applied + 1, leader.log().firstIndex());
    leader.propose(ReplicationTest.cmd("after"));
    c.isolated.clear();
    AtomicInteger snapshots = new AtomicInteger();
    c.dropIf =
        m -> {
          if (m instanceof SnapshotRequest) {
            snapshots.incrementAndGet();
          }
          return false;
        };
    c.ticks(10);
    assertEquals(1, snapshots.get());
    // The follower's Ready carried the snapshot for its state machine.
    assertArrayEquals(new byte[] {42}, c.restored.get(f).get(0).data());
    RaftNode fn = c.node(f);
    assertEquals(applied, fn.log().snapshot().index());
    assertEquals(leader.log().lastIndex(), fn.log().lastIndex());
    // MemStorage holds the snapshot durably.
    Storage.State st = c.storages.get(f).load();
    assertArrayEquals(new byte[] {42}, st.snapshot().orElseThrow().data());
  }

  @Test
  void lostSnapshotIsResent() {
    TestCluster c = new TestCluster(3, 32);
    RaftNode leader = c.awaitLeader();
    String f = follower(c, leader);
    c.isolated.add(f);
    for (int i = 0; i < 10; i++) {
      leader.propose(ReplicationTest.cmd("c" + i));
    }
    c.flush();
    leader.compact(leader.commitIndex(), new byte[] {1});
    c.isolated.clear();
    AtomicInteger sent = new AtomicInteger();
    c.dropIf = m -> m instanceof SnapshotRequest && sent.incrementAndGet() == 1;
    c.ticks(3 * c.opts.electionTicks());
    assertTrue(sent.get() >= 2, "snapshot was not resent");
    assertEquals(leader.log().lastIndex(), c.node(f).log().lastIndex());
    assertEquals(RaftNode.Role.LEADER, leader.role());
  }

  @Test
  void restartedNodeRestoresItsSnapshotFirst() {
    TestCluster c = new TestCluster(1, 33);
    RaftNode n = c.awaitLeader();
    n.propose(ReplicationTest.cmd("x"));
    c.flush();
    n.compact(n.commitIndex(), new byte[] {7});
    c.storages.get("n1").sync();
    RaftNode restarted = new RaftNode("n1", c.config, c.opts, c.storages.get("n1"), new Random(1));
    Ready r = restarted.ready();
    assertArrayEquals(new byte[] {7}, r.snapshot().orElseThrow().data());
    assertTrue(restarted.ready().snapshot().isEmpty());
    assertEquals(n.commitIndex(), restarted.commitIndex());
  }

  @Test
  void followerKeepsItsSuffixAndIgnoresOldSnapshots() {
    TestCluster c = new TestCluster(3, 34);
    RaftNode leader = c.awaitLeader();
    for (int i = 0; i < 5; i++) {
      leader.propose(ReplicationTest.cmd("c" + i));
    }
    c.flush();
    String f = follower(c, leader);
    RaftNode fn = c.node(f);
    long last = fn.log().lastIndex();
    long commit = fn.commitIndex();
    // A snapshot of an index the follower already committed is acknowledged without changes.
    SnapshotMeta old = new SnapshotMeta(1, fn.log().termAt(1), c.config);
    fn.step(new SnapshotRequest(leader.id(), f, leader.term(), new Snapshot(old, new byte[0]), 0));
    SnapshotResponse reply = (SnapshotResponse) fn.ready().messages().get(0);
    assertEquals(commit, reply.matchIndex());
    // A follower that holds a longer log than the snapshot keeps the entries after it.
    List<Entry> entries = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      entries.add(new Entry(i, 1, EntryType.COMMAND, new byte[] {(byte) i}));
    }
    RaftNode lagging =
        new RaftNode("n1", c.config, c.opts, ReplicationTest.preloaded(entries, 1), new Random(1));
    SnapshotMeta prefix = new SnapshotMeta(3, 1, c.config);
    lagging.step(new SnapshotRequest("n2", "n1", 1, new Snapshot(prefix, new byte[0]), 0));
    assertEquals(5, lagging.log().lastIndex());
    assertEquals(4, lagging.log().firstIndex());
    assertEquals(3, lagging.commitIndex());
    assertTrue(last > 0);
  }

  @Test
  void compactionRules() {
    TestCluster c = new TestCluster(1, 36);
    RaftNode n = c.awaitLeader();
    n.propose(ReplicationTest.cmd("x"));
    assertThrows(IllegalArgumentException.class, () -> n.compact(n.log().lastIndex(), new byte[0]));
    c.flush();
    n.compact(n.commitIndex(), new byte[0]);
    long first = n.log().firstIndex();
    n.compact(n.commitIndex(), new byte[0]);
    assertEquals(first, n.log().firstIndex());
  }

  @Test
  void candidateReceivingASnapshotBecomesFollower() {
    Configuration three = new TestCluster(3, 0).config;
    RaftNode n = new RaftNode("n1", three, RaftOptions.DEFAULTS, new MemStorage(), new Random(1));
    for (int i = 0; i < 40 && n.role() != RaftNode.Role.PRE_CANDIDATE; i++) {
      n.tick();
    }
    SnapshotMeta meta = new SnapshotMeta(3, 1, three);
    n.step(new SnapshotRequest("n2", "n1", 1, new Snapshot(meta, new byte[] {5}), 0));
    assertEquals(RaftNode.Role.FOLLOWER, n.role());
    assertEquals("n2", n.leader().orElseThrow());
    assertEquals(3, n.commitIndex());
    n.step(new SnapshotResponse("n2", "n1", 1, 3, 0));
    assertEquals(RaftNode.Role.FOLLOWER, n.role());
  }
}
