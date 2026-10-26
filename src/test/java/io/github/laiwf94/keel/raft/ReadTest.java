package io.github.laiwf94.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class ReadTest {
  static List<String> followers(TestCluster c, RaftNode leader) {
    return c.nodes.keySet().stream().filter(k -> !k.equals(leader.id())).toList();
  }

  @Test
  void readIndexWaitsForAQuorumHeartbeatRound() {
    TestCluster c = new TestCluster(3, 41);
    RaftNode leader = c.awaitLeader();
    leader.propose(ReplicationTest.cmd("x"));
    c.flush();
    long commit = leader.commitIndex();
    leader.read(7, ReadMode.LINEARIZABLE);
    assertTrue(c.reads.get(leader.id()).isEmpty());
    c.flush();
    assertEquals(List.of(new Ready.ReadState(7, commit)), c.reads.get(leader.id()));
  }

  @Test
  void isolatedLeaderNeverAnswersAReadIndexRead() {
    TestCluster c = new TestCluster(3, 42);
    RaftNode leader = c.awaitLeader();
    c.isolated.add(leader.id());
    leader.read(1, ReadMode.LINEARIZABLE);
    c.ticks(5 * c.opts.electionTicks());
    assertTrue(c.reads.get(leader.id()).isEmpty());
    assertNotEquals(RaftNode.Role.LEADER, leader.role());
  }

  @Test
  void leaseReadIsServedLocallyUntilTheLeaseRunsOut() {
    TestCluster c = new TestCluster(3, 43);
    RaftNode leader = c.awaitLeader();
    c.ticks(c.opts.heartbeatTicks() * 2);
    leader.read(1, ReadMode.LEASE);
    // Served without a heartbeat round: it is in the very next Ready.
    assertEquals(1, leader.ready().reads().size());
    c.isolated.add(leader.id());
    c.ticks(c.opts.leaseTicks() + 1);
    leader.read(2, ReadMode.LEASE);
    c.ticks(5 * c.opts.electionTicks());
    assertTrue(c.reads.get(leader.id()).isEmpty(), "expired lease must fall back to ReadIndex");
  }

  @Test
  void readsWaitUntilTheLeaderCommitsAnEntryOfItsTerm() {
    Configuration three = new TestCluster(3, 0).config;
    RaftNode n = new RaftNode("n1", three, RaftOptions.DEFAULTS, new MemStorage(), new Random(1));
    while (n.role() != RaftNode.Role.PRE_CANDIDATE) {
      n.tick();
    }
    n.step(new Message.VoteResponse("n2", "n1", 1, true, true));
    n.step(new Message.VoteResponse("n2", "n1", 1, true, false));
    assertEquals(RaftNode.Role.LEADER, n.role());
    n.ready();
    n.read(9, ReadMode.LINEARIZABLE);
    n.read(10, ReadMode.LEASE);
    assertTrue(n.ready().reads().isEmpty());
    // n2 stores the no-op and acknowledges the first heartbeat round. The no-op commits, the
    // acknowledged round gives the leader a lease, so the lease read is served at once and the
    // ReadIndex read starts a new round.
    n.step(new Message.AppendResponse("n2", "n1", 1, true, 1, 0, 0, 0, 1));
    Ready r = n.ready();
    assertEquals(List.of(new Ready.ReadState(10, 1)), r.reads());
    long round =
        r.messages().stream()
            .filter(m -> m instanceof Message.AppendRequest)
            .mapToLong(m -> ((Message.AppendRequest) m).seq())
            .max()
            .orElseThrow();
    n.step(new Message.AppendResponse("n2", "n1", 1, true, 1, 0, 0, 0, round));
    assertEquals(List.of(new Ready.ReadState(9, 1)), n.ready().reads());
  }

  @Test
  void singleNodeReadsImmediately() {
    TestCluster c = new TestCluster(1, 45);
    RaftNode n = c.awaitLeader();
    n.read(3, ReadMode.LINEARIZABLE);
    assertEquals(List.of(new Ready.ReadState(3, n.commitIndex())), n.ready().reads());
  }

  @Test
  void followersRefuseReads() {
    TestCluster c = new TestCluster(3, 46);
    RaftNode leader = c.awaitLeader();
    RaftNode f = c.node(followers(c, leader).get(0));
    assertThrows(NotLeaderException.class, () -> f.read(1, ReadMode.LINEARIZABLE));
  }

  @Test
  void steppingDownDropsUnconfirmedReads() {
    TestCluster c = new TestCluster(3, 47);
    RaftNode leader = c.awaitLeader();
    c.isolated.add(leader.id());
    leader.read(1, ReadMode.LINEARIZABLE);
    leader.step(
        new Message.AppendResponse("n9", leader.id(), leader.term() + 1, false, 0, 0, 0, 0, 0));
    c.isolated.clear();
    c.ticks(50);
    assertTrue(c.reads.get(leader.id()).stream().noneMatch(r -> r.context() == 1));
  }

  /**
   * The lease is only as good as its clock assumption. If the followers' clocks run several times
   * faster than the leader's, they elect a new leader that commits a write while the old leader
   * still trusts its lease and answers from stale state. ReadIndex has no such dependency.
   */
  @Test
  void leaseReadsBreakWhenClocksDriftBeyondTheBound() {
    TestCluster c = new TestCluster(3, 48);
    RaftNode old = c.awaitLeader();
    c.ticks(c.opts.heartbeatTicks() * 2);
    long staleCommit = old.commitIndex();
    List<String> fs = followers(c, old);
    c.isolated.add(old.id());
    RaftNode next = null;
    for (int i = 0; i < 200 && next == null; i++) {
      c.tickOnly(fs);
      next =
          fs.stream()
              .map(c::node)
              .filter(n -> n.role() == RaftNode.Role.LEADER)
              .findFirst()
              .orElse(null);
    }
    assertTrue(next != null, "followers did not elect a new leader");
    next.propose(ReplicationTest.cmd("new"));
    c.tickOnly(fs);
    assertTrue(next.commitIndex() > staleCommit);
    // The old leader has seen no time pass, so its lease still holds.
    old.read(5, ReadMode.LEASE);
    Ready.ReadState stale = old.ready().reads().get(0);
    assertEquals(staleCommit, stale.index());
    assertTrue(stale.index() < next.commitIndex());
    // The same read through ReadIndex is never confirmed.
    old.read(6, ReadMode.LINEARIZABLE);
    c.flush();
    assertTrue(c.reads.get(old.id()).stream().noneMatch(r -> r.context() == 6));
  }
}
