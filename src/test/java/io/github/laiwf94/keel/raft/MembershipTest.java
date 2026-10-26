package io.github.laiwf94.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class MembershipTest {
  static Member member(String id) {
    return new Member(id, id + ":7000", id + ":8080", true);
  }

  /** Starts a node outside the configuration, the way a new server joins. */
  static RaftNode join(TestCluster c, String id) {
    c.storages.put(id, new MemStorage());
    c.applied.put(id, new java.util.ArrayList<>());
    c.restored.put(id, new java.util.ArrayList<>());
    c.reads.put(id, new java.util.ArrayList<>());
    RaftNode n =
        new RaftNode(
            id, Configuration.EMPTY, c.opts, c.storages.get(id), new Random(id.hashCode()));
    c.nodes.put(id, n);
    return n;
  }

  @Test
  void newServerJoinsAsLearnerAndIsPromotedOnceCaughtUp() {
    TestCluster c = new TestCluster(3, 51);
    RaftNode leader = c.awaitLeader();
    for (int i = 0; i < 20; i++) {
      leader.propose(ReplicationTest.cmd("c" + i));
    }
    c.flush();
    RaftNode n4 = join(c, "n4");
    long idx = leader.addServer(member("n4"));
    assertFalse(leader.config().isVoter("n4"));
    assertTrue(leader.config().contains("n4"));
    c.ticks(10);
    assertTrue(leader.commitIndex() > idx);
    assertTrue(leader.config().isVoter("n4"), "learner was not promoted");
    assertEquals(4, leader.config().voterIds().size());
    assertEquals(leader.log().lastIndex(), n4.log().lastIndex());
    assertTrue(n4.config().isVoter("n4"));
  }

  @Test
  void oneChangeAtATime() {
    TestCluster c = new TestCluster(3, 52);
    RaftNode leader = c.awaitLeader();
    c.flush();
    join(c, "n4");
    leader.addServer(member("n4"));
    assertThrows(ConfigChangeException.class, () -> leader.addServer(member("n5")));
    assertThrows(ConfigChangeException.class, () -> leader.removeServer("n4"));
  }

  @Test
  void leaderWaitsForAnEntryOfItsTermBeforeChanging() {
    Configuration three = new TestCluster(3, 0).config;
    RaftNode n = new RaftNode("n1", three, RaftOptions.DEFAULTS, new MemStorage(), new Random(1));
    while (n.role() != RaftNode.Role.PRE_CANDIDATE) {
      n.tick();
    }
    n.step(new Message.VoteResponse("n2", "n1", 1, true, true));
    n.step(new Message.VoteResponse("n2", "n1", 1, true, false));
    assertEquals(RaftNode.Role.LEADER, n.role());
    assertThrows(ConfigChangeException.class, () -> n.removeServer("n3"));
  }

  @Test
  void removedLeaderStepsDownAndStaysQuiet() {
    TestCluster c = new TestCluster(3, 53);
    RaftNode leader = c.awaitLeader();
    c.flush();
    leader.removeServer(leader.id());
    c.ticks(5);
    assertFalse(leader.config().contains(leader.id()));
    assertEquals(RaftNode.Role.FOLLOWER, leader.role());
    RaftNode next = c.awaitLeader();
    assertTrue(!next.id().equals(leader.id()));
    long term = next.term();
    c.ticks(200);
    assertEquals(RaftNode.Role.FOLLOWER, leader.role());
    assertEquals(term, next.term(), "the removed server disrupted the cluster");
    assertEquals(2, next.config().voterIds().size());
  }

  @Test
  void removingAFollowerShrinksTheQuorum() {
    TestCluster c = new TestCluster(3, 54);
    RaftNode leader = c.awaitLeader();
    c.flush();
    List<String> fs = c.nodes.keySet().stream().filter(k -> !k.equals(leader.id())).toList();
    leader.removeServer(fs.get(0));
    c.ticks(3);
    c.crash(fs.get(0));
    c.crash(fs.get(1));
    // With one voter left the leader commits alone.
    leader.removeServer(fs.get(1));
    leader.propose(ReplicationTest.cmd("x"));
    c.flush();
    assertEquals(List.of(leader.id()), leader.config().voterIds());
    assertEquals(leader.log().lastIndex(), leader.commitIndex());
  }

  @Test
  void invalidChangesAreRefused() {
    TestCluster c = new TestCluster(1, 55);
    RaftNode leader = c.awaitLeader();
    c.flush();
    assertThrows(ConfigChangeException.class, () -> leader.addServer(member("n1")));
    assertThrows(ConfigChangeException.class, () -> leader.removeServer("n9"));
    assertThrows(ConfigChangeException.class, () -> leader.removeServer("n1"));
    TestCluster three = new TestCluster(3, 56);
    RaftNode l3 = three.awaitLeader();
    RaftNode f = three.nodes.values().stream().filter(n -> n != l3).findFirst().orElseThrow();
    assertThrows(NotLeaderException.class, () -> f.addServer(member("n9")));
  }
}
