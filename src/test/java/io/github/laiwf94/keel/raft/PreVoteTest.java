package io.github.laiwf94.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.raft.Message.VoteRequest;
import io.github.laiwf94.keel.raft.Message.VoteResponse;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class PreVoteTest {
  @Test
  void partitionedFollowerDoesNotRaiseTheTermOrDeposeTheLeader() {
    TestCluster c = new TestCluster(3, 11);
    RaftNode leader = c.awaitLeader();
    long term = leader.term();
    String follower =
        c.nodes.keySet().stream().filter(k -> !k.equals(leader.id())).findFirst().get();
    c.isolated.add(follower);
    c.ticks(200);
    assertEquals(term, c.node(follower).term(), "pre-vote must keep the term unchanged");
    c.isolated.clear();
    c.ticks(20);
    assertEquals(List.of(leader), c.leaders());
    assertEquals(term, leader.term());
    assertEquals(RaftNode.Role.FOLLOWER, c.node(follower).role());
  }

  @Test
  void isolatedLeaderStepsDownWithinTwoElectionTimeouts() {
    TestCluster c = new TestCluster(3, 12);
    RaftNode leader = c.awaitLeader();
    c.isolated.add(leader.id());
    c.ticks(2 * c.opts.electionTicks());
    assertNotEquals(RaftNode.Role.LEADER, leader.role());
    c.ticks(100);
    RaftNode next =
        c.leaders().stream().filter(l -> !l.id().equals(leader.id())).findFirst().orElseThrow();
    assertTrue(next.term() > leader.term());
  }

  @Test
  void followersIgnoreHigherTermVotesWhileTheLeaderIsAlive() {
    TestCluster c = new TestCluster(3, 13);
    RaftNode leader = c.awaitLeader();
    long term = leader.term();
    for (RaftNode n : c.nodes.values()) {
      if (n != leader) {
        n.step(new VoteRequest("n9", n.id(), term + 5, 100, term + 5, false));
        n.step(new VoteRequest("n9", n.id(), term + 5, 100, term + 5, true));
        assertTrue(n.ready().messages().isEmpty());
        assertEquals(term, n.term());
      }
    }
    leader.step(new VoteRequest("n9", leader.id(), term + 5, 100, term + 5, false));
    assertEquals(RaftNode.Role.LEADER, leader.role());
  }

  @Test
  void freshlyStartedNodeWaitsOutItsBootGuardBeforeVoting() {
    Configuration three = new TestCluster(3, 0).config;
    RaftNode n = new RaftNode("n1", three, RaftOptions.DEFAULTS, new MemStorage(), new Random(1));
    n.step(new VoteRequest("n2", "n1", 1, 0, 0, false));
    assertTrue(n.ready().messages().isEmpty());
    assertEquals(0, n.term());
  }

  @Test
  void preVoteDoesNotChangeTheVotersTermAndStaleOnesAreRejected() {
    MemStorage s = new MemStorage();
    RaftNode n = ElectionTest.fresh("n1", s);
    n.step(new VoteRequest("n2", "n1", 4, 0, 0, true));
    assertEquals(List.of(new VoteResponse("n1", "n2", 4, true, true)), n.ready().messages());
    assertEquals(0, n.term());
    n.step(new VoteRequest("n2", "n1", 6, 0, 0, false));
    n.ready();
    assertEquals(6, n.term());
    n.step(new VoteRequest("n3", "n1", 5, 0, 0, true));
    assertEquals(List.of(new VoteResponse("n1", "n3", 6, false, true)), n.ready().messages());
    // A pre-vote for the current term from someone else is refused: n1 already voted in it.
    n.step(new VoteRequest("n3", "n1", 6, 0, 0, true));
    assertEquals(List.of(new VoteResponse("n1", "n3", 6, false, true)), n.ready().messages());
  }

  @Test
  void preCandidateAdoptsHigherTermFromARejectionAndFailsOnMajorityRejection() {
    Configuration three = new TestCluster(3, 0).config;
    RaftNode n = new RaftNode("n1", three, RaftOptions.DEFAULTS, new MemStorage(), new Random(1));
    for (int i = 0; i < 40 && n.role() != RaftNode.Role.PRE_CANDIDATE; i++) {
      n.tick();
    }
    n.step(new VoteResponse("n2", "n1", 0, false, true));
    n.step(new VoteResponse("n3", "n1", 0, false, true));
    assertEquals(RaftNode.Role.FOLLOWER, n.role());
    for (int i = 0; i < 40 && n.role() != RaftNode.Role.PRE_CANDIDATE; i++) {
      n.tick();
    }
    n.step(new VoteResponse("n2", "n1", 7, false, true));
    assertEquals(RaftNode.Role.FOLLOWER, n.role());
    assertEquals(7, n.term());
    // Responses for a role the node is not in are ignored.
    n.step(new VoteResponse("n2", "n1", 7, true, false));
    assertEquals(RaftNode.Role.FOLLOWER, n.role());
  }
}
