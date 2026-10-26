package io.github.laiwf94.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.raft.Message.AppendRequest;
import io.github.laiwf94.keel.raft.Message.AppendResponse;
import io.github.laiwf94.keel.raft.Message.VoteRequest;
import io.github.laiwf94.keel.raft.Message.VoteResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class ElectionTest {
  static Configuration three() {
    return new TestCluster(3, 0).config;
  }

  /** A follower that has waited out its boot guard but not yet campaigned. */
  static RaftNode fresh(String id, MemStorage s) {
    RaftOptions o = new RaftOptions(10, 2, 256, 1 << 20, 4096, 2);
    RaftNode n = new RaftNode(id, three(), o, s, new Random(5));
    for (int i = 0; i < o.electionTicks(); i++) {
      n.tick();
    }
    n.ready();
    s.sync();
    return n;
  }

  @Test
  void threeNodesElectExactlyOneLeader() {
    TestCluster c = new TestCluster(3, 1);
    RaftNode leader = c.awaitLeader();
    c.ticks(50);
    assertEquals(List.of(leader), c.leaders());
    for (RaftNode n : c.nodes.values()) {
      assertEquals(leader.term(), n.term());
      assertEquals(leader.id(), n.leader().orElseThrow());
    }
  }

  @Test
  void atMostOneLeaderPerTermUnderMessageLoss() {
    for (long seed = 0; seed < 50; seed++) {
      TestCluster c = new TestCluster(5, seed);
      Random drop = new Random(seed);
      c.dropIf = m -> drop.nextInt(4) == 0;
      Map<Long, String> leaderOfTerm = new HashMap<>();
      for (int t = 0; t < 400; t++) {
        c.tick();
        for (RaftNode l : c.leaders()) {
          String prev = leaderOfTerm.putIfAbsent(l.term(), l.id());
          assertTrue(prev == null || prev.equals(l.id()), "two leaders in term " + l.term());
        }
      }
      assertFalse(leaderOfTerm.isEmpty(), "seed " + seed + " never elected a leader");
    }
  }

  @Test
  void singleNodeElectsItself() {
    TestCluster c = new TestCluster(1, 3);
    RaftNode n = c.awaitLeader();
    assertEquals("n1", n.id());
    assertEquals(1, n.term());
  }

  @Test
  void voteSurvivesRestartAndIsNotGivenTwice() {
    MemStorage s = new MemStorage();
    RaftNode n = fresh("n1", s);
    n.step(new VoteRequest("n2", "n1", 5, 0, 0, false));
    Ready r = n.ready();
    s.sync();
    assertEquals(List.of(new VoteResponse("n1", "n2", 5, true, false)), r.messages());

    RaftNode restarted = fresh("n1", s);
    assertEquals(5, restarted.term());
    restarted.step(new VoteRequest("n3", "n1", 5, 0, 0, false));
    assertEquals(
        List.of(new VoteResponse("n1", "n3", 5, false, false)), restarted.ready().messages());
    // The same candidate asking again (a retransmission) gets the same answer.
    restarted.step(new VoteRequest("n2", "n1", 5, 0, 0, false));
    assertTrue(((VoteResponse) restarted.ready().messages().get(0)).granted());
  }

  @Test
  void unsyncedVoteIsLostOnCrash() {
    MemStorage s = new MemStorage();
    RaftNode n = fresh("n1", s);
    n.step(new VoteRequest("n2", "n1", 5, 0, 0, false));
    n.ready();
    s.crash();
    assertEquals(0, new RaftNode("n1", three(), RaftOptions.DEFAULTS, s, new Random(1)).term());
  }

  @Test
  void voteRestrictionRejectsShorterLogs() {
    MemStorage s = new MemStorage();
    s.append(
        List.of(
            new Entry(1, 1, EntryType.COMMAND, new byte[0]),
            new Entry(2, 2, EntryType.COMMAND, new byte[0])));
    s.saveHardState(new HardState(2, ""));
    s.sync();
    RaftNode n = fresh("n1", s);
    n.step(new VoteRequest("n2", "n1", 3, 5, 1, false));
    assertFalse(((VoteResponse) n.ready().messages().get(0)).granted());
    n.step(new VoteRequest("n3", "n1", 3, 2, 2, false));
    assertTrue(((VoteResponse) n.ready().messages().get(0)).granted());
  }

  @Test
  void candidateStepsDownWhenAMajorityRejects() {
    RaftNode n = new RaftNode("n1", three(), RaftOptions.DEFAULTS, new MemStorage(), new Random(1));
    for (int i = 0; i < 40 && n.role() != RaftNode.Role.PRE_CANDIDATE; i++) {
      n.tick();
    }
    assertEquals(RaftNode.Role.PRE_CANDIDATE, n.role());
    assertEquals(0, n.term());
    n.step(new VoteResponse("n2", "n1", 1, true, true));
    assertEquals(RaftNode.Role.CANDIDATE, n.role());
    long t = n.term();
    assertEquals(1, t);
    n.step(new VoteResponse("n2", "n1", t, false, false));
    assertEquals(RaftNode.Role.CANDIDATE, n.role());
    n.step(new VoteResponse("n3", "n1", t, false, false));
    assertEquals(RaftNode.Role.FOLLOWER, n.role());
    assertEquals(t, n.term());
  }

  @Test
  void staleLeaderLearnsNewTermFromReply() {
    TestCluster c = new TestCluster(3, 7);
    RaftNode leader = c.awaitLeader();
    String other = leader.id().equals("n1") ? "n2" : "n1";
    c.node(other).step(new AppendRequest(leader.id(), other, 0, 0, 0, List.of(), 0, 0));
    Ready r = c.node(other).ready();
    AppendResponse reply = (AppendResponse) r.messages().get(0);
    assertFalse(reply.success());
    leader.step(new VoteResponse(other, leader.id(), leader.term() + 1, false, false));
    assertEquals(RaftNode.Role.FOLLOWER, leader.role());
  }

  @Test
  void leaderStepsDownOnHigherTermAndCandidateOnSameTermLeader() {
    TestCluster c = new TestCluster(3, 9);
    RaftNode leader = c.awaitLeader();
    long t = leader.term();
    leader.step(new AppendResponse("n9", leader.id(), t + 2, false, 0, 0, 0, 0, 0));
    assertEquals(RaftNode.Role.FOLLOWER, leader.role());
    assertEquals(t + 2, leader.term());
    assertTrue(leader.leader().isEmpty());

    RaftNode n = new RaftNode("n1", three(), RaftOptions.DEFAULTS, new MemStorage(), new Random(1));
    for (int i = 0; i < 40 && n.role() != RaftNode.Role.PRE_CANDIDATE; i++) {
      n.tick();
    }
    n.step(new AppendRequest("n2", "n1", n.term(), 0, 0, List.of(), 0, 0));
    assertEquals(RaftNode.Role.FOLLOWER, n.role());
    assertEquals("n2", n.leader().orElseThrow());
  }

  @Test
  void ignoresMisaddressedMessagesAndNonVotersDoNotCampaign() {
    RaftNode n = new RaftNode("n1", three(), RaftOptions.DEFAULTS, new MemStorage(), new Random(1));
    n.step(new VoteRequest("n2", "n3", 9, 0, 0, false));
    n.step(new VoteRequest("n1", "n1", 9, 0, 0, false));
    assertEquals(0, n.term());

    Configuration withLearner =
        new Configuration(
            List.of(new Member("n1", "a:1", "a:2", false), new Member("n2", "b:1", "b:2", true)));
    RaftNode learner =
        new RaftNode("n1", withLearner, RaftOptions.DEFAULTS, new MemStorage(), new Random(1));
    for (int i = 0; i < 100; i++) {
      learner.tick();
    }
    assertEquals(RaftNode.Role.FOLLOWER, learner.role());
    assertEquals(0, learner.term());
    assertTrue(learner.ready().messages().isEmpty());
    assertEquals("n1", learner.status().id());
    assertEquals(0, learner.commitIndex());
    assertEquals(withLearner, learner.config());
    assertEquals(0, learner.log().lastIndex());
  }

  @Test
  void optionsAndExceptionsValidate() {
    assertEquals(8, RaftOptions.DEFAULTS.leaseTicks());
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> new RaftOptions(2, 2, 1, 1, 1, 1));
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> new RaftOptions(10, 2, 0, 1, 1, 1));
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> new RaftOptions(10, 2, 1, 1, 1, 10));
    assertTrue(new NotLeaderException("").leaderHint().isEmpty());
    assertEquals("n2", new NotLeaderException("n2").leaderHint().orElseThrow());
    assertTrue(new NotLeaderException("n2").getMessage().contains("n2"));
  }

  @Test
  void memStorageModelsPendingWrites() {
    MemStorage s = new MemStorage();
    s.append(List.of());
    s.sync();
    assertEquals(0, s.syncs());
    s.append(List.of(new Entry(1, 1, EntryType.NOOP, new byte[0])));
    org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, s::load);
    s.sync();
    s.append(List.of(new Entry(3, 1, EntryType.NOOP, new byte[0])));
    org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, s::sync);
  }
}
