package io.github.laiwf94.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.raft.Message.AppendRequest;
import io.github.laiwf94.keel.raft.Message.AppendResponse;
import io.github.laiwf94.keel.raft.Message.TimeoutNow;
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

  static String otherThan(TestCluster c, String... not) {
    List<String> skip = List.of(not);
    return c.nodes.keySet().stream().filter(k -> !skip.contains(k)).findFirst().orElseThrow();
  }

  @Test
  void transferHandsLeadershipToAnUpToDateVoterWithinOneRoundTrip() {
    TestCluster c = new TestCluster(3, 21);
    RaftNode leader = c.awaitLeader();
    leader.propose(ReplicationTest.cmd("before"));
    c.flush();
    long t = leader.term();
    String target = otherThan(c, leader.id());
    // No tick passes: the vote and the new leader's first round finish inside one flush.
    leader.transferLeadership(target);
    c.flush();
    RaftNode next = c.node(target);
    assertEquals(RaftNode.Role.LEADER, next.role());
    assertEquals(t + 1, next.term());
    assertEquals(RaftNode.Role.FOLLOWER, leader.role());
    assertEquals(target, leader.leader().orElseThrow());
    assertTrue(leader.transferee().isEmpty());
    assertEquals(List.of(next), c.leaders());
    // Nothing committed before the transfer is lost, and the new leader takes writes.
    next.propose(ReplicationTest.cmd("after"));
    c.ticks(c.opts.heartbeatTicks()); // followers learn the commit index on the next heartbeat
    for (String id : c.nodes.keySet()) {
      List<Entry> cmds =
          c.applied.get(id).stream().filter(e -> e.type() == EntryType.COMMAND).toList();
      assertEquals(2, cmds.size(), id);
    }
  }

  @Test
  void transferCatchesALaggingTargetUpBeforeSendingTimeoutNow() {
    TestCluster c = new TestCluster(3, 22);
    RaftNode leader = c.awaitLeader();
    String target = otherThan(c, leader.id());
    c.isolated.add(target);
    for (int i = 0; i < 20; i++) {
      leader.propose(ReplicationTest.cmd("c" + i));
    }
    c.flush();
    assertTrue(c.node(target).log().lastIndex() < leader.log().lastIndex());
    c.isolated.remove(target);
    List<Message> sent = new java.util.ArrayList<>();
    java.util.function.Predicate<Message> record =
        m -> {
          sent.add(m);
          return false;
        };
    c.dropIf = record;
    leader.transferLeadership(target);
    // While the target catches up, proposals are refused with a pointer to it.
    NotLeaderException e =
        assertThrows(NotLeaderException.class, () -> leader.propose(ReplicationTest.cmd("x")));
    assertEquals(target, e.leaderHint().orElseThrow());
    c.flush();
    // The leader probes the target on its heartbeats until their logs meet.
    for (int i = 0; i < c.opts.electionTicks() - 1 && leader.role() == RaftNode.Role.LEADER; i++) {
      c.tick();
    }
    int firstTimeoutNow = -1;
    int lastAppendToTarget = -1;
    for (int i = 0; i < sent.size(); i++) {
      Message m = sent.get(i);
      if (m instanceof TimeoutNow && firstTimeoutNow < 0) {
        firstTimeoutNow = i;
      }
      if (m instanceof AppendRequest a
          && a.from().equals(leader.id())
          && a.to().equals(target)
          && !a.entries().isEmpty()) {
        lastAppendToTarget = i;
      }
    }
    assertTrue(firstTimeoutNow > lastAppendToTarget, "TimeoutNow went out before catch-up");
    assertEquals(RaftNode.Role.LEADER, c.node(target).role());
    assertEquals(leader.log().lastIndex(), c.node(target).log().lastIndex());
  }

  @Test
  void transferToAnUnreachableTargetIsAbortedAfterOneElectionTimeout() {
    TestCluster c = new TestCluster(3, 23);
    RaftNode leader = c.awaitLeader();
    long t = leader.term();
    String target = otherThan(c, leader.id());
    c.isolated.add(target);
    leader.transferLeadership(target);
    assertEquals(target, leader.transferee().orElseThrow());
    assertThrows(ConfigChangeException.class, () -> leader.removeServer(target));
    c.ticks(c.opts.electionTicks() - 1);
    assertEquals(target, leader.transferee().orElseThrow());
    assertThrows(NotLeaderException.class, () -> leader.propose(ReplicationTest.cmd("x")));
    c.tick();
    assertTrue(leader.transferee().isEmpty());
    assertEquals(RaftNode.Role.LEADER, leader.role());
    assertEquals(t, leader.term());
    long idx = leader.propose(ReplicationTest.cmd("resumed"));
    c.flush();
    assertTrue(leader.commitIndex() >= idx);
  }

  @Test
  void staleOrMisdirectedTimeoutNowIsIgnored() {
    TestCluster c = new TestCluster(3, 24);
    RaftNode leader = c.awaitLeader();
    long t = leader.term();
    String f = otherThan(c, leader.id());
    String other = otherThan(c, leader.id(), f);
    // From an earlier term.
    c.node(f).step(new TimeoutNow(leader.id(), f, t - 1));
    // From a node that is not this follower's leader.
    c.node(f).step(new TimeoutNow(other, f, t));
    c.flush();
    assertEquals(RaftNode.Role.FOLLOWER, c.node(f).role());
    assertEquals(t, c.node(f).term());
    assertEquals(List.of(leader), c.leaders());
    // A leader ignores one too.
    leader.step(new TimeoutNow(f, leader.id(), t));
    assertEquals(RaftNode.Role.LEADER, leader.role());
    // A plain vote for a higher term is still refused while the leader's lease holds.
    c.node(f).step(new VoteRequest(other, f, t + 1, 100, t, false));
    assertEquals(t, c.node(f).term());
    // A transfer vote is not.
    c.node(f).step(new VoteRequest(other, f, t + 1, 100, t, false, true));
    assertEquals(t + 1, c.node(f).term());
    assertTrue(((VoteResponse) c.node(f).ready().messages().get(0)).granted());
  }

  @Test
  void timeoutNowFromANewerTermStillTriggersTheElection() {
    TestCluster c = new TestCluster(3, 25);
    RaftNode leader = c.awaitLeader();
    String f = otherThan(c, leader.id());
    long t = leader.term();
    c.node(f).step(new TimeoutNow(leader.id(), f, t + 1));
    assertEquals(RaftNode.Role.CANDIDATE, c.node(f).role());
    assertEquals(t + 2, c.node(f).term());
  }

  @Test
  void transferRejectsBadTargetsAndKeepsTheLatestRequest() {
    TestCluster c = new TestCluster(3, 26);
    RaftNode leader = c.awaitLeader();
    String a = otherThan(c, leader.id());
    String b = otherThan(c, leader.id(), a);
    assertThrows(IllegalArgumentException.class, () -> leader.transferLeadership(leader.id()));
    assertThrows(IllegalArgumentException.class, () -> leader.transferLeadership("n9"));
    NotLeaderException e =
        assertThrows(NotLeaderException.class, () -> c.node(a).transferLeadership(b));
    assertEquals(leader.id(), e.leaderHint().orElseThrow());
    c.isolated.add(a);
    c.isolated.add(b);
    leader.transferLeadership(a);
    c.ticks(c.opts.electionTicks() / 2);
    leader.transferLeadership(a); // same target: the clock keeps running
    leader.transferLeadership(b); // new target: replaces it and restarts the clock
    assertEquals(b, leader.transferee().orElseThrow());
    c.ticks(c.opts.electionTicks() - 1);
    assertEquals(b, leader.transferee().orElseThrow());
    c.tick();
    assertTrue(leader.transferee().isEmpty());
    c.isolated.clear();
    c.ticks(c.opts.electionTicks());
    assertEquals(List.of(leader), c.leaders());
  }

  @Test
  void aLeaderThatSentTimeoutNowStopsServingLeaseReadsLocally() {
    TestCluster c = new TestCluster(3, 27);
    RaftNode leader = c.awaitLeader();
    c.ticks(c.opts.heartbeatTicks() * 2);
    String target = otherThan(c, leader.id());
    // The TimeoutNow is lost, so the transfer fails and this node stays leader.
    c.dropIf = m -> m instanceof TimeoutNow;
    leader.transferLeadership(target);
    c.flush();
    c.ticks(c.opts.electionTicks());
    assertEquals(RaftNode.Role.LEADER, leader.role());
    assertTrue(leader.transferee().isEmpty());
    // The target may still act on a delayed TimeoutNow, so this term's lease is gone for good:
    // a lease read now waits for a heartbeat round instead of being answered at once.
    c.isolated.add(leader.id());
    leader.read(1, ReadMode.LEASE);
    c.flush();
    assertTrue(c.reads.get(leader.id()).isEmpty());
    c.isolated.clear();
    c.ticks(c.opts.heartbeatTicks());
    assertEquals(1, c.reads.get(leader.id()).size());
  }

  @Test
  void membershipChangesWaitForTheTransfer() {
    TestCluster c = new TestCluster(3, 28);
    RaftNode leader = c.awaitLeader();
    String target = otherThan(c, leader.id());
    c.isolated.add(target);
    leader.transferLeadership(target);
    ConfigChangeException e =
        assertThrows(
            ConfigChangeException.class,
            () -> leader.addServer(new Member("n9", "n9:1", "n9:2", true)));
    assertTrue(e.getMessage().contains("transfer"));
  }
}
