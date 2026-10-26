package io.github.laiwf94.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.raft.Message.AppendRequest;
import io.github.laiwf94.keel.raft.Message.AppendResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ReplicationTest {
  static byte[] cmd(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  static List<String> commands(List<Entry> applied) {
    List<String> out = new ArrayList<>();
    for (Entry e : applied) {
      if (e.type() == EntryType.COMMAND) {
        out.add(new String(e.data(), StandardCharsets.UTF_8));
      }
    }
    return out;
  }

  @Test
  void commandsReachEveryNodeInOrder() {
    TestCluster c = new TestCluster(3, 21);
    RaftNode leader = c.awaitLeader();
    List<String> sent = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      sent.add("c" + i);
      leader.propose(cmd("c" + i));
      if (i % 7 == 0) {
        c.flush();
      }
    }
    c.ticks(5);
    for (String id : c.nodes.keySet()) {
      assertEquals(sent, commands(c.applied.get(id)), id);
    }
    assertEquals(leader.log().lastIndex(), leader.commitIndex());
  }

  @Test
  void singleNodeCommitsImmediately() {
    TestCluster c = new TestCluster(1, 22);
    RaftNode n = c.awaitLeader();
    long idx = n.propose(cmd("x"));
    assertEquals(idx, n.commitIndex());
    assertEquals(List.of("x"), commands(n.ready().committed()));
  }

  @Test
  void followersRejectProposals() {
    TestCluster c = new TestCluster(3, 23);
    RaftNode leader = c.awaitLeader();
    RaftNode f = c.nodes.values().stream().filter(n -> n != leader).findFirst().orElseThrow();
    NotLeaderException e = assertThrows(NotLeaderException.class, () -> f.propose(cmd("x")));
    assertEquals(leader.id(), e.leaderHint().orElseThrow());
  }

  @Test
  void survivesMessageLossAndRecoversLaggingFollower() {
    TestCluster c = new TestCluster(3, 24, new RaftOptions(10, 2, 4, 1 << 20, 16, 2));
    RaftNode leader = c.awaitLeader();
    Random r = new Random(5);
    c.dropIf = m -> r.nextInt(3) == 0 && m.term() == leader.term();
    for (int i = 0; i < 200; i++) {
      leader.propose(cmd("c" + i));
      if (i % 10 == 0) {
        c.tick();
      }
    }
    c.dropIf = m -> false;
    c.ticks(60);
    for (String id : c.nodes.keySet()) {
      assertEquals(200, commands(c.applied.get(id)).size(), id);
    }
  }

  @Test
  void divergentFollowerConvergesInAFewRoundTrips() {
    // Leader-to-be: 10 entries of term 1, then 20 of term 3. The follower has the same 10, then
    // 50 entries of term 2 that were never committed.
    List<Entry> leaderLog = new ArrayList<>();
    List<Entry> followerLog = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      leaderLog.add(new Entry(i, 1, EntryType.COMMAND, cmd("a" + i)));
      followerLog.add(new Entry(i, 1, EntryType.COMMAND, cmd("a" + i)));
    }
    for (int i = 11; i <= 30; i++) {
      leaderLog.add(new Entry(i, 3, EntryType.COMMAND, cmd("b" + i)));
    }
    for (int i = 11; i <= 60; i++) {
      followerLog.add(new Entry(i, 2, EntryType.COMMAND, cmd("x" + i)));
    }
    TestCluster c = new TestCluster(3, 25);
    c.storages.put("n1", preloaded(leaderLog, 3));
    c.storages.put("n2", preloaded(leaderLog, 3));
    c.storages.put("n3", preloaded(followerLog, 3));
    c.isolated.add("n3");
    for (String id : List.of("n1", "n2", "n3")) {
      c.start(id);
    }
    RaftNode leader = c.awaitLeader();
    assertTrue(Set.of("n1", "n2").contains(leader.id()));
    AtomicInteger appendsToN3 = new AtomicInteger();
    c.dropIf =
        m -> {
          if (m instanceof AppendRequest && m.to().equals("n3")) {
            appendsToN3.incrementAndGet();
          }
          return false;
        };
    c.isolated.clear();
    c.ticks(3);
    assertEquals(leader.log().lastIndex(), c.node("n3").log().lastIndex());
    assertEquals(3, c.node("n3").log().termAt(30));
    assertTrue(appendsToN3.get() <= 4, "took " + appendsToN3.get() + " appends");
  }

  static MemStorage preloaded(List<Entry> entries, long term) {
    MemStorage s = new MemStorage();
    s.append(entries);
    s.saveHardState(new HardState(term, ""));
    s.sync();
    return s;
  }

  /**
   * Figure 8 of the Raft paper: an entry from an older term that reaches a majority must not be
   * committed by counting replicas, because a node with a later term can still overwrite it.
   */
  @Test
  void figureEightOlderTermEntryIsNotCommittedByCounting() {
    for (long seed = 0; seed < 20; seed++) {
      figureEight(seed);
    }
  }

  private void figureEight(long seed) {
    TestCluster c = new TestCluster(5, seed, new RaftOptions(10, 2, 1, 1 << 20, 4096, 2));
    RaftNode a = c.awaitLeader();
    c.ticks(5);
    List<String> others = c.nodes.keySet().stream().filter(k -> !k.equals(a.id())).toList();
    String b = others.get(0);
    // (a) A replicates an entry of its term to B only.
    c.isolated.addAll(others.subList(1, 4));
    a.propose(cmd("old"));
    c.flush();
    long oldTerm = a.term();
    assertEquals(oldTerm, c.node(b).log().termAt(2));
    // (b) A crashes; one of the other three wins a later term, but its no-op reaches nobody.
    c.crash(a.id());
    c.isolated.clear();
    c.isolated.add(b);
    c.dropIf = m -> m instanceof AppendRequest ar && !ar.entries().isEmpty();
    RaftNode cNode = c.awaitLeader();
    assertTrue(cNode.term() > oldTerm);
    assertEquals(cNode.term(), cNode.log().termAt(2));
    // (c) C crashes; A or B wins again and copies the old entry to a majority, but the new
    // no-op at index 3 is held back.
    c.crash(cNode.id());
    c.start(a.id());
    c.isolated.clear();
    c.dropIf =
        m -> m instanceof AppendRequest ar && ar.entries().stream().anyMatch(e -> e.index() >= 3);
    RaftNode x = c.awaitLeader();
    assertTrue(Set.of(a.id(), b).contains(x.id()));
    c.ticks(10);
    long holders =
        c.nodes.values().stream()
            .filter(n -> !c.down.contains(n.id()) && n.log().termAt(2) == oldTerm)
            .count();
    assertTrue(holders >= 3, "old entry should be on a majority, was on " + holders);
    assertTrue(x.commitIndex() < 2, "an older-term entry must not commit by counting");
    // (d) Why: with A and B down, C wins and overwrites index 2 everywhere it can reach.
    c.crash(a.id());
    c.crash(b);
    c.start(cNode.id());
    c.dropIf = m -> false;
    RaftNode d = c.awaitLeader();
    assertEquals(cNode.id(), d.id());
    c.ticks(5);
    for (RaftNode n : c.nodes.values()) {
      if (!c.down.contains(n.id())) {
        assertEquals(cNode.log().termAt(2), n.log().termAt(2));
      }
    }
  }

  @Test
  void staleAndDuplicateRepliesAreHarmless() {
    TestCluster c = new TestCluster(3, 26);
    RaftNode leader = c.awaitLeader();
    String f = c.nodes.keySet().stream().filter(k -> !k.equals(leader.id())).findFirst().get();
    for (int i = 0; i < 5; i++) {
      leader.propose(cmd("c" + i));
    }
    c.flush();
    long commit = leader.commitIndex();
    leader.step(new AppendResponse(f, leader.id(), leader.term(), false, 0, 1, 1, 0, 0));
    leader.step(new AppendResponse(f, leader.id(), leader.term(), true, 2, 0, 0, 0, 0));
    leader.step(new AppendResponse("n9", leader.id(), leader.term(), true, 99, 0, 0, 0, 0));
    c.flush();
    assertEquals(commit, leader.commitIndex());
    // A follower replying to a heartbeat from before its snapshot point still reports success.
    RaftNode follower = c.node(f);
    follower.step(
        new AppendRequest(leader.id(), f, leader.term(), 2, leader.term(), List.of(), 2, 0));
    assertTrue(((AppendResponse) follower.ready().messages().get(0)).success());
  }
}
