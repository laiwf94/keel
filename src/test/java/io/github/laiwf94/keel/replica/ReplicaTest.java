package io.github.laiwf94.keel.replica;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.MemStorage;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.Message;
import io.github.laiwf94.keel.raft.RaftNode;
import io.github.laiwf94.keel.raft.RaftOptions;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class ReplicaTest {
  static final class Echo implements StateMachine {
    final List<String> seen = new ArrayList<>();

    @Override
    public byte[] apply(long index, byte[] command) {
      seen.add(new String(command, StandardCharsets.UTF_8));
      return command;
    }

    @Override
    public byte[] snapshot() {
      return new byte[0];
    }

    @Override
    public void restore(byte[] snapshot) {
      seen.clear();
    }
  }

  static Configuration config(int n) {
    List<Member> ms = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      ms.add(new Member("n" + i, "h:1", "h:2", true));
    }
    return new Configuration(ms);
  }

  static Replica single(Echo sm, List<Message> sent) {
    Replica r =
        new Replica(
            "n1", config(1), RaftOptions.DEFAULTS, new MemStorage(), sm, new Random(1), sent::add);
    while (r.node().role() != RaftNode.Role.LEADER) {
      r.tick();
      r.process();
    }
    return r;
  }

  @Test
  void appliedWritesReturnTheMachineResult() {
    Echo sm = new Echo();
    Replica r = single(sm, new ArrayList<>());
    List<Outcome> out = new ArrayList<>();
    r.propose("a".getBytes(StandardCharsets.UTF_8), out::add);
    r.process();
    Outcome.Applied a = assertInstanceOf(Outcome.Applied.class, out.get(0));
    assertEquals("a", new String(a.result(), StandardCharsets.UTF_8));
    assertEquals(2, a.index());
    assertTrue(a.toString().contains("2"));
    assertEquals(2, r.appliedIndex());
    assertEquals(RaftNode.Role.LEADER, r.status().role());
  }

  @Test
  void followersAnswerNotLeader() {
    List<Outcome> out = new ArrayList<>();
    Replica r =
        new Replica(
            "n1",
            config(3),
            RaftOptions.DEFAULTS,
            new MemStorage(),
            new Echo(),
            new Random(1),
            m -> {});
    r.propose(new byte[] {1}, out::add);
    assertEquals(new Outcome.NotLeader(java.util.Optional.empty()), out.get(0));
  }

  @Test
  void losingLeadershipFailsPendingAsUnknown() {
    List<Message> sent = new ArrayList<>();
    Replica r =
        new Replica(
            "n1",
            config(3),
            RaftOptions.DEFAULTS,
            new MemStorage(),
            new Echo(),
            new Random(1),
            sent::add);
    while (r.node().role() != RaftNode.Role.PRE_CANDIDATE) {
      r.tick();
      r.process();
    }
    r.receive(new Message.VoteResponse("n2", "n1", 1, true, true));
    r.process();
    r.receive(new Message.VoteResponse("n2", "n1", 1, true, false));
    r.process();
    assertEquals(RaftNode.Role.LEADER, r.node().role());
    List<Outcome> out = new ArrayList<>();
    r.propose(new byte[] {1}, out::add);
    r.process();
    assertTrue(out.isEmpty());
    r.receive(new Message.AppendRequest("n3", "n1", 5, 0, 0, List.of(), 0, 0));
    r.process();
    assertInstanceOf(Outcome.Unknown.class, out.get(0));
  }

  @Test
  void overwrittenEntriesAreReportedUnknown() {
    List<Message> sent = new ArrayList<>();
    Replica r =
        new Replica(
            "n1",
            config(3),
            RaftOptions.DEFAULTS,
            new MemStorage(),
            new Echo(),
            new Random(1),
            sent::add);
    while (r.node().role() != RaftNode.Role.PRE_CANDIDATE) {
      r.tick();
      r.process();
    }
    r.receive(new Message.VoteResponse("n2", "n1", 1, true, true));
    r.process();
    r.receive(new Message.VoteResponse("n2", "n1", 1, true, false));
    r.process();
    List<Outcome> out = new ArrayList<>();
    r.propose(new byte[] {1}, out::add);
    r.propose(new byte[] {2}, out::add);
    // Pretend the new leader's entries at the same indexes arrive and commit while we still
    // hold the callbacks: the replica must not report our commands as applied.
    var other =
        List.of(
            new io.github.laiwf94.keel.raft.Entry(
                1, 1, io.github.laiwf94.keel.raft.EntryType.NOOP, new byte[0]),
            new io.github.laiwf94.keel.raft.Entry(
                2, 2, io.github.laiwf94.keel.raft.EntryType.COMMAND, new byte[] {9}),
            new io.github.laiwf94.keel.raft.Entry(
                3, 2, io.github.laiwf94.keel.raft.EntryType.COMMAND, new byte[] {8}));
    r.receive(new Message.AppendRequest("n2", "n1", 2, 0, 0, other, 3, 0));
    r.process();
    assertEquals(2, out.size());
    out.forEach(o -> assertInstanceOf(Outcome.Unknown.class, o));
  }
}
