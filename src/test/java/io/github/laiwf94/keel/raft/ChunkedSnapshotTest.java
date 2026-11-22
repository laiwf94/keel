package io.github.laiwf94.keel.raft;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.raft.Message.SnapshotRequest;
import io.github.laiwf94.keel.raft.Message.SnapshotResponse;
import io.github.laiwf94.keel.raft.Message.SnapshotResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/** InstallSnapshot in chunks: the leader's window and resumption, the follower's reassembly. */
class ChunkedSnapshotTest {
  static final int CHUNK = 4;

  /** A three-node cluster whose leader holds a 37-byte snapshot that one follower needs. */
  static final class Setup {
    final TestCluster c;
    final RaftNode leader;
    final String f;
    final byte[] data;
    final List<SnapshotRequest> attempts = new ArrayList<>();
    final List<SnapshotRequest> chunks = new ArrayList<>();
    final List<SnapshotResponse> replies = new ArrayList<>();
    Predicate<Message> drop = m -> false;

    Setup(long seed, int window) {
      c = new TestCluster(3, seed, RaftOptions.DEFAULTS.withSnapshotChunks(CHUNK, window));
      leader = c.awaitLeader();
      f = SnapshotTest.follower(c, leader);
      c.isolated.add(f);
      for (int i = 0; i < 20; i++) {
        leader.propose(ReplicationTest.cmd("c" + i));
      }
      c.flush();
      data = new byte[37];
      new Random(seed).nextBytes(data);
      leader.compact(leader.commitIndex(), data);
      leader.propose(ReplicationTest.cmd("after"));
      c.flush();
      c.isolated.clear();
      c.dropIf =
          m -> {
            if (m instanceof SnapshotRequest sr && sr.to().equals(f)) {
              attempts.add(sr);
            }
            boolean dropped = drop.test(m);
            if (!dropped && m instanceof SnapshotRequest sr && sr.to().equals(f)) {
              chunks.add(sr);
            }
            if (!dropped && m instanceof SnapshotResponse sr && sr.from().equals(f)) {
              replies.add(sr);
            }
            return dropped;
          };
    }

    /** Ticks until the leader has sent the first chunk. */
    void startTransfer() {
      for (int i = 0; i < 100 && attempts.isEmpty(); i++) {
        c.tick();
      }
      assertTrue(!attempts.isEmpty(), "no transfer started");
    }

    /** How many times each offset was delivered to the follower. */
    Map<Long, Integer> deliveries() {
      return count(chunks);
    }

    /** How many times the leader sent each offset, delivered or not. */
    Map<Long, Integer> sends() {
      return count(attempts);
    }

    private static Map<Long, Integer> count(List<SnapshotRequest> list) {
      Map<Long, Integer> n = new HashMap<>();
      for (SnapshotRequest sr : list) {
        n.merge(sr.offset(), 1, Integer::sum);
      }
      return n;
    }

    void assertInstalled() {
      RaftNode fn = c.node(f);
      assertEquals(leader.log().snapshot().index(), fn.log().snapshot().index());
      assertEquals(leader.log().lastIndex(), fn.log().lastIndex());
      List<Snapshot> restored = c.restored.get(f);
      assertArrayEquals(data, restored.get(restored.size() - 1).data());
      assertArrayEquals(data, c.storages.get(f).load().snapshot().orElseThrow().data());
      assertEquals(RaftNode.Role.LEADER, leader.role());
    }
  }

  @Test
  void snapshotGoesOverInChunksAndIsInstalledWhole() {
    Setup s = new Setup(41, 2);
    s.c.ticks(5);
    s.assertInstalled();
    // Ten chunks of at most four bytes, each sent once, in order, the last one marked done.
    assertEquals(10, s.chunks.size());
    for (int i = 0; i < s.chunks.size(); i++) {
      SnapshotRequest sr = s.chunks.get(i);
      assertEquals(4L * i, sr.offset());
      assertTrue(sr.intact());
      assertEquals(i == 9, sr.done());
      assertArrayEquals(Arrays.copyOfRange(s.data, 4 * i, Math.min(37, 4 * i + 4)), sr.data());
    }
    // Every chunk but the last was acknowledged with the offset after it; the last installed.
    for (int i = 0; i < 9; i++) {
      assertEquals(SnapshotResult.ACCEPTED, s.replies.get(i).result());
      assertEquals(4L * (i + 1), s.replies.get(i).nextOffset());
    }
    SnapshotResponse last = s.replies.get(9);
    assertEquals(SnapshotResult.INSTALLED, last.result());
    assertEquals(s.leader.log().snapshot().index(), last.matchIndex());
  }

  @Test
  void leaderKeepsAtMostTheWindowInFlight() {
    Setup s = new Setup(42, 3);
    // No reply gets through: the leader sends three chunks and then waits.
    s.drop = m -> m instanceof SnapshotResponse;
    s.startTransfer();
    s.c.ticks(s.c.opts.electionTicks() - 1);
    assertEquals(List.of(0L, 4L, 8L), s.chunks.stream().map(SnapshotRequest::offset).toList());
    // After an election timeout without progress it resends the same window.
    s.c.ticks(1);
    assertEquals(6, s.chunks.size());
    assertEquals(0L, s.chunks.get(3).offset());
    s.drop = m -> false;
    s.c.ticks(2 * s.c.opts.electionTicks());
    s.assertInstalled();
  }

  @Test
  void lostChunkIsResentFromTheGapAndEarlierChunksAreNot() {
    Setup s = new Setup(43, 4);
    int[] lost = {0};
    s.drop = m -> m instanceof SnapshotRequest sr && sr.offset() == 8 && lost[0]++ == 0;
    s.c.ticks(5);
    s.assertInstalled();
    Map<Long, Integer> n = s.sends();
    assertEquals(1, n.get(0L));
    assertEquals(1, n.get(4L));
    // Chunks 12, 16 and 20 arrived after the gap and were each rejected with offset 8. The leader
    // went back once, not once per rejection, so no chunk went out more than twice.
    long rejected = s.replies.stream().filter(r -> r.result() == SnapshotResult.REJECTED).count();
    assertTrue(rejected >= 3, "rejections: " + rejected);
    assertTrue(
        s.replies.stream()
            .filter(r -> r.result() == SnapshotResult.REJECTED)
            .allMatch(r -> r.nextOffset() == 8));
    for (Map.Entry<Long, Integer> e : n.entrySet()) {
      assertTrue(e.getValue() <= 2, "offset " + e.getKey() + " sent " + e.getValue() + " times");
    }
    assertEquals(2, n.get(8L));
    assertEquals(1, s.deliveries().get(8L));
  }

  @Test
  void lostAcknowledgementsResumeFromTheLastConfirmedOffset() {
    Setup s = new Setup(44, 2);
    int[] lost = {0};
    // The replies for offsets 20 and 24 are lost; the leader stalls with 16 confirmed and 24 sent.
    s.drop =
        m ->
            m instanceof SnapshotResponse sr
                && sr.result() == SnapshotResult.ACCEPTED
                && sr.nextOffset() >= 20
                && lost[0]++ < 2;
    s.c.ticks(3 * s.c.opts.electionTicks());
    s.assertInstalled();
    Map<Long, Integer> n = s.deliveries();
    for (long off = 0; off < 16; off += 4) {
      assertEquals(1, n.get(off), "offset " + off);
    }
    assertEquals(2, n.get(16L));
    assertEquals(2, n.get(20L));
  }

  @Test
  void followerThatRestartsMidTransferMakesTheLeaderStartOver() {
    Setup s = new Setup(45, 2);
    boolean[] crashed = {false};
    s.drop =
        m -> {
          if (!crashed[0] && m instanceof SnapshotRequest sr && sr.offset() == 12) {
            crashed[0] = true;
            s.c.crash(s.f);
            return true;
          }
          return false;
        };
    for (int i = 0; i < 100 && !crashed[0]; i++) {
      s.c.tick();
    }
    assertTrue(crashed[0]);
    s.c.start(s.f);
    s.c.ticks(3 * s.c.opts.electionTicks());
    s.assertInstalled();
    // The restarted follower rejected the leader's resumed chunk with offset 0.
    assertTrue(
        s.replies.stream()
            .anyMatch(r -> r.result() == SnapshotResult.REJECTED && r.nextOffset() == 0));
    assertEquals(2, s.deliveries().get(0L));
  }

  @Test
  void leaderMovesOnlyOnRepliesAboutItsTransfer() {
    Setup s = new Setup(46, 2);
    s.drop = m -> m instanceof SnapshotRequest;
    s.startTransfer();
    RaftNode l = s.leader;
    long idx = l.log().snapshot().index();
    long t = l.term();
    String f = s.f;
    l.ready();
    // A reply about another snapshot.
    l.step(reply(f, l, SnapshotResult.ACCEPTED, idx - 1, 4));
    assertEquals(List.of(), offsetsSent(l));
    // The first chunk arrived: one more fits in the window of two.
    l.step(reply(f, l, SnapshotResult.ACCEPTED, idx, 4));
    assertEquals(List.of(8L), offsetsSent(l));
    // A late or duplicated acceptance does not move the transfer back.
    l.step(reply(f, l, SnapshotResult.ACCEPTED, idx, 4));
    assertEquals(List.of(), offsetsSent(l));
    // A rejection goes back to the offset named, once.
    l.step(reply(f, l, SnapshotResult.REJECTED, idx, 0));
    assertEquals(List.of(0L, 4L), offsetsSent(l));
    l.step(reply(f, l, SnapshotResult.REJECTED, idx, 0));
    assertEquals(List.of(), offsetsSent(l));
    // Progress clears that, and the window moves on from the confirmed offset.
    l.step(reply(f, l, SnapshotResult.ACCEPTED, idx, 8));
    assertEquals(List.of(8L, 12L), offsetsSent(l));
    // Confirmation of bytes the leader had not sent yet skips ahead to them.
    l.step(reply(f, l, SnapshotResult.ACCEPTED, idx, 32));
    assertEquals(List.of(32L, 36L), offsetsSent(l));
    // A confirmation past the end is taken as the end. The last chunk is already out.
    l.step(reply(f, l, SnapshotResult.ACCEPTED, idx, 1000));
    assertEquals(List.of(), offsetsSent(l));
    // Without the installation an election timeout later, the leader resends from the confirmed
    // end, which leaves only an empty last chunk.
    for (int i = 0; i < s.c.opts.electionTicks(); i++) {
      l.tick();
    }
    List<SnapshotRequest> end = chunksSent(l);
    assertEquals(1, end.size());
    assertEquals(37, end.get(0).offset());
    assertEquals(0, end.get(0).data().length);
    assertTrue(end.get(0).done());
    // Installation ends the transfer; later chunk replies are ignored.
    l.step(SnapshotResponse.installed(f, l.id(), t, idx, idx, 0));
    assertEquals(List.of(), offsetsSent(l));
    l.step(reply(f, l, SnapshotResult.REJECTED, idx, 0));
    assertEquals(List.of(), offsetsSent(l));
  }

  @Test
  void followerKeepsOnlyChunksThatContinueWhatItHolds() {
    Configuration three = new TestCluster(3, 0).config;
    MemStorage storage = new MemStorage();
    RaftNode n = new RaftNode("n1", three, RaftOptions.DEFAULTS, storage, new Random(1));
    SnapshotMeta a = new SnapshotMeta(5, 1, three);
    SnapshotMeta b = new SnapshotMeta(6, 1, three);
    byte[] data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

    // Nothing held yet: a chunk past the start is rejected back to 0.
    assertReply(n, chunk("n2", 1, a, data, 4, 8), SnapshotResult.REJECTED, 5, 0);
    assertReply(n, chunk("n2", 1, a, data, 0, 4), SnapshotResult.ACCEPTED, 5, 4);
    // A duplicate is acknowledged with what is held.
    assertReply(n, chunk("n2", 1, a, data, 0, 4), SnapshotResult.ACCEPTED, 5, 4);
    // A gap is rejected back to the end of what is held.
    assertReply(n, chunk("n2", 1, a, data, 8, 11), SnapshotResult.REJECTED, 5, 4);
    // A chunk that fails its checksum is rejected and not kept.
    byte[] four = Arrays.copyOfRange(data, 4, 8);
    SnapshotRequest corrupt = new SnapshotRequest("n2", "n1", 1, a, 4, four, false, 0, 0);
    assertReply(n, corrupt, SnapshotResult.REJECTED, 5, 4);
    // A chunk of another snapshot that does not start it leaves this transfer alone.
    assertReply(n, chunk("n2", 1, b, data, 4, 8), SnapshotResult.REJECTED, 6, 0);
    assertReply(n, chunk("n2", 1, a, data, 4, 8), SnapshotResult.ACCEPTED, 5, 8);
    // A chunk that overlaps what is held adds only its new bytes.
    Ready partial = assertReply(n, chunk("n2", 1, a, data, 6, 10), SnapshotResult.ACCEPTED, 5, 10);
    assertTrue(partial.snapshot().isEmpty());
    // The last chunk installs the whole snapshot.
    Ready r = assertReply(n, chunk("n2", 1, a, data, 10, 11), SnapshotResult.INSTALLED, 5, 0);
    assertArrayEquals(data, r.snapshot().orElseThrow().data());
    assertEquals(5, n.commitIndex());
    storage.sync();
    assertArrayEquals(data, storage.load().snapshot().orElseThrow().data());
    // Any chunk of a snapshot the node already covers is answered as installed.
    SnapshotResponse again = only(n, chunk("n2", 1, a, data, 4, 8));
    assertEquals(SnapshotResult.INSTALLED, again.result());
    assertEquals(5, again.matchIndex());
  }

  @Test
  void transferIsTiedToOneLeaderAndTerm() {
    Configuration three = new TestCluster(3, 0).config;
    MemStorage storage = new MemStorage();
    RaftNode n = new RaftNode("n1", three, RaftOptions.DEFAULTS, storage, new Random(1));
    SnapshotMeta a = new SnapshotMeta(5, 1, three);
    SnapshotMeta b = new SnapshotMeta(7, 1, three);
    byte[] data = {1, 2, 3, 4, 5, 6, 7, 8};

    assertReply(n, chunk("n2", 1, a, data, 0, 4), SnapshotResult.ACCEPTED, 5, 4);
    // The first chunk of another snapshot replaces the transfer.
    assertReply(n, chunk("n2", 1, b, data, 0, 4), SnapshotResult.ACCEPTED, 7, 4);
    assertReply(n, chunk("n2", 1, a, data, 4, 8), SnapshotResult.REJECTED, 5, 0);
    // A new leader in a later term starts over, even with the same snapshot.
    assertReply(n, chunk("n3", 2, b, data, 4, 8), SnapshotResult.REJECTED, 7, 0);
    assertReply(n, chunk("n3", 2, b, data, 0, 4), SnapshotResult.ACCEPTED, 7, 4);
    // A restart loses the bytes received so far.
    storage.sync();
    RaftNode restarted = new RaftNode("n1", three, RaftOptions.DEFAULTS, storage, new Random(2));
    assertReply(restarted, chunk("n3", 2, b, data, 4, 8), SnapshotResult.REJECTED, 7, 0);
    // An empty snapshot is one empty last chunk.
    SnapshotMeta c = new SnapshotMeta(9, 2, three);
    Ready r =
        assertReply(
            restarted, chunk("n3", 2, c, new byte[0], 0, 0), SnapshotResult.INSTALLED, 9, 0);
    assertArrayEquals(new byte[0], r.snapshot().orElseThrow().data());
    assertEquals(9, restarted.commitIndex());
  }

  @Test
  void anElectionDropsATransferInProgress() {
    TestCluster c = new TestCluster(3, 47);
    RaftNode n = c.node("n1");
    SnapshotMeta a = new SnapshotMeta(5, 1, c.config);
    byte[] data = {1, 2, 3, 4, 5, 6, 7, 8};
    n.step(chunk("n2", 1, a, data, 0, 4));
    n.ready();
    // n1 times out and campaigns in term 2; the transfer from term 1 is gone.
    for (int i = 0; i < 4 * c.opts.electionTicks() && n.term() < 2; i++) {
      n.tick();
    }
    assertEquals(RaftNode.Role.PRE_CANDIDATE, n.role());
    for (int i = 0; i < 3 && n.term() < 2; i++) {
      n.step(new Message.VoteResponse("n2", "n1", n.term() + 1, true, true));
      n.step(new Message.VoteResponse("n3", "n1", n.term() + 1, true, true));
    }
    assertEquals(2, n.term());
    n.ready();
    assertReply(n, chunk("n2", n.term(), a, data, 4, 8), SnapshotResult.REJECTED, 5, 0);
  }

  private static SnapshotRequest chunk(
      String from, long term, SnapshotMeta meta, byte[] data, int start, int end) {
    return SnapshotRequest.of(
        from, "n1", term, meta, start, Arrays.copyOfRange(data, start, end), end == data.length, 0);
  }

  private static SnapshotResponse reply(
      String f, RaftNode leader, SnapshotResult result, long index, long next) {
    return new SnapshotResponse(f, leader.id(), leader.term(), result, index, next, 0, 0);
  }

  private static SnapshotResponse only(RaftNode n, SnapshotRequest sr) {
    return only(stepAndReady(n, sr));
  }

  private static Ready stepAndReady(RaftNode n, SnapshotRequest sr) {
    n.step(sr);
    return n.ready();
  }

  private static SnapshotResponse only(Ready ready) {
    List<SnapshotResponse> out =
        ready.messages().stream()
            .filter(SnapshotResponse.class::isInstance)
            .map(SnapshotResponse.class::cast)
            .toList();
    assertEquals(1, out.size(), out.toString());
    return out.get(0);
  }

  private static Ready assertReply(
      RaftNode n, SnapshotRequest sr, SnapshotResult result, long index, long next) {
    Ready ready = stepAndReady(n, sr);
    SnapshotResponse r = only(ready);
    assertEquals(result, r.result(), r.toString());
    assertEquals(index, r.snapshotIndex(), r.toString());
    if (result != SnapshotResult.INSTALLED) {
      assertEquals(next, r.nextOffset(), r.toString());
    } else {
      assertEquals(index, r.matchIndex(), r.toString());
    }
    return ready;
  }

  private static List<SnapshotRequest> chunksSent(RaftNode l) {
    return l.ready().messages().stream()
        .filter(SnapshotRequest.class::isInstance)
        .map(SnapshotRequest.class::cast)
        .toList();
  }

  private static List<Long> offsetsSent(RaftNode l) {
    return chunksSent(l).stream().map(SnapshotRequest::offset).toList();
  }
}
