package io.github.laiwf94.keel.storage;

import io.github.laiwf94.keel.bench.FillerMachine;
import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.Message;
import io.github.laiwf94.keel.raft.RaftNode;
import io.github.laiwf94.keel.raft.RaftOptions;
import io.github.laiwf94.keel.replica.Outcome;
import io.github.laiwf94.keel.replica.Replica;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Sends one large snapshot from a leader to a new server, both on {@link FileStorage}, through
 * {@link Replica} with messages passed in memory. The state machine's snapshot is a header and
 * {@code snapshotBytes} of generated filler that restore checks byte for byte ({@link
 * FillerMachine}), so the transfer is verified end to end without either side holding the state in
 * memory. {@link #main} runs it in a JVM with a small heap.
 */
final class StreamedTransfer {
  /** What a transfer did. */
  record Result(
      long snapshotIndex,
      long snapshotBytes,
      long leaderApplied,
      long followerApplied,
      boolean statesMatch,
      int restores,
      int chunksSent,
      int chunksDropped,
      boolean promoted,
      long millis) {}

  private StreamedTransfer() {}

  static Member member(String id) {
    return new Member(id, id + ":7000", id + ":8080", true);
  }

  /**
   * Runs one transfer in {@code dir}. Every {@code dropEvery}th InstallSnapshot chunk is lost on
   * the way (0 loses none), so the transfer also resumes from files.
   */
  static Result run(Path dir, long filler, int chunkBytes, int dropEvery) {
    long start = System.nanoTime();
    RaftOptions opts = RaftOptions.DEFAULTS.withSnapshotChunks(chunkBytes, 4);
    Map<String, ArrayDeque<Message>> inboxes =
        Map.of("n1", new ArrayDeque<>(), "n2", new ArrayDeque<>());
    int[] chunks = new int[2];
    FillerMachine m1 = new FillerMachine(filler);
    FillerMachine m2 = new FillerMachine(filler);
    try (FileStorage s1 = new FileStorage(dir.resolve("n1"));
        FileStorage s2 = new FileStorage(dir.resolve("n2"))) {
      java.util.function.Consumer<Message> net =
          msg -> {
            if (msg instanceof Message.SnapshotRequest) {
              chunks[0]++;
              if (dropEvery > 0 && chunks[0] % dropEvery == 0) {
                chunks[1]++;
                return;
              }
            }
            ArrayDeque<Message> q = inboxes.get(msg.to());
            if (q != null) {
              q.add(msg);
            }
          };
      Replica leader =
          new Replica(
              "n1", new Configuration(List.of(member("n1"))), opts, s1, m1, new Random(1), net, 10);
      while (leader.node().role() != RaftNode.Role.LEADER) {
        leader.tick();
        leader.process();
      }
      List<Outcome> out = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        leader.propose(("c" + i).getBytes(StandardCharsets.UTF_8), out::add);
        leader.process();
      }
      long snapIndex = leader.node().log().snapshot().index();
      if (snapIndex == 0) {
        throw new AssertionError("the leader did not compact");
      }
      Replica follower =
          new Replica("n2", Configuration.EMPTY, opts, s2, m2, new Random(2), net, 1_000_000);
      leader.addServer(member("n2"), out::add);
      Map<String, Replica> replicas = Map.of("n1", leader, "n2", follower);
      for (int round = 0; round < 1_000_000; round++) {
        boolean moved = false;
        for (Map.Entry<String, Replica> e : replicas.entrySet()) {
          ArrayDeque<Message> q = inboxes.get(e.getKey());
          while (!q.isEmpty()) {
            e.getValue().receive(q.poll());
            moved = true;
          }
          e.getValue().process();
        }
        if (!moved) {
          leader.tick();
          follower.tick();
          leader.process();
          follower.process();
        }
        if (leader.node().config().isVoter("n2")
            && follower.appliedIndex() == leader.appliedIndex()) {
          break;
        }
      }
      return new Result(
          snapIndex,
          s1.currentSnapshot().size(),
          leader.appliedIndex(),
          follower.appliedIndex(),
          m1.sameStateAs(m2),
          m2.restores(),
          chunks[0],
          chunks[1],
          leader.node().config().isVoter("n2"),
          (System.nanoTime() - start) / 1_000_000);
    }
  }

  /** Runs a transfer: {@code main dir fillerBytes chunkBytes dropEvery}; prints the result. */
  public static void main(String[] args) {
    Result r =
        run(
            Path.of(args[0]),
            Long.parseLong(args[1]),
            Integer.parseInt(args[2]),
            Integer.parseInt(args[3]));
    Runtime rt = Runtime.getRuntime();
    System.out.println(r);
    System.out.println("maxHeap=" + rt.maxMemory());
  }
}
