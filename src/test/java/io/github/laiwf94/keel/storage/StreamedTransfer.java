package io.github.laiwf94.keel.storage;

import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.Message;
import io.github.laiwf94.keel.raft.RaftNode;
import io.github.laiwf94.keel.raft.RaftOptions;
import io.github.laiwf94.keel.replica.Outcome;
import io.github.laiwf94.keel.replica.Replica;
import io.github.laiwf94.keel.replica.StateMachine;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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
 * {@code snapshotBytes} of generated filler that restore checks byte for byte, so the transfer is
 * verified end to end without either side holding the state in memory. {@link #main} runs it in a
 * JVM with a small heap.
 */
final class StreamedTransfer {
  /** Snapshot bytes written per call to the stream, the way a state machine writes records. */
  private static final int BLOCK = 64 << 10;

  /**
   * A state machine whose state is a count and a digest of the commands applied, and whose snapshot
   * adds {@code filler} bytes generated from that state.
   */
  static final class FillerMachine implements StateMachine {
    final long filler;
    long applied;
    long digest = 17;
    int restores;

    FillerMachine(long filler) {
      this.filler = filler;
    }

    @Override
    public byte[] apply(long index, byte[] command) {
      applied++;
      for (byte b : command) {
        digest = digest * 31 + b;
      }
      digest = digest * 31 + index;
      return new byte[0];
    }

    private static void fill(ByteBuffer b, long[] seed) {
      b.clear();
      while (b.remaining() >= 8) {
        long x = seed[0];
        x ^= x << 13;
        x ^= x >>> 7;
        x ^= x << 17;
        seed[0] = x;
        b.putLong(x);
      }
      b.flip();
    }

    @Override
    public void snapshot(OutputStream out) throws IOException {
      ByteBuffer head = ByteBuffer.allocate(16).putLong(applied).putLong(digest);
      out.write(head.array());
      long[] seed = {digest | 1};
      ByteBuffer b = ByteBuffer.allocate(BLOCK);
      for (long left = filler; left > 0; left -= BLOCK) {
        fill(b, seed);
        out.write(b.array(), 0, (int) Math.min(BLOCK, left));
      }
    }

    @Override
    public void restore(InputStream in) throws IOException {
      byte[] head = in.readNBytes(16);
      if (head.length != 16) {
        throw new IOException("snapshot header truncated");
      }
      ByteBuffer h = ByteBuffer.wrap(head);
      long a = h.getLong();
      long d = h.getLong();
      long[] seed = {d | 1};
      ByteBuffer want = ByteBuffer.allocate(BLOCK);
      byte[] got = new byte[BLOCK];
      for (long left = filler; left > 0; left -= BLOCK) {
        int n = (int) Math.min(BLOCK, left);
        fill(want, seed);
        if (in.readNBytes(got, 0, n) != n
            || !java.util.Arrays.equals(got, 0, n, want.array(), 0, n)) {
          throw new IOException("snapshot filler differs before byte " + (filler - left + n));
        }
      }
      if (in.read() >= 0) {
        throw new IOException("snapshot has trailing bytes");
      }
      applied = a;
      digest = d;
      restores++;
    }
  }

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
          m1.digest == m2.digest && m1.applied == m2.applied,
          m2.restores,
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
