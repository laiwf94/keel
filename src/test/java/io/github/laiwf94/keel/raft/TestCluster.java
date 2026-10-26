package io.github.laiwf94.keel.raft;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;

/** A synchronous, in-order cluster for unit tests. The full simulator lives in the sim package. */
final class TestCluster {
  final Map<String, RaftNode> nodes = new LinkedHashMap<>();
  final Map<String, MemStorage> storages = new LinkedHashMap<>();
  final Map<String, List<Entry>> applied = new LinkedHashMap<>();
  final Map<String, List<Snapshot>> restored = new LinkedHashMap<>();
  final Map<String, List<Ready.ReadState>> reads = new LinkedHashMap<>();
  final Set<String> down = new HashSet<>();
  final Set<String> isolated = new HashSet<>();
  final Deque<Message> wire = new ArrayDeque<>();
  final Configuration config;
  final RaftOptions opts;
  final Random random;
  Predicate<Message> dropIf = m -> false;

  TestCluster(int n, long seed) {
    this(n, seed, RaftOptions.DEFAULTS);
  }

  TestCluster(int n, long seed, RaftOptions opts) {
    List<Member> ms = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      ms.add(new Member("n" + i, "n" + i + ":7000", "n" + i + ":8080", true));
    }
    this.config = new Configuration(ms);
    this.opts = opts;
    this.random = new Random(seed);
    for (Member m : ms) {
      storages.put(m.id(), new MemStorage());
      applied.put(m.id(), new ArrayList<>());
      restored.put(m.id(), new ArrayList<>());
      reads.put(m.id(), new ArrayList<>());
      start(m.id());
    }
  }

  RaftNode start(String id) {
    RaftNode node = new RaftNode(id, config, opts, storages.get(id), new Random(random.nextLong()));
    nodes.put(id, node);
    down.remove(id);
    return node;
  }

  RaftNode node(String id) {
    return nodes.get(id);
  }

  void crash(String id) {
    storages.get(id).crash();
    down.add(id);
  }

  void tick() {
    for (var e : nodes.entrySet()) {
      if (!down.contains(e.getKey())) {
        e.getValue().tick();
      }
    }
    flush();
  }

  /** Ticks only the given nodes, as if their clocks ran faster than everyone else's. */
  void tickOnly(java.util.Collection<String> ids) {
    for (String id : ids) {
      if (!down.contains(id)) {
        nodes.get(id).tick();
      }
    }
    flush();
  }

  void ticks(int n) {
    for (int i = 0; i < n; i++) {
      tick();
    }
  }

  /** Collects output from every node and delivers it until the cluster is quiet. */
  void flush() {
    for (int rounds = 0; rounds < 10_000; rounds++) {
      for (var e : nodes.entrySet()) {
        if (down.contains(e.getKey())) {
          continue;
        }
        Ready r = e.getValue().ready();
        storages.get(e.getKey()).sync();
        e.getValue().markDurable();
        wire.addAll(r.messages());
        applied.get(e.getKey()).addAll(r.committed());
        r.snapshot().ifPresent(restored.get(e.getKey())::add);
        reads.get(e.getKey()).addAll(r.reads());
      }
      if (wire.isEmpty()
          && nodes.entrySet().stream()
              .noneMatch(e -> !down.contains(e.getKey()) && e.getValue().hasReady())) {
        return;
      }
      while (!wire.isEmpty()) {
        Message m = wire.poll();
        if (down.contains(m.to())
            || isolated.contains(m.to())
            || isolated.contains(m.from())
            || dropIf.test(m)) {
          continue;
        }
        nodes.get(m.to()).step(m);
      }
    }
    throw new AssertionError("cluster did not go quiet");
  }

  List<RaftNode> leaders() {
    return nodes.values().stream()
        .filter(n -> !down.contains(n.id()) && n.role() == RaftNode.Role.LEADER)
        .toList();
  }

  RaftNode awaitLeader() {
    for (int i = 0; i < 500; i++) {
      tick();
      List<RaftNode> ls = leaders().stream().filter(l -> !isolated.contains(l.id())).toList();
      if (ls.size() == 1) {
        return ls.get(0);
      }
    }
    throw new AssertionError("no leader elected");
  }
}
