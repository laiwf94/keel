package io.github.laiwf94.keel.replica;

import io.github.laiwf94.keel.raft.ConfigChangeException;
import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.Entry;
import io.github.laiwf94.keel.raft.EntryType;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.Message;
import io.github.laiwf94.keel.raft.NotLeaderException;
import io.github.laiwf94.keel.raft.RaftNode;
import io.github.laiwf94.keel.raft.RaftOptions;
import io.github.laiwf94.keel.raft.ReadMode;
import io.github.laiwf94.keel.raft.Ready;
import io.github.laiwf94.keel.raft.Snapshot;
import io.github.laiwf94.keel.raft.Storage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Drives one {@link RaftNode}: it persists, sends and applies what the node produces, and matches
 * applied entries back to the callers that proposed them. It is single-threaded and has no clock,
 * so the simulator calls it directly and the server wraps it in one event-loop thread.
 *
 * <p>Order inside {@link #process()} is what makes a crash safe: storage is synced before any
 * message leaves, and a client hears about a write only after it is committed and applied.
 */
public final class Replica {
  private record Pending(long term, Consumer<Outcome> callback) {}

  private final RaftNode node;
  private final Storage storage;
  private final StateMachine machine;
  private final Consumer<Message> transport;
  private final long snapshotEvery;
  private final TreeMap<Long, Pending> pending = new TreeMap<>();
  private final Map<Long, Consumer<ReadOutcome>> reads = new HashMap<>();
  private final TreeMap<Long, List<Consumer<ReadOutcome>>> readsAwaitingApply = new TreeMap<>();
  private long nextReadContext;
  private long appliedIndex;
  private Ready.Status status;

  public Replica(
      String id,
      Configuration initial,
      RaftOptions opts,
      Storage storage,
      StateMachine machine,
      Random random,
      Consumer<Message> transport,
      long snapshotEvery) {
    if (snapshotEvery < 1) {
      throw new IllegalArgumentException("snapshotEvery must be positive");
    }
    this.snapshotEvery = snapshotEvery;
    this.storage = storage;
    this.machine = machine;
    this.transport = transport;
    this.node = new RaftNode(id, initial, opts, storage, random);
    this.status = node.status();
    this.appliedIndex = node.log().snapshot().index();
  }

  public RaftNode node() {
    return node;
  }

  public long appliedIndex() {
    return appliedIndex;
  }

  public Ready.Status status() {
    return status;
  }

  public void tick() {
    node.tick();
  }

  public void receive(Message m) {
    node.step(m);
  }

  /**
   * Proposes a command. The callback runs on this replica's thread once the outcome is known, from
   * inside {@link #process()} or immediately if this node is not the leader.
   */
  public void propose(byte[] command, Consumer<Outcome> callback) {
    submit(() -> node.propose(command), callback);
  }

  /** Adds a server as a learner; the leader promotes it to voter once it has caught up. */
  public void addServer(Member member, Consumer<Outcome> callback) {
    submit(() -> node.addServer(member), callback);
  }

  public void removeServer(String id, Consumer<Outcome> callback) {
    submit(() -> node.removeServer(id), callback);
  }

  private void submit(LongSupplier append, Consumer<Outcome> callback) {
    long index;
    try {
      index = append.getAsLong();
    } catch (NotLeaderException e) {
      callback.accept(new Outcome.NotLeader(e.leaderHint()));
      return;
    } catch (ConfigChangeException e) {
      callback.accept(new Outcome.Rejected(e.getMessage()));
      return;
    }
    pending.put(index, new Pending(node.term(), callback));
  }

  /**
   * Asks to serve a read. The callback runs on this replica's thread once the read is safe (or has
   * failed), so it can read the state machine directly.
   */
  public void read(ReadMode mode, Consumer<ReadOutcome> callback) {
    long context = ++nextReadContext;
    try {
      node.read(context, mode);
    } catch (NotLeaderException e) {
      callback.accept(new ReadOutcome.NotLeader(e.leaderHint()));
      return;
    }
    reads.put(context, callback);
  }

  /**
   * Runs one round: persist, send, apply, answer. Call after every batch of ticks and steps.
   *
   * <p>A leader's AppendEntries and InstallSnapshot messages go out before the fsync: they promise
   * nothing about the leader's own disk, so the leader writes in parallel with its followers
   * (thesis section 10.2.1) and counts itself toward a commit only once its fsync returns. Every
   * other message (votes, acknowledgements) waits for the fsync, because it is a promise about
   * durable state.
   */
  public void process() {
    for (int round = 0; round < 4; round++) {
      Ready r = node.ready();
      List<Message> afterSync = new ArrayList<>();
      for (Message m : r.messages()) {
        if (m instanceof Message.AppendRequest || m instanceof Message.SnapshotRequest) {
          transport.accept(m);
        } else {
          afterSync.add(m);
        }
      }
      storage.sync();
      node.markDurable();
      afterSync.forEach(transport);
      apply(r);
      if (!node.hasReady()) {
        return;
      }
    }
  }

  private void apply(Ready r) {
    r.snapshot().ifPresent(this::restore);
    for (Entry e : r.committed()) {
      apply(e);
    }
    for (Ready.ReadState rs : r.reads()) {
      Consumer<ReadOutcome> cb = reads.remove(rs.context());
      if (cb != null) {
        readsAwaitingApply.computeIfAbsent(rs.index(), k -> new ArrayList<>()).add(cb);
      }
    }
    while (!readsAwaitingApply.isEmpty() && readsAwaitingApply.firstKey() <= appliedIndex) {
      for (Consumer<ReadOutcome> cb : readsAwaitingApply.pollFirstEntry().getValue()) {
        cb.accept(new ReadOutcome.Served(appliedIndex));
      }
    }
    if (appliedIndex - node.log().snapshot().index() >= snapshotEvery) {
      node.compact(appliedIndex, machine.snapshot());
    }
    Ready.Status before = status;
    status = r.status();
    if (status.role() != RaftNode.Role.LEADER || status.term() != before.term()) {
      failPending("leadership changed before the entry was applied");
      failReads();
    }
  }

  private void failReads() {
    List<Consumer<ReadOutcome>> all = new ArrayList<>(reads.values());
    reads.clear();
    for (Consumer<ReadOutcome> cb : all) {
      cb.accept(new ReadOutcome.Failed("leadership changed before the read was confirmed"));
    }
  }

  private void restore(Snapshot s) {
    machine.restore(s.data());
    appliedIndex = s.meta().index();
    // Proposals the snapshot covers were either applied by others or overwritten; we cannot
    // tell which from here.
    while (!pending.isEmpty() && pending.firstKey() <= appliedIndex) {
      pending
          .pollFirstEntry()
          .getValue()
          .callback()
          .accept(new Outcome.Unknown("covered by a snapshot"));
    }
  }

  private void apply(Entry e) {
    byte[] result =
        e.type() == EntryType.COMMAND ? machine.apply(e.index(), e.data()) : new byte[0];
    appliedIndex = e.index();
    // Anything proposed at a lower index that is still pending was overwritten by another leader.
    while (!pending.isEmpty() && pending.firstKey() < e.index()) {
      pending.pollFirstEntry().getValue().callback().accept(new Outcome.Unknown("overwritten"));
    }
    Pending p = pending.remove(e.index());
    if (p == null) {
      return;
    }
    if (p.term() == e.term()) {
      p.callback().accept(new Outcome.Applied(e.index(), result));
    } else {
      p.callback().accept(new Outcome.Unknown("overwritten by another leader"));
    }
  }

  private void failPending(String reason) {
    if (pending.isEmpty()) {
      return;
    }
    List<Pending> all = new ArrayList<>(pending.values());
    pending.clear();
    for (Pending p : all) {
      p.callback().accept(new Outcome.Unknown(reason));
    }
  }
}
