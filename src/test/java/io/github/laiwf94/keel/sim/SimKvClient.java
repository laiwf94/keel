package io.github.laiwf94.keel.sim;

import io.github.laiwf94.keel.kv.Command;
import io.github.laiwf94.keel.kv.KvMachine;
import io.github.laiwf94.keel.kv.Result;
import io.github.laiwf94.keel.raft.ReadMode;
import io.github.laiwf94.keel.replica.Outcome;
import io.github.laiwf94.keel.replica.ReadOutcome;
import io.github.laiwf94.keel.replica.StateMachine;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simulated client. It keeps one session, issues one operation at a time, retries a write under
 * the same sequence number until it gets an answer, and gives up after a deadline, leaving the
 * operation open in the history.
 */
final class SimKvClient {
  static final int ATTEMPT_TIMEOUT_MS = 150;
  static final int OP_TIMEOUT_MS = 1_500;

  private final int id;
  private final Simulation sim;
  private final History history;
  private final List<String> keys;
  private final long stopAt;
  private final Map<String, Long> lastSeen = new HashMap<>();
  private final List<String> nodeIds;
  private final ReadMode readMode;
  private String target;
  private long clientId;
  private long seq;
  private long attempt;
  int retries;

  SimKvClient(
      int id, Simulation sim, History history, List<String> keys, long stopAt, ReadMode readMode) {
    this.id = id;
    this.readMode = readMode;
    this.sim = sim;
    this.history = history;
    this.keys = keys;
    this.stopAt = stopAt;
    this.nodeIds = new ArrayList<>(sim.nodes.keySet());
    this.target = nodeIds.get(sim.rnd.nextInt(nodeIds.size()));
  }

  static KvMachine kv(StateMachine m) {
    return m instanceof KvSimulationTest.ForgetfulSessions f ? f.inner : (KvMachine) m;
  }

  void start() {
    sim.schedule(sim.rnd.nextInt(50), this::register);
  }

  private void register() {
    if (sim.now() >= stopAt) {
      return;
    }
    long gen = ++attempt;
    sim.propose(
        target,
        Command.register(sim.now()).encode(),
        o -> sim.schedule(0, () -> onRegister(gen, o)));
    sim.schedule(
        ATTEMPT_TIMEOUT_MS,
        () -> {
          if (gen == attempt) {
            redirect(null);
            register();
          }
        });
  }

  private void onRegister(long gen, Outcome o) {
    if (gen != attempt) {
      return;
    }
    attempt++;
    if (o instanceof Outcome.Applied a) {
      clientId = Result.decode(a.result()).version();
      seq = 0;
      next();
      return;
    }
    redirect(o);
    sim.schedule(5, this::register);
  }

  private void redirect(Outcome o) {
    if (o instanceof Outcome.NotLeader nl && nl.leaderHint().isPresent()) {
      target = nl.leaderHint().get();
    } else {
      target = nodeIds.get(sim.rnd.nextInt(nodeIds.size()));
    }
  }

  private void next() {
    if (sim.now() >= stopAt) {
      return;
    }
    String key = keys.get(sim.rnd.nextInt(keys.size()));
    int pick = sim.rnd.nextInt(13);
    if (pick >= 10) {
      read(history.call(id, History.Kind.GET, key, null, -1), sim.now() + OP_TIMEOUT_MS);
      return;
    }
    History.Op op;
    Command cmd;
    seq++;
    if (pick < 6) {
      String value = "c" + id + "-" + seq;
      op = history.call(id, History.Kind.PUT, key, value, -1);
      cmd = Command.put(sim.now(), clientId, seq, key, Simulation.bytes(value), -1);
    } else if (pick < 8) {
      String value = "c" + id + "-" + seq;
      long expected = lastSeen.getOrDefault(key, Command.ABSENT);
      op = history.call(id, History.Kind.PUT, key, value, expected);
      cmd = Command.put(sim.now(), clientId, seq, key, Simulation.bytes(value), expected);
    } else {
      long expected = pick == 8 ? -1 : lastSeen.getOrDefault(key, Command.ABSENT);
      op = history.call(id, History.Kind.DELETE, key, null, expected);
      cmd = Command.delete(sim.now(), clientId, seq, key, expected);
    }
    send(op, cmd.encode(), sim.now() + OP_TIMEOUT_MS);
  }

  private void send(History.Op op, byte[] cmd, long deadline) {
    if (sim.now() >= deadline) {
      next(); // give up; the operation stays open in the history
      return;
    }
    long gen = ++attempt;
    sim.propose(target, cmd, o -> sim.schedule(0, () -> onReply(gen, op, cmd, deadline, o)));
    sim.schedule(
        ATTEMPT_TIMEOUT_MS,
        () -> {
          if (gen == attempt) {
            retries++;
            redirect(null);
            send(op, cmd, deadline);
          }
        });
  }

  private void read(History.Op op, long deadline) {
    if (sim.now() >= deadline) {
      next();
      return;
    }
    long gen = ++attempt;
    sim.read(
        target,
        readMode,
        (node, o) -> {
          if (gen != attempt) {
            return;
          }
          attempt++;
          if (o instanceof ReadOutcome.Served) {
            // Read the state machine now, on the replica's turn, as the server does.
            var v = kv(node.machine).get(op.key);
            history.read(
                op,
                v.map(x -> new String(x.bytes(), StandardCharsets.UTF_8)).orElse(null),
                v.map(KvMachine.Value::version).orElse(0L));
            sim.schedule(sim.rnd.nextInt(10), this::next);
            return;
          }
          retries++;
          redirect(
              o instanceof ReadOutcome.NotLeader nl
                  ? new Outcome.NotLeader(nl.leaderHint())
                  : null);
          sim.schedule(5 + sim.rnd.nextInt(10), () -> read(op, deadline));
        });
    sim.schedule(
        ATTEMPT_TIMEOUT_MS,
        () -> {
          if (gen == attempt) {
            retries++;
            redirect(null);
            read(op, deadline);
          }
        });
  }

  private void onReply(long gen, History.Op op, byte[] cmd, long deadline, Outcome o) {
    if (gen != attempt) {
      return;
    }
    attempt++;
    if (o instanceof Outcome.Applied a) {
      Result r = Result.decode(a.result());
      switch (r.status()) {
        case OK, CONFLICT -> {
          boolean ok = r.status() == Result.Status.OK;
          history.write(op, ok, r.version());
          long seen = ok && op.kind == History.Kind.DELETE ? 0 : r.version();
          lastSeen.put(op.key, seen);
          sim.schedule(sim.rnd.nextInt(10), this::next);
        }
        default -> sim.schedule(0, this::register); // session gone: the write stays open
      }
      return;
    }
    retries++;
    redirect(o);
    sim.schedule(5 + sim.rnd.nextInt(10), () -> send(op, cmd, deadline));
  }
}
