package io.github.laiwf94.keel.sim;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.client.KeelClient;
import io.github.laiwf94.keel.client.KeelException;
import io.github.laiwf94.keel.server.ClusterAccess;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Concurrent clients against real servers on real sockets, with the leader stopped and restarted
 * partway through. The recorded history goes through the same checker as the simulator's.
 */
class RealClusterLinearizabilityTest {
  @TempDir Path tmp;

  @Test
  void concurrentClientsSeeALinearizableStoreAcrossALeaderChange() throws Exception {
    History history = new History();
    AtomicBoolean stop = new AtomicBoolean();
    List<String> keys = List.of("a", "b", "c");
    try (ClusterAccess c = new ClusterAccess(tmp, 3).startAll()) {
      c.leaderId();
      List<URI> eps = c.ids().stream().map(id -> URI.create(c.url(id))).toList();
      List<Thread> threads = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        int id = i;
        threads.add(
            Thread.ofVirtual()
                .start(
                    () -> {
                      try (KeelClient k =
                          new KeelClient(
                              new KeelClient.Options(
                                  eps,
                                  Optional.empty(),
                                  Duration.ofSeconds(1),
                                  Duration.ofSeconds(3)))) {
                        run(id, k, history, keys, stop);
                      }
                    }));
      }
      Thread.sleep(1_500);
      String leader = c.leaderId();
      c.stop(leader);
      Thread.sleep(1_500);
      c.start(leader);
      Thread.sleep(1_000);
      stop.set(true);
      for (Thread t : threads) {
        t.join();
      }
    }
    Optional<String> violation;
    synchronized (history) {
      assertTrue(history.completed() > 100, "only " + history.completed() + " operations");
      violation = Linearizability.check(history.ops);
    }
    assertTrue(violation.isEmpty(), () -> violation.get());
  }

  private static void run(
      int client, KeelClient k, History h, List<String> keys, AtomicBoolean stop) {
    Random r = new Random(client);
    Map<String, Long> lastSeen = new HashMap<>();
    try {
      k.openSession();
    } catch (KeelException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
    for (int n = 0; !stop.get(); n++) {
      String key = keys.get(r.nextInt(keys.size()));
      String value = "c" + client + "-" + n;
      int pick = r.nextInt(10);
      History.Op op;
      try {
        if (pick < 3) {
          synchronized (h) {
            op = h.call(client, History.Kind.PUT, key, value, -1);
          }
          long v = k.put(key, value.getBytes(StandardCharsets.UTF_8));
          synchronized (h) {
            h.write(op, true, v);
          }
          lastSeen.put(key, v);
        } else if (pick < 5) {
          long expected = lastSeen.getOrDefault(key, 0L);
          synchronized (h) {
            op = h.call(client, History.Kind.PUT, key, value, expected);
          }
          try {
            long v = k.putIf(key, value.getBytes(StandardCharsets.UTF_8), expected);
            synchronized (h) {
              h.write(op, true, v);
            }
            lastSeen.put(key, v);
          } catch (KeelException.Conflict e) {
            synchronized (h) {
              h.write(op, false, e.currentVersion());
            }
            lastSeen.put(key, e.currentVersion());
          }
        } else if (pick < 6) {
          synchronized (h) {
            op = h.call(client, History.Kind.DELETE, key, null, -1);
          }
          long v = k.delete(key);
          synchronized (h) {
            h.write(op, true, v);
          }
          lastSeen.put(key, 0L);
        } else {
          synchronized (h) {
            op = h.call(client, History.Kind.GET, key, null, -1);
          }
          var v = k.get(key, pick < 8 ? "linearizable" : "lease");
          synchronized (h) {
            h.read(
                op,
                v.map(KeelClient.Versioned::text).orElse(null),
                v.map(KeelClient.Versioned::version).orElse(0L));
          }
        }
      } catch (KeelException e) {
        // No definite answer: the operation stays open in the history.
        if (e.status() == 410) {
          try {
            k.openSession();
          } catch (KeelException | InterruptedException ex) {
            return;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }
}
