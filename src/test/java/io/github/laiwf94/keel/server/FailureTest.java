package io.github.laiwf94.keel.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.client.KeelClient;
import io.github.laiwf94.keel.client.KeelException;
import io.github.laiwf94.keel.raft.RaftNode;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Failures against real servers: real sockets, real files, real elections. */
class FailureTest {
  @TempDir Path tmp;

  static void await(BooleanSupplier cond, String what) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for " + what);
      }
      Thread.sleep(10);
    }
  }

  static KeelClient client(LocalCluster c) {
    List<URI> eps = c.ports.keySet().stream().map(id -> URI.create(c.url(id))).toList();
    return new KeelClient(
        new KeelClient.Options(
            eps, Optional.empty(), Duration.ofSeconds(1), Duration.ofSeconds(20)));
  }

  /** Writers that record every acknowledged write, for checking durability afterwards. */
  static final class Writers {
    final Map<String, String> acked = new ConcurrentHashMap<>();
    final AtomicBoolean stop = new AtomicBoolean();
    final List<Thread> threads = new ArrayList<>();

    Writers(LocalCluster c, int n) {
      this(() -> client(c), n);
    }

    Writers(java.util.function.Supplier<KeelClient> clients, int n) {
      for (int i = 0; i < n; i++) {
        String prefix = "w" + i + "-";
        threads.add(
            Thread.ofVirtual()
                .start(
                    () -> {
                      try (KeelClient k = clients.get()) {
                        k.openSession();
                        for (int j = 0; !stop.get(); j++) {
                          String key = prefix + j;
                          String value = "v" + j;
                          try {
                            k.put(key, value.getBytes(StandardCharsets.UTF_8));
                            acked.put(key, value);
                          } catch (KeelException e) {
                            // Unknown or refused: not acknowledged, so not promised.
                          }
                        }
                      } catch (KeelException | InterruptedException e) {
                        throw new IllegalStateException(e);
                      }
                    }));
      }
    }

    void close() throws InterruptedException {
      stop.set(true);
      for (Thread t : threads) {
        t.join();
      }
    }
  }

  static void verifyAll(LocalCluster c, Map<String, String> acked) throws Exception {
    try (KeelClient k = client(c)) {
      for (var e : acked.entrySet()) {
        var v = k.get(e.getKey(), "linearizable");
        assertTrue(v.isPresent(), "acknowledged write lost: " + e.getKey());
        assertEquals(e.getValue(), v.get().text());
      }
    }
  }

  @Test
  void killingTheLeaderMidLoadLosesNoAcknowledgedWrite() throws Exception {
    try (LocalCluster c = new LocalCluster(tmp, 3).startAll()) {
      String old = c.leaderId();
      long oldTerm = c.servers.get(old).loop().status().term();
      Writers w = new Writers(c, 4);
      Thread.sleep(800);
      int before = w.acked.size();
      c.stop(old);
      KeelServer next = c.awaitLeader();
      assertTrue(next.loop().status().term() > oldTerm);
      Thread.sleep(800);
      w.close();
      assertTrue(w.acked.size() > before, "no progress after the leader died");
      verifyAll(c, w.acked);
      // The old leader restarts from its files and catches up.
      KeelServer back = c.start(old);
      long commit = next.loop().status().commitIndex();
      await(() -> back.loop().status().appliedIndex() >= commit, "catch-up");
      assertEquals(RaftNode.Role.FOLLOWER, back.loop().status().role());
    }
  }

  @Test
  void anIsolatedLeaderCannotCommitAndRejoinsAfterHealing() throws Exception {
    try (LocalCluster c = new LocalCluster(tmp, 3, true)) {
      c.requestTimeout = Duration.ofSeconds(2);
      c.startAll();
      String old = c.leaderId();
      HttpClient http = HttpClient.newHttpClient();
      c.isolate(old);
      HttpResponse<String> minority =
          http.send(
              HttpRequest.newBuilder(URI.create(c.url(old) + "/v1/kv/minority"))
                  .PUT(HttpRequest.BodyPublishers.ofString("lost"))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertTrue(
          minority.statusCode() == 503
              || minority.statusCode() == 504
              || minority.statusCode() == 307,
          minority.statusCode() + " " + minority.body());
      await(
          () ->
              c.servers.values().stream()
                  .anyMatch(
                      s ->
                          s.loop().status().role() == RaftNode.Role.LEADER
                              && !s.loop().status().id().equals(old)),
          "a new leader in the majority");
      assertNotEquals(RaftNode.Role.LEADER, c.servers.get(old).loop().status().role());
      try (KeelClient k = client(c)) {
        k.put("majority", "kept".getBytes(StandardCharsets.UTF_8));
        c.heal();
        await(
            () ->
                c.servers.get(old).loop().status().appliedIndex()
                    >= c.servers.values().stream()
                        .mapToLong(s -> s.loop().status().commitIndex())
                        .max()
                        .getAsLong(),
            "the old leader catching up");
        assertTrue(k.get("minority", "linearizable").isEmpty(), "a minority write committed");
        assertEquals("kept", k.get("majority", "linearizable").orElseThrow().text());
      }
      http.close();
    }
  }

  @Test
  void garbageOnBothPortsDoesNotHurtTheCluster() throws Exception {
    try (LocalCluster c = new LocalCluster(tmp, 3).startAll()) {
      String leader = c.leaderId();
      Random r = new Random(7);
      for (int i = 0; i < 20; i++) {
        byte[] junk = new byte[r.nextInt(2048) + 1];
        r.nextBytes(junk);
        for (int port : new int[] {c.ports.get(leader)[0], c.ports.get(leader)[1]}) {
          try (Socket s = new Socket(LocalCluster.LOOPBACK, port)) {
            OutputStream out = s.getOutputStream();
            out.write(junk);
            out.flush();
            s.shutdownOutput();
            s.getInputStream().readAllBytes();
          } catch (java.io.IOException e) {
            // The server may reset the connection; that is fine.
          }
        }
      }
      try (KeelClient k = client(c)) {
        k.put("still", "works".getBytes(StandardCharsets.UTF_8));
        assertEquals("works", k.get("still", "linearizable").orElseThrow().text());
      }
      HttpClient http = HttpClient.newHttpClient();
      String metrics =
          http.send(
                  HttpRequest.newBuilder(URI.create(c.url(leader) + "/metrics")).build(),
                  HttpResponse.BodyHandlers.ofString())
              .body();
      assertTrue(metrics.contains("keel_peer_rejected_total"), metrics);
      assertTrue(!metrics.contains("keel_peer_rejected_total 0\n"), "garbage was not rejected");
      http.close();
    }
  }

  @Test
  void writesWithoutAQuorumTimeOutWithAnUnknownOutcome() throws Exception {
    try (LocalCluster c = new LocalCluster(tmp, 3)) {
      c.requestTimeout = Duration.ofMillis(300);
      c.startAll();
      String leader = c.leaderId();
      for (String id : List.copyOf(c.servers.keySet())) {
        if (!id.equals(leader)) {
          c.stop(id);
        }
      }
      HttpClient http = HttpClient.newHttpClient();
      HttpResponse<String> r =
          http.send(
              HttpRequest.newBuilder(URI.create(c.url(leader) + "/v1/kv/k"))
                  .PUT(HttpRequest.BodyPublishers.ofString("v"))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertTrue(r.statusCode() == 504 || r.statusCode() == 503, r.statusCode() + " " + r.body());
      assertTrue(r.body().contains("unknown") || r.body().contains("no leader"), r.body());
      await(
          () -> c.servers.get(leader).loop().status().role() != RaftNode.Role.LEADER,
          "the leader to step down");
      HttpResponse<String> read =
          http.send(
              HttpRequest.newBuilder(URI.create(c.url(leader) + "/v1/kv/k")).build(),
              HttpResponse.BodyHandlers.ofString());
      assertEquals(503, read.statusCode(), read.body());
      http.close();
    }
  }
}
