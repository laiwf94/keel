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
    verifyAll(() -> client(c), acked);
  }

  static void verifyAll(java.util.function.Supplier<KeelClient> clients, Map<String, String> acked)
      throws Exception {
    try (KeelClient k = clients.get()) {
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

  static long field(String json, String name) {
    java.util.regex.Matcher m =
        java.util.regex.Pattern.compile("\"" + name + "\":(\\d+)").matcher(json);
    assertTrue(m.find(), name + " missing from " + json);
    return Long.parseLong(m.group(1));
  }

  @Test
  void leadershipMovesOverHttpUnderLoadWithoutLosingAWrite() throws Exception {
    try (ProcessCluster c = new ProcessCluster(tmp, 3).startAll()) {
      c.leader();
      Writers w = new Writers(c::client, 4);
      Thread.sleep(800);
      // Read the leader after the warm-up: with 10 ms ticks a pause on a busy machine can cost the
      // first leader its term while the JVMs warm up under load (#78).
      String old = c.leader();
      long term = c.status(old).orElseThrow().term();
      String target = c.ports.keySet().stream().filter(id -> !id.equals(old)).findFirst().get();
      String third =
          c.ports.keySet().stream()
              .filter(id -> !id.equals(old) && !id.equals(target))
              .findFirst()
              .get();
      long start = System.nanoTime();
      HttpResponse<String> r = c.transfer(old, target);
      long ms = (System.nanoTime() - start) / 1_000_000;
      assertEquals(200, r.statusCode(), r.body());
      assertTrue(r.body().contains("\"leader\":\"" + target + "\""), r.body());
      assertTrue(r.body().contains("\"previous\":\"" + old + "\""), r.body());
      // One election, started by TimeoutNow: the term moves by exactly one. With 10 ms ticks the
      // election timeout is 100 to 200 ms; the handover is bounded by catch-up and one round trip.
      assertEquals(term + 1, field(r.body(), "term"), r.body());
      assertTrue(ms < 2_000, "transfer took " + ms + " ms");
      assertEquals("leader", c.status(target).orElseThrow().role());
      // A node that does not lead points the caller at the one that does.
      HttpResponse<String> redirected = c.transfer(third, old);
      assertEquals(307, redirected.statusCode(), redirected.body());
      assertEquals(
          c.url(target) + "/v1/leader?to=" + old,
          redirected.headers().firstValue("Location").orElseThrow());
      // Without a target, the most caught-up voter takes over.
      HttpResponse<String> any = c.transfer(target, null);
      assertEquals(200, any.statusCode(), any.body());
      assertEquals(term + 2, field(any.body(), "term"), any.body());
      assertTrue(!any.body().contains("\"leader\":\"" + target + "\""), any.body());
      int before = w.acked.size();
      Thread.sleep(800);
      w.close();
      assertTrue(w.acked.size() > before, "no progress after the transfers");
      verifyAll(c::client, w.acked);
    }
  }

  @Test
  void sigtermOnTheLeaderHandsOffBeforeExit() throws Exception {
    try (ProcessCluster c = new ProcessCluster(tmp, 3).startAll()) {
      c.leader();
      Writers w = new Writers(c::client, 4);
      Thread.sleep(800);
      // The leader as of now, not as of start-up (#78).
      String old = c.leader();
      long term = c.status(old).orElseThrow().term();
      Process p = c.procs.get(old);
      p.destroy(); // SIGTERM
      assertTrue(p.waitFor(10, java.util.concurrent.TimeUnit.SECONDS), "the leader did not exit");
      String log = c.log(old);
      int handoff = log.indexOf("\"event\":\"handoff\"");
      int stopping = log.indexOf("\"event\":\"stopping\"");
      assertTrue(handoff >= 0, "no handoff before exit:\n" + log);
      assertTrue(handoff < stopping, "the ports closed before the handoff:\n" + log);
      String next = c.leader(old);
      assertEquals(term + 1, c.status(next).orElseThrow().term());
      assertTrue(log.contains("\"leader\":\"" + next + "\""), log);
      int before = w.acked.size();
      Thread.sleep(800);
      c.launch(old);
      Thread.sleep(500);
      w.close();
      assertTrue(w.acked.size() > before, "no progress after the handoff");
      verifyAll(c::client, w.acked);
    }
  }

  @Test
  void aTransferToAnUnreachableVoterFailsAndTheLeaderKeepsLeading() throws Exception {
    try (LocalCluster c = new LocalCluster(tmp, 3, true)) {
      c.adminToken = Optional.of("admin");
      c.startAll();
      String leader = c.leaderId();
      long term = c.servers.get(leader).loop().status().term();
      String target = c.ports.keySet().stream().filter(id -> !id.equals(leader)).findFirst().get();
      c.isolate(target);
      HttpClient http = HttpClient.newHttpClient();
      HttpResponse<String> r =
          http.send(
              HttpRequest.newBuilder(URI.create(c.url(leader) + "/v1/leader?to=" + target))
                  .header("Authorization", "Bearer admin")
                  .POST(HttpRequest.BodyPublishers.noBody())
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertEquals(503, r.statusCode(), r.body());
      assertTrue(r.body().contains("still leads"), r.body());
      var s = c.servers.get(leader).loop().status();
      assertEquals(RaftNode.Role.LEADER, s.role());
      assertEquals(term, s.term());
      try (KeelClient k = client(c)) {
        k.put("after", "ok".getBytes(StandardCharsets.UTF_8));
      }
      http.close();
    }
  }

  @Test
  void gracefulShutdownHandsOffInProcessAndALoneVoterJustStops() throws Exception {
    try (LocalCluster c = new LocalCluster(tmp.resolve("three"), 3).startAll()) {
      String old = c.leaderId();
      long term = c.servers.get(old).loop().status().term();
      KeelServer s = c.servers.get(old);
      var o = s.handOff().orElseThrow();
      assertTrue(o instanceof TransferOutcome.Transferred, o.toString());
      String next = ((TransferOutcome.Transferred) o).leader();
      assertNotEquals(old, next);
      assertEquals(term + 1, ((TransferOutcome.Transferred) o).term());
      assertTrue(s.handOff().isEmpty(), "a follower has nothing to hand off");
      c.shutdown(old);
      c.shutdown(next); // hands off again, to the last node standing
      assertEquals(1, c.servers.size());
    }
    try (LocalCluster c = new LocalCluster(tmp.resolve("one"), 1).startAll()) {
      KeelServer s = c.awaitLeader();
      var o = s.handOff().orElseThrow();
      assertTrue(o instanceof TransferOutcome.Rejected, o.toString());
      c.shutdown("n1");
      assertTrue(c.logs.toString(StandardCharsets.UTF_8).contains("handoff_failed"));
    }
  }
}
