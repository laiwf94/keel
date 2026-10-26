package io.github.laiwf94.keel.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.client.KeelClient;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Runs three nodes as separate JVMs and kills one with SIGKILL in the middle of a write load. */
class ProcessCrashTest {
  @TempDir Path tmp;

  final Map<String, int[]> ports = new LinkedHashMap<>();
  final Map<String, Process> procs = new LinkedHashMap<>();
  String members;

  Process launch(String id) throws IOException {
    int[] p = ports.get(id);
    String java = ProcessHandle.current().info().command().orElse("java");
    ProcessBuilder pb =
        new ProcessBuilder(
            java,
            "-cp",
            System.getProperty("java.class.path"),
            "io.github.laiwf94.keel.Main",
            "server",
            "--id",
            id,
            "--members",
            members,
            "--peer-bind",
            "127.0.0.1:" + p[0],
            "--http-bind",
            "127.0.0.1:" + p[1],
            "--data",
            tmp.resolve(id).toString(),
            "--secret",
            "process crash test secret",
            "--tick-ms",
            "10",
            "--snapshot-every",
            "200");
    pb.redirectErrorStream(true);
    pb.redirectOutput(tmp.resolve(id + ".log").toFile());
    Process proc = pb.start();
    procs.put(id, proc);
    return proc;
  }

  String leader(HttpClient http) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (System.nanoTime() < deadline) {
      for (String id : procs.keySet()) {
        if (!procs.get(id).isAlive()) {
          continue;
        }
        try {
          String body =
              http.send(
                      HttpRequest.newBuilder(
                              URI.create("http://127.0.0.1:" + ports.get(id)[1] + "/v1/status"))
                          .timeout(Duration.ofMillis(500))
                          .build(),
                      HttpResponse.BodyHandlers.ofString())
                  .body();
          if (body.contains("\"role\":\"leader\"")) {
            return id;
          }
        } catch (IOException e) {
          // Not up yet.
        }
      }
      Thread.sleep(50);
    }
    throw new AssertionError("no leader");
  }

  @Test
  void sigkillMidLoadLosesNothingAndTheNodeRecoversFromItsFiles() throws Exception {
    List<String> ms = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      int[] p = {LocalCluster.freePort(), LocalCluster.freePort()};
      ports.put("n" + i, p);
      ms.add("n" + i + "@127.0.0.1:" + p[0] + "@127.0.0.1:" + p[1]);
    }
    members = String.join(",", ms);
    HttpClient http = HttpClient.newHttpClient();
    try {
      for (String id : ports.keySet()) {
        launch(id);
      }
      String victim = leader(http);
      List<URI> eps =
          ports.values().stream().map(p -> URI.create("http://127.0.0.1:" + p[1])).toList();
      FailureTest.Writers w = writers(eps);
      Thread.sleep(1_000);
      procs.get(victim).destroyForcibly().waitFor();
      Thread.sleep(1_000);
      launch(victim);
      Thread.sleep(1_000);
      w.close();
      assertTrue(w.acked.size() > 50, "only " + w.acked.size() + " writes acknowledged");
      try (KeelClient k =
          new KeelClient(
              new KeelClient.Options(
                  eps, Optional.empty(), Duration.ofSeconds(1), Duration.ofSeconds(30)))) {
        for (var e : w.acked.entrySet()) {
          var v = k.get(e.getKey(), "linearizable");
          assertTrue(v.isPresent(), "acknowledged write lost: " + e.getKey());
          assertEquals(e.getValue(), v.get().text());
        }
      }
      assertTrue(procs.get(victim).isAlive(), "the restarted node died");
    } finally {
      procs.values().forEach(Process::destroyForcibly);
      http.close();
    }
  }

  FailureTest.Writers writers(List<URI> eps) {
    return new FailureTest.Writers(
        () ->
            new KeelClient(
                new KeelClient.Options(
                    eps, Optional.empty(), Duration.ofSeconds(1), Duration.ofSeconds(30))),
        4);
  }
}
