package io.github.laiwf94.keel.server;

import io.github.laiwf94.keel.client.KeelClient;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** keel nodes as separate JVMs on loopback ports, each logging to a file in {@code root}. */
final class ProcessCluster implements AutoCloseable {
  static final String ADMIN_TOKEN = "process cluster admin token";
  private static final Pattern ROLE = Pattern.compile("\"role\":\"([a-z_]+)\"");
  private static final Pattern TERM = Pattern.compile("\"term\":(\\d+)");

  final Path root;
  final Map<String, int[]> ports = new LinkedHashMap<>();
  final Map<String, Process> procs = new LinkedHashMap<>();
  final HttpClient http = HttpClient.newHttpClient();
  private final String members;

  ProcessCluster(Path root, int n) throws IOException {
    this.root = root;
    List<String> ms = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      int[] p = {LocalCluster.freePort(), LocalCluster.freePort()};
      ports.put("n" + i, p);
      ms.add("n" + i + "@127.0.0.1:" + p[0] + "@127.0.0.1:" + p[1]);
    }
    this.members = String.join(",", ms);
  }

  ProcessCluster startAll() throws IOException {
    for (String id : ports.keySet()) {
      launch(id);
    }
    return this;
  }

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
            root.resolve(id).toString(),
            "--secret",
            "process crash test secret",
            "--admin-token",
            ADMIN_TOKEN,
            "--tick-ms",
            "10",
            "--snapshot-every",
            "200");
    pb.redirectErrorStream(true);
    pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile(id).toFile()));
    Process proc = pb.start();
    procs.put(id, proc);
    return proc;
  }

  Path logFile(String id) {
    return root.resolve(id + ".log");
  }

  String log(String id) throws IOException {
    return Files.readString(logFile(id), StandardCharsets.UTF_8);
  }

  URI url(String id) {
    return URI.create("http://127.0.0.1:" + ports.get(id)[1]);
  }

  List<URI> endpoints() {
    return ports.keySet().stream().map(this::url).toList();
  }

  KeelClient client() {
    return new KeelClient(
        new KeelClient.Options(
            endpoints(), Optional.empty(), Duration.ofSeconds(1), Duration.ofSeconds(30)));
  }

  /** A node's role and term from its status endpoint; empty if it does not answer. */
  record Status(String role, long term) {}

  Optional<Status> status(String id) throws InterruptedException {
    if (!procs.containsKey(id) || !procs.get(id).isAlive()) {
      return Optional.empty();
    }
    try {
      String body =
          http.send(
                  HttpRequest.newBuilder(url(id).resolve("/v1/status"))
                      .timeout(Duration.ofMillis(500))
                      .build(),
                  HttpResponse.BodyHandlers.ofString())
              .body();
      Matcher r = ROLE.matcher(body);
      Matcher t = TERM.matcher(body);
      if (!r.find() || !t.find()) {
        return Optional.empty();
      }
      return Optional.of(new Status(r.group(1), Long.parseLong(t.group(1))));
    } catch (IOException e) {
      return Optional.empty();
    }
  }

  /** Waits for a live node, other than those in {@code not}, that reports itself leader. */
  String leader(String... not) throws InterruptedException {
    List<String> skip = List.of(not);
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (System.nanoTime() < deadline) {
      for (String id : procs.keySet()) {
        if (skip.contains(id)) {
          continue;
        }
        Optional<Status> s = status(id);
        if (s.isPresent() && s.get().role().equals("leader")) {
          return id;
        }
      }
      Thread.sleep(20);
    }
    throw new AssertionError("no leader");
  }

  /** POST /v1/leader on {@code node}, with {@code to} if it is not null. */
  HttpResponse<String> transfer(String node, String to) throws IOException, InterruptedException {
    String q = to == null ? "" : "?to=" + to;
    return http.send(
        HttpRequest.newBuilder(url(node).resolve("/v1/leader" + q))
            .header("Authorization", "Bearer " + ADMIN_TOKEN)
            .timeout(Duration.ofSeconds(10))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  @Override
  public void close() {
    procs.values().forEach(Process::destroyForcibly);
    for (Process p : procs.values()) {
      try {
        p.waitFor();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    http.close();
  }
}
