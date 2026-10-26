package io.github.laiwf94.keel.server;

import io.github.laiwf94.keel.obs.Log;
import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.RaftNode;
import io.github.laiwf94.keel.raft.RaftOptions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Real keel servers in this JVM, on loopback ports, with data in a temporary directory. */
final class LocalCluster implements AutoCloseable {
  static final String SECRET = "a test secret of enough length";
  static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

  final Path root;
  Configuration config;
  private final boolean proxied;
  final Map<String, KeelServer> servers = new LinkedHashMap<>();
  // Per node: peer port, HTTP port, and the proxy port peers dial when proxies are on.
  final Map<String, int[]> ports = new LinkedHashMap<>();
  final Map<String, PartitionProxy> proxies = new LinkedHashMap<>();
  final java.util.Set<String> isolated = java.util.concurrent.ConcurrentHashMap.newKeySet();
  final ByteArrayOutputStream logs = new ByteArrayOutputStream();
  Optional<String> clientToken = Optional.empty();
  Optional<String> adminToken = Optional.empty();
  Duration requestTimeout = Duration.ofSeconds(3);

  LocalCluster(Path root, int n) throws IOException {
    this(root, n, false);
  }

  /** With {@code proxied}, peers reach each node through a {@link PartitionProxy}. */
  LocalCluster(Path root, int n, boolean proxied) throws IOException {
    this.root = root;
    this.proxied = proxied;
    assignPorts(n);
  }

  /**
   * Picks free ports for every node. A port found free can be taken by another process before a
   * server binds it, so {@link #startAll()} picks again and retries when a bind fails.
   */
  private void assignPorts(int n) throws IOException {
    proxies.values().forEach(PartitionProxy::close);
    proxies.clear();
    ports.clear();
    List<Member> ms = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      String id = "n" + i;
      int[] p = {freePort(), freePort(), freePort()};
      ports.put(id, p);
      int advertised = proxied ? p[2] : p[0];
      if (proxied) {
        proxies.put(id, new PartitionProxy(id, p[2], p[0], isolated));
      }
      ms.add(new Member(id, "127.0.0.1:" + advertised, "127.0.0.1:" + p[1], true));
    }
    this.config = new Configuration(ms);
  }

  /** Cuts every peer connection to and from {@code id}. */
  void isolate(String id) {
    isolated.add(id);
    proxies.values().forEach(PartitionProxy::enforce);
  }

  void heal() {
    isolated.clear();
  }

  static int freePort() throws IOException {
    try (ServerSocket s = new ServerSocket(0, 1, LOOPBACK)) {
      return s.getLocalPort();
    }
  }

  KeelServer.Config configFor(String id, Configuration initial) {
    int[] p = ports.get(id);
    return new KeelServer.Config(
        id,
        root.resolve(id),
        new InetSocketAddress(LOOPBACK, p[0]),
        new InetSocketAddress(LOOPBACK, p[1]),
        initial,
        "test",
        SECRET,
        clientToken,
        adminToken,
        RaftOptions.DEFAULTS,
        new NodeLoop.Settings(10, 10_000, 1024),
        50,
        requestTimeout,
        60_000,
        1_000,
        Log.Level.DEBUG);
  }

  KeelServer start(String id) throws IOException {
    KeelServer s =
        KeelServer.start(
            configFor(id, config), new PrintStream(logs, true, StandardCharsets.UTF_8), () -> {});
    servers.put(id, s);
    return s;
  }

  LocalCluster startAll() throws IOException {
    for (int attempt = 1; ; attempt++) {
      try {
        for (String id : ports.keySet()) {
          start(id);
        }
        return this;
      } catch (java.net.BindException e) {
        if (attempt == 3) {
          throw e;
        }
        for (String id : new ArrayList<>(servers.keySet())) {
          stop(id);
        }
        assignPorts(ports.size());
      }
    }
  }

  void stop(String id) {
    KeelServer s = servers.remove(id);
    if (s != null) {
      s.close();
    }
  }

  String url(String id) {
    return "http://127.0.0.1:" + ports.get(id)[1];
  }

  KeelServer awaitLeader() throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    while (System.nanoTime() < deadline) {
      for (KeelServer s : servers.values()) {
        if (s.loop().status().role() == RaftNode.Role.LEADER) {
          return s;
        }
      }
      Thread.sleep(10);
    }
    throw new AssertionError("no leader elected");
  }

  String leaderId() throws InterruptedException {
    return awaitLeader().loop().status().id();
  }

  @Override
  public void close() {
    for (String id : new ArrayList<>(servers.keySet())) {
      stop(id);
    }
    proxies.values().forEach(PartitionProxy::close);
  }
}
