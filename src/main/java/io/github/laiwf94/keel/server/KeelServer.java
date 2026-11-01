package io.github.laiwf94.keel.server;

import io.github.laiwf94.keel.kv.KvMachine;
import io.github.laiwf94.keel.net.PeerTransport;
import io.github.laiwf94.keel.obs.Log;
import io.github.laiwf94.keel.obs.Metrics;
import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.RaftNode;
import io.github.laiwf94.keel.raft.RaftOptions;
import io.github.laiwf94.keel.storage.FileStorage;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/** One keel node: storage, peer transport, event loop and HTTP API wired together. */
public final class KeelServer implements AutoCloseable {
  /**
   * Everything a node needs to start.
   *
   * @param initial the cluster's first configuration, identical on every founding node; {@link
   *     Configuration#EMPTY} for a node that will be added to a running cluster
   * @param secret shared by all nodes; peers prove they know it before any message is accepted
   */
  public record Config(
      String id,
      Path dataDir,
      InetSocketAddress peerBind,
      InetSocketAddress httpBind,
      Configuration initial,
      String clusterId,
      String secret,
      Optional<String> clientToken,
      Optional<String> adminToken,
      RaftOptions raft,
      NodeLoop.Settings loop,
      long snapshotEvery,
      Duration requestTimeout,
      long sessionTimeoutMs,
      int maxSessions,
      Log.Level logLevel) {}

  private final FileStorage storage;
  private final PeerTransport transport;
  private final NodeLoop loop;
  private final HttpApi http;
  private final Log log;
  private final Duration handoffTimeout;
  private boolean closed;

  private KeelServer(
      FileStorage storage,
      PeerTransport transport,
      NodeLoop loop,
      HttpApi http,
      Log log,
      Duration handoffTimeout) {
    this.storage = storage;
    this.transport = transport;
    this.loop = loop;
    this.http = http;
    this.log = log;
    this.handoffTimeout = handoffTimeout;
  }

  /**
   * How long a leader that is shutting down waits for another voter to take over: the transfer
   * itself gives up after one election timeout, and the new leader announces itself with its first
   * heartbeat, so two of the longest election timeouts is ample.
   */
  static Duration handoffTimeout(RaftOptions raft, NodeLoop.Settings loop) {
    return Duration.ofMillis(4L * raft.electionTicks() * loop.tickMs());
  }

  /** Opens storage, binds both ports and starts serving. */
  public static KeelServer start(Config c, PrintStream logOut, Runnable onFatal)
      throws IOException {
    Log log = new Log(c.id(), logOut, Clock.systemUTC(), c.logLevel());
    Metrics metrics = new Metrics();
    FileStorage storage = new FileStorage(c.dataDir());
    AtomicReference<NodeLoop> loopRef = new AtomicReference<>();
    PeerTransport transport = null;
    NodeLoop loop = null;
    try {
      transport =
          new PeerTransport(
              c.id(),
              c.clusterId(),
              c.secret().getBytes(StandardCharsets.UTF_8),
              c.peerBind(),
              peer ->
                  loopRef.get().status().config().member(peer).map(m -> address(m.peerAddress())),
              m -> loopRef.get().receive(m),
              PeerTransport.Options.DEFAULTS);
      PeerTransport t = transport;
      metrics.counter("keel_peer_sent_total", "Messages sent to peers.", t::sentCount);
      metrics.counter(
          "keel_peer_dropped_total", "Messages dropped on the way out.", t::droppedCount);
      metrics.counter(
          "keel_peer_received_total", "Messages received from peers.", t::receivedCount);
      metrics.counter(
          "keel_peer_rejected_total", "Peer connections or frames refused.", t::rejectedCount);
      KvMachine kv = new KvMachine(c.sessionTimeoutMs(), c.maxSessions());
      loop =
          new NodeLoop(
              c.id(),
              c.initial(),
              c.raft(),
              storage,
              kv,
              t::send,
              c.snapshotEvery(),
              c.loop(),
              log,
              metrics,
              onFatal);
      loopRef.set(loop);
      HttpApi http =
          new HttpApi(
              c.httpBind(),
              loop,
              kv,
              metrics,
              log,
              new HttpApi.Options(
                  c.clientToken(),
                  c.adminToken(),
                  c.requestTimeout(),
                  handoffTimeout(c.raft(), c.loop()),
                  Clock.systemUTC()));
      if (storage.tornBytesDropped() > 0) {
        log.warn("torn_tail", "bytes_dropped", storage.tornBytesDropped());
      }
      transport.start();
      loop.start();
      http.start();
      log.info(
          "listening",
          "peer_port",
          transport.port(),
          "http_port",
          http.port(),
          "members",
          loop.status().config().ids());
      return new KeelServer(
          storage, transport, loop, http, log, handoffTimeout(c.raft(), c.loop()));
    } catch (IOException | RuntimeException e) {
      if (loop != null) {
        loop.close();
      }
      if (transport != null) {
        transport.close();
      }
      storage.close();
      throw e;
    }
  }

  /** Parses {@code host:port} or {@code [v6]:port}. */
  static InetSocketAddress address(String hostPort) {
    int colon = hostPort.lastIndexOf(':');
    if (colon <= 0 || colon == hostPort.length() - 1) {
      throw new IllegalArgumentException("expected host:port, got " + hostPort);
    }
    String host = hostPort.substring(0, colon);
    if (host.startsWith("[") && host.endsWith("]")) {
      host = host.substring(1, host.length() - 1);
    }
    int port;
    try {
      port = Integer.parseInt(hostPort.substring(colon + 1));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("bad port in " + hostPort, e);
    }
    return new InetSocketAddress(host, port);
  }

  public int httpPort() {
    return http.port();
  }

  public int peerPort() {
    return transport.port();
  }

  public NodeLoop loop() {
    return loop;
  }

  /**
   * Stops the node gracefully: a leader first hands leadership to the most caught-up voter, while
   * both ports stay open so the handoff can finish and clients are pointed at the new leader, then
   * everything closes. A leader that cannot hand off (it is the only voter, or no voter takes over
   * in time) closes anyway and the others elect a leader after an election timeout, as after a
   * crash.
   */
  @Override
  public synchronized void close() {
    if (!closed) {
      handOff();
    }
    closeWithoutHandoff();
  }

  /** Stops the node at once, leaving any leadership to be won in an election. */
  public synchronized void closeWithoutHandoff() {
    if (closed) {
      return;
    }
    closed = true;
    log.info("stopping");
    http.close();
    transport.close();
    loop.close();
    storage.close();
  }

  /** Hands leadership to another voter if this node leads; returns how that went. */
  public Optional<TransferOutcome> handOff() {
    if (loop.status().role() != RaftNode.Role.LEADER) {
      return Optional.empty();
    }
    long start = System.nanoTime();
    TransferOutcome o;
    try {
      o =
          loop.transferLeadership(Optional.empty(), handoffTimeout)
              .get(handoffTimeout.toMillis() + 1_000, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      o = new TransferOutcome.Failed("the event loop did not answer in time");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      o = new TransferOutcome.Failed("interrupted");
    } catch (ExecutionException e) {
      o = new TransferOutcome.Failed(e.getCause().toString());
    }
    long ms = (System.nanoTime() - start) / 1_000_000;
    if (o instanceof TransferOutcome.Transferred t) {
      log.info("handoff", "leader", t.leader(), "term", t.term(), "ms", ms);
    } else {
      log.warn("handoff_failed", "outcome", o, "ms", ms);
    }
    return Optional.of(o);
  }
}
