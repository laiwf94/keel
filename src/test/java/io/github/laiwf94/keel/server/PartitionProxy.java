package io.github.laiwf94.keel.server;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A TCP forwarder in front of one node's peer port. It reads the dialer's id from the first
 * handshake frame, so it knows both ends of every connection, and drops any connection that touches
 * an isolated node. Isolating a node this way cuts its traffic in both directions: a symmetric
 * network partition at the transport layer, with the nodes themselves untouched.
 */
final class PartitionProxy implements AutoCloseable {
  private final String target;
  private final int targetPort;
  private final Set<String> isolated;
  private final ServerSocket server;
  private final Map<Socket, String> sources = new ConcurrentHashMap<>();
  private volatile boolean closed;

  PartitionProxy(String target, int listenPort, int targetPort, Set<String> isolated)
      throws IOException {
    this.target = target;
    this.targetPort = targetPort;
    this.isolated = isolated;
    this.server = new ServerSocket();
    server.setReuseAddress(true);
    server.bind(new InetSocketAddress(LocalCluster.LOOPBACK, listenPort));
    Thread.ofVirtual().start(this::acceptLoop);
  }

  private void acceptLoop() {
    while (!closed) {
      try {
        Socket in = server.accept();
        Thread.ofVirtual().start(() -> connect(in));
      } catch (IOException e) {
        if (closed) {
          return;
        }
      }
    }
  }

  private void connect(Socket in) {
    try {
      // Handshake frame: length, magic, version, cluster id, dialer id, ...
      DataInputStream din = new DataInputStream(in.getInputStream());
      int len = din.readInt();
      if (len < 0 || len > 512) {
        throw new IOException("not a handshake");
      }
      byte[] first = new byte[len];
      din.readFully(first);
      DataInputStream p = new DataInputStream(new java.io.ByteArrayInputStream(first));
      p.readInt();
      p.readByte();
      p.skipBytes(p.readInt());
      byte[] dialer = new byte[p.readInt()];
      p.readFully(dialer);
      String source = new String(dialer, StandardCharsets.UTF_8);
      if (isolated.contains(source) || isolated.contains(target)) {
        in.close();
        return;
      }
      Socket out = new Socket(LocalCluster.LOOPBACK, targetPort);
      ByteArrayOutputStream replay = new ByteArrayOutputStream();
      new DataOutputStream(replay).writeInt(len);
      replay.write(first);
      out.getOutputStream().write(replay.toByteArray());
      sources.put(in, source);
      sources.put(out, source);
      Thread.ofVirtual().start(() -> pump(in, out));
      Thread.ofVirtual().start(() -> pump(out, in));
    } catch (IOException e) {
      closeQuietly(in);
    }
  }

  private void pump(Socket from, Socket to) {
    byte[] buf = new byte[65536];
    try (InputStream in = from.getInputStream();
        OutputStream out = to.getOutputStream()) {
      int n;
      while ((n = in.read(buf)) >= 0) {
        out.write(buf, 0, n);
        out.flush();
      }
    } catch (IOException e) {
      // One side closed; close the other.
    } finally {
      sources.remove(from);
      sources.remove(to);
      closeQuietly(from);
      closeQuietly(to);
    }
  }

  /** Closes open connections that the current isolation forbids. */
  void enforce() {
    for (var e : sources.entrySet()) {
      if (isolated.contains(e.getValue()) || isolated.contains(target)) {
        closeQuietly(e.getKey());
      }
    }
  }

  private static void closeQuietly(Socket s) {
    try {
      s.close();
    } catch (IOException ignored) {
      // Already closed.
    }
  }

  @Override
  public void close() {
    closed = true;
    sources.keySet().forEach(PartitionProxy::closeQuietly);
    try {
      server.close();
    } catch (IOException ignored) {
      // Closing anyway.
    }
  }
}
