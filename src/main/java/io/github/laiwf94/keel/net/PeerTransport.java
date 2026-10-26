package io.github.laiwf94.keel.net;

import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.codec.CodecException;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.Message;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Sends Raft messages to peers over TCP and receives theirs.
 *
 * <p>Each node dials one connection to each peer and only writes on it; it only reads on the
 * connections peers dial to it. Every connection starts with the {@link Handshake}, and every frame
 * after that carries an HMAC over a per-connection sequence number and the payload, so a frame
 * cannot be forged, altered, replayed or reordered within the connection.
 *
 * <p>Sending never blocks the caller: messages go into a bounded queue per peer and are dropped
 * when it is full or the peer is unreachable. Raft already tolerates lost messages, and a slow peer
 * must not stall the leader's event loop. Each peer's queue is drained by its own virtual thread,
 * which reconnects with exponential backoff.
 */
public final class PeerTransport implements AutoCloseable {
  /** Limits; see the field names. */
  public record Options(
      int queueCapacity,
      int maxInboundConnections,
      Duration handshakeTimeout,
      Duration connectTimeout,
      Duration idleTimeout,
      int maxFrameBytes) {
    public static final Options DEFAULTS =
        new Options(
            8192,
            64,
            Duration.ofSeconds(5),
            Duration.ofSeconds(2),
            Duration.ofSeconds(30),
            300 << 20);
  }

  private static final int TAG_BYTES = 32;

  private final String selfId;
  private final String clusterId;
  private final byte[] secret;
  private final Options opts;
  private final Function<String, Optional<InetSocketAddress>> resolver;
  private final Consumer<Message> inbound;
  private final ServerSocket server;
  private final Semaphore inboundSlots;
  private final Map<String, Peer> peers = new ConcurrentHashMap<>();
  private final List<Socket> inboundSockets = new ArrayList<>();
  private volatile boolean closed;
  private Thread acceptor;

  final AtomicLong sent = new AtomicLong();
  final AtomicLong dropped = new AtomicLong();
  final AtomicLong received = new AtomicLong();
  final AtomicLong rejected = new AtomicLong();

  /**
   * Binds the peer port. Call {@link #start()} to begin accepting connections.
   *
   * @param resolver maps a peer id to its address, from the current configuration
   * @param inbound receives every authenticated message addressed to this node; it is called from
   *     transport threads and must be thread-safe
   */
  public PeerTransport(
      String selfId,
      String clusterId,
      byte[] secret,
      InetSocketAddress bind,
      Function<String, Optional<InetSocketAddress>> resolver,
      Consumer<Message> inbound,
      Options opts)
      throws IOException {
    if (secret.length < 16) {
      throw new IllegalArgumentException("the cluster secret must be at least 16 bytes");
    }
    this.selfId = Member.validateId(selfId);
    this.clusterId = clusterId;
    this.secret = secret.clone();
    this.resolver = resolver;
    this.inbound = inbound;
    this.opts = opts;
    this.inboundSlots = new Semaphore(opts.maxInboundConnections());
    this.server = new ServerSocket();
    server.setReuseAddress(true);
    server.bind(bind);
  }

  public void start() {
    acceptor = Thread.ofVirtual().name("keel-peer-accept").start(this::acceptLoop);
  }

  public int port() {
    return server.getLocalPort();
  }

  public long sentCount() {
    return sent.get();
  }

  public long droppedCount() {
    return dropped.get();
  }

  public long receivedCount() {
    return received.get();
  }

  /** Connections or frames refused: failed handshakes, bad tags, malformed or misaddressed. */
  public long rejectedCount() {
    return rejected.get();
  }

  /** Queues a message for its peer. Never blocks; drops the message if the queue is full. */
  public void send(Message m) {
    if (closed) {
      return;
    }
    Peer p = peers.computeIfAbsent(m.to(), Peer::new);
    if (!p.queue.offer(m)) {
      dropped.incrementAndGet();
    }
  }

  private void acceptLoop() {
    while (!closed) {
      Socket s;
      try {
        s = server.accept();
      } catch (IOException e) {
        if (closed) {
          return;
        }
        continue;
      }
      if (!inboundSlots.tryAcquire()) {
        rejected.incrementAndGet();
        closeQuietly(s);
        continue;
      }
      synchronized (inboundSockets) {
        inboundSockets.add(s);
      }
      Thread.ofVirtual().name("keel-peer-in").start(() -> serveInbound(s));
    }
  }

  private void serveInbound(Socket s) {
    try (s) {
      s.setSoTimeout((int) opts.handshakeTimeout().toMillis());
      s.setTcpNoDelay(true);
      DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream(), 65536));
      DataOutputStream out = new DataOutputStream(s.getOutputStream());
      Handshake.Session session = Handshake.accept(in, out, clusterId, selfId, secret);
      s.setSoTimeout((int) opts.idleTimeout().toMillis());
      FrameReader frames = new FrameReader(in, session.key(), opts.maxFrameBytes());
      while (!closed) {
        Message m = MessageCodec.decode(frames.next());
        // The peer may only speak for itself, and only to us.
        if (!m.from().equals(session.peerId()) || !m.to().equals(selfId)) {
          throw new IOException("misaddressed message from " + session.peerId());
        }
        received.incrementAndGet();
        inbound.accept(m);
      }
    } catch (IOException | CodecException e) {
      if (!closed && !(e instanceof SocketException && "Socket closed".equals(e.getMessage()))) {
        rejected.incrementAndGet();
      }
    } finally {
      synchronized (inboundSockets) {
        inboundSockets.remove(s);
      }
      inboundSlots.release();
    }
  }

  /** Reads authenticated frames: [length][payload][HMAC(key, sequence | payload)]. */
  static final class FrameReader {
    private final DataInputStream in;
    private final Mac mac;
    private final int maxFrame;
    private long seq;

    FrameReader(DataInputStream in, byte[] key, int maxFrame) {
      this.in = in;
      this.mac = newMac(key);
      this.maxFrame = maxFrame;
    }

    byte[] next() throws IOException {
      int len = in.readInt();
      if (len < 1 || len > maxFrame) {
        throw new IOException("frame of " + len + " bytes is outside [1, " + maxFrame + "]");
      }
      byte[] payload = new byte[len];
      in.readFully(payload);
      byte[] tag = new byte[TAG_BYTES];
      in.readFully(tag);
      if (!MessageDigest.isEqual(tag, tag(mac, seq++, payload))) {
        throw new IOException("frame failed authentication");
      }
      return payload;
    }
  }

  /** Writes frames in the format {@link FrameReader} reads. */
  static final class FrameWriter {
    private final DataOutputStream out;
    private final Mac mac;
    private long seq;

    FrameWriter(DataOutputStream out, byte[] key) {
      this.out = out;
      this.mac = newMac(key);
    }

    void write(byte[] payload) throws IOException {
      out.writeInt(payload.length);
      out.write(payload);
      out.write(tag(mac, seq++, payload));
    }

    void flush() throws IOException {
      out.flush();
    }
  }

  private static Mac newMac(byte[] key) {
    try {
      Mac m = Mac.getInstance("HmacSHA256");
      m.init(new SecretKeySpec(key, "HmacSHA256"));
      return m;
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("HmacSHA256 is unavailable", e);
    }
  }

  private static byte[] tag(Mac mac, long seq, byte[] payload) {
    mac.update(new ByteWriter(8).writeLong(seq).toByteArray());
    return mac.doFinal(payload);
  }

  /** The outbound side of one peer: a queue and the virtual thread that drains it. */
  private final class Peer {
    final String id;
    final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(opts.queueCapacity());
    final Thread thread;
    Socket socket;
    FrameWriter writer;
    long backoffMs;

    Peer(String id) {
      this.id = id;
      this.thread = Thread.ofVirtual().name("keel-peer-out-" + id).start(this::run);
    }

    void run() {
      List<Message> batch = new ArrayList<>();
      while (!closed) {
        try {
          batch.add(queue.take());
        } catch (InterruptedException e) {
          return;
        }
        queue.drainTo(batch, 1024);
        try {
          FrameWriter w = connection();
          for (Message m : batch) {
            w.write(MessageCodec.encode(m));
          }
          w.flush();
          sent.addAndGet(batch.size());
          backoffMs = 0;
        } catch (IOException | RuntimeException e) {
          dropped.addAndGet(batch.size());
          disconnect();
          if (!backoff()) {
            return;
          }
        }
        batch.clear();
      }
    }

    private FrameWriter connection() throws IOException {
      if (writer != null) {
        return writer;
      }
      InetSocketAddress addr =
          resolver.apply(id).orElseThrow(() -> new IOException("no address for " + id));
      Socket s = new Socket();
      try {
        s.setTcpNoDelay(true);
        s.connect(addr, (int) opts.connectTimeout().toMillis());
        s.setSoTimeout((int) opts.handshakeTimeout().toMillis());
        DataInputStream in = new DataInputStream(s.getInputStream());
        DataOutputStream out =
            new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 65536));
        Handshake.Session session = Handshake.dial(in, out, clusterId, selfId, id, secret);
        socket = s;
        writer = new FrameWriter(out, session.key());
        return writer;
      } catch (IOException | RuntimeException e) {
        closeQuietly(s);
        throw e;
      }
    }

    private void disconnect() {
      writer = null;
      if (socket != null) {
        closeQuietly(socket);
        socket = null;
      }
    }

    /** Sleeps 50 ms, doubling up to 1 s, and drops what queued meanwhile. False if closing. */
    private boolean backoff() {
      backoffMs = backoffMs == 0 ? 50 : Math.min(1_000, backoffMs * 2);
      try {
        Thread.sleep(backoffMs);
      } catch (InterruptedException e) {
        return false;
      }
      List<Message> stale = new ArrayList<>();
      queue.drainTo(stale);
      dropped.addAndGet(stale.size());
      return !closed;
    }
  }

  private static void closeQuietly(Socket s) {
    try {
      s.close();
    } catch (IOException ignored) {
      // Nothing useful to do; the socket is being discarded.
    }
  }

  @Override
  public void close() {
    closed = true;
    try {
      server.close();
    } catch (IOException ignored) {
      // Closing anyway.
    }
    if (acceptor != null) {
      acceptor.interrupt();
    }
    for (Peer p : peers.values()) {
      p.thread.interrupt();
      p.disconnect();
    }
    synchronized (inboundSockets) {
      inboundSockets.forEach(PeerTransport::closeQuietly);
    }
  }
}
