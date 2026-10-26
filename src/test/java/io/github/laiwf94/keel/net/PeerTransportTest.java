package io.github.laiwf94.keel.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.raft.Message;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PeerTransportTest {
  static final byte[] SECRET = "correct horse battery staple".getBytes(StandardCharsets.UTF_8);
  static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
  static final InetSocketAddress ANY = new InetSocketAddress(LOOPBACK, 0);

  final Map<String, InetSocketAddress> addresses = new ConcurrentHashMap<>();
  final List<PeerTransport> open = new CopyOnWriteArrayList<>();

  PeerTransport transport(String id, String cluster, byte[] secret, List<Message> inbox)
      throws IOException {
    return transport(id, cluster, secret, inbox, PeerTransport.Options.DEFAULTS);
  }

  PeerTransport transport(
      String id, String cluster, byte[] secret, List<Message> inbox, PeerTransport.Options o)
      throws IOException {
    PeerTransport t =
        new PeerTransport(
            id, cluster, secret, ANY, p -> Optional.ofNullable(addresses.get(p)), inbox::add, o);
    t.start();
    addresses.put(id, new InetSocketAddress(LOOPBACK, t.port()));
    open.add(t);
    return t;
  }

  @AfterEach
  void closeAll() {
    open.forEach(PeerTransport::close);
  }

  static void await(BooleanSupplier cond, String what) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for " + what);
      }
      Thread.sleep(5);
    }
  }

  static Message vote(String from, String to, long term) {
    return new Message.VoteRequest(from, to, term, 0, 0, false);
  }

  @Test
  void peersExchangeMessagesInBothDirections() throws Exception {
    List<Message> aIn = new CopyOnWriteArrayList<>();
    List<Message> bIn = new CopyOnWriteArrayList<>();
    PeerTransport a = transport("a", "c1", SECRET, aIn);
    PeerTransport b = transport("b", "c1", SECRET, bIn);
    for (int i = 1; i <= 500; i++) {
      a.send(vote("a", "b", i));
    }
    b.send(vote("b", "a", 1));
    await(() -> bIn.size() == 500 && aIn.size() == 1, "delivery");
    for (int i = 0; i < 500; i++) {
      assertEquals(i + 1, bIn.get(i).term(), "order within a connection is kept");
    }
    assertEquals(501, a.sentCount() + b.sentCount());
    assertEquals(500, b.receivedCount());
  }

  @Test
  void wrongSecretOrClusterIsRefused() throws Exception {
    List<Message> bIn = new CopyOnWriteArrayList<>();
    PeerTransport b = transport("b", "c1", SECRET, bIn);
    PeerTransport wrongSecret =
        transport(
            "a", "c1", "another sixteen byte secret".getBytes(StandardCharsets.UTF_8), List.of());
    wrongSecret.send(vote("a", "b", 1));
    await(() -> b.rejectedCount() >= 1, "rejection");
    addresses.remove("a");
    PeerTransport wrongCluster = transport("x", "c2", SECRET, List.of());
    wrongCluster.send(vote("x", "b", 1));
    await(() -> b.rejectedCount() >= 2, "rejection");
    Thread.sleep(100);
    assertTrue(bIn.isEmpty());
    await(() -> wrongSecret.droppedCount() >= 1, "drop on the sender");
  }

  @Test
  void dialerChecksItReachedTheRightNode() throws Exception {
    List<Message> bIn = new CopyOnWriteArrayList<>();
    transport("b", "c1", SECRET, bIn);
    PeerTransport a = transport("a", "c1", SECRET, List.of());
    // c's address points at b: b refuses a connection meant for someone else.
    addresses.put("c", addresses.get("b"));
    a.send(vote("a", "c", 1));
    await(() -> a.droppedCount() >= 1, "drop");
    assertTrue(bIn.isEmpty());
  }

  @Test
  void spoofedSenderIsRefused() throws Exception {
    List<Message> bIn = new CopyOnWriteArrayList<>();
    PeerTransport b = transport("b", "c1", SECRET, bIn);
    try (Socket s = new Socket(LOOPBACK, b.port())) {
      DataInputStream in = new DataInputStream(s.getInputStream());
      DataOutputStream out = new DataOutputStream(s.getOutputStream());
      Handshake.Session session = Handshake.dial(in, out, "c1", "a", "b", SECRET);
      PeerTransport.FrameWriter w = new PeerTransport.FrameWriter(out, session.key());
      w.write(MessageCodec.encode(vote("a", "b", 1)));
      w.write(MessageCodec.encode(vote("z", "b", 2)));
      w.flush();
      await(() -> b.rejectedCount() >= 1, "rejection");
    }
    assertEquals(1, bIn.size());
  }

  @Test
  void tamperedReplayedAndOversizedFramesCloseTheConnection() throws Exception {
    List<Message> bIn = new CopyOnWriteArrayList<>();
    PeerTransport b = transport("b", "c1", SECRET, bIn);
    byte[] payload = MessageCodec.encode(vote("a", "b", 1));
    // A frame whose tag was computed for another sequence number (a replay) is refused.
    try (Socket s = new Socket(LOOPBACK, b.port())) {
      DataOutputStream out = new DataOutputStream(s.getOutputStream());
      Handshake.Session session =
          Handshake.dial(new DataInputStream(s.getInputStream()), out, "c1", "a", "b", SECRET);
      java.io.ByteArrayOutputStream first = new java.io.ByteArrayOutputStream();
      PeerTransport.FrameWriter w =
          new PeerTransport.FrameWriter(new DataOutputStream(first), session.key());
      w.write(payload);
      w.flush();
      out.write(first.toByteArray());
      out.write(first.toByteArray());
      out.flush();
      await(() -> b.rejectedCount() >= 1, "replay rejection");
    }
    assertEquals(1, bIn.size());
    try (Socket s = new Socket(LOOPBACK, b.port())) {
      DataOutputStream out = new DataOutputStream(s.getOutputStream());
      Handshake.dial(new DataInputStream(s.getInputStream()), out, "c1", "a", "b", SECRET);
      out.writeInt(Integer.MAX_VALUE);
      out.flush();
      await(() -> b.rejectedCount() >= 2, "oversize rejection");
    }
    try (Socket s = new Socket(LOOPBACK, b.port())) {
      DataOutputStream out = new DataOutputStream(s.getOutputStream());
      Handshake.Session session =
          Handshake.dial(new DataInputStream(s.getInputStream()), out, "c1", "a", "b", SECRET);
      out.writeInt(payload.length);
      out.write(payload);
      out.write(new byte[32]);
      out.flush();
      await(() -> b.rejectedCount() >= 3, "bad tag rejection");
      assertTrue(session.peerId().equals("b"));
    }
    // The transport still works for honest peers afterwards.
    PeerTransport a = transport("a", "c1", SECRET, List.of());
    a.send(vote("a", "b", 9));
    await(() -> bIn.size() == 2, "delivery after attacks");
  }

  @Test
  void garbageOnThePeerPortIsRefused() throws Exception {
    List<Message> bIn = new CopyOnWriteArrayList<>();
    PeerTransport b = transport("b", "c1", SECRET, bIn);
    for (byte[] junk :
        List.of(
            "GET / HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.US_ASCII),
            new byte[] {0, 0, 0, 5, 1, 2, 3, 4, 5},
            new byte[] {0x7f, 0, 0, 0},
            new byte[] {0, 0, 0, 30, 0x4b, 0x45, 0x45, 0x4c, 9})) {
      try (Socket s = new Socket(LOOPBACK, b.port())) {
        OutputStream out = s.getOutputStream();
        out.write(junk);
        out.flush();
        s.shutdownOutput();
        s.getInputStream().readAllBytes();
      }
    }
    await(() -> b.rejectedCount() >= 4, "rejections");
    assertTrue(bIn.isEmpty());
  }

  @Test
  void reconnectsAfterThePeerRestarts() throws Exception {
    List<Message> bIn = new CopyOnWriteArrayList<>();
    PeerTransport a = transport("a", "c1", SECRET, List.of());
    PeerTransport b = transport("b", "c1", SECRET, bIn);
    a.send(vote("a", "b", 1));
    await(() -> bIn.size() == 1, "first delivery");
    b.close();
    List<Message> bIn2 = new CopyOnWriteArrayList<>();
    transport("b", "c1", SECRET, bIn2);
    await(
        () -> {
          a.send(vote("a", "b", 2));
          return !bIn2.isEmpty();
        },
        "delivery after restart");
  }

  @Test
  void fullQueuesAndUnknownPeersDrop() throws Exception {
    PeerTransport.Options tiny =
        new PeerTransport.Options(
            1, 1, Duration.ofSeconds(1), Duration.ofMillis(200), Duration.ofSeconds(5), 1024);
    PeerTransport a = transport("a", "c1", SECRET, List.of(), tiny);
    for (int i = 0; i < 100; i++) {
      a.send(vote("a", "nowhere", i));
    }
    await(() -> a.droppedCount() >= 99, "drops");
    a.close();
    a.send(vote("a", "b", 1));
  }

  @Test
  void inboundConnectionsAreCapped() throws Exception {
    PeerTransport.Options one =
        new PeerTransport.Options(
            16, 1, Duration.ofSeconds(1), Duration.ofMillis(200), Duration.ofSeconds(5), 1024);
    PeerTransport b = transport("b", "c1", SECRET, List.of(), one);
    try (Socket first = new Socket(LOOPBACK, b.port());
        Socket second = new Socket(LOOPBACK, b.port())) {
      await(() -> b.rejectedCount() >= 1, "second connection refused");
      assertTrue(first.isConnected() && second.isConnected());
    }
  }

  @Test
  void shortSecretsAreRefused() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PeerTransport(
                "a",
                "c",
                new byte[8],
                ANY,
                p -> Optional.empty(),
                m -> {},
                PeerTransport.Options.DEFAULTS));
  }
}
