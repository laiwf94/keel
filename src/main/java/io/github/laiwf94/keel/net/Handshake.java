package io.github.laiwf94.keel.net;

import io.github.laiwf94.keel.codec.ByteReader;
import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.raft.Member;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Mutual challenge-response authentication between peers that share a cluster secret, followed by a
 * per-connection key for authenticating every frame.
 *
 * <pre>
 * dialer   -> listener : magic, version, cluster id, dialer id, listener id it expects, nonce_d
 * listener -> dialer   : listener id, nonce_l, HMAC(secret, "keel-listener" | ids | nonces)
 * dialer   -> listener : HMAC(secret, "keel-dialer" | ids | nonces)
 * key = HMAC(secret, "keel-session" | ids | nonces)
 * </pre>
 *
 * Each side proves it knows the secret over the other side's fresh nonce, so a recorded handshake
 * cannot be replayed, and the distinct labels stop one side's proof from being reflected back as
 * the other's. Traffic is authenticated, not encrypted.
 */
final class Handshake {
  static final int MAGIC = 0x4b45454c; // "KEEL"
  static final int VERSION = 1;
  static final int NONCE_BYTES = 32;
  static final int MAX_HANDSHAKE_BYTES = 512;

  /** The result of a successful handshake: who is on the other end and the frame key. */
  static final class Session {
    private final String peerId;
    private final byte[] key;

    Session(String peerId, byte[] key) {
      this.peerId = peerId;
      this.key = key;
    }

    String peerId() {
      return peerId;
    }

    byte[] key() {
      return key;
    }
  }

  private static final SecureRandom RANDOM = new SecureRandom();

  private Handshake() {}

  static Session dial(
      DataInputStream in,
      DataOutputStream out,
      String clusterId,
      String selfId,
      String expectedPeer,
      byte[] secret)
      throws IOException {
    byte[] nonceD = nonce();
    writeFrame(
        out,
        new ByteWriter()
            .writeInt(MAGIC)
            .writeByte(VERSION)
            .writeString(clusterId)
            .writeString(selfId)
            .writeString(expectedPeer)
            .writeRaw(nonceD)
            .toByteArray());
    ByteReader r = new ByteReader(readFrame(in));
    String peer = r.readString(64);
    byte[] nonceL = readRaw(r, NONCE_BYTES);
    byte[] proof = readRaw(r, 32);
    r.expectEnd();
    if (!peer.equals(expectedPeer)) {
      throw new AuthenticationException("expected " + expectedPeer + " but reached " + peer);
    }
    byte[] transcript = transcript(clusterId, selfId, peer, nonceD, nonceL);
    if (!MessageDigest.isEqual(proof, hmac(secret, "keel-listener", transcript))) {
      throw new AuthenticationException("listener " + peer + " does not know the cluster secret");
    }
    writeFrame(out, hmac(secret, "keel-dialer", transcript));
    return new Session(peer, hmac(secret, "keel-session", transcript));
  }

  static Session accept(
      DataInputStream in, DataOutputStream out, String clusterId, String selfId, byte[] secret)
      throws IOException {
    ByteReader r = new ByteReader(readFrame(in));
    if (r.readInt() != MAGIC) {
      throw new AuthenticationException("not a keel peer");
    }
    int version = r.readByte();
    if (version != VERSION) {
      throw new AuthenticationException("unsupported protocol version " + version);
    }
    String cluster = r.readString(64);
    String dialer = r.readString(64);
    String target = r.readString(64);
    byte[] nonceD = readRaw(r, NONCE_BYTES);
    r.expectEnd();
    if (!cluster.equals(clusterId)) {
      throw new AuthenticationException("peer belongs to cluster " + cluster);
    }
    if (!target.equals(selfId)) {
      throw new AuthenticationException("peer expected " + target + ", this is " + selfId);
    }
    try {
      Member.validateId(dialer);
    } catch (IllegalArgumentException e) {
      throw new AuthenticationException("bad peer id");
    }
    byte[] nonceL = nonce();
    byte[] transcript = transcript(clusterId, dialer, selfId, nonceD, nonceL);
    writeFrame(
        out,
        new ByteWriter()
            .writeString(selfId)
            .writeRaw(nonceL)
            .writeRaw(hmac(secret, "keel-listener", transcript))
            .toByteArray());
    byte[] proof = readFrame(in);
    if (!MessageDigest.isEqual(proof, hmac(secret, "keel-dialer", transcript))) {
      throw new AuthenticationException("peer " + dialer + " does not know the cluster secret");
    }
    return new Session(dialer, hmac(secret, "keel-session", transcript));
  }

  private static byte[] transcript(
      String cluster, String dialer, String listener, byte[] nonceD, byte[] nonceL) {
    return new ByteWriter()
        .writeString(cluster)
        .writeString(dialer)
        .writeString(listener)
        .writeRaw(nonceD)
        .writeRaw(nonceL)
        .toByteArray();
  }

  private static byte[] nonce() {
    byte[] b = new byte[NONCE_BYTES];
    RANDOM.nextBytes(b);
    return b;
  }

  static byte[] hmac(byte[] key, String label, byte[] data) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      mac.update(label.getBytes(StandardCharsets.US_ASCII));
      return mac.doFinal(data);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("HmacSHA256 is unavailable", e);
    }
  }

  private static byte[] readRaw(ByteReader r, int n) {
    byte[] b = new byte[n];
    for (int i = 0; i < n; i++) {
      b[i] = (byte) r.readByte();
    }
    return b;
  }

  private static void writeFrame(DataOutputStream out, byte[] payload) throws IOException {
    out.writeInt(payload.length);
    out.write(payload);
    out.flush();
  }

  private static byte[] readFrame(DataInputStream in) throws IOException {
    int len = in.readInt();
    if (len < 0 || len > MAX_HANDSHAKE_BYTES) {
      throw new AuthenticationException("handshake frame of " + len + " bytes");
    }
    byte[] b = new byte[len];
    in.readFully(b);
    return b;
  }

  /** The other side failed authentication or spoke an unexpected protocol. */
  static final class AuthenticationException extends IOException {
    private static final long serialVersionUID = 1L;

    AuthenticationException(String message) {
      super(message);
    }
  }
}
