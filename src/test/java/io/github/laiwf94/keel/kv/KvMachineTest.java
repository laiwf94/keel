package io.github.laiwf94.keel.kv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.codec.CodecException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class KvMachineTest {
  static byte[] b(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  static Result apply(KvMachine m, long index, Command c) {
    return Result.decode(m.apply(index, c.encode()));
  }

  static String value(KvMachine m, String key) {
    return m.get(key).map(v -> new String(v.bytes(), StandardCharsets.UTF_8)).orElse(null);
  }

  @Test
  void putGetDeleteWithVersions() {
    KvMachine m = new KvMachine(60_000, 100);
    assertEquals(
        new Result(Result.Status.OK, 3), apply(m, 3, Command.put(1, 0, 0, "k", b("v1"), -1)));
    assertEquals("v1", value(m, "k"));
    assertEquals(3, m.get("k").orElseThrow().version());
    assertEquals(
        new Result(Result.Status.CONFLICT, 3), apply(m, 4, Command.put(1, 0, 0, "k", b("x"), 2)));
    assertEquals(
        new Result(Result.Status.OK, 5), apply(m, 5, Command.put(1, 0, 0, "k", b("v2"), 3)));
    assertEquals(
        new Result(Result.Status.CONFLICT, 5),
        apply(m, 6, Command.put(1, 0, 0, "k", b("x"), Command.ABSENT)));
    assertEquals(
        new Result(Result.Status.OK, 7),
        apply(m, 7, Command.put(1, 0, 0, "new", b("n"), Command.ABSENT)));
    assertEquals(new Result(Result.Status.OK, 5), apply(m, 8, Command.delete(1, 0, 0, "k", 5)));
    assertEquals(null, value(m, "k"));
    assertEquals(new Result(Result.Status.OK, 0), apply(m, 9, Command.delete(1, 0, 0, "k", -1)));
    assertEquals(
        new Result(Result.Status.CONFLICT, 7), apply(m, 10, Command.delete(1, 0, 0, "new", 1)));
    assertEquals(1, m.size());
  }

  @Test
  void retriedRequestAppliesOnceAndReturnsTheFirstResult() {
    KvMachine m = new KvMachine(60_000, 100);
    long client = apply(m, 1, Command.register(10)).version();
    assertEquals(1, client);
    Command put = Command.put(11, client, 1, "k", b("a"), -1);
    assertEquals(new Result(Result.Status.OK, 2), apply(m, 2, put));
    // Someone else overwrites the key, then the client's retry of seq 1 arrives.
    apply(m, 3, Command.put(12, 0, 0, "k", b("other"), -1));
    assertEquals(new Result(Result.Status.OK, 2), apply(m, 4, put));
    assertEquals("other", value(m, "k"));
    assertEquals(
        new Result(Result.Status.OK, 5), apply(m, 5, Command.put(13, client, 2, "k", b("b"), -1)));
    assertEquals(new Result(Result.Status.STALE, 0), apply(m, 6, put));
    assertEquals("b", value(m, "k"));
  }

  @Test
  void sessionsExpireByLogTimeAndTheTableIsBounded() {
    KvMachine m = new KvMachine(1_000, 2);
    long a = apply(m, 1, Command.register(0)).version();
    long b = apply(m, 2, Command.register(500)).version();
    apply(m, 3, Command.put(900, a, 1, "k", b("x"), -1));
    // At t=1600 b has been idle for 1100 ms and expires; a was active at 900.
    assertEquals(
        new Result(Result.Status.SESSION_EXPIRED, 0),
        apply(m, 4, Command.put(1600, b, 1, "k", b("y"), -1)));
    assertEquals(1, m.sessionCount());
    // Clock never runs backwards, even if a new leader's clock is behind.
    assertEquals(
        new Result(Result.Status.OK, 5), apply(m, 5, Command.put(100, a, 2, "k", b("z"), -1)));
    long c = apply(m, 6, Command.register(1700)).version();
    long d = apply(m, 7, Command.register(1700)).version();
    assertEquals(2, m.sessionCount());
    assertEquals(
        new Result(Result.Status.SESSION_EXPIRED, 0),
        apply(m, 8, Command.put(1700, a, 3, "k", b("q"), -1)));
    assertEquals(Result.Status.OK, apply(m, 9, Command.put(1700, c, 1, "k", b("c"), -1)).status());
    assertEquals(Result.Status.OK, apply(m, 10, Command.put(1700, d, 1, "k", b("d"), -1)).status());
  }

  @Test
  void snapshotRoundTripPreservesDataSessionsAndEvictionOrder() {
    KvMachine m = new KvMachine(1_000, 2);
    long a = apply(m, 1, Command.register(0)).version();
    long b = apply(m, 2, Command.register(0)).version();
    apply(m, 3, Command.put(10, a, 1, "x", b("1"), -1));
    apply(m, 4, Command.put(10, 0, 0, "y", b("2"), -1));
    KvMachine copy = new KvMachine(1_000, 2);
    copy.restore(m.snapshot());
    assertArrayEquals(m.snapshot(), copy.snapshot());
    assertEquals("1", value(copy, "x"));
    // The retry is still deduplicated after restore.
    assertEquals(
        new Result(Result.Status.OK, 3), apply(copy, 5, Command.put(20, a, 1, "x", b("9"), -1)));
    // Registering a third session evicts b, the least recently used, in both copies.
    apply(m, 6, Command.register(30));
    apply(copy, 6, Command.register(30));
    assertEquals(
        Result.Status.SESSION_EXPIRED,
        apply(copy, 7, Command.put(30, b, 1, "z", b("z"), -1)).status());
    // A session that registered but never wrote also survives a snapshot.
    KvMachine fresh = new KvMachine(1_000, 2);
    apply(fresh, 1, Command.register(0));
    KvMachine fresh2 = new KvMachine(1_000, 2);
    fresh2.restore(fresh.snapshot());
    assertEquals(1, fresh2.sessionCount());
  }

  @Test
  void corruptCommandsAreRejectedDeterministically() {
    KvMachine m = new KvMachine(1_000, 2);
    assertEquals(
        new Result(Result.Status.INVALID, 0), Result.decode(m.apply(1, new byte[] {9, 9})));
    assertEquals(new Result(Result.Status.INVALID, 0), Result.decode(m.apply(1, new byte[0])));
    assertThrows(CodecException.class, () -> m.restore(new byte[] {1, 2, 3}));
    assertThrows(CodecException.class, () -> Result.decode(new byte[] {7, 0, 0, 0, 0, 0, 0, 0, 0}));
    byte[] badKey =
        new ByteWriter()
            .writeByte(2)
            .writeLong(0)
            .writeLong(0)
            .writeLong(0)
            .writeString("")
            .writeLong(-1)
            .writeBytes(new byte[0])
            .toByteArray();
    assertThrows(CodecException.class, () -> Command.decode(badKey));
  }

  @Test
  void commandValidation() {
    assertThrows(IllegalArgumentException.class, () -> Command.put(0, 0, 0, "", b("v"), -1));
    assertThrows(IllegalArgumentException.class, () -> Command.put(0, 0, 0, "a\nb", b("v"), -1));
    assertThrows(
        IllegalArgumentException.class, () -> Command.put(0, 0, 0, "k".repeat(1025), b(""), -1));
    assertThrows(
        IllegalArgumentException.class,
        () -> Command.put(0, 0, 0, "k", new byte[Command.MAX_VALUE_BYTES + 1], -1));
    assertThrows(IllegalArgumentException.class, () -> Command.put(-1, 0, 0, "k", b(""), -1));
    assertThrows(IllegalArgumentException.class, () -> Command.put(0, 0, 0, "k", b(""), -2));
    assertThrows(IllegalArgumentException.class, () -> Command.put(0, 5, 0, "k", b(""), -1));
    assertThrows(NullPointerException.class, () -> Command.validateKey(null));
    assertThrows(IllegalArgumentException.class, () -> new KvMachine(0, 1));
    assertThrows(IllegalArgumentException.class, () -> new KvMachine(1, 0));
    Command c = Command.delete(5, 1, 2, "ключ", 7);
    Command d = Command.decode(c.encode());
    assertEquals(Command.Op.DELETE, d.op());
    assertEquals(5, d.timestamp());
    assertEquals(1, d.clientId());
    assertEquals(2, d.seq());
    assertEquals("ключ", d.key());
    assertEquals(7, d.expectedVersion());
    assertEquals(0, d.value().length);
    assertTrue(Command.decode(Command.register(3).encode()).op() == Command.Op.REGISTER);
  }

  @Test
  void identicalLogsGiveIdenticalReplicas() {
    KvMachine x = new KvMachine(500, 3);
    KvMachine y = new KvMachine(500, 3);
    java.util.Random r = new java.util.Random(1);
    for (long i = 1; i <= 2_000; i++) {
      Command c =
          switch (r.nextInt(4)) {
            case 0 -> Command.register(i * 3);
            case 1 ->
                Command.put(
                    i * 3, r.nextInt(5), 1 + r.nextInt(3), "k" + r.nextInt(20), b("v" + i), -1);
            case 2 -> Command.delete(i * 3, 0, 0, "k" + r.nextInt(20), r.nextInt(3) - 1);
            default -> Command.put(i * 3, 0, 0, "k" + r.nextInt(20), b("w" + i), r.nextInt(50));
          };
      byte[] cmd = c.encode();
      assertArrayEquals(x.apply(i, cmd), y.apply(i, cmd));
    }
    assertArrayEquals(x.snapshot(), y.snapshot());
  }
}
