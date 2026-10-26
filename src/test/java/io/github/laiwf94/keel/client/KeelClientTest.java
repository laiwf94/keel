package io.github.laiwf94.keel.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.server.ClusterAccess;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KeelClientTest {
  @TempDir Path tmp;

  static byte[] b(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  static KeelClient client(ClusterAccess c, String first, Duration retryFor) {
    List<URI> eps =
        c.ids().stream()
            .sorted((x, y) -> x.equals(first) ? -1 : y.equals(first) ? 1 : x.compareTo(y))
            .map(id -> URI.create(c.url(id)))
            .toList();
    return new KeelClient(
        new KeelClient.Options(eps, Optional.of("tok"), Duration.ofSeconds(1), retryFor));
  }

  @Test
  void findsTheLeaderAndKeepsWorkingWhenItDies() throws Exception {
    try (ClusterAccess c = new ClusterAccess(tmp, 3).tokens("tok", null).startAll()) {
      String leader = c.leaderId();
      String follower = c.ids().stream().filter(i -> !i.equals(leader)).findFirst().get();
      try (KeelClient k = client(c, follower, Duration.ofSeconds(15))) {
        k.openSession();
        long v1 = k.put("k", b("one"));
        assertTrue(k.current().toString().equals(c.url(leader)), "client follows the redirect");
        assertEquals("one", k.get("k", "linearizable").orElseThrow().text());
        assertEquals(v1, k.get("k", "lease").orElseThrow().version());
        assertTrue(k.status().contains("\"leader\""));

        c.stop(leader);
        long v2 = k.put("k", b("two"));
        assertTrue(v2 > v1);
        assertEquals("two", k.get("k", "linearizable").orElseThrow().text());
        assertFalse(k.current().toString().equals(c.url(leader)));

        KeelException.Conflict conflict =
            assertThrows(KeelException.Conflict.class, () -> k.putIf("k", b("x"), v1));
        assertEquals(v2, conflict.currentVersion());
        assertEquals(412, conflict.status());
        long v3 = k.putIf("k", b("three"), v2);
        assertTrue(k.putIf("fresh", b("f"), 0) > v3);
        assertEquals(v3, k.delete("k"));
        assertTrue(k.get("k", "linearizable").isEmpty());
        assertTrue(k.get("k", "stale").isEmpty());
        assertEquals("three", new String(k.get("fresh", "stale").map(v -> "three").orElse("")));
      }
    }
  }

  @Test
  void definiteErrorsAndGivingUp() throws Exception {
    try (ClusterAccess c = new ClusterAccess(tmp, 1).tokens("tok", "adm").startAll()) {
      c.leaderId();
      try (KeelClient k = client(c, "n1", Duration.ofSeconds(5))) {
        KeelException e =
            assertThrows(KeelException.class, () -> k.get("bad\u0001key", "linearizable"));
        assertEquals(400, e.status());
        assertFalse(e.outcomeUnknown());
        assertThrows(KeelException.class, () -> k.addMember("n9", "h:1", "h:2"));
      }
      KeelClient admin =
          new KeelClient(
              new KeelClient.Options(
                  List.of(URI.create(c.url("n1"))),
                  Optional.of("adm"),
                  Duration.ofSeconds(1),
                  Duration.ofSeconds(5)));
      try (admin) {
        assertTrue(admin.addMember("n9", "127.0.0.1:1", "127.0.0.1:2").contains("n9"));
        assertFalse(admin.removeMember("n9").contains("\"n9\""));
      }
      c.stop("n1");
      try (KeelClient k = client(c, "n1", Duration.ofMillis(300))) {
        KeelException e = assertThrows(KeelException.class, () -> k.put("k", b("v")));
        assertTrue(e.getMessage().startsWith("gave up"));
        assertTrue(e.outcomeUnknown());
        assertEquals(0, e.status());
      }
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> new KeelClient.Options(List.of(), Optional.empty(), Duration.ZERO, Duration.ZERO));
    assertEquals("a%20b%2Fc", KeelClient.encode("a b/c"));
  }
}
