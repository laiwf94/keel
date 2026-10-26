package io.github.laiwf94.keel.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HttpApiTest {
  @TempDir static Path tmp;
  LocalCluster cluster;
  String leader;
  String follower;
  final HttpClient http = HttpClient.newHttpClient();

  @BeforeAll
  void start() throws Exception {
    cluster = new LocalCluster(tmp, 3);
    cluster.clientToken = java.util.Optional.of("client-token");
    cluster.adminToken = java.util.Optional.of("admin-token");
    cluster.startAll();
    leader = cluster.leaderId();
    follower = cluster.ports.keySet().stream().filter(k -> !k.equals(leader)).findFirst().get();
  }

  @AfterAll
  void stop() {
    cluster.close();
    http.close();
  }

  HttpRequest.Builder req(String node, String path) {
    return HttpRequest.newBuilder(URI.create(cluster.url(node) + path))
        .header("Authorization", "Bearer client-token");
  }

  HttpResponse<String> send(HttpRequest r) throws IOException, InterruptedException {
    return http.send(r, HttpResponse.BodyHandlers.ofString());
  }

  HttpResponse<String> put(String key, String value, String... headers) throws Exception {
    HttpRequest.Builder b =
        req(leader, "/v1/kv/" + key).PUT(HttpRequest.BodyPublishers.ofString(value));
    for (int i = 0; i < headers.length; i += 2) {
      b.header(headers[i], headers[i + 1]);
    }
    return send(b.build());
  }

  HttpResponse<String> get(String node, String key) throws Exception {
    return send(req(node, "/v1/kv/" + key).GET().build());
  }

  /** Sends a request line the HTTP client would refuse to build; returns the status code. */
  int raw(String requestLine) throws IOException {
    try (java.net.Socket sock =
        new java.net.Socket(LocalCluster.LOOPBACK, cluster.ports.get(leader)[1])) {
      String req =
          requestLine
              + "\r\nHost: x\r\nAuthorization: Bearer client-token\r\nConnection: close\r\n\r\n";
      sock.getOutputStream().write(req.getBytes(StandardCharsets.UTF_8));
      String resp = new String(sock.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      return Integer.parseInt(resp.substring(9, 12));
    }
  }

  static long number(String json, String field) {
    Matcher m = Pattern.compile("\"" + field + "\":(\\d+)").matcher(json);
    assertTrue(m.find(), field + " in " + json);
    return Long.parseLong(m.group(1));
  }

  @Test
  void putGetDeleteWithVersions() throws Exception {
    HttpResponse<String> p = put("fruit", "apple");
    assertEquals(200, p.statusCode(), p.body());
    long v1 = number(p.body(), "version");
    assertEquals("\"" + v1 + "\"", p.headers().firstValue("ETag").orElseThrow());

    HttpResponse<String> g = get(leader, "fruit");
    assertEquals(200, g.statusCode());
    assertEquals("apple", g.body());
    assertEquals(Long.toString(v1), g.headers().firstValue("Keel-Version").orElseThrow());

    HttpResponse<String> conflict = put("fruit", "pear", "If-Match", "\"" + (v1 + 100) + "\"");
    assertEquals(412, conflict.statusCode());
    assertEquals(v1, number(conflict.body(), "current_version"));
    HttpResponse<String> cas = put("fruit", "pear", "If-Match", "\"" + v1 + "\"");
    assertEquals(200, cas.statusCode());
    assertEquals(412, put("fruit", "x", "If-None-Match", "*").statusCode());
    assertEquals(200, put("veg", "leek", "If-None-Match", "*").statusCode());

    HttpResponse<String> del =
        send(req(leader, "/v1/kv/fruit").DELETE().header("If-Match", "1").build());
    assertEquals(412, del.statusCode());
    del = send(req(leader, "/v1/kv/fruit").DELETE().build());
    assertEquals(200, del.statusCode());
    assertEquals(number(cas.body(), "version"), number(del.body(), "deleted_version"));
    assertEquals(404, get(leader, "fruit").statusCode());
    HttpResponse<Void> head =
        http.send(
            req(leader, "/v1/kv/veg").method("HEAD", HttpRequest.BodyPublishers.noBody()).build(),
            HttpResponse.BodyHandlers.discarding());
    assertEquals(200, head.statusCode());
  }

  @Test
  void everyConsistencyLevelReadsTheWrite() throws Exception {
    put("c", "1");
    for (String c : new String[] {"linearizable", "lease", "stale"}) {
      HttpResponse<String> g = send(req(leader, "/v1/kv/c?consistency=" + c).GET().build());
      assertEquals(200, g.statusCode(), c);
      assertEquals("1", g.body());
    }
    assertEquals(
        400, send(req(leader, "/v1/kv/c?consistency=eventual").GET().build()).statusCode());
    // A stale read on a follower answers locally instead of redirecting.
    HttpResponse<String> stale = send(req(follower, "/v1/kv/c?consistency=stale").GET().build());
    assertTrue(stale.statusCode() == 200 || stale.statusCode() == 404);
  }

  @Test
  void followersRedirectToTheLeader() throws Exception {
    HttpResponse<String> g = send(req(follower, "/v1/kv/x?consistency=lease").GET().build());
    assertEquals(307, g.statusCode());
    assertEquals(
        cluster.url(leader) + "/v1/kv/x?consistency=lease",
        g.headers().firstValue("Location").orElseThrow());
    assertEquals(leader, g.headers().firstValue("Keel-Leader").orElseThrow());
    HttpResponse<String> p =
        send(req(follower, "/v1/kv/x").PUT(HttpRequest.BodyPublishers.ofString("v")).build());
    assertEquals(307, p.statusCode());
    HttpResponse<String> s =
        send(req(follower, "/v1/sessions").POST(HttpRequest.BodyPublishers.noBody()).build());
    assertEquals(307, s.statusCode());
  }

  @Test
  void sessionRetriesApplyOnce() throws Exception {
    HttpResponse<String> s =
        send(req(leader, "/v1/sessions").POST(HttpRequest.BodyPublishers.noBody()).build());
    assertEquals(201, s.statusCode());
    String client = Long.toString(number(s.body(), "client_id"));
    HttpResponse<String> first = put("counter", "1", "Keel-Client", client, "Keel-Seq", "1");
    put("counter", "other");
    HttpResponse<String> retry = put("counter", "1", "Keel-Client", client, "Keel-Seq", "1");
    assertEquals(200, retry.statusCode());
    assertEquals(number(first.body(), "version"), number(retry.body(), "version"));
    assertEquals("other", get(leader, "counter").body(), "the retry must not apply again");
    assertEquals(200, put("counter", "2", "Keel-Client", client, "Keel-Seq", "2").statusCode());
    assertEquals(409, put("counter", "1", "Keel-Client", client, "Keel-Seq", "1").statusCode());
    assertEquals(410, put("counter", "1", "Keel-Client", "999999", "Keel-Seq", "1").statusCode());
    assertEquals(400, put("counter", "1", "Keel-Client", client).statusCode());
    assertEquals(400, put("counter", "1", "Keel-Client", "x", "Keel-Seq", "1").statusCode());
    assertEquals(400, put("counter", "1", "Keel-Client", "0", "Keel-Seq", "1").statusCode());
  }

  @Test
  void malformedInputIsRefusedBeforeAnythingIsProposed() throws Exception {
    long before = cluster.servers.get(leader).loop().status().lastIndex();
    assertEquals(400, raw("GET /v1/kv/%ZZ HTTP/1.1"));
    assertEquals(400, raw("GET /v1/kv/%4 HTTP/1.1"));
    assertEquals(400, raw("GET /v1/kv/caf\u00e9 HTTP/1.1"));
    assertEquals(400, get(leader, "a%0Ab").statusCode());
    assertEquals(400, get(leader, "%C3%28").statusCode());
    assertEquals(400, get(leader, "k".repeat(1025)).statusCode());
    assertEquals(400, get(leader, "k".repeat(3100)).statusCode());
    assertEquals(400, put("k", "v", "If-Match", "abc").statusCode());
    assertEquals(400, put("k", "v", "If-Match", "1", "If-None-Match", "*").statusCode());
    assertEquals(400, put("k", "v", "If-None-Match", "\"3\"").statusCode());
    assertEquals(
        400,
        send(req(leader, "/v1/kv/k").DELETE().header("If-None-Match", "*").build()).statusCode());
    // Too large by its declared length: refused without reading the body.
    HttpResponse<String> big =
        send(
            req(leader, "/v1/kv/big")
                .PUT(HttpRequest.BodyPublishers.ofByteArray(new byte[(1 << 20) + 1]))
                .build());
    assertEquals(413, big.statusCode());
    // No Content-Length at all (chunked).
    HttpResponse<String> chunked =
        send(
            req(leader, "/v1/kv/k")
                .PUT(
                    HttpRequest.BodyPublishers.ofInputStream(
                        () -> new ByteArrayInputStream(new byte[] {1})))
                .build());
    assertEquals(411, chunked.statusCode());
    assertEquals(before, cluster.servers.get(leader).loop().status().lastIndex());
    // Keys with slashes, spaces and non-ASCII characters round-trip when percent-encoded.
    assertEquals(200, put("a%2Fb%20c%E2%82%AC", "ok").statusCode());
    assertEquals("ok", get(leader, "a%2Fb%20c%E2%82%AC").body());
  }

  @Test
  void tokensAreRequired() throws Exception {
    HttpRequest noToken =
        HttpRequest.newBuilder(URI.create(cluster.url(leader) + "/v1/kv/k")).build();
    assertEquals(401, send(noToken).statusCode());
    HttpRequest wrong =
        HttpRequest.newBuilder(URI.create(cluster.url(leader) + "/v1/status"))
            .header("Authorization", "Bearer nope")
            .build();
    assertEquals(401, send(wrong).statusCode());
    HttpRequest basic =
        HttpRequest.newBuilder(URI.create(cluster.url(leader) + "/v1/status"))
            .header("Authorization", "Basic abc")
            .build();
    assertEquals(401, send(basic).statusCode());
    // The client token does not grant membership changes.
    HttpResponse<String> add =
        send(
            req(leader, "/v1/members?id=n9&peer=127.0.0.1:1&client=127.0.0.1:2")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build());
    assertEquals(401, add.statusCode());
  }

  @Test
  void membershipEndpoints() throws Exception {
    HttpResponse<String> list = send(req(leader, "/v1/members").GET().build());
    assertEquals(200, list.statusCode());
    assertTrue(list.body().contains("\"voter\":true"));
    HttpRequest.Builder admin =
        HttpRequest.newBuilder(
                URI.create(
                    cluster.url(leader) + "/v1/members?id=n9&peer=127.0.0.1:1&client=127.0.0.1:2"))
            .header("Authorization", "Bearer admin-token");
    HttpResponse<String> add = send(admin.POST(HttpRequest.BodyPublishers.noBody()).build());
    assertEquals(200, add.statusCode(), add.body());
    assertTrue(add.body().contains("\"n9\""));
    HttpResponse<String> again = send(admin.POST(HttpRequest.BodyPublishers.noBody()).build());
    assertEquals(409, again.statusCode(), again.body());
    HttpResponse<String> bad =
        send(
            HttpRequest.newBuilder(URI.create(cluster.url(leader) + "/v1/members?id=bad%20id"))
                .header("Authorization", "Bearer admin-token")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build());
    assertEquals(400, bad.statusCode());
    HttpRequest.Builder remove =
        HttpRequest.newBuilder(URI.create(cluster.url(leader) + "/v1/members/n9"))
            .header("Authorization", "Bearer admin-token");
    HttpResponse<String> removed = send(remove.DELETE().build());
    assertEquals(200, removed.statusCode(), removed.body());
    assertTrue(!removed.body().contains("\"n9\""));
    HttpResponse<String> badId =
        send(
            HttpRequest.newBuilder(URI.create(cluster.url(leader) + "/v1/members/bad%20id"))
                .header("Authorization", "Bearer admin-token")
                .DELETE()
                .build());
    assertEquals(400, badId.statusCode());
    HttpResponse<String> onFollower =
        send(
            HttpRequest.newBuilder(URI.create(cluster.url(follower) + "/v1/members/n9"))
                .header("Authorization", "Bearer admin-token")
                .DELETE()
                .build());
    assertEquals(307, onFollower.statusCode());
  }

  @Test
  void operationalEndpoints() throws Exception {
    HttpResponse<String> status = send(req(leader, "/v1/status").GET().build());
    assertEquals(200, status.statusCode());
    assertTrue(status.body().contains("\"role\":\"leader\""), status.body());
    HttpResponse<String> metrics =
        send(HttpRequest.newBuilder(URI.create(cluster.url(leader) + "/metrics")).build());
    assertEquals(200, metrics.statusCode());
    assertTrue(metrics.body().contains("keel_raft_leader 1"));
    assertTrue(metrics.body().contains("keel_peer_sent_total"));
    HttpResponse<String> health =
        send(HttpRequest.newBuilder(URI.create(cluster.url(follower) + "/healthz")).build());
    assertEquals("ok\n", health.body());
    assertEquals(404, send(req(leader, "/nope").GET().build()).statusCode());
    assertEquals(
        405,
        send(req(leader, "/v1/kv/k").POST(HttpRequest.BodyPublishers.noBody()).build())
            .statusCode());
    assertEquals(405, send(req(leader, "/v1/status").DELETE().build()).statusCode());
    assertEquals(405, send(req(leader, "/v1/members/x").GET().build()).statusCode());
    HttpResponse<String> withId =
        send(req(leader, "/v1/status").header("X-Request-Id", "abc-123").build());
    assertEquals("abc-123", withId.headers().firstValue("X-Request-Id").orElseThrow());
    HttpResponse<String> badId =
        send(req(leader, "/v1/status").header("X-Request-Id", "bad id!").build());
    assertTrue(badId.headers().firstValue("X-Request-Id").orElseThrow().length() >= 32);
  }

  @Test
  void addressParsing() {
    assertEquals(7000, KeelServer.address("[::1]:7000").getPort());
    assertEquals("localhost", KeelServer.address("localhost:1").getHostString());
    assertThrows(IllegalArgumentException.class, () -> KeelServer.address("nohost"));
    assertThrows(IllegalArgumentException.class, () -> KeelServer.address("h:"));
    assertThrows(IllegalArgumentException.class, () -> KeelServer.address("h:x"));
    assertTrue(StandardCharsets.UTF_8.name().length() > 0);
  }
}
