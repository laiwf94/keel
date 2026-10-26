package io.github.laiwf94.keel.server;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.laiwf94.keel.kv.Command;
import io.github.laiwf94.keel.kv.KvMachine;
import io.github.laiwf94.keel.kv.Result;
import io.github.laiwf94.keel.obs.Json;
import io.github.laiwf94.keel.obs.Log;
import io.github.laiwf94.keel.obs.Metrics;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.replica.Outcome;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * The client HTTP API, served by the JDK's built-in server on virtual threads.
 *
 * <pre>
 * GET    /v1/kv/{key}[?consistency=linearizable|lease|stale]
 * PUT    /v1/kv/{key}        body = value; If-Match: "version" | If-None-Match: *
 * DELETE /v1/kv/{key}        If-Match: "version"
 * POST   /v1/sessions        returns a client id for Keel-Client / Keel-Seq headers
 * GET    /v1/status
 * GET    /v1/members         POST /v1/members?id=&amp;peer=&amp;client=   DELETE /v1/members/{id}
 * GET    /metrics            GET /healthz
 * </pre>
 *
 * Every input is checked before anything is proposed: key length and characters, value size against
 * Content-Length before the body is read, header formats and tokens.
 */
public final class HttpApi implements AutoCloseable {
  /** Settings for the API: tokens, timeouts and the clock used to stamp commands. */
  public record Options(
      Optional<String> clientToken,
      Optional<String> adminToken,
      Duration requestTimeout,
      Clock clock) {}

  private static final Pattern REQUEST_ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");
  private static final Pattern VERSION = Pattern.compile("\"?(\\d{1,18})\"?");

  private final NodeLoop loop;
  private final KvMachine kv;
  private final Metrics metrics;
  private final Log log;
  private final Options opts;
  private final HttpServer server;
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private final byte[] clientTokenHash;
  private final byte[] adminTokenHash;
  private final Metrics.Counter requests;
  private final Metrics.Counter errors;

  public HttpApi(
      InetSocketAddress bind, NodeLoop loop, KvMachine kv, Metrics metrics, Log log, Options opts)
      throws IOException {
    this.loop = loop;
    this.kv = kv;
    this.metrics = metrics;
    this.log = log;
    this.opts = opts;
    this.clientTokenHash = opts.clientToken().map(HttpApi::sha256).orElse(null);
    this.adminTokenHash = opts.adminToken().map(HttpApi::sha256).orElse(null);
    this.requests = metrics.counter("keel_http_requests_total", "HTTP requests handled.");
    this.errors = metrics.counter("keel_http_errors_total", "HTTP responses with status 5xx.");
    this.server = HttpServer.create(bind, 1024);
    server.setExecutor(executor);
    server.createContext("/", this::handle);
  }

  public void start() {
    server.start();
  }

  public int port() {
    return server.getAddress().getPort();
  }

  /** A response ready to send. */
  private static final class Response {
    private final int status;
    private final Map<String, String> headers;
    private final String contentType;
    private final byte[] body;

    Response(int status, Map<String, String> headers, String contentType, byte[] body) {
      this.status = status;
      this.headers = headers;
      this.contentType = contentType;
      this.body = body;
    }

    int status() {
      return status;
    }

    Map<String, String> headers() {
      return headers;
    }

    String contentType() {
      return contentType;
    }

    byte[] body() {
      return body;
    }

    static Response json(int status, Object body) {
      return new Response(
          status,
          Map.of(),
          "application/json",
          (Json.toJson(body) + "\n").getBytes(StandardCharsets.UTF_8));
    }

    static Response error(int status, String message) {
      return json(status, Map.of("error", message));
    }

    Response with(String header, String value) {
      Map<String, String> h = new LinkedHashMap<>(headers);
      h.put(header, value);
      return new Response(status, h, contentType, body);
    }
  }

  /** Thrown inside handlers to answer with a 4xx. */
  private static final class BadRequest extends Exception {
    private static final long serialVersionUID = 1L;
    final transient Response response;

    BadRequest(int status, String message) {
      super(message, null, false, false);
      this.response = Response.error(status, message);
    }
  }

  private void handle(HttpExchange ex) {
    long start = System.nanoTime();
    requests.inc();
    String requestId = ex.getRequestHeaders().getFirst("X-Request-Id");
    if (requestId == null || !REQUEST_ID.matcher(requestId).matches()) {
      requestId = UUID.randomUUID().toString();
    }
    Response r;
    try {
      r = route(ex);
    } catch (BadRequest e) {
      r = e.response;
    } catch (RuntimeException e) {
      log.error("http_error", "request_id", requestId, "error", e.toString());
      r = Response.error(500, "internal error");
    }
    r = r.with("X-Request-Id", requestId);
    if (r.status() >= 500) {
      errors.inc();
      log.warn(
          "http",
          "request_id",
          requestId,
          "method",
          ex.getRequestMethod(),
          "path",
          ex.getRequestURI().getRawPath(),
          "status",
          r.status(),
          "ms",
          (System.nanoTime() - start) / 1_000_000);
    } else {
      log.debug(
          "http",
          "request_id",
          requestId,
          "method",
          ex.getRequestMethod(),
          "path",
          ex.getRequestURI().getRawPath(),
          "status",
          r.status(),
          "ms",
          (System.nanoTime() - start) / 1_000_000);
    }
    send(ex, r);
  }

  private static void send(HttpExchange ex, Response r) {
    try (ex) {
      Headers h = ex.getResponseHeaders();
      h.set("Content-Type", r.contentType());
      h.set("Cache-Control", "no-store");
      h.set("X-Content-Type-Options", "nosniff");
      r.headers().forEach(h::set);
      boolean noBody = "HEAD".equals(ex.getRequestMethod()) || r.status() == 204;
      ex.sendResponseHeaders(r.status(), noBody ? -1 : r.body().length);
      if (!noBody) {
        try (OutputStream out = ex.getResponseBody()) {
          out.write(r.body());
        }
      }
    } catch (IOException e) {
      // The client went away; nothing to tell it.
    }
  }

  private Response route(HttpExchange ex) throws BadRequest {
    String path = ex.getRequestURI().getRawPath();
    String method = ex.getRequestMethod();
    if (path.equals("/healthz")) {
      requireMethod(method, "GET");
      return loop.isRunning()
          ? new Response(200, Map.of(), "text/plain", "ok\n".getBytes(StandardCharsets.UTF_8))
          : Response.error(503, "event loop stopped");
    }
    if (path.equals("/metrics")) {
      requireMethod(method, "GET");
      return new Response(
          200,
          Map.of(),
          "text/plain; version=0.0.4",
          metrics.render().getBytes(StandardCharsets.UTF_8));
    }
    if (path.startsWith("/v1/kv/")) {
      authorize(ex, clientTokenHash);
      String key = decodeKey(path.substring("/v1/kv/".length()));
      return switch (method) {
        case "GET", "HEAD" -> get(ex, key);
        case "PUT" -> put(ex, key);
        case "DELETE" -> delete(ex, key);
        default -> notAllowed("GET, HEAD, PUT, DELETE");
      };
    }
    if (path.equals("/v1/sessions")) {
      authorize(ex, clientTokenHash);
      requireMethod(method, "POST");
      return register(ex);
    }
    if (path.equals("/v1/status")) {
      authorize(ex, clientTokenHash);
      requireMethod(method, "GET");
      return Response.json(200, statusJson());
    }
    if (path.equals("/v1/members")) {
      if (method.equals("GET")) {
        authorize(ex, clientTokenHash);
        return Response.json(200, Map.of("members", membersJson()));
      }
      requireMethod(method, "POST");
      authorizeAdmin(ex);
      return addMember(ex);
    }
    if (path.startsWith("/v1/members/")) {
      requireMethod(method, "DELETE");
      authorizeAdmin(ex);
      String id = path.substring("/v1/members/".length());
      try {
        Member.validateId(id);
      } catch (IllegalArgumentException e) {
        throw new BadRequest(400, "invalid member id");
      }
      return membershipOutcome(ex, await(loop.removeServer(id)));
    }
    return Response.error(404, "no such endpoint");
  }

  /** A JSON object whose fields keep the order given, so responses read the same every time. */
  private static Map<String, Object> fields(Object... kv) {
    Map<String, Object> m = new LinkedHashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], kv[i + 1]);
    }
    return m;
  }

  private static void requireMethod(String method, String allowed) throws BadRequest {
    if (!method.equals(allowed)) {
      throw new BadRequest(405, "method not allowed; use " + allowed);
    }
  }

  private static Response notAllowed(String allow) {
    return Response.error(405, "method not allowed").with("Allow", allow);
  }

  private static byte[] sha256(String s) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  /** Checks a bearer token by comparing hashes in constant time; no token configured = open. */
  private static void authorize(HttpExchange ex, byte[] expected) throws BadRequest {
    if (expected == null) {
      return;
    }
    String h = ex.getRequestHeaders().getFirst("Authorization");
    if (h == null || !h.startsWith("Bearer ")) {
      throw new BadRequest(401, "missing bearer token");
    }
    if (!MessageDigest.isEqual(sha256(h.substring(7)), expected)) {
      throw new BadRequest(401, "invalid token");
    }
  }

  private void authorizeAdmin(HttpExchange ex) throws BadRequest {
    if (adminTokenHash == null) {
      throw new BadRequest(403, "membership changes are disabled: no admin token is configured");
    }
    authorize(ex, adminTokenHash);
  }

  /** Strict percent-decoding of a path segment into a valid UTF-8 key. */
  private static String decodeKey(String raw) throws BadRequest {
    if (raw.isEmpty() || raw.length() > Command.MAX_KEY_BYTES * 3) {
      throw new BadRequest(400, "key must be 1 to " + Command.MAX_KEY_BYTES + " bytes");
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream(raw.length());
    for (int i = 0; i < raw.length(); i++) {
      char c = raw.charAt(i);
      if (c == '%') {
        if (i + 2 >= raw.length()) {
          throw new BadRequest(400, "bad percent-encoding in key");
        }
        int hi = Character.digit(raw.charAt(i + 1), 16);
        int lo = Character.digit(raw.charAt(i + 2), 16);
        if (hi < 0 || lo < 0) {
          throw new BadRequest(400, "bad percent-encoding in key");
        }
        out.write(hi * 16 + lo);
        i += 2;
      } else if (c > 0x7f) {
        throw new BadRequest(400, "key must be percent-encoded");
      } else {
        out.write(c);
      }
    }
    String key;
    try {
      key =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .decode(ByteBuffer.wrap(out.toByteArray()))
              .toString();
    } catch (CharacterCodingException e) {
      throw new BadRequest(400, "key is not valid UTF-8");
    }
    try {
      return Command.validateKey(key);
    } catch (IllegalArgumentException e) {
      throw new BadRequest(400, e.getMessage());
    }
  }

  private static Map<String, String> query(HttpExchange ex) throws BadRequest {
    Map<String, String> q = new LinkedHashMap<>();
    String raw = ex.getRequestURI().getRawQuery();
    if (raw == null || raw.isEmpty()) {
      return q;
    }
    if (raw.length() > 2048) {
      throw new BadRequest(414, "query too long");
    }
    for (String part : raw.split("&", -1)) {
      int eq = part.indexOf('=');
      String k = eq < 0 ? part : part.substring(0, eq);
      String v = eq < 0 ? "" : part.substring(eq + 1);
      q.put(k, java.net.URLDecoder.decode(v, StandardCharsets.UTF_8));
    }
    return q;
  }

  private Response get(HttpExchange ex, String key) throws BadRequest {
    String c = query(ex).getOrDefault("consistency", "linearizable");
    Consistency consistency =
        Consistency.parse(c)
            .orElseThrow(
                () -> new BadRequest(400, "consistency must be linearizable, lease or stale"));
    ReadResult<Optional<KvMachine.Value>> r = await(loop.read(consistency, () -> kv.get(key)));
    return switch (r) {
      case ReadResult.Value<Optional<KvMachine.Value>> v -> {
        if (v.value().isEmpty()) {
          yield Response.error(404, "no such key")
              .with("Keel-Applied-Index", "" + v.appliedIndex());
        }
        KvMachine.Value value = v.value().get();
        yield new Response(200, Map.of(), "application/octet-stream", value.bytes())
            .with("ETag", "\"" + value.version() + "\"")
            .with("Keel-Version", Long.toString(value.version()))
            .with("Keel-Applied-Index", Long.toString(v.appliedIndex()));
      }
      case ReadResult.NotLeader<Optional<KvMachine.Value>> n -> redirect(ex, n.leaderHint());
      case ReadResult.Failed<Optional<KvMachine.Value>> f ->
          Response.error(503, f.reason()).with("Retry-After", "1");
    };
  }

  private Response put(HttpExchange ex, String key) throws BadRequest {
    byte[] value = readBody(ex, Command.MAX_VALUE_BYTES);
    Session s = session(ex);
    long expected = expectedVersion(ex, true);
    Command c = Command.put(now(), s.client(), s.seq(), key, value, expected);
    return writeOutcome(ex, await(loop.propose(c.encode())), "version");
  }

  private Response delete(HttpExchange ex, String key) throws BadRequest {
    Session s = session(ex);
    long expected = expectedVersion(ex, false);
    Command c = Command.delete(now(), s.client(), s.seq(), key, expected);
    return writeOutcome(ex, await(loop.propose(c.encode())), "deleted_version");
  }

  private Response register(HttpExchange ex) throws BadRequest {
    Outcome o = await(loop.propose(Command.register(now()).encode()));
    if (o instanceof Outcome.Applied a) {
      return Response.json(201, Map.of("client_id", Result.decode(a.result()).version()));
    }
    return failedWrite(ex, o);
  }

  private long now() {
    return opts.clock().millis();
  }

  private record Session(long client, long seq) {}

  private static Session session(HttpExchange ex) throws BadRequest {
    String client = ex.getRequestHeaders().getFirst("Keel-Client");
    String seq = ex.getRequestHeaders().getFirst("Keel-Seq");
    if (client == null && seq == null) {
      return new Session(0, 0);
    }
    if (client == null || seq == null) {
      throw new BadRequest(400, "Keel-Client and Keel-Seq go together");
    }
    long c = positive(client, "Keel-Client");
    long s = positive(seq, "Keel-Seq");
    return new Session(c, s);
  }

  private static long positive(String s, String name) throws BadRequest {
    try {
      long v = Long.parseLong(s.trim());
      if (v < 1) {
        throw new NumberFormatException();
      }
      return v;
    } catch (NumberFormatException e) {
      throw new BadRequest(400, name + " must be a positive integer");
    }
  }

  private static long expectedVersion(HttpExchange ex, boolean allowCreateOnly) throws BadRequest {
    String ifMatch = ex.getRequestHeaders().getFirst("If-Match");
    String ifNoneMatch = ex.getRequestHeaders().getFirst("If-None-Match");
    if (ifMatch != null && ifNoneMatch != null) {
      throw new BadRequest(400, "use either If-Match or If-None-Match");
    }
    if (ifNoneMatch != null) {
      if (!allowCreateOnly || !ifNoneMatch.trim().equals("*")) {
        throw new BadRequest(400, "only If-None-Match: * is supported, on PUT");
      }
      return Command.ABSENT;
    }
    if (ifMatch == null) {
      return Command.ANY_VERSION;
    }
    var m = VERSION.matcher(ifMatch.trim());
    if (!m.matches()) {
      throw new BadRequest(400, "If-Match must be a version number");
    }
    return Long.parseLong(m.group(1));
  }

  /** Reads the body, refusing it from the declared length alone if it is too large. */
  private static byte[] readBody(HttpExchange ex, int max) throws BadRequest {
    String len = ex.getRequestHeaders().getFirst("Content-Length");
    if (len == null) {
      throw new BadRequest(411, "Content-Length is required");
    }
    long n;
    try {
      n = Long.parseLong(len.trim());
    } catch (NumberFormatException e) {
      throw new BadRequest(400, "bad Content-Length");
    }
    if (n < 0) {
      throw new BadRequest(400, "bad Content-Length");
    }
    if (n > max) {
      // Refused from the declared length, without buffering it. The body is read and thrown away
      // (up to a limit) only so that closing the connection does not reset it before the client
      // has read the 413.
      discard(ex, Math.min(n, 4L * max));
      throw new BadRequest(413, "value larger than " + max + " bytes");
    }
    try (InputStream in = ex.getRequestBody()) {
      byte[] body = in.readNBytes((int) n);
      if (body.length != n) {
        throw new BadRequest(400, "body shorter than Content-Length");
      }
      return body;
    } catch (IOException e) {
      throw new BadRequest(400, "could not read the body");
    }
  }

  private static void discard(HttpExchange ex, long n) {
    try (InputStream in = ex.getRequestBody()) {
      byte[] buf = new byte[8192];
      long left = n;
      int r;
      while (left > 0 && (r = in.read(buf, 0, (int) Math.min(buf.length, left))) > 0) {
        left -= r;
      }
    } catch (IOException e) {
      // The client gave up; the 413 is moot.
    }
  }

  private <T> T await(CompletableFuture<T> f) throws BadRequest {
    try {
      return f.get(opts.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new BadRequest(
          504, "timed out; the outcome is unknown, retry with the same Keel-Seq to be safe");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new BadRequest(503, "interrupted");
    } catch (ExecutionException e) {
      throw new IllegalStateException(e.getCause());
    }
  }

  private Response writeOutcome(HttpExchange ex, Outcome o, String versionField) {
    if (!(o instanceof Outcome.Applied a)) {
      return failedWrite(ex, o);
    }
    Result r = Result.decode(a.result());
    return switch (r.status()) {
      case OK ->
          Response.json(200, Map.of(versionField, r.version()))
              .with("ETag", "\"" + r.version() + "\"");
      case CONFLICT ->
          Response.json(412, fields("error", "version conflict", "current_version", r.version()));
      case SESSION_EXPIRED -> Response.error(410, "session expired; register a new one");
      case STALE -> Response.error(409, "request is older than one this session already completed");
      case INVALID -> Response.error(400, "invalid command");
    };
  }

  private Response failedWrite(HttpExchange ex, Outcome o) {
    return switch (o) {
      case Outcome.NotLeader n -> redirect(ex, n.leaderHint());
      case Outcome.Unknown u ->
          Response.json(
                  503,
                  fields(
                      "error",
                      u.reason() + "; retry with the same Keel-Seq to be safe",
                      "outcome",
                      "unknown"))
              .with("Retry-After", "1");
      case Outcome.Rejected rj -> Response.error(503, rj.reason()).with("Retry-After", "1");
      case Outcome.Applied a -> throw new IllegalStateException("not a failure");
    };
  }

  private Response membershipOutcome(HttpExchange ex, Outcome o) {
    return switch (o) {
      case Outcome.Applied a ->
          Response.json(200, fields("index", a.index(), "members", membersJson()));
      case Outcome.Rejected r -> Response.error(409, r.reason());
      default -> failedWrite(ex, o);
    };
  }

  /** Points the client at the leader, keeping the path and query. */
  private Response redirect(HttpExchange ex, Optional<String> leader) {
    Optional<Member> m = leader.flatMap(id -> loop.status().config().member(id));
    if (m.isEmpty()) {
      return Response.error(503, "no leader is known yet").with("Retry-After", "1");
    }
    String target = "http://" + m.get().clientAddress() + ex.getRequestURI().getRawPath();
    if (ex.getRequestURI().getRawQuery() != null) {
      target += "?" + ex.getRequestURI().getRawQuery();
    }
    return Response.json(307, fields("error", "not the leader", "leader", m.get().id()))
        .with("Location", target)
        .with("Keel-Leader", m.get().id());
  }

  private Response addMember(HttpExchange ex) throws BadRequest {
    Map<String, String> q = query(ex);
    Member m;
    try {
      m =
          new Member(
              q.getOrDefault("id", ""),
              q.getOrDefault("peer", ""),
              q.getOrDefault("client", ""),
              false);
    } catch (IllegalArgumentException e) {
      throw new BadRequest(400, e.getMessage());
    }
    return membershipOutcome(ex, await(loop.addServer(m)));
  }

  private Map<String, Object> statusJson() {
    NodeStatus s = loop.status();
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("id", s.id());
    m.put("role", s.role().name().toLowerCase(Locale.ROOT));
    m.put("term", s.term());
    m.put("leader", s.leader().isEmpty() ? null : s.leader());
    m.put("commit_index", s.commitIndex());
    m.put("applied_index", s.appliedIndex());
    m.put("last_index", s.lastIndex());
    m.put("snapshot_index", s.snapshotIndex());
    m.put("members", membersJson());
    return m;
  }

  private List<Map<String, Object>> membersJson() {
    return loop.status().config().members().stream()
        .map(
            mem -> {
              Map<String, Object> m = new LinkedHashMap<>();
              m.put("id", mem.id());
              m.put("peer", mem.peerAddress());
              m.put("client", mem.clientAddress());
              m.put("voter", mem.voter());
              return m;
            })
        .toList();
  }

  @Override
  public void close() {
    server.stop(0);
    executor.shutdownNow();
  }
}
