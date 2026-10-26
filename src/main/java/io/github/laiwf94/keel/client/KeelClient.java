package io.github.laiwf94.keel.client;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A client for one keel cluster. It finds the leader by following redirects, retries on leader
 * changes and timeouts, and keeps a session so that a retried write applies at most once.
 *
 * <p>Requests on one client are serialized: a session allows one outstanding request, which is what
 * makes "same sequence number, same result" sufficient. Use one client per thread.
 */
public final class KeelClient implements AutoCloseable {
  /**
   * Client settings.
   *
   * @param endpoints base URLs of some or all nodes
   * @param retryFor how long one call keeps retrying before giving up
   */
  public record Options(
      List<URI> endpoints, Optional<String> token, Duration requestTimeout, Duration retryFor) {
    public Options {
      if (endpoints.isEmpty()) {
        throw new IllegalArgumentException("at least one endpoint is needed");
      }
      endpoints = List.copyOf(endpoints);
    }
  }

  /** A value and its version. */
  public static final class Versioned {
    private final byte[] value;
    private final long version;

    Versioned(byte[] value, long version) {
      this.value = value;
      this.version = version;
    }

    public byte[] value() {
      return value.clone();
    }

    public String text() {
      return new String(value, StandardCharsets.UTF_8);
    }

    public long version() {
      return version;
    }
  }

  private static final Pattern NUMBER_FIELD = Pattern.compile("\"(\\w+)\":(-?\\d+)");

  private final Options opts;
  private final HttpClient http;
  private URI current;
  private int next;
  private long clientId;
  private long seq;

  public KeelClient(Options opts) {
    this.opts = opts;
    this.http =
        HttpClient.newBuilder()
            .connectTimeout(opts.requestTimeout())
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    this.current = opts.endpoints().get(0);
  }

  /** The node the client is talking to now; after a write this is usually the leader. */
  public synchronized URI current() {
    return current;
  }

  /** Registers a session; later writes from this client are applied at most once. */
  public synchronized long openSession() throws KeelException, InterruptedException {
    HttpResponse<byte[]> r = call("POST", "/v1/sessions", null, List.of(), false);
    clientId = field(r, "client_id");
    seq = 0;
    return clientId;
  }

  public synchronized long put(String key, byte[] value)
      throws KeelException, InterruptedException {
    return write("PUT", key, value, List.of(), "version");
  }

  /** Writes only if the key's version is {@code expected} (0 means "only if absent"). */
  public synchronized long putIf(String key, byte[] value, long expected)
      throws KeelException, InterruptedException {
    List<String> h =
        expected == 0 ? List.of("If-None-Match", "*") : List.of("If-Match", "\"" + expected + "\"");
    return write("PUT", key, value, h, "version");
  }

  /** Deletes a key; returns the version removed, or 0 if it was absent. */
  public synchronized long delete(String key) throws KeelException, InterruptedException {
    return write("DELETE", key, null, List.of(), "deleted_version");
  }

  public synchronized Optional<Versioned> get(String key, String consistency)
      throws KeelException, InterruptedException {
    HttpResponse<byte[]> r =
        call("GET", "/v1/kv/" + encode(key) + "?consistency=" + consistency, null, List.of(), true);
    if (r.statusCode() == 404) {
      return Optional.empty();
    }
    long version = Long.parseLong(r.headers().firstValue("Keel-Version").orElse("0"));
    return Optional.of(new Versioned(r.body(), version));
  }

  /** The raw JSON of {@code GET /v1/status} from the current node. */
  public synchronized String status() throws KeelException, InterruptedException {
    return new String(
        call("GET", "/v1/status", null, List.of(), false).body(), StandardCharsets.UTF_8);
  }

  /** Adds a server as a learner; needs the admin token. Returns the new member list as JSON. */
  public synchronized String addMember(String id, String peer, String client)
      throws KeelException, InterruptedException {
    String q = "?id=" + encode(id) + "&peer=" + encode(peer) + "&client=" + encode(client);
    return new String(
        call("POST", "/v1/members" + q, null, List.of(), false).body(), StandardCharsets.UTF_8);
  }

  /** Removes a server; needs the admin token. Returns the new member list as JSON. */
  public synchronized String removeMember(String id) throws KeelException, InterruptedException {
    return new String(
        call("DELETE", "/v1/members/" + encode(id), null, List.of(), false).body(),
        StandardCharsets.UTF_8);
  }

  private long write(String method, String key, byte[] body, List<String> headers, String field)
      throws KeelException, InterruptedException {
    List<String> h = new ArrayList<>(headers);
    if (clientId != 0) {
      h.add("Keel-Client");
      h.add(Long.toString(clientId));
      h.add("Keel-Seq");
      h.add(Long.toString(++seq));
    }
    HttpResponse<byte[]> r = call(method, "/v1/kv/" + encode(key), body, h, false);
    return field(r, field);
  }

  static String encode(String key) {
    return URLEncoder.encode(key, StandardCharsets.UTF_8).replace("+", "%20");
  }

  private static long field(HttpResponse<byte[]> r, String name) {
    Matcher m = NUMBER_FIELD.matcher(new String(r.body(), StandardCharsets.UTF_8));
    while (m.find()) {
      if (m.group(1).equals(name)) {
        return Long.parseLong(m.group(2));
      }
    }
    throw new IllegalStateException("response has no " + name);
  }

  /**
   * Sends a request, following redirects and retrying until a definite answer or the deadline.
   * Writes are retried with the same headers, so a write under a session keeps its sequence.
   */
  private HttpResponse<byte[]> call(
      String method, String path, byte[] body, List<String> headers, boolean notFoundOk)
      throws KeelException, InterruptedException {
    long deadline = System.nanoTime() + opts.retryFor().toNanos();
    long backoffMs = 10;
    String lastError = "no attempt made";
    int lastStatus = 0;
    boolean unknown = false;
    while (System.nanoTime() < deadline) {
      HttpRequest.Builder b =
          HttpRequest.newBuilder(current.resolve(path))
              .timeout(opts.requestTimeout())
              .method(
                  method,
                  body == null
                      ? HttpRequest.BodyPublishers.noBody()
                      : HttpRequest.BodyPublishers.ofByteArray(body));
      opts.token().ifPresent(t -> b.header("Authorization", "Bearer " + t));
      for (int i = 0; i < headers.size(); i += 2) {
        b.header(headers.get(i), headers.get(i + 1));
      }
      HttpResponse<byte[]> r;
      try {
        r = http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
      } catch (HttpTimeoutException e) {
        lastError = "timed out talking to " + current;
        unknown |= !"GET".equals(method);
        rotate();
        continue;
      } catch (IOException e) {
        lastError = "cannot reach " + current + ": " + e.getMessage();
        // The request may have reached the server before the connection broke.
        unknown |= !"GET".equals(method);
        rotate();
        sleep(backoffMs);
        backoffMs = Math.min(backoffMs * 2, 500);
        continue;
      }
      int s = r.statusCode();
      lastStatus = s;
      if (s == 200 || s == 201 || (s == 404 && notFoundOk)) {
        return r;
      }
      if (s == 307) {
        Optional<String> loc = r.headers().firstValue("Location");
        if (loc.isPresent()) {
          URI l = URI.create(loc.get());
          current = URI.create(l.getScheme() + "://" + l.getRawAuthority());
          continue;
        }
      }
      String text = new String(r.body(), StandardCharsets.UTF_8);
      if (s == 412) {
        long currentVersion = 0;
        Matcher m = NUMBER_FIELD.matcher(text);
        while (m.find()) {
          if (m.group(1).equals("current_version")) {
            currentVersion = Long.parseLong(m.group(2));
          }
        }
        throw new KeelException.Conflict(currentVersion);
      }
      if (s == 503 || s == 504) {
        lastError = text.trim();
        unknown |= text.contains("unknown") || s == 504;
        if (text.contains("no leader") || text.contains("overloaded")) {
          rotate();
        }
        sleep(backoffMs);
        backoffMs = Math.min(backoffMs * 2, 500);
        continue;
      }
      // A definite refusal: 4xx other than the cases above. A 410 after a retry means the
      // first attempt may have applied before the session expired.
      throw new KeelException(text.trim(), s, unknown && s == 410);
    }
    throw new KeelException("gave up: " + lastError, lastStatus, unknown);
  }

  private void rotate() {
    next = (next + 1) % opts.endpoints().size();
    current = opts.endpoints().get(next);
  }

  private static void sleep(long ms) throws InterruptedException {
    Thread.sleep(ms + ThreadLocalRandom.current().nextLong(ms / 2 + 1));
  }

  @Override
  public void close() {
    http.close();
  }
}
