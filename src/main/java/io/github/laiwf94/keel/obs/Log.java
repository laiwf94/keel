package io.github.laiwf94.keel.obs;

import java.io.PrintStream;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Structured logs: one JSON object per line with a timestamp, level, node id, event name and
 * key-value fields. Request handlers pass a {@code request_id} field so one client request can be
 * followed across log lines.
 */
public final class Log {
  public enum Level {
    DEBUG,
    INFO,
    WARN,
    ERROR
  }

  private final String node;
  private final PrintStream out;
  private final Clock clock;
  private final Level min;
  private final Map<String, Long> lastByKey = new ConcurrentHashMap<>();

  public Log(String node, PrintStream out, Clock clock, Level min) {
    this.node = node;
    this.out = out;
    this.clock = clock;
    this.min = min;
  }

  public void debug(String event, Object... kv) {
    log(Level.DEBUG, event, kv);
  }

  public void info(String event, Object... kv) {
    log(Level.INFO, event, kv);
  }

  public void warn(String event, Object... kv) {
    log(Level.WARN, event, kv);
  }

  public void error(String event, Object... kv) {
    log(Level.ERROR, event, kv);
  }

  /**
   * Logs a warning at most once per {@code intervalMs} for {@code key}, so a peer that keeps
   * failing (or an attacker probing the port) cannot flood the log.
   */
  public void warnLimited(String key, long intervalMs, String event, Object... kv) {
    long now = clock.millis();
    Long last = lastByKey.get(key);
    if (last != null && now - last < intervalMs) {
      return;
    }
    lastByKey.put(key, now);
    log(Level.WARN, event, kv);
  }

  public void log(Level level, String event, Object... kv) {
    if (level.compareTo(min) < 0) {
      return;
    }
    if (kv.length % 2 != 0) {
      throw new IllegalArgumentException("fields must come in key, value pairs");
    }
    StringBuilder sb = new StringBuilder(128);
    sb.append("{\"ts\":\"").append(Instant.now(clock)).append('"');
    sb.append(",\"level\":\"").append(level).append('"');
    sb.append(",\"node\":");
    Json.string(sb, node);
    sb.append(",\"event\":");
    Json.string(sb, event);
    for (int i = 0; i < kv.length; i += 2) {
      sb.append(',');
      Json.string(sb, String.valueOf(kv[i]));
      sb.append(':');
      Json.value(sb, kv[i + 1]);
    }
    sb.append('}');
    out.println(sb);
  }
}
