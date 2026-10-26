package io.github.laiwf94.keel.obs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ObsTest {
  final ByteArrayOutputStream buf = new ByteArrayOutputStream();
  final PrintStream out = new PrintStream(buf, true, StandardCharsets.UTF_8);
  final Clock clock = Clock.fixed(Instant.parse("2000-01-01T00:00:00Z"), ZoneOffset.UTC);

  String lines() {
    return buf.toString(StandardCharsets.UTF_8);
  }

  @Test
  void logsOneJsonObjectPerLine() {
    Log log = new Log("n1", out, clock, Log.Level.INFO);
    log.info("role", "term", 3, "leader", "n\"2", "ok", true, "none", null);
    log.debug("hidden");
    log.warn("w");
    log.error("e", "error", "line1\nline2\t\u0001");
    String[] ls = lines().lines().toArray(String[]::new);
    assertEquals(3, ls.length);
    assertEquals(
        "{\"ts\":\"2000-01-01T00:00:00Z\",\"level\":\"INFO\",\"node\":\"n1\",\"event\":\"role\","
            + "\"term\":3,\"leader\":\"n\\\"2\",\"ok\":true,\"none\":null}",
        ls[0]);
    assertTrue(ls[2].contains("\"error\":\"line1\\nline2\\t\\u0001\""), ls[2]);
    assertThrows(IllegalArgumentException.class, () -> log.info("bad", "odd"));
  }

  @Test
  void rateLimitedWarnings() {
    long[] now = {0};
    Clock ticking =
        new Clock() {
          @Override
          public ZoneOffset getZone() {
            return ZoneOffset.UTC;
          }

          @Override
          public Clock withZone(java.time.ZoneId zone) {
            return this;
          }

          @Override
          public Instant instant() {
            return Instant.ofEpochMilli(now[0]);
          }
        };
    Log log = new Log("n1", out, ticking, Log.Level.DEBUG);
    log.warnLimited("k", 1000, "x");
    log.warnLimited("k", 1000, "x");
    now[0] = 1500;
    log.warnLimited("k", 1000, "x");
    log.debug("d");
    assertEquals(3, lines().lines().count());
  }

  @Test
  void jsonValues() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("list", List.of(1, "a", false));
    m.put("nested", Map.of("x", 1.5));
    m.put("ls", " \\\r");
    assertEquals(
        "{\"list\":[1,\"a\",false],\"nested\":{\"x\":1.5},\"ls\":\"\\u2028\\\\\\r\"}",
        Json.toJson(m));
    assertEquals("{}", Json.toJson(Map.of()));
    assertEquals("[]", Json.toJson(List.of()));
  }

  @Test
  void metricsRenderInPrometheusFormat() {
    Metrics m = new Metrics();
    Metrics.Counter c = m.counter("keel_x_total", "Things.");
    c.inc();
    c.add(2);
    assertEquals(3, c.get());
    assertEquals(c, m.counter("keel_x_total", "Things."));
    m.gauge("keel_g", "A gauge.", () -> 2.5);
    m.gauge("keel_h", "Whole.", () -> 7);
    m.gauge("keel_nan", "Not a number.", () -> Double.NaN);
    m.gauge("keel_inf", "Infinite.", () -> Double.NEGATIVE_INFINITY);
    m.gauge("keel_pinf", "Infinite.", () -> Double.POSITIVE_INFINITY);
    Metrics.Histogram h = m.histogram("keel_lat_seconds", "Latency.");
    h.observeNanos(30_000);
    h.observeNanos(2_000_000);
    h.observeNanos(60_000_000_000L);
    assertEquals(3, h.count());
    String text = m.render();
    assertTrue(text.contains("# TYPE keel_x_total counter\nkeel_x_total 3\n"), text);
    assertTrue(text.contains("keel_g 2.5\n"));
    assertTrue(text.contains("keel_h 7\n"));
    assertTrue(text.contains("keel_nan NaN\n"));
    assertTrue(text.contains("keel_inf -Inf\n"));
    assertTrue(text.contains("keel_pinf +Inf\n"));
    assertTrue(text.contains("keel_lat_seconds_bucket{le=\"0.00005\"} 1\n"), text);
    assertTrue(text.contains("keel_lat_seconds_bucket{le=\"0.0025\"} 2\n"), text);
    assertTrue(text.contains("keel_lat_seconds_bucket{le=\"10\"} 2\n"), text);
    assertTrue(text.contains("keel_lat_seconds_bucket{le=\"+Inf\"} 3\n"), text);
    assertTrue(text.contains("keel_lat_seconds_count 3\n"), text);
    assertTrue(
        text.contains("# HELP keel_lat_seconds Latency.\n# TYPE keel_lat_seconds histogram"));
  }
}
