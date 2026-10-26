package io.github.laiwf94.keel.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.client.KeelClient;
import io.github.laiwf94.keel.server.ClusterAccess;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchTest {
  @TempDir Path tmp;

  @Test
  void histogramPercentilesAreWithinItsPrecision() {
    LatencyHistogram h = new LatencyHistogram();
    assertEquals(0, h.percentile(50));
    for (long v = 1; v <= 100_000; v++) {
      h.record(v);
    }
    assertEquals(100_000, h.count());
    for (double p : new double[] {1, 50, 90, 99, 99.9}) {
      long exact = (long) Math.ceil(p / 100 * 100_000);
      long got = h.percentile(p);
      assertTrue(Math.abs(got - exact) <= exact / 64 + 1, p + ": " + got + " vs " + exact);
    }
    assertEquals(100_000, h.percentile(100));
    assertEquals(100_000, h.max());
    LatencyHistogram other = new LatencyHistogram();
    other.record(-5);
    other.record(Long.MAX_VALUE / 2);
    h.merge(other);
    assertEquals(100_002, h.count());
    assertEquals(Long.MAX_VALUE / 2, h.percentile(100));
    assertEquals(0, other.percentile(1));
    Random r = new Random(1);
    LatencyHistogram small = new LatencyHistogram();
    for (int i = 0; i < 1000; i++) {
      small.record(r.nextInt(64));
    }
    assertTrue(small.percentile(50) < 64);
  }

  @Test
  void runsEveryWorkloadAgainstALiveCluster() throws Exception {
    try (ClusterAccess c = new ClusterAccess(tmp, 3).startAll()) {
      c.leaderId();
      List<URI> eps = c.ids().stream().map(id -> URI.create(c.url(id))).toList();
      KeelClient.Options opts =
          new KeelClient.Options(
              eps, Optional.empty(), Duration.ofSeconds(1), Duration.ofSeconds(5));
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      PrintStream out = new PrintStream(buf, true, StandardCharsets.UTF_8);
      List<Bench.Result> results =
          List.of(
              Bench.run(
                  () -> new KeelClient(opts),
                  Bench.Workload.WRITE,
                  4,
                  50,
                  64,
                  Duration.ofMillis(200),
                  Duration.ofMillis(700)),
              Bench.run(
                  () -> new KeelClient(opts),
                  Bench.Workload.READ_LINEARIZABLE,
                  4,
                  50,
                  64,
                  Duration.ofMillis(200),
                  Duration.ofMillis(700)),
              Bench.run(
                  () -> new KeelClient(opts),
                  Bench.Workload.READ_LEASE,
                  4,
                  50,
                  64,
                  Duration.ofMillis(200),
                  Duration.ofMillis(700)));
      Bench.print(out, results);
      for (Bench.Result r : results) {
        assertTrue(r.ops() > 10, r.workload() + " did " + r.ops());
        assertEquals(0, r.errors());
        assertTrue(r.opsPerSecond() > 0);
      }
      String table = buf.toString(StandardCharsets.UTF_8);
      assertTrue(table.startsWith("workload"), table);
      assertTrue(table.contains("read-lease"), table);
    }
  }
}
