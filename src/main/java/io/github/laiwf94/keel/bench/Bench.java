package io.github.laiwf94.keel.bench;

import io.github.laiwf94.keel.client.KeelClient;
import io.github.laiwf94.keel.client.KeelException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Closed-loop load: a fixed number of clients, each sending its next request as soon as the last
 * one answers. A warmup period is run and discarded, then throughput and latency are measured over
 * a fixed window.
 */
public final class Bench {
  /** What each client does in a loop. */
  public enum Workload {
    WRITE,
    READ_LINEARIZABLE,
    READ_LEASE;

    String consistency() {
      return this == READ_LEASE ? "lease" : "linearizable";
    }
  }

  /** One measured run. */
  public record Result(
      Workload workload, int clients, long ops, long errors, Duration window, LatencyHistogram h) {
    public double opsPerSecond() {
      return ops / (window.toNanos() / 1e9);
    }
  }

  private Bench() {}

  /**
   * Runs one workload. {@code clients} supplies a new client per worker; each worker writes or
   * reads keys {@code bench-0 .. bench-(keys-1)} with values of {@code valueSize} bytes.
   */
  public static Result run(
      Supplier<KeelClient> clients,
      Workload workload,
      int workers,
      int keys,
      int valueSize,
      Duration warmup,
      Duration measure)
      throws InterruptedException {
    byte[] value = new byte[valueSize];
    ThreadLocalRandom.current().nextBytes(value);
    if (workload != Workload.WRITE) {
      preload(clients, keys, value);
    }
    long start = System.nanoTime();
    long measureFrom = start + warmup.toNanos();
    long end = measureFrom + measure.toNanos();
    AtomicLong errors = new AtomicLong();
    List<LatencyHistogram> perThread = new ArrayList<>();
    List<Thread> threads = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      LatencyHistogram h = new LatencyHistogram();
      perThread.add(h);
      KeelClient k = clients.get();
      threads.add(
          Thread.ofVirtual()
              .start(
                  () -> {
                    try (k) {
                      if (workload == Workload.WRITE) {
                        k.openSession();
                      }
                      ThreadLocalRandom r = ThreadLocalRandom.current();
                      while (true) {
                        long t0 = System.nanoTime();
                        if (t0 >= end) {
                          return;
                        }
                        String key = "bench-" + r.nextInt(keys);
                        try {
                          if (workload == Workload.WRITE) {
                            k.put(key, value);
                          } else {
                            k.get(key, workload.consistency());
                          }
                          long t1 = System.nanoTime();
                          if (t0 >= measureFrom && t1 <= end) {
                            h.record((t1 - t0) / 1_000);
                          }
                        } catch (KeelException e) {
                          if (t0 >= measureFrom) {
                            errors.incrementAndGet();
                          }
                        }
                      }
                    } catch (KeelException | InterruptedException e) {
                      errors.incrementAndGet();
                    }
                  }));
    }
    for (Thread t : threads) {
      t.join();
    }
    LatencyHistogram all = new LatencyHistogram();
    perThread.forEach(all::merge);
    return new Result(workload, workers, all.count(), errors.get(), measure, all);
  }

  private static void preload(Supplier<KeelClient> clients, int keys, byte[] value)
      throws InterruptedException {
    try (KeelClient k = clients.get()) {
      for (int i = 0; i < keys; i++) {
        try {
          k.put("bench-" + i, value);
        } catch (KeelException e) {
          throw new IllegalStateException("could not preload keys: " + e.getMessage(), e);
        }
      }
    }
  }

  public static String header() {
    return String.format(
        Locale.ROOT,
        "%-18s %7s %10s %9s %9s %9s %7s",
        "workload",
        "clients",
        "ops/s",
        "p50 ms",
        "p99 ms",
        "max ms",
        "errors");
  }

  public static String row(Result r) {
    return String.format(
        Locale.ROOT,
        "%-18s %7d %10.0f %9.2f %9.2f %9.2f %7d",
        r.workload().name().toLowerCase(Locale.ROOT).replace('_', '-'),
        r.clients(),
        r.opsPerSecond(),
        r.h().percentile(50) / 1000.0,
        r.h().percentile(99) / 1000.0,
        r.h().max() / 1000.0,
        r.errors());
  }

  public static void print(PrintStream out, List<Result> results) {
    out.println(header());
    results.forEach(r -> out.println(row(r)));
    out.flush();
  }
}
