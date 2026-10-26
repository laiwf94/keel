package io.github.laiwf94.keel.obs;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.DoubleSupplier;

/** Counters, gauges and histograms rendered in the Prometheus text exposition format. */
public final class Metrics {
  /** Anything that can render itself in the exposition format. */
  public sealed interface Metric permits Counter, Histogram, Gauge {
    void render(StringBuilder sb, String name);

    String help();

    String type();
  }

  /** A monotonically increasing count. */
  public static final class Counter implements Metric {
    private final String help;
    private final LongAdder value = new LongAdder();

    Counter(String help) {
      this.help = help;
    }

    public void inc() {
      value.increment();
    }

    public void add(long n) {
      value.add(n);
    }

    public long get() {
      return value.sum();
    }

    @Override
    public void render(StringBuilder sb, String name) {
      sb.append(name).append(' ').append(value.sum()).append('\n');
    }

    @Override
    public String help() {
      return help;
    }

    @Override
    public String type() {
      return "counter";
    }
  }

  /** A histogram of durations in seconds, with fixed buckets from 50 microseconds to 10 s. */
  public static final class Histogram implements Metric {
    private static final double[] BOUNDS = {
      0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
      2.5, 5, 10
    };
    private final String help;
    private final AtomicLongArray counts = new AtomicLongArray(BOUNDS.length + 1);
    private final DoubleAdder sum = new DoubleAdder();

    Histogram(String help) {
      this.help = help;
    }

    public void observeNanos(long nanos) {
      double seconds = nanos / 1e9;
      int i = 0;
      while (i < BOUNDS.length && seconds > BOUNDS[i]) {
        i++;
      }
      counts.incrementAndGet(i);
      sum.add(seconds);
    }

    public long count() {
      long n = 0;
      for (int i = 0; i < counts.length(); i++) {
        n += counts.get(i);
      }
      return n;
    }

    @Override
    public void render(StringBuilder sb, String name) {
      long cumulative = 0;
      for (int i = 0; i < BOUNDS.length; i++) {
        cumulative += counts.get(i);
        sb.append(name)
            .append("_bucket{le=\"")
            .append(format(BOUNDS[i]))
            .append("\"} ")
            .append(cumulative)
            .append('\n');
      }
      cumulative += counts.get(BOUNDS.length);
      sb.append(name).append("_bucket{le=\"+Inf\"} ").append(cumulative).append('\n');
      sb.append(name).append("_sum ").append(format(sum.sum())).append('\n');
      sb.append(name).append("_count ").append(cumulative).append('\n');
    }

    @Override
    public String help() {
      return help;
    }

    @Override
    public String type() {
      return "histogram";
    }
  }

  record Gauge(String help, DoubleSupplier value) implements Metric {
    @Override
    public void render(StringBuilder sb, String name) {
      sb.append(name).append(' ').append(format(value.getAsDouble())).append('\n');
    }

    @Override
    public String type() {
      return "gauge";
    }
  }

  private final Map<String, Metric> metrics = new ConcurrentSkipListMap<>();

  public Counter counter(String name, String help) {
    return (Counter) metrics.computeIfAbsent(name, n -> new Counter(help));
  }

  public Histogram histogram(String name, String help) {
    return (Histogram) metrics.computeIfAbsent(name, n -> new Histogram(help));
  }

  public void gauge(String name, String help, DoubleSupplier value) {
    metrics.put(name, new Gauge(help, value));
  }

  static String format(double d) {
    if (d == Math.rint(d) && Math.abs(d) < 1e15) {
      return Long.toString((long) d);
    }
    if (Double.isNaN(d)) {
      return "NaN";
    }
    if (Double.isInfinite(d)) {
      return d > 0 ? "+Inf" : "-Inf";
    }
    return BigDecimal.valueOf(d).stripTrailingZeros().toPlainString();
  }

  public String render() {
    StringBuilder sb = new StringBuilder(4096);
    for (Map.Entry<String, Metric> e : metrics.entrySet()) {
      Metric m = e.getValue();
      sb.append("# HELP ").append(e.getKey()).append(' ').append(m.help()).append('\n');
      sb.append("# TYPE ").append(e.getKey()).append(' ').append(m.type()).append('\n');
      m.render(sb, e.getKey());
    }
    return sb.toString();
  }
}
