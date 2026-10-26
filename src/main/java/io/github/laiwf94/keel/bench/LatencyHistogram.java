package io.github.laiwf94.keel.bench;

/**
 * A log-linear latency histogram in the style of HdrHistogram: values are grouped by their power of
 * two, and each power of two is split into 64 linear sub-buckets, so any recorded value is reported
 * within 1/64 (about 1.6%) of its true value. Values are in microseconds, up to about 4,000
 * seconds. Not thread-safe; give each thread its own and {@link #merge} them.
 */
public final class LatencyHistogram {
  private static final int SUB_BITS = 6;
  private static final int SUB = 1 << SUB_BITS;
  private static final int MAGNITUDES = 32;

  private final long[] counts = new long[MAGNITUDES * SUB];
  private long total;
  private long max;

  private static int index(long value) {
    if (value < SUB) {
      return (int) value;
    }
    int magnitude = 63 - Long.numberOfLeadingZeros(value) - SUB_BITS + 1;
    int sub = (int) (value >>> (magnitude - 1)) - SUB;
    return Math.min(magnitude * SUB + sub, MAGNITUDES * SUB - 1);
  }

  /** The largest value that maps to bucket {@code i}. */
  private static long upperBound(int i) {
    if (i < SUB) {
      return i;
    }
    int magnitude = i / SUB;
    long sub = (long) (i % SUB) + SUB;
    return ((sub + 1) << (magnitude - 1)) - 1;
  }

  public void record(long micros) {
    long v = Math.max(0, micros);
    counts[index(v)]++;
    total++;
    max = Math.max(max, v);
  }

  public void merge(LatencyHistogram other) {
    for (int i = 0; i < counts.length; i++) {
      counts[i] += other.counts[i];
    }
    total += other.total;
    max = Math.max(max, other.max);
  }

  public long count() {
    return total;
  }

  public long max() {
    return max;
  }

  /** The value at {@code percentile} (0 to 100), in microseconds; 0 when empty. */
  public long percentile(double percentile) {
    if (total == 0) {
      return 0;
    }
    long rank = Math.max(1, (long) Math.ceil(percentile / 100.0 * total));
    long seen = 0;
    for (int i = 0; i < counts.length; i++) {
      seen += counts[i];
      if (seen >= rank) {
        // The last bucket also holds every value past the range, so its bound means nothing.
        return i == counts.length - 1 ? max : Math.min(upperBound(i), max);
      }
    }
    return max;
  }
}
