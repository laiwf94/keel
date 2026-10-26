package io.github.laiwf94.keel;

import io.github.laiwf94.keel.client.KeelClient;
import io.github.laiwf94.keel.client.KeelException;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** The {@code load} and {@code verify} commands used by the demo. */
final class Workloads {
  private Workloads() {}

  /**
   * Writes unique keys from several clients until the duration ends, and appends every write the
   * cluster acknowledged to a file: {@code key, version, value} per line. Writes whose outcome is
   * unknown are counted but not recorded, because the cluster never promised them.
   */
  static int load(Main.Args a, PrintStream out)
      throws Main.UsageException, IOException, InterruptedException {
    Duration duration = a.duration("duration", "10s");
    int clients = a.integer("clients", 4);
    Path file = Path.of(a.get("out", "acked.log"));
    String prefix = a.get("prefix", "load");
    long end = System.nanoTime() + duration.toNanos();
    AtomicLong acked = new AtomicLong();
    AtomicLong unknown = new AtomicLong();
    AtomicLong errors = new AtomicLong();
    List<Thread> threads = new ArrayList<>();
    try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
      for (int c = 0; c < clients; c++) {
        KeelClient k = Main.client(a, a.get("token"));
        String keyPrefix = prefix + "-" + c + "-";
        threads.add(
            Thread.ofVirtual().start(() -> loadLoop(k, keyPrefix, end, w, acked, unknown, errors)));
      }
      long started = System.nanoTime();
      while (threads.stream().anyMatch(Thread::isAlive)) {
        Thread.sleep(1_000);
        out.printf(
            "t=%ds acked=%d unknown=%d errors=%d%n",
            Duration.ofNanos(System.nanoTime() - started).toSeconds(),
            acked.get(),
            unknown.get(),
            errors.get());
      }
    }
    out.printf(
        "load done: acked=%d unknown=%d errors=%d%n", acked.get(), unknown.get(), errors.get());
    return 0;
  }

  private static void loadLoop(
      KeelClient k,
      String keyPrefix,
      long end,
      BufferedWriter w,
      AtomicLong acked,
      AtomicLong unknown,
      AtomicLong errors) {
    try (k) {
      boolean session = false;
      long n = 0;
      while (System.nanoTime() < end) {
        try {
          if (!session) {
            k.openSession();
            session = true;
          }
          String key = keyPrefix + n++;
          String value = Main.randomValue();
          long version = k.put(key, value.getBytes(StandardCharsets.UTF_8));
          Main.appendLine(w, key + "\t" + version + "\t" + value, acked);
        } catch (KeelException e) {
          (e.outcomeUnknown() ? unknown : errors).incrementAndGet();
          if (e.status() == 410) {
            session = false;
          }
        }
      }
    } catch (IOException e) {
      errors.incrementAndGet();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Reads back every write in a load file with linearizable reads. Exit code 1 if any is lost. */
  static int verify(Main.Args a, PrintStream out)
      throws Main.UsageException, IOException, InterruptedException {
    Path file = Path.of(a.get("in", "acked.log"));
    List<String[]> writes = new ArrayList<>();
    for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
      String[] f = line.split("\t", -1);
      if (f.length != 3) {
        throw new Main.UsageException("bad line in " + file + ": " + line);
      }
      writes.add(f);
    }
    int workers = Math.max(1, Math.min(a.integer("clients", 8), writes.size()));
    AtomicLong present = new AtomicLong();
    AtomicLong lost = new AtomicLong();
    List<String> problems = java.util.Collections.synchronizedList(new ArrayList<>());
    List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < workers; t++) {
      KeelClient k = Main.client(a, a.get("token"));
      int offset = t;
      threads.add(
          Thread.ofVirtual()
              .start(
                  () -> {
                    try (k) {
                      for (int i = offset; i < writes.size(); i += workers) {
                        String[] f = writes.get(i);
                        Optional<KeelClient.Versioned> v = k.get(f[0], "linearizable");
                        if (v.isPresent() && v.get().text().equals(f[2])) {
                          present.incrementAndGet();
                        } else {
                          lost.incrementAndGet();
                          problems.add("LOST " + f[0] + " version " + f[1]);
                        }
                      }
                    } catch (KeelException e) {
                      problems.add("ERROR " + e.getMessage());
                      lost.incrementAndGet();
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                  }));
    }
    for (Thread t : threads) {
      t.join();
    }
    problems.stream().limit(20).forEach(out::println);
    out.printf("verify: checked=%d present=%d lost=%d%n", writes.size(), present.get(), lost.get());
    return lost.get() == 0 ? 0 : 1;
  }
}
