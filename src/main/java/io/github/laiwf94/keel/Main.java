package io.github.laiwf94.keel;

import io.github.laiwf94.keel.client.KeelClient;
import io.github.laiwf94.keel.client.KeelException;
import io.github.laiwf94.keel.obs.Log;
import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.Member;
import io.github.laiwf94.keel.raft.RaftOptions;
import io.github.laiwf94.keel.server.KeelServer;
import io.github.laiwf94.keel.server.NodeLoop;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/** The {@code keel} command line: run a node, or talk to a cluster. */
public final class Main {
  static final String USAGE =
      """
      usage: keel <command> [options]

        server                     run a node (options or KEEL_* environment variables)
        put <key> <value>          write a value
        get <key>                  read a value (--consistency linearizable|lease|stale)
        delete <key>               delete a key
        status                     show the node's view of the cluster
        members add <id> <peer> <client> | members remove <id>
        load                       write unique keys and record every acknowledged write
        verify                     read back every write recorded by load

      client options: --endpoints http://h:p,...  --token T  (or KEEL_ENDPOINTS, KEEL_TOKEN)
      """;

  /** Command-line problems: printed with the usage and exit code 2. */
  static final class UsageException extends Exception {
    private static final long serialVersionUID = 1L;

    UsageException(String message) {
      super(message);
    }
  }

  /** Parsed {@code --flag value} options with environment fallbacks. */
  static final class Args {
    final List<String> positional = new ArrayList<>();
    final Map<String, String> flags = new HashMap<>();
    final Map<String, String> env;

    Args(String[] argv, int from, Map<String, String> env) throws UsageException {
      this.env = env;
      for (int i = from; i < argv.length; i++) {
        String a = argv[i];
        if (a.startsWith("--")) {
          String name = a.substring(2);
          if (name.equals("join")) {
            flags.put(name, "true");
            continue;
          }
          if (i + 1 >= argv.length) {
            throw new UsageException("--" + name + " needs a value");
          }
          flags.put(name, argv[++i]);
        } else {
          positional.add(a);
        }
      }
    }

    Optional<String> get(String flag) {
      String v = flags.get(flag);
      if (v == null) {
        v = env.get("KEEL_" + flag.toUpperCase(Locale.ROOT).replace('-', '_'));
      }
      return Optional.ofNullable(v).filter(s -> !s.isEmpty());
    }

    String get(String flag, String fallback) {
      return get(flag).orElse(fallback);
    }

    String require(String flag) throws UsageException {
      return get(flag).orElseThrow(() -> new UsageException("--" + flag + " is required"));
    }

    int integer(String flag, int fallback) throws UsageException {
      String v = get(flag, Integer.toString(fallback));
      try {
        return Integer.parseInt(v);
      } catch (NumberFormatException e) {
        throw new UsageException("--" + flag + " must be an integer");
      }
    }

    Duration duration(String flag, String fallback) throws UsageException {
      return parseDuration(get(flag, fallback), flag);
    }
  }

  private Main() {}

  public static void main(String[] argv) {
    int code = run(argv, System.getenv(), System.out, System.err);
    if (code != 0) {
      System.exit(code);
    }
  }

  static int run(String[] argv, Map<String, String> env, PrintStream out, PrintStream err) {
    if (argv.length == 0) {
      err.print(USAGE);
      return 2;
    }
    try {
      Args a = new Args(argv, 1, env);
      return switch (argv[0]) {
        case "server" -> server(a, out);
        case "put", "get", "delete", "status", "members" -> client(argv[0], a, out, err);
        case "load" -> Workloads.load(a, out);
        case "verify" -> Workloads.verify(a, out);
        default -> throw new UsageException("unknown command " + argv[0]);
      };
    } catch (UsageException e) {
      err.println("keel: " + e.getMessage());
      err.print(USAGE);
      return 2;
    } catch (KeelException e) {
      err.println("keel: " + e.getMessage() + (e.outcomeUnknown() ? " (outcome unknown)" : ""));
      return 1;
    } catch (IOException e) {
      err.println("keel: " + e.getMessage());
      return 1;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return 130;
    }
  }

  static Duration parseDuration(String s, String flag) throws UsageException {
    try {
      if (s.endsWith("ms")) {
        return Duration.ofMillis(Long.parseLong(s.substring(0, s.length() - 2)));
      }
      long n = Long.parseLong(s.substring(0, s.length() - 1));
      return switch (s.charAt(s.length() - 1)) {
        case 's' -> Duration.ofSeconds(n);
        case 'm' -> Duration.ofMinutes(n);
        case 'h' -> Duration.ofHours(n);
        default -> throw new NumberFormatException();
      };
    } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
      throw new UsageException("--" + flag + " must look like 500ms, 10s, 5m or 1h");
    }
  }

  /** Parses {@code id@peerHost:port@clientHost:port,...} into a configuration of voters. */
  static Configuration parseMembers(String spec) throws UsageException {
    List<Member> ms = new ArrayList<>();
    for (String part : spec.split(",", -1)) {
      String[] f = part.trim().split("@", -1);
      if (f.length != 3) {
        throw new UsageException("member must be id@peer-host:port@client-host:port: " + part);
      }
      try {
        ms.add(new Member(f[0], f[1], f[2], true));
      } catch (IllegalArgumentException e) {
        throw new UsageException(e.getMessage());
      }
    }
    try {
      return new Configuration(ms);
    } catch (IllegalArgumentException e) {
      throw new UsageException(e.getMessage());
    }
  }

  static InetSocketAddress bind(String hostPort, String flag) throws UsageException {
    int colon = hostPort.lastIndexOf(':');
    try {
      return new InetSocketAddress(
          hostPort.substring(0, colon), Integer.parseInt(hostPort.substring(colon + 1)));
    } catch (RuntimeException e) {
      throw new UsageException("--" + flag + " must be host:port");
    }
  }

  static KeelServer.Config serverConfig(Args a) throws UsageException, IOException {
    String id = a.require("id");
    try {
      Member.validateId(id);
    } catch (IllegalArgumentException e) {
      throw new UsageException(e.getMessage());
    }
    boolean join = Boolean.parseBoolean(a.get("join", "false"));
    Configuration initial = Configuration.EMPTY;
    if (!join) {
      initial = parseMembers(a.require("members"));
      if (!initial.contains(id)) {
        throw new UsageException("--members must include this node (" + id + ")");
      }
    }
    String secret = a.get("secret").orElse(null);
    if (secret == null && a.get("secret-file").isPresent()) {
      secret = Files.readString(Path.of(a.get("secret-file").get()), StandardCharsets.UTF_8).trim();
    }
    if (secret == null || secret.getBytes(StandardCharsets.UTF_8).length < 16) {
      throw new UsageException("--secret (or --secret-file) of at least 16 bytes is required");
    }
    int tickMs = a.integer("tick-ms", 25);
    int electionTicks = a.integer("election-ticks", 10);
    int heartbeatTicks = a.integer("heartbeat-ticks", 2);
    RaftOptions raft;
    try {
      raft = new RaftOptions(electionTicks, heartbeatTicks, 512, 1 << 20, 8192, 2);
    } catch (IllegalArgumentException e) {
      throw new UsageException(e.getMessage());
    }
    if (tickMs < 1) {
      throw new UsageException("--tick-ms must be positive");
    }
    Log.Level level;
    try {
      level = Log.Level.valueOf(a.get("log-level", "info").toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new UsageException("--log-level must be debug, info, warn or error");
    }
    return new KeelServer.Config(
        id,
        Path.of(a.get("data", "data/" + id)),
        bind(a.get("peer-bind", "0.0.0.0:7000"), "peer-bind"),
        bind(a.get("http-bind", "0.0.0.0:8080"), "http-bind"),
        initial,
        a.get("cluster-id", "keel"),
        secret,
        a.get("client-token"),
        a.get("admin-token"),
        raft,
        new NodeLoop.Settings(tickMs, 100_000, 4096),
        a.integer("snapshot-every", 10_000),
        a.duration("request-timeout", "5s"),
        a.duration("session-timeout", "10m").toMillis(),
        a.integer("max-sessions", 100_000),
        level);
  }

  private static int server(Args a, PrintStream out) throws UsageException, IOException {
    KeelServer.Config c = serverConfig(a);
    // Peer addresses are host names that can move (containers, rescheduled pods). The JVM caches
    // successful lookups for 30 s by default; keep that short so a moved peer is found again.
    java.security.Security.setProperty("networkaddress.cache.ttl", "5");
    CountDownLatch stopped = new CountDownLatch(1);
    // A node that cannot write to disk must stop at once rather than keep talking to peers.
    KeelServer s = KeelServer.start(c, out, () -> Runtime.getRuntime().halt(70));
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  s.close();
                  stopped.countDown();
                }));
    try {
      stopped.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return 0;
  }

  static KeelClient client(Args a, Optional<String> token) throws UsageException {
    List<URI> endpoints = new ArrayList<>();
    for (String e : a.get("endpoints", "http://127.0.0.1:8080").split(",", -1)) {
      try {
        URI u = URI.create(e.trim());
        if (u.getScheme() == null || u.getRawAuthority() == null) {
          throw new IllegalArgumentException();
        }
        endpoints.add(u);
      } catch (IllegalArgumentException ex) {
        throw new UsageException("bad endpoint " + e);
      }
    }
    return new KeelClient(
        new KeelClient.Options(
            endpoints, token, a.duration("timeout", "2s"), a.duration("retry-for", "10s")));
  }

  private static int client(String cmd, Args a, PrintStream out, PrintStream err)
      throws UsageException, KeelException, InterruptedException {
    List<String> p = a.positional;
    boolean admin = cmd.equals("members") && !p.isEmpty() && !p.get(0).equals("list");
    try (KeelClient c = client(a, admin ? a.get("admin-token") : a.get("token"))) {
      switch (cmd) {
        case "put" -> {
          need(p, 2, "put <key> <value>");
          out.println("version " + c.put(p.get(0), p.get(1).getBytes(StandardCharsets.UTF_8)));
        }
        case "get" -> {
          need(p, 1, "get <key>");
          var v = c.get(p.get(0), a.get("consistency", "linearizable"));
          if (v.isEmpty()) {
            err.println("not found");
            return 1;
          }
          out.println(v.get().text());
        }
        case "delete" -> {
          need(p, 1, "delete <key>");
          out.println("deleted version " + c.delete(p.get(0)));
        }
        case "status" -> out.println(c.status().trim());
        default -> {
          need(p, 1, "members add|remove ...");
          switch (p.get(0)) {
            case "add" -> {
              need(p, 4, "members add <id> <peer-host:port> <client-host:port>");
              out.println(c.addMember(p.get(1), p.get(2), p.get(3)).trim());
            }
            case "remove" -> {
              need(p, 2, "members remove <id>");
              out.println(c.removeMember(p.get(1)).trim());
            }
            default -> throw new UsageException("members add|remove");
          }
        }
      }
    }
    return 0;
  }

  static void need(List<String> p, int n, String usage) throws UsageException {
    if (p.size() < n) {
      throw new UsageException("usage: keel " + usage);
    }
  }

  /** Helpers shared by the workload commands. */
  static String randomValue() {
    byte[] b = new byte[16];
    ThreadLocalRandom.current().nextBytes(b);
    return HexFormat.of().formatHex(b);
  }

  static void appendLine(BufferedWriter w, String line, AtomicLong count) throws IOException {
    synchronized (w) {
      w.write(line);
      w.newLine();
      w.flush();
    }
    count.incrementAndGet();
  }
}
