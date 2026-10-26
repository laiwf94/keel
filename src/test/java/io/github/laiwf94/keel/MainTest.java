package io.github.laiwf94.keel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.laiwf94.keel.server.ClusterAccess;
import io.github.laiwf94.keel.server.KeelServer;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MainTest {
  @TempDir Path tmp;
  final ByteArrayOutputStream out = new ByteArrayOutputStream();
  final ByteArrayOutputStream err = new ByteArrayOutputStream();

  int run(Map<String, String> env, String... args) {
    out.reset();
    err.reset();
    return Main.run(
        args,
        env,
        new PrintStream(out, true, StandardCharsets.UTF_8),
        new PrintStream(err, true, StandardCharsets.UTF_8));
  }

  String out() {
    return out.toString(StandardCharsets.UTF_8);
  }

  String err() {
    return err.toString(StandardCharsets.UTF_8);
  }

  @Test
  void clientCommandsAgainstALiveCluster() throws Exception {
    try (ClusterAccess c = new ClusterAccess(tmp.resolve("c"), 3).tokens(null, "adm").startAll()) {
      c.leaderId();
      Map<String, String> env = Map.of("KEEL_ENDPOINTS", c.endpoints());
      assertEquals(0, run(env, "put", "greeting", "hello"));
      assertTrue(out().startsWith("version "), out());
      assertEquals(0, run(env, "get", "greeting", "--consistency", "lease"));
      assertEquals("hello\n", out());
      assertEquals(0, run(env, "delete", "greeting"));
      assertEquals(1, run(env, "get", "greeting"));
      assertEquals("not found\n", err());
      assertEquals(0, run(env, "status"));
      assertTrue(out().contains("\"members\""));
      assertEquals(
          0,
          run(env, "members", "add", "n9", "127.0.0.1:1", "127.0.0.1:2", "--admin-token", "adm"));
      assertEquals(0, run(env, "members", "remove", "n9", "--admin-token", "adm"));
      assertEquals(1, run(env, "members", "remove", "n9", "--admin-token", "wrong"));
      assertTrue(err().contains("invalid token"), err());
      assertEquals(2, run(env, "members", "rename"));
      assertEquals(2, run(env, "members"));
      assertEquals(2, run(env, "put", "only-key"));

      Path log = tmp.resolve("acked.log");
      assertEquals(
          0, run(env, "load", "--duration", "2s", "--clients", "3", "--out", log.toString()));
      assertTrue(out().contains("load done: acked="), out());
      long acked = Files.readAllLines(log).size();
      assertTrue(acked > 10, "only " + acked + " writes");
      assertEquals(0, run(env, "verify", "--in", log.toString()));
      assertTrue(out().contains("lost=0"), out());
      Files.writeString(log, "never-written\t1\tx\n", java.nio.file.StandardOpenOption.APPEND);
      assertEquals(1, run(env, "verify", "--in", log.toString()));
      assertTrue(out().contains("LOST never-written"), out());
      Files.writeString(log, "malformed\n", java.nio.file.StandardOpenOption.APPEND);
      assertEquals(2, run(env, "verify", "--in", log.toString()));
    }
  }

  @Test
  void usageErrors() {
    Map<String, String> env = Map.of();
    assertEquals(2, run(env));
    assertTrue(err().contains("usage: keel"));
    assertEquals(2, run(env, "frobnicate"));
    assertEquals(2, run(env, "get", "k", "--endpoints"));
    assertEquals(2, run(env, "get", "k", "--endpoints", "not a url"));
    assertEquals(2, run(env, "get", "k", "--endpoints", "relative/path"));
    assertEquals(2, run(env, "get", "k", "--timeout", "soon"));
    assertEquals(2, run(env, "load", "--clients", "many"));
    assertEquals(2, run(env, "server"));
    assertEquals(1, run(env, "verify", "--in", tmp.resolve("missing").toString()));
  }

  @Test
  void durationsAndMembers() throws Exception {
    assertEquals(Duration.ofMillis(250), Main.parseDuration("250ms", "x"));
    assertEquals(Duration.ofSeconds(3), Main.parseDuration("3s", "x"));
    assertEquals(Duration.ofMinutes(2), Main.parseDuration("2m", "x"));
    assertEquals(Duration.ofHours(1), Main.parseDuration("1h", "x"));
    assertThrows(Main.UsageException.class, () -> Main.parseDuration("3d", "x"));
    assertThrows(Main.UsageException.class, () -> Main.parseDuration("", "x"));
    assertEquals(List.of("a", "b"), Main.parseMembers("a@h:1@h:2, b@h:3@h:4").voterIds());
    assertThrows(Main.UsageException.class, () -> Main.parseMembers("a@h:1"));
    assertThrows(Main.UsageException.class, () -> Main.parseMembers("bad id@h:1@h:2"));
    assertThrows(Main.UsageException.class, () -> Main.parseMembers("a@h:1@h:2,a@h:3@h:4"));
    assertThrows(Main.UsageException.class, () -> Main.bind("nonsense", "x"));
  }

  @Test
  void serverConfigFromFlagsAndEnvironment() throws Exception {
    Map<String, String> env = new HashMap<>();
    env.put("KEEL_ID", "n1");
    env.put("KEEL_MEMBERS", "n1@127.0.0.1:7001@127.0.0.1:8081,n2@127.0.0.1:7002@127.0.0.1:8082");
    env.put("KEEL_SECRET", "sixteen bytes at least");
    env.put("KEEL_DATA", tmp.resolve("d").toString());
    KeelServer.Config c = Main.serverConfig(new Main.Args(new String[] {"server"}, 1, env));
    assertEquals("n1", c.id());
    assertEquals(2, c.initial().voterIds().size());
    assertEquals(8080, c.httpBind().getPort());
    assertEquals(Duration.ofSeconds(5), c.requestTimeout());

    Path secretFile = tmp.resolve("secret");
    Files.writeString(secretFile, "from a file, long enough\n");
    Map<String, String> joinEnv = new HashMap<>(env);
    joinEnv.remove("KEEL_SECRET");
    joinEnv.remove("KEEL_MEMBERS");
    joinEnv.put("KEEL_SECRET_FILE", secretFile.toString());
    KeelServer.Config j =
        Main.serverConfig(
            new Main.Args(
                new String[] {
                  "server",
                  "--join",
                  "--http-bind",
                  "127.0.0.1:9",
                  "--log-level",
                  "debug",
                  "--tick-ms",
                  "10"
                },
                1,
                joinEnv));
    assertTrue(j.initial().isEmpty());
    assertEquals("from a file, long enough", j.secret());
    assertEquals(9, j.httpBind().getPort());

    for (String[] bad :
        new String[][] {
          {"--id", "bad id"},
          {"--members", "n2@h:1@h:2"},
          {"--secret", "short"},
          {"--election-ticks", "1"},
          {"--tick-ms", "0"},
          {"--log-level", "loud"},
          {"--peer-bind", "x"},
        }) {
      String[] argv = new String[bad.length + 1];
      argv[0] = "server";
      System.arraycopy(bad, 0, argv, 1, bad.length);
      assertThrows(
          Main.UsageException.class,
          () -> Main.serverConfig(new Main.Args(argv, 1, env)),
          String.join(" ", bad));
    }
  }
}
