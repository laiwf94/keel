package io.github.laiwf94.keel.server;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Runs three nodes as separate JVMs and kills one with SIGKILL in the middle of a write load. */
class ProcessCrashTest {
  @TempDir Path tmp;

  @Test
  void sigkillMidLoadLosesNothingAndTheNodeRecoversFromItsFiles() throws Exception {
    try (ProcessCluster c = new ProcessCluster(tmp, 3).startAll()) {
      String victim = c.leader();
      FailureTest.Writers w = new FailureTest.Writers(c::client, 4);
      Thread.sleep(1_000);
      c.procs.get(victim).destroyForcibly().waitFor();
      Thread.sleep(1_000);
      c.launch(victim);
      Thread.sleep(1_000);
      w.close();
      assertTrue(w.acked.size() > 50, "only " + w.acked.size() + " writes acknowledged");
      FailureTest.verifyAll(c::client, w.acked);
      assertTrue(c.procs.get(victim).isAlive(), "the restarted node died");
    }
  }
}
