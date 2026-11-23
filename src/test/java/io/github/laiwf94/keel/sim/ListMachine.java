package io.github.laiwf94.keel.sim;

import io.github.laiwf94.keel.replica.StateMachine;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** A state machine whose state is the ordered list of commands it applied. */
final class ListMachine implements StateMachine {
  final List<String> applied = new ArrayList<>();

  @Override
  public byte[] apply(long index, byte[] command) {
    applied.add(new String(command, StandardCharsets.UTF_8));
    return Long.toString(index).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void snapshot(OutputStream out) throws IOException {
    out.write(String.join("\n", applied).getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void restore(InputStream in) throws IOException {
    String s = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    applied.clear();
    if (!s.isEmpty()) {
      applied.addAll(Arrays.asList(s.split("\n", -1)));
    }
  }
}
