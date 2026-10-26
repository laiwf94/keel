package io.github.laiwf94.keel.server;

import java.util.Optional;

/** The result of a read submitted to a {@link NodeLoop}. */
public sealed interface ReadResult<T> {
  /** The query ran against state that includes {@code appliedIndex}. */
  record Value<T>(T value, long appliedIndex) implements ReadResult<T> {}

  record NotLeader<T>(Optional<String> leaderHint) implements ReadResult<T> {}

  record Failed<T>(String reason) implements ReadResult<T> {}
}
