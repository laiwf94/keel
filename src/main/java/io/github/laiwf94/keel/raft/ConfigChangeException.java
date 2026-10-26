package io.github.laiwf94.keel.raft;

/** Thrown when a membership change is refused; nothing was changed. */
public final class ConfigChangeException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public ConfigChangeException(String message) {
    super(message);
  }
}
