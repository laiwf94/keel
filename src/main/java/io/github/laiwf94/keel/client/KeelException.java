package io.github.laiwf94.keel.client;

/** A request that failed. {@link #outcomeUnknown()} says whether a write may still have applied. */
public class KeelException extends Exception {
  private static final long serialVersionUID = 1L;
  private final int status;
  private final boolean outcomeUnknown;

  public KeelException(String message, int status, boolean outcomeUnknown) {
    super(message);
    this.status = status;
    this.outcomeUnknown = outcomeUnknown;
  }

  /** The HTTP status of the last response, or 0 if no response arrived. */
  public int status() {
    return status;
  }

  public boolean outcomeUnknown() {
    return outcomeUnknown;
  }

  /** A conditional write found a different version; nothing changed. */
  public static final class Conflict extends KeelException {
    private static final long serialVersionUID = 1L;
    private final long currentVersion;

    public Conflict(long currentVersion) {
      super("version conflict; current version " + currentVersion, 412, false);
      this.currentVersion = currentVersion;
    }

    public long currentVersion() {
      return currentVersion;
    }
  }
}
