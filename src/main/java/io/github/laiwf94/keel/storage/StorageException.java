package io.github.laiwf94.keel.storage;

/**
 * A storage failure the node cannot recover from by itself: an I/O error, or data on disk that is
 * corrupt in a way that is not a torn final write. A node that sees one must stop, because it can
 * no longer keep the promises it made to its peers.
 */
public final class StorageException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public StorageException(String message) {
    super(message);
  }

  public StorageException(String message, Throwable cause) {
    super(message, cause);
  }
}
