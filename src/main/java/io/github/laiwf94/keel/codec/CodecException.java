package io.github.laiwf94.keel.codec;

/** Thrown when bytes from disk or the network do not decode to a valid value. */
public final class CodecException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public CodecException(String message) {
    super(message);
  }
}
