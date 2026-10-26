package io.github.laiwf94.keel.raft;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A server in the cluster.
 *
 * @param id stable node id
 * @param peerAddress host:port where other nodes reach its Raft transport
 * @param clientAddress host:port that clients use for its HTTP API
 * @param voter whether it counts toward quorums; learners receive the log but do not vote
 */
public record Member(String id, String peerAddress, String clientAddress, boolean voter) {
  private static final Pattern ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_.-]{0,62}");
  private static final Pattern ADDRESS = Pattern.compile("[A-Za-z0-9_.\\[\\]:-]{1,255}");

  public Member {
    validateId(id);
    Objects.requireNonNull(peerAddress, "peerAddress");
    Objects.requireNonNull(clientAddress, "clientAddress");
    if (!ADDRESS.matcher(peerAddress).matches()) {
      throw new IllegalArgumentException("bad peer address: " + peerAddress);
    }
    if (!ADDRESS.matcher(clientAddress).matches()) {
      throw new IllegalArgumentException("bad client address: " + clientAddress);
    }
  }

  public static String validateId(String id) {
    if (id == null || !ID.matcher(id).matches()) {
      throw new IllegalArgumentException("bad node id: " + id);
    }
    return id;
  }

  public Member asVoter() {
    return new Member(id, peerAddress, clientAddress, true);
  }
}
