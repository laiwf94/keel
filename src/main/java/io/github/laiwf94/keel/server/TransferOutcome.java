package io.github.laiwf94.keel.server;

import java.util.Optional;

/** How a leadership transfer asked of a {@link NodeLoop} ended. */
public sealed interface TransferOutcome {
  /** {@code leader} leads in {@code term}; it is this node if it already led. */
  record Transferred(String leader, long term) implements TransferOutcome {}

  /** This node does not lead, so it cannot hand leadership over. */
  record NotLeader(Optional<String> leaderHint) implements TransferOutcome {}

  /** The request makes no sense for this cluster, for example the target is not a voter. */
  record Rejected(String reason) implements TransferOutcome {}

  /** The transfer was tried and did not finish in time; leadership may or may not have moved. */
  record Failed(String reason) implements TransferOutcome {}
}
