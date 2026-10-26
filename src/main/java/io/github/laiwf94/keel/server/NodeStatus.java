package io.github.laiwf94.keel.server;

import io.github.laiwf94.keel.raft.Configuration;
import io.github.laiwf94.keel.raft.RaftNode;

/** A consistent snapshot of a node's state, published by the event loop after every round. */
public record NodeStatus(
    String id,
    RaftNode.Role role,
    long term,
    String leader,
    long commitIndex,
    long appliedIndex,
    long lastIndex,
    long snapshotIndex,
    Configuration config) {}
