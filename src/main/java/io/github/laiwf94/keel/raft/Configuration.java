package io.github.laiwf94.keel.raft;

import io.github.laiwf94.keel.codec.ByteReader;
import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.codec.CodecException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The cluster membership. A configuration is either empty (a node waiting to be added) or has at
 * least one voter. Members are kept sorted by id so equal configurations encode identically.
 */
public record Configuration(List<Member> members) {
  public static final Configuration EMPTY = new Configuration(List.of());
  public static final int MAX_MEMBERS = 64;

  public Configuration {
    members = members.stream().sorted((a, b) -> a.id().compareTo(b.id())).toList();
    if (members.size() > MAX_MEMBERS) {
      throw new IllegalArgumentException("too many members: " + members.size());
    }
    Set<String> ids = new HashSet<>();
    boolean anyVoter = false;
    for (Member m : members) {
      if (!ids.add(m.id())) {
        throw new IllegalArgumentException("duplicate member " + m.id());
      }
      anyVoter |= m.voter();
    }
    if (!members.isEmpty() && !anyVoter) {
      throw new IllegalArgumentException("a configuration needs at least one voter");
    }
  }

  public static Configuration ofVoters(Collection<Member> voters) {
    return new Configuration(voters.stream().map(Member::asVoter).toList());
  }

  public boolean isEmpty() {
    return members.isEmpty();
  }

  public Optional<Member> member(String id) {
    return members.stream().filter(m -> m.id().equals(id)).findFirst();
  }

  public boolean contains(String id) {
    return member(id).isPresent();
  }

  public boolean isVoter(String id) {
    return member(id).map(Member::voter).orElse(false);
  }

  public List<String> voterIds() {
    return members.stream().filter(Member::voter).map(Member::id).toList();
  }

  public List<String> ids() {
    return members.stream().map(Member::id).toList();
  }

  /** Votes needed for a majority of voters. */
  public int quorum() {
    return voterIds().size() / 2 + 1;
  }

  /** Whether the given set of nodes includes a majority of voters. */
  public boolean hasQuorum(Set<String> nodes) {
    long n = members.stream().filter(m -> m.voter() && nodes.contains(m.id())).count();
    return n >= quorum();
  }

  /** Adds a non-voting learner. */
  public Configuration withLearner(Member m) {
    if (contains(m.id())) {
      throw new IllegalArgumentException(m.id() + " is already a member");
    }
    List<Member> next = new ArrayList<>(members);
    next.add(new Member(m.id(), m.peerAddress(), m.clientAddress(), false));
    return new Configuration(next);
  }

  /** Turns a learner into a voter. */
  public Configuration withPromoted(String id) {
    Member m = member(id).orElseThrow(() -> new IllegalArgumentException(id + " is not a member"));
    if (m.voter()) {
      throw new IllegalArgumentException(id + " is already a voter");
    }
    List<Member> next = new ArrayList<>(members);
    next.replaceAll(x -> x.id().equals(id) ? x.asVoter() : x);
    return new Configuration(next);
  }

  /** Removes a member, voter or learner. */
  public Configuration without(String id) {
    if (!contains(id)) {
      throw new IllegalArgumentException(id + " is not a member");
    }
    return new Configuration(members.stream().filter(m -> !m.id().equals(id)).toList());
  }

  public byte[] encode() {
    ByteWriter w = new ByteWriter();
    writeTo(w);
    return w.toByteArray();
  }

  public void writeTo(ByteWriter w) {
    w.writeInt(members.size());
    for (Member m : members) {
      w.writeString(m.id());
      w.writeString(m.peerAddress());
      w.writeString(m.clientAddress());
      w.writeBoolean(m.voter());
    }
  }

  public static Configuration decode(byte[] bytes) {
    ByteReader r = new ByteReader(bytes);
    Configuration c = readFrom(r);
    r.expectEnd();
    return c;
  }

  public static Configuration readFrom(ByteReader r) {
    int n = r.readCount(MAX_MEMBERS);
    List<Member> ms = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      String id = r.readString(64);
      String peer = r.readString(255);
      String client = r.readString(255);
      boolean voter = r.readBoolean();
      try {
        ms.add(new Member(id, peer, client, voter));
      } catch (IllegalArgumentException e) {
        throw new CodecException(e.getMessage());
      }
    }
    try {
      return new Configuration(ms);
    } catch (IllegalArgumentException e) {
      throw new CodecException(e.getMessage());
    }
  }
}
