package io.github.laiwf94.keel.net;

import io.github.laiwf94.keel.codec.ByteReader;
import io.github.laiwf94.keel.codec.ByteWriter;
import io.github.laiwf94.keel.codec.CodecException;
import io.github.laiwf94.keel.raft.Codec;
import io.github.laiwf94.keel.raft.Entry;
import io.github.laiwf94.keel.raft.Message;
import io.github.laiwf94.keel.raft.Message.AppendRequest;
import io.github.laiwf94.keel.raft.Message.AppendResponse;
import io.github.laiwf94.keel.raft.Message.SnapshotRequest;
import io.github.laiwf94.keel.raft.Message.SnapshotResponse;
import io.github.laiwf94.keel.raft.Message.TimeoutNow;
import io.github.laiwf94.keel.raft.Message.VoteRequest;
import io.github.laiwf94.keel.raft.Message.VoteResponse;
import java.util.ArrayList;
import java.util.List;

/**
 * Binary encoding of Raft messages. Every field read from the wire is bounds-checked, and the
 * message records validate the rest, so malformed input ends in a {@link CodecException} and never
 * in an unchecked exception or a large allocation.
 */
public final class MessageCodec {
  private static final int VOTE_REQUEST = 1;
  private static final int VOTE_RESPONSE = 2;
  private static final int APPEND_REQUEST = 3;
  private static final int APPEND_RESPONSE = 4;
  private static final int SNAPSHOT_REQUEST = 5;
  private static final int SNAPSHOT_RESPONSE = 6;
  private static final int TIMEOUT_NOW = 7;

  /** Most entries accepted in one AppendEntries message. */
  public static final int MAX_ENTRIES = 100_000;

  private MessageCodec() {}

  public static byte[] encode(Message m) {
    ByteWriter w = new ByteWriter(64);
    switch (m) {
      case VoteRequest v -> {
        header(w, VOTE_REQUEST, m);
        w.writeLong(v.lastLogIndex()).writeLong(v.lastLogTerm()).writeBoolean(v.preVote());
        w.writeBoolean(v.transfer());
      }
      case VoteResponse v -> {
        header(w, VOTE_RESPONSE, m);
        w.writeBoolean(v.granted()).writeBoolean(v.preVote());
      }
      case AppendRequest a -> {
        header(w, APPEND_REQUEST, m);
        w.writeLong(a.prevLogIndex()).writeLong(a.prevLogTerm());
        w.writeLong(a.leaderCommit()).writeLong(a.seq());
        w.writeInt(a.entries().size());
        for (Entry e : a.entries()) {
          Codec.writeEntry(w, e);
        }
      }
      case AppendResponse a -> {
        header(w, APPEND_RESPONSE, m);
        w.writeBoolean(a.success()).writeLong(a.matchIndex()).writeLong(a.rejectedIndex());
        w.writeLong(a.conflictIndex()).writeLong(a.conflictTerm()).writeLong(a.seq());
      }
      case SnapshotRequest s -> {
        header(w, SNAPSHOT_REQUEST, m);
        w.writeLong(s.seq());
        Codec.writeSnapshot(w, s.snapshot());
      }
      case SnapshotResponse s -> {
        header(w, SNAPSHOT_RESPONSE, m);
        w.writeLong(s.matchIndex()).writeLong(s.seq());
      }
      case TimeoutNow t -> header(w, TIMEOUT_NOW, t);
    }
    return w.toByteArray();
  }

  private static void header(ByteWriter w, int type, Message m) {
    w.writeByte(type).writeString(m.from()).writeString(m.to()).writeLong(m.term());
  }

  public static Message decode(byte[] bytes) {
    ByteReader r = new ByteReader(bytes);
    int type = r.readByte();
    String from = r.readString(64);
    String to = r.readString(64);
    long term = r.readLong();
    Message m;
    try {
      m =
          switch (type) {
            case VOTE_REQUEST ->
                new VoteRequest(
                    from, to, term, r.readLong(), r.readLong(), r.readBoolean(), r.readBoolean());
            case VOTE_RESPONSE ->
                new VoteResponse(from, to, term, r.readBoolean(), r.readBoolean());
            case APPEND_REQUEST -> {
              long prevIndex = r.readLong();
              long prevTerm = r.readLong();
              long commit = r.readLong();
              long seq = r.readLong();
              int n = r.readCount(MAX_ENTRIES);
              List<Entry> entries = new ArrayList<>(Math.min(n, 1024));
              for (int i = 0; i < n; i++) {
                entries.add(Codec.readEntry(r));
              }
              yield new AppendRequest(from, to, term, prevIndex, prevTerm, entries, commit, seq);
            }
            case APPEND_RESPONSE ->
                new AppendResponse(
                    from,
                    to,
                    term,
                    r.readBoolean(),
                    r.readLong(),
                    r.readLong(),
                    r.readLong(),
                    r.readLong(),
                    r.readLong());
            case SNAPSHOT_REQUEST -> {
              long seq = r.readLong();
              yield new SnapshotRequest(from, to, term, Codec.readSnapshot(r), seq);
            }
            case SNAPSHOT_RESPONSE ->
                new SnapshotResponse(from, to, term, r.readLong(), r.readLong());
            case TIMEOUT_NOW -> new TimeoutNow(from, to, term);
            default -> throw new CodecException("unknown message type " + type);
          };
    } catch (IllegalArgumentException e) {
      throw new CodecException("invalid message: " + e.getMessage());
    }
    r.expectEnd();
    return m;
  }
}
