package org.streamrune.core;

import java.util.List;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/** Persistence abstraction for append-only event streams with optional snapshots. */
public interface EventStore {

  /**
   * Loads the aggregate history. If a snapshot exists, returns the snapshot state together with
   * only the events recorded after that snapshot.
   */
  AggregateHistory load(StreamId streamId);

  /**
   * Appends new events to the stream. Throws {@link OptimisticLockException} if {@code
   * expectedVersion} does not exactly equal the stream's actual current version (head) — see
   * Conflict detection below.
   *
   * <p>Contract — every implementation must honor these semantics so callers can switch backends
   * without behavior changes:
   *
   * <ul>
   *   <li><b>Version assignment.</b> The store assigns versions {@code expectedVersion + 1 ..
   *       expectedVersion + n} to the {@code n} events in list order; the {@link
   *       EventEnvelope#version()} carried by the input envelopes is not consulted for persistence.
   *       Callers should still pre-set those same values, because the input envelopes flow on to
   *       interceptors and audit trails. A new stream starts at {@code expectedVersion =} {@link
   *       Version#initial()} (0), so its first event is stored as version 1.
   *   <li><b>Conflict detection — strict.</b> Fails with {@link OptimisticLockException} if and
   *       only if {@code expectedVersion} does not equal the stream's ACTUAL current head at the
   *       moment of the append, checked atomically with the append itself. A STALE {@code
   *       expectedVersion} (behind the head) and a FUTURE one (beyond the head) are both rejected
   *       identically — there is no way to leave a version gap or make a retroactive lower-numbered
   *       append through this method, and global append order and per-stream version order can
   *       never diverge as a result of one. An implementation MAY additionally enforce the {@code
   *       (streamId, version)} unique-constraint model (the computed versions {@code
   *       expectedVersion + 1 .. expectedVersion + n} must not already exist) as defense-in-depth
   *       against a writer that bypasses the strict check (e.g. direct SQL), but that check alone
   *       is NOT sufficient to satisfy this contract, because it does not reject a future {@code
   *       expectedVersion}. A stream that already contains a gap — rows written, deleted or
   *       restored outside the store, such a bypassing writer included — is not retroactively
   *       repaired by this method; an implementation SHOULD surface it as an operational diagnostic
   *       on {@link #load}, and MUST NOT renumber it automatically (reconstructing the intended
   *       history is a data-recovery decision outside this interface's scope).
   *   <li><b>Empty appends.</b> An empty {@code events} list is a no-op returning {@code new
   *       AppendResult(List.of(), expectedVersion)}. The no-op covers the EVENTS only — it never
   *       licenses silently skipping another side effect the same call was asked to perform. {@link
   *       #appendWithKey} still claims the idempotency key for a zero-event command, and an
   *       implementation overload that additionally accepts outbox entries (see {@code
   *       PostgresEventStore.append(StreamId, List, Version, List)}) must REJECT non-empty entries
   *       with {@link IllegalArgumentException} rather than discard them: those entries commit
   *       atomically with the events, so with no events there is nothing to commit them with.
   *   <li><b>Atomicity.</b> A multi-event append is all-or-nothing: either every event is persisted
   *       or none is.
   *   <li><b>No idempotence by default.</b> {@code metadata().eventId()} uniqueness is not
   *       enforced; re-appending an envelope with the same eventId stores a new event. Use {@link
   *       #appendWithKey} for an idempotent, effectively-once append: {@code appendWithKey} claims
   *       the {@link org.streamrune.core.types.IdempotencyKey} and appends events atomically so a
   *       redelivered call with the same key is a no-op.
   * </ul>
   *
   * @param streamId the stream to append to
   * @param events the events to append
   * @param expectedVersion the expected current version for optimistic locking
   * @return result containing the assigned global offsets for each event and the final version
   */
  AppendResult append(StreamId streamId, List<EventEnvelope> events, Version expectedVersion);

  /** Persists a snapshot of the aggregate state at the given version. */
  void saveSnapshot(StreamId streamId, Version version, AggregateState state);

  /**
   * Reserved {@code expectedSnapshotVersion} value: never consult a stored snapshot at all — the
   * read-side counterpart of {@link SnapshotPolicy#never()}. {@code snapshotState} is always {@code
   * null} and every event is returned, exactly as if the stream had no snapshot.
   *
   * <p><b>Why this exists.</b> {@code expectedSnapshotVersion == 0} ("skip the check, use any
   * stored snapshot") is the wrong read-side behavior for {@link SnapshotPolicy#never()}: switching
   * an actively-snapshotted aggregate to {@code never()} does not delete its existing snapshot
   * rows, so {@code 0} would keep silently hydrating from that increasingly stale snapshot — a
   * field added to the state class after the policy switch deserializes as {@code null} into every
   * future {@code decide()} call, forever, because {@code maybeSnapshot} also no-ops under {@code
   * never()} and can never overwrite it. {@code never()} must mean "off" on the read side too, not
   * merely "stop writing new ones."
   */
  int IGNORE_SNAPSHOT = -1;

  /**
   * Loads the aggregate history, ignoring the snapshot if its stored schema version does not match
   * {@code expectedSnapshotVersion}.
   *
   * <p>When {@code expectedSnapshotVersion} is {@code 0}, no version check is performed and any
   * stored snapshot is used (equivalent to {@link #load(StreamId)}). When it is {@link
   * #IGNORE_SNAPSHOT} ({@code -1}), no snapshot is read at all — see that constant's javadoc.
   * Stored snapshot schema versions start at {@code 1} (see {@link #saveSnapshot(StreamId, Version,
   * AggregateState, int)}), so neither {@code 0} nor {@link #IGNORE_SNAPSHOT} can ever collide with
   * a real stored version. Values below {@link #IGNORE_SNAPSHOT} are invalid; implementations must
   * reject them with {@link IllegalArgumentException} rather than treating them as a never-matching
   * version.
   *
   * <p>The default implementation supports {@code expectedSnapshotVersion == 0} AND {@code
   * expectedSnapshotVersion == }{@link #IGNORE_SNAPSHOT} identically — both delegate to {@link
   * #load(StreamId)} — and throws {@link UnsupportedOperationException} for any other value. This
   * matters because {@code SnapshotPolicy.never()} — the DEFAULT policy {@code
   * VirtualThreadCommandBus} uses — drives every load through {@link #IGNORE_SNAPSHOT}: an
   * implementation that never overrides this method (i.e. has no real snapshot storage to begin
   * with, so {@link #load(StreamId)} never returns a snapshot anyway) must keep working out of the
   * box for that default policy, exactly as it already did for {@code 0}. Implementations that DO
   * store snapshots — and therefore already override this method to honor {@code >= 1} — are the
   * ones that must give {@link #IGNORE_SNAPSHOT} its real, efficient meaning (never even read the
   * snapshot); see that constant's javadoc. Silently skipping the version check would hand
   * stale-schema snapshots to callers that explicitly asked for a version match.
   *
   * @param streamId the stream to load
   * @param expectedSnapshotVersion the required snapshot schema version (>= 1), {@code 0} to skip
   *     the check, or {@link #IGNORE_SNAPSHOT} to never consult a snapshot at all
   * @return aggregate history; if the snapshot version mismatches (or is ignored), {@code
   *     snapshotState} is {@code null} and all events are returned
   * @throws IllegalArgumentException if {@code expectedSnapshotVersion} is below {@link
   *     #IGNORE_SNAPSHOT}
   * @throws UnsupportedOperationException if {@code expectedSnapshotVersion >= 1} and the
   *     implementation does not support snapshot schema versioning
   */
  default AggregateHistory load(StreamId streamId, int expectedSnapshotVersion) {
    if (expectedSnapshotVersion < IGNORE_SNAPSHOT) {
      throw new IllegalArgumentException(
          "expectedSnapshotVersion must be >= "
              + IGNORE_SNAPSHOT
              + " (IGNORE_SNAPSHOT skips reading any snapshot, 0 skips the version check), got "
              + expectedSnapshotVersion);
    }
    if (expectedSnapshotVersion != 0 && expectedSnapshotVersion != IGNORE_SNAPSHOT) {
      throw new UnsupportedOperationException(
          getClass().getName()
              + " does not support snapshot schema versioning — override"
              + " load(StreamId, int) to honor expectedSnapshotVersion "
              + expectedSnapshotVersion);
    }
    return load(streamId);
  }

  /**
   * Persists a snapshot of the aggregate state at the given version, tagged with the given snapshot
   * schema version.
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}: silently dropping
   * {@code snapshotVersion} would persist an untagged snapshot that a later versioned {@link
   * #load(StreamId, int)} could not validate. Implementations that store snapshot schema versions
   * must override this method and must reject {@code snapshotVersion < 1} with {@link
   * IllegalArgumentException} — {@code 0} is reserved by {@code load(StreamId, int)} as the
   * skip-the-check value and must never be persisted as a schema version.
   *
   * @param streamId the stream to snapshot
   * @param version the event version at which the snapshot was taken
   * @param state the state to persist
   * @param snapshotVersion the schema version of {@code state} (must be >= 1)
   * @throws IllegalArgumentException if {@code snapshotVersion < 1}
   * @throws UnsupportedOperationException if the implementation does not support snapshot schema
   *     versioning
   */
  default void saveSnapshot(
      StreamId streamId, Version version, AggregateState state, int snapshotVersion) {
    if (snapshotVersion < 1) {
      throw new IllegalArgumentException("snapshotVersion must be >= 1, got " + snapshotVersion);
    }
    throw new UnsupportedOperationException(
        getClass().getName()
            + " does not support snapshot schema versioning — override"
            + " saveSnapshot(StreamId, Version, AggregateState, int) to persist snapshotVersion");
  }

  /**
   * Returns the highest global offset assigned in this event store, or {@link GlobalOffset#initial}
   * if no events have been appended yet.
   *
   * <p>Used for subscription lag computation: health checks compare a subscription's checkpoint
   * against this value to detect consumers falling behind the head of the stream.
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}. Implementations
   * backed by a real store should override it (e.g. {@code SELECT max(global_offset)}).
   *
   * @return the highest assigned global offset, or {@link GlobalOffset#initial()} when the store is
   *     empty
   * @throws UnsupportedOperationException if the implementation does not track the global head
   */
  default GlobalOffset lastGlobalOffset() {
    throw new UnsupportedOperationException(
        getClass().getName() + " does not implement lastGlobalOffset()");
  }

  /**
   * Reads events from the global stream starting after the given offset.
   *
   * @param afterOffset read events with global_offset strictly greater than this value
   * @param maxCount maximum number of events to return
   * @return list of event envelopes ordered by global offset, empty if none available
   */
  List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount);

  /**
   * Reads events from a specific stream starting after the given version.
   *
   * @param streamId the stream to read from
   * @param afterVersion read events with version strictly greater than this value
   * @param maxCount maximum number of events to return
   * @return list of event envelopes ordered by version
   */
  List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount);

  /**
   * Result of an append operation containing assigned global offsets.
   *
   * @param globalOffsets the assigned global offsets, exactly one per appended event in the order
   *     the events were given (empty for an empty append); never null, stored as an immutable copy
   * @param finalVersion the version of the stream after appending
   */
  record AppendResult(List<GlobalOffset> globalOffsets, Version finalVersion) {

    public AppendResult {
      if (globalOffsets == null) {
        throw new IllegalArgumentException("globalOffsets is required");
      }
      globalOffsets = List.copyOf(globalOffsets);
      if (finalVersion == null) {
        throw new IllegalArgumentException("finalVersion is required");
      }
    }
  }

  /**
   * Idempotent, atomic append: claims {@code key} in the command inbox and appends {@code events}
   * in a single database transaction (claim → append → backfill offsets). If {@code key} was
   * already claimed by a prior call, nothing is appended and the previously recorded offsets and
   * version are returned with {@code alreadyApplied() == true}. An empty {@code events} list still
   * claims the key so a zero-event command is not re-run on redelivery.
   *
   * <p>The atomicity guarantee is the foundation of effectively-once delivery for saga commands:
   * the inbox row and the command's events commit together, so either both are visible or neither
   * is — a crash between claim and append can never leave a stale claimed-but-unapplied key.
   *
   * <p><b>An already-claimed key must be verified before its result is returned (MANDATORY).</b>
   * "Already claimed" is only a replay when the claiming row was recorded by the SAME {@code
   * commandType} on the SAME {@code streamId}. Implementations must call {@link
   * CommandInbox.InboxResult#requireBoundTo} on the recorded row before returning {@code
   * alreadyApplied() == true}, and so propagate its {@link IllegalArgumentException} on a mismatch.
   * Returning the recorded offsets unverified reports a fabricated success to a caller whose events
   * were never appended and never will be — the key is permanently claimed — and hands it another
   * aggregate's stream metadata. This is not merely the command bus's sequential pre-check
   * duplicated: two concurrent executions of one key both miss that pre-check, so this method is
   * the ONLY place their collision can be caught. The check is a pure comparison of two columns the
   * inbox has recorded {@code NOT NULL} since it was created, so it needs no schema change and no
   * backfill.
   *
   * <p>Implementations that do not support the command inbox throw {@link
   * UnsupportedOperationException} (the default).
   *
   * @throws IllegalArgumentException if {@code idempotencyKey} is already recorded against a
   *     different command type or a different stream
   */
  default IdempotentAppendResult appendWithKey(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      IdempotencyKey idempotencyKey,
      String commandType) {
    throw new UnsupportedOperationException(
        "This EventStore does not support idempotent append (command inbox)");
  }

  /**
   * Result of {@link #appendWithKey}: the events' offsets + final version, and whether the key was
   * already present (so no new events were appended).
   */
  record IdempotentAppendResult(
      List<GlobalOffset> globalOffsets, Version finalVersion, boolean alreadyApplied) {
    public IdempotentAppendResult {
      if (globalOffsets == null) {
        throw new IllegalArgumentException("globalOffsets is required");
      }
      globalOffsets = List.copyOf(globalOffsets);
      if (finalVersion == null) {
        throw new IllegalArgumentException("finalVersion is required");
      }
    }
  }
}
