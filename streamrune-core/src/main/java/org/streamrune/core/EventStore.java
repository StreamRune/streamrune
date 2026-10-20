package org.streamrune.core;

import java.util.List;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaOrigin;
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

  /**
   * Persists a snapshot of the aggregate state at the given version, at snapshot schema version
   * {@code 1}, of a stream whose snapshot was never purged. Calls {@link #saveSnapshot(StreamId,
   * Version, AggregateState, int)} with {@code 1}.
   *
   * @throws IllegalStateException if the stream's snapshot was purged (see {@link
   *     #saveSnapshot(StreamId, Version, AggregateState, int, long)})
   */
  default void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
    saveSnapshot(streamId, version, state, 1);
  }

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
   * schema version, of a stream whose snapshot was never purged: calls {@link
   * #saveSnapshot(StreamId, Version, AggregateState, int, long)} with purge generation {@code 0}.
   * Use it where no load stands behind the state (a test, a tool); the command bus uses the
   * purge-aware method with the generation of its load.
   *
   * @param streamId the stream to snapshot
   * @param version the event version at which the snapshot was taken
   * @param state the state to persist
   * @param snapshotVersion the schema version of {@code state} (must be >= 1)
   * @throws IllegalArgumentException if {@code snapshotVersion < 1}
   * @throws IllegalStateException if the stream's snapshot was purged, so the store refused a save
   *     at generation {@code 0}: load the stream and save with {@link
   *     AggregateHistory#snapshotPurgeGeneration()}
   */
  default void saveSnapshot(
      StreamId streamId, Version version, AggregateState state, int snapshotVersion) {
    if (snapshotVersion < 1) {
      throw new IllegalArgumentException("snapshotVersion must be >= 1, got " + snapshotVersion);
    }
    if (!saveSnapshot(streamId, version, state, snapshotVersion, 0)) {
      throw new IllegalStateException(
          "Snapshot of stream "
              + org.streamrune.core.types.LogSanitizer.sanitizeForLog(streamId.value())
              + " not stored: the stream's snapshot was purged by a GDPR forget, so a save at"
              + " purge generation 0 is refused. Load the stream and save with"
              + " AggregateHistory.snapshotPurgeGeneration().");
    }
  }

  /**
   * Persists a snapshot of a state built from a load, unless the stream's snapshot was purged since
   * that load. This is the method the command bus calls, and the one every store implements.
   *
   * <p><b>Why the purge generation.</b> A command loads its aggregate, decides and appends, and
   * snapshots the resulting state afterwards. A forget can run in between: it deletes the subject's
   * key and its {@link org.streamrune.core.gdpr.SnapshotPurger} purges the stream's snapshot. The
   * state the command loaded before the forget still holds whatever it copied in the clear from the
   * subject's {@code @Encrypted} fields, and saving it would put that data back into the store
   * after the forget reported the snapshot purged. {@code snapshotPurgeGeneration} is the
   * generation the command's load saw ({@link AggregateHistory#snapshotPurgeGeneration()}); a purge
   * raises the stream's generation, and a save whose generation is no longer the stream's writes
   * nothing and returns {@code false}. The next command loads after the purge, sees the new
   * generation, and its snapshot is stored.
   *
   * <p>A store whose snapshots a {@code SnapshotPurger} purges makes the comparison and the write
   * one atomic step against the purge (for {@code PostgresEventStore}, one conditional upsert on
   * the row the purge also writes). A store without a purge record returns {@code true} after
   * storing, whatever the generation. A store that keeps no snapshots throws {@link
   * UnsupportedOperationException}.
   *
   * <p>Every implementation must reject {@code snapshotVersion < 1} with {@link
   * IllegalArgumentException}: {@code 0} is reserved by {@link #load(StreamId, int)} as the
   * skip-the-check value and must never be persisted as a schema version.
   *
   * @param streamId the stream to snapshot
   * @param version the event version at which the snapshot was taken
   * @param state the state to persist
   * @param snapshotVersion the schema version of {@code state} (must be >= 1)
   * @param snapshotPurgeGeneration the purge generation of the load {@code state} was built from
   * @return {@code true} if the snapshot was stored, {@code false} if the stream's snapshot was
   *     purged since the load and nothing was written
   * @throws IllegalArgumentException if {@code snapshotVersion < 1} or {@code
   *     snapshotPurgeGeneration < 0}
   */
  boolean saveSnapshot(
      StreamId streamId,
      Version version,
      AggregateState state,
      int snapshotVersion,
      long snapshotPurgeGeneration);

  /**
   * Returns the head of the global stream: the highest global offset assigned in this event store
   * to an event that is readable, or {@link GlobalOffset#initial} if no events have been appended
   * yet.
   *
   * <p><b>Contract: nothing appears at or below the head.</b> No event at or below the returned
   * offset becomes readable later: an event that becomes readable after this method returned has a
   * greater offset than the one returned. An offset that is assigned and not readable yet (an
   * append in progress, a transaction that has not committed) is therefore not the head, and a
   * store whose offsets can become readable out of order reports the highest offset below which
   * nothing is still to come. A decorator that tracks the head itself keeps the same contract.
   *
   * <p>Two consumers read it:
   *
   * <ul>
   *   <li>subscription lag computation: health checks compare a subscription's checkpoint against
   *       this value to detect consumers falling behind the head of the stream;
   *   <li>the feed of the Server-Sent Events endpoint ({@code SseEventFeed} in {@code
   *       streamrune-runtime}): while no client is subscribed it moves its position to this value
   *       without reading the events up to it. It relies on the contract above: an event that
   *       turned readable at or below a head the feed had moved to would never be delivered, to a
   *       client that subscribed before it was stored either.
   * </ul>
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}. Implementations
   * backed by a real store should override it (e.g. {@code SELECT max(global_offset)} over
   * committed rows, where offsets are committed in the order they are assigned).
   *
   * @return the highest global offset of a readable event, below which no event becomes readable
   *     later, or {@link GlobalOffset#initial()} when the store is empty
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
   * {@link #appendWithKey(StreamId, List, Version, IdempotencyKey, String)} for a saga's forward
   * command, fenced against the saga's row when {@code forwardOf} is not {@code null}.
   *
   * <p><b>The fence.</b> When {@code idempotencyKey} is not yet claimed, the store reads the row of
   * the saga {@code forwardOf} names inside the append's own transaction, under a lock that every
   * write which stamps a compensation episode on that row, ends the saga or removes the row
   * conflicts with, and holds the lock until the transaction commits or rolls back. It refuses the
   * append with {@link SagaForwardFencedException} when the row carries an episode stamp, is
   * terminal ({@code COMPLETED}, {@code COMPENSATED}, {@code FAILED}), is of another saga type, or
   * is gone. A refused append stores nothing: no event, no inbox row (the key stays unclaimed), no
   * outbox or audit row. Every forward append of a saga therefore commits wholly before the write
   * that opens its compensation episode, or is refused. An empty {@code events} list is fenced too:
   * claiming the key is a durable effect.
   *
   * <p>A key that is already claimed is answered from the inbox before the fence is read, exactly
   * as the five-argument method answers it: the command it records committed earlier, and its
   * result stands.
   *
   * <p>With {@code forwardOf == null} this is the five-argument method, unfenced.
   *
   * <p>The default delegates to the five-argument method when {@code forwardOf} is {@code null} and
   * throws {@link UnsupportedOperationException} otherwise: a store that reports no saga rows
   * ({@link #sagaFenceStorageIdentity()} is {@code null}) cannot fence, and must not append
   * unfenced on a fenced call.
   *
   * @param forwardOf the saga whose forward command this append stores, or {@code null} for an
   *     unfenced append
   * @throws SagaForwardFencedException if the saga has left its forward phase
   * @throws UnsupportedOperationException if {@code forwardOf} is given and the store cannot fence
   */
  default IdempotentAppendResult appendWithKey(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      IdempotencyKey idempotencyKey,
      String commandType,
      SagaOrigin forwardOf) {
    if (forwardOf == null) {
      return appendWithKey(streamId, events, expectedVersion, idempotencyKey, commandType);
    }
    throw new UnsupportedOperationException(
        getClass().getName()
            + " cannot fence a saga's forward command: it reports no saga rows"
            + " to read (sagaFenceStorageIdentity() is null)");
  }

  /**
   * Identity of the saga rows this store fences a saga's forward command against (see {@link
   * #appendWithKey(StreamId, List, Version, IdempotencyKey, String, SagaOrigin)}), compared with
   * {@link org.streamrune.core.saga.SagaStore#storageIdentity()}: equal values mean the store reads
   * the rows that saga store writes. {@code null} when the store cannot fence. Cheap, no I/O.
   *
   * <p>Defaults to {@code null}.
   */
  default Object sagaFenceStorageIdentity() {
    return null;
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
