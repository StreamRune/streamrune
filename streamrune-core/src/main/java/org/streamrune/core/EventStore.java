package org.streamrune.core;

import java.util.List;
import java.util.Objects;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StoredLength;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * Persistence abstraction for append-only event streams with optional snapshots.
 *
 * <h2>Read path</h2>
 *
 * <p>{@link #load}, {@link #readStream} and {@link #readGlobalStream} turn a stored event into its
 * {@link EventEnvelope} the same way, and every implementation provides the following on all three:
 *
 * <ul>
 *   <li><b>Schema version.</b> An append stores each event with a schema version: the {@link
 *       org.streamrune.core.upcasting.EventUpcaster#currentVersion() currentVersion()} of the
 *       store's upcaster for the event's type at the time of the append, {@code 1} for a type
 *       without one. It records the upcasters the writing store had, not the payload's shape (see
 *       {@link org.streamrune.core.upcasting.EventUpcaster#upcast}).
 *   <li><b>Upcasting.</b> A store built with event upcasters reads an event stored below its type's
 *       current version by passing the stored payload through the type's upcaster one version at a
 *       time, from the stored version up to the current one, and binding the result to the event
 *       class. An event stored at the current version is bound as stored.
 *   <li><b>Schema ahead.</b> An event stored above the version the store's upcasters reach for its
 *       type ({@code 1} for a type without an upcaster, whatever upcasters the store has for other
 *       types) is refused with {@link EventSchemaAheadException}: a store with upcasters this one
 *       lacks wrote it, and reading it would bind a payload of a newer shape to this reader's
 *       class.
 *   <li><b>Read failures.</b> A read that cannot turn a stored event into its envelope fails as a
 *       whole: it never skips the event, and never returns the events before it. The exception says
 *       whether a retry can succeed:
 *       <ul>
 *         <li>{@link UnknownEventTypeException}: the event's type does not resolve in the store's
 *             {@link EventTypeRegistry}.
 *         <li>{@link EventDeserializationException}: no retry can read the stored event. Its
 *             payload or metadata cannot be parsed or bound to the resolved class, an upcaster
 *             threw or returned data the class cannot bind, or the crypto layer refused it with
 *             {@link org.streamrune.core.crypto.CryptoMappingException}, for example for a class
 *             whose constructor rejects the tombstone a field of an erased subject reads as.
 *         <li>{@link EventSchemaAheadException}: see above; a store whose upcasters reach the
 *             stored version reads the event.
 *         <li>a plain {@link EventStoreException}, none of the subtypes above: the storage or a key
 *             store failed, with a database error or with a {@link
 *             org.streamrune.core.crypto.CryptoOperationException} that is not a {@code
 *             CryptoMappingException} (a key-store outage, a disabled or inaccessible key). A retry
 *             may succeed.
 *       </ul>
 *       Callers act on the split. The command bus dead-letters a command that fails with any of
 *       them, and counts toward its circuit breaker only the plain one (see {@link
 *       CommandFailureClassification}); the read-poison classifier of the subscriptions ({@code
 *       ReadPoisonClassifier} in {@code streamrune-runtime}) quarantines the first two after a
 *       bounded streak and retries the other two. A store that raised {@code
 *       EventDeserializationException} for a key-store outage would quarantine readable events, and
 *       one that raised a plain exception for a corrupt payload would retry it forever.
 * </ul>
 *
 * <h2>Interrupted writes</h2>
 *
 * <p>The process running a write can die at any point of it. A store over durable storage provides
 * the following to the process that starts over the same storage afterwards:
 *
 * <ul>
 *   <li>A write interrupted before it became durable leaves nothing of itself: no event of an
 *       append is readable, no key of an {@link #appendWithKey} is recorded, no outbox entry is
 *       stored, and a snapshot save leaves the snapshot stored before it, if any. The next append
 *       takes offsets above the head, and a reader paging by checkpoint visits every stored event
 *       once and in order.
 *   <li>A write interrupted after it became durable, before its caller learned so, is stored whole,
 *       as if it had returned.
 *   <li>An {@link #appendWithKey} repeated with the same key after either converges on exactly one
 *       effect: it stores its events once, or returns the recorded result with {@code
 *       alreadyApplied()} and stores nothing. A plain {@link #append} repeated after a durable
 *       interrupted one stores its events again: an append is not idempotent.
 * </ul>
 *
 * <h2>Several instances</h2>
 *
 * <p>Several store instances over the same storage, as the processes of one deployment run them,
 * keep among themselves what one instance keeps among the threads that call it: strict conflict
 * detection and atomicity of appends racing for one version, one offset per event for appends to
 * distinct streams, atomic visibility, paging by checkpoint and the global head while others
 * append, and one effect for concurrent executions of one idempotency key. A store whose storage
 * belongs to one instance, as an in-process store's does, says so.
 *
 * <h2>Decorators</h2>
 *
 * <p>A store that wraps another one (to observe, meter or fail its calls) forwards to the wrapped
 * store every method of this interface, the {@code default} methods included. The defaults of
 * {@code load(StreamId, int)}, {@code saveSnapshot}, {@code lastGlobalOffset()} and the two {@code
 * appendWithKey} methods do not reach the wrapped store's own implementation, and the default of
 * {@link #sagaFenceStorageIdentity()}, a wiring probe, reports a store that cannot fence a saga's
 * forward command: a command bus built over such a decorator is refused by a {@code SagaRunner}
 * whose forward fence is on. A decorator calls the wrapped store on the calling thread.
 *
 * <p>{@code ForwardingEventStore} in {@code streamrune-test} forwards all of them and is a base
 * class for such a decorator. A new method added to this interface comes with a {@code default}; a
 * decorator forwards it as well once it wants the wrapped store's answer.
 *
 * <h2>Transactional outbox</h2>
 *
 * <p>No method of this interface accepts outbox entries, and {@link
 * org.streamrune.core.outbox.OutboxStore} reads entries, records their delivery and has only a
 * standalone {@code save}: the transactional outbox is not reachable through the SPI. A store that
 * offers it does so in its own implementation, as {@code PostgresEventStore} does with its {@code
 * append(StreamId, List, Version, List)} overload, which stores the entries it is given, and with
 * the entries its configured {@link org.streamrune.core.outbox.OutboxEventMapper} derives from the
 * events of every {@link #append} and {@link #appendWithKey}. Such a store provides all of the
 * following:
 *
 * <ul>
 *   <li>The entries are written in the same atomic step as the events (for {@link #appendWithKey},
 *       as the inbox row too), into the storage the relay's {@code OutboxStore} reads: every entry
 *       is committed with the events or none is. An append that is refused or fails (a version
 *       conflict, a fence refusal, any error) stores no entry.
 *   <li>Entries given with an empty events list are rejected with {@link IllegalArgumentException},
 *       never dropped: they commit with the events, and there are none to commit them with (see
 *       Empty appends on {@link #append}).
 *   <li>Each entry carries, as its {@link org.streamrune.core.outbox.OutboxEntry#traceLink()
 *       traceLink}, the link to the span of the command that wrote its source event, {@link
 *       org.streamrune.core.tracing.SpanLink#of(EventMetadata)} of that event's metadata ({@link
 *       org.streamrune.core.tracing.SpanLink#NONE} when the command was not traced), replacing any
 *       link set before, so the relay's publish span links to that command. The entries given to
 *       one append all take the link of its first event: one append's events come from one command.
 * </ul>
 */
public interface EventStore {

  /**
   * The longest snapshot state type name, in code points: the width of the {@code
   * snapshot_store.state_type} column. A longer one is refused by {@link #requireStateType} before
   * anything is written.
   */
  int MAX_STATE_TYPE_LENGTH = 255;

  /**
   * Checks the type name a snapshot is stored under: present and at most {@value
   * #MAX_STATE_TYPE_LENGTH} characters, counted in code points as the {@code state_type
   * VARCHAR(255)} column counts it. Every {@link #saveSnapshot(StreamId, Version, AggregateState,
   * int, long)} implementation that stores a type name calls it before it writes anything, and
   * {@link SimpleEventTypeRegistry.Builder#registerState} calls it on registration.
   *
   * @param stateType the state type name
   * @return {@code stateType}
   * @throws NullPointerException if {@code stateType} is {@code null}
   * @throws IllegalArgumentException if it is longer than {@value #MAX_STATE_TYPE_LENGTH}
   *     characters; the message states the length and the maximum and does not echo the value
   */
  static String requireStateType(String stateType) {
    Objects.requireNonNull(stateType, "stateType is required");
    return StoredLength.requireAtMost(stateType, MAX_STATE_TYPE_LENGTH, "stateType");
  }

  /**
   * Loads the aggregate history. If a snapshot exists, returns the snapshot state together with
   * only the events recorded after that snapshot. The snapshot is used or discarded as {@link
   * #load(StreamId, int)} with {@code 0} describes, and each event is read as Read path in the
   * class javadoc describes.
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
   *       AppendResult(List.of(), expectedVersion)}, without consulting the stream's head: it never
   *       fails with {@link OptimisticLockException}, whatever {@code expectedVersion} is, because
   *       it carries no events to order against the stream. The conflict detection above applies to
   *       a non-empty list. The no-op covers the EVENTS only — it never licenses silently skipping
   *       another side effect the same call was asked to perform. {@link #appendWithKey} still
   *       claims the idempotency key for a zero-event command, and an implementation overload that
   *       additionally accepts outbox entries (see {@code PostgresEventStore.append(StreamId, List,
   *       Version, List)}, and Transactional outbox in the class javadoc) must REJECT non-empty
   *       entries with {@link IllegalArgumentException} rather than discard them: those entries
   *       commit atomically with the events, so with no events there is nothing to commit them
   *       with.
   *   <li><b>Atomicity.</b> A multi-event append is all-or-nothing: either every event is persisted
   *       or none is, and its events become readable together. No read ({@link #load}, {@link
   *       #readStream}, {@link #readGlobalStream}) returns some events of an append without the
   *       others.
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
   * {@code expectedSnapshotVersion} and the store cannot migrate it to that version. Each event is
   * read as Read path in the class javadoc describes.
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
   * <p><b>Using the stored snapshot.</b> A snapshot is a cache derived from the events, so a stored
   * snapshot the store cannot use is discarded: {@code snapshotState} is {@code null}, {@code
   * lastSnapshotVersion} is {@link Version#initial()} and every event of the stream is returned, as
   * if it had no snapshot. A snapshot whose stored schema version matches (or with {@code
   * expectedSnapshotVersion == 0}) is used when its stored state type resolves in the store's
   * {@link EventTypeRegistry} and its state deserializes as that type. On a mismatch, a store built
   * with {@link SnapshotMigration}s deserializes the state as its stored type and applies a
   * complete chain of steps from the stored version to exactly {@code expectedSnapshotVersion} (see
   * {@link SnapshotMigration}). A used snapshot, migrated or not, comes with only the events after
   * its version. The snapshot is discarded when:
   *
   * <ul>
   *   <li>its stored state type does not resolve in the registry (the state class was retired);
   *   <li>its version mismatches and no complete chain reaches {@code expectedSnapshotVersion}: the
   *       store has no migrations, a step is missing, a step goes past it, or the stored version is
   *       above it;
   *   <li>its state does not deserialize as its stored type;
   *   <li>a migration step throws, returns {@code null}, or returns an instance of another class
   *       than its declared {@link SnapshotMigration#toType()}.
   * </ul>
   *
   * <p>A crypto failure is not discarded. When deserializing or migrating the state of a snapshot
   * the store would use fails with a {@link org.streamrune.core.crypto.CryptoOperationException} in
   * the cause chain, the load fails, because replaying the events would meet the same encrypted
   * fields: with a plain {@link EventStoreException} when the chain carries a {@code
   * CryptoOperationException} that is not a {@link
   * org.streamrune.core.crypto.CryptoMappingException} (a key-store outage, a retry may succeed),
   * with {@link EventDeserializationException} otherwise (no retry can read it), as on the read
   * path. A snapshot discarded for its type or its version is not deserialized, so it raises no
   * crypto failure.
   *
   * @param streamId the stream to load
   * @param expectedSnapshotVersion the required snapshot schema version (>= 1), {@code 0} to skip
   *     the check, or {@link #IGNORE_SNAPSHOT} to never consult a snapshot at all
   * @return aggregate history; if the snapshot is ignored or discarded, {@code snapshotState} is
   *     {@code null} and all events are returned
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
   * raises the stream's generation, and a save whose generation is not the stream's writes nothing
   * and returns {@code false}. The next command loads after the purge, sees the new generation, and
   * its snapshot is stored.
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
   * <p>A store that reads the state back through an {@link EventTypeRegistry} stores with it a type
   * name that {@link EventTypeRegistry#resolveStateType} resolves back to the state's class (the
   * shipped stores use the simple name of the class), and refuses with {@link EventStoreException},
   * storing nothing, a state whose type name does not resolve or resolves to another class: the
   * snapshot could not be read back as the class it was saved from. A stored type that does not
   * resolve when the stream is loaded discards the snapshot (see {@link #load(StreamId, int)}).
   * Such a store first checks the type name with {@link #requireStateType}: a name longer than
   * {@value #MAX_STATE_TYPE_LENGTH} characters is refused with {@link IllegalArgumentException}.
   *
   * @param streamId the stream to snapshot
   * @param version the event version at which the snapshot was taken
   * @param state the state to persist
   * @param snapshotVersion the schema version of {@code state} (must be >= 1)
   * @param snapshotPurgeGeneration the purge generation of the load {@code state} was built from
   * @return {@code true} if the snapshot was stored, {@code false} if the stream's snapshot was
   *     purged since the load and nothing was written
   * @throws IllegalArgumentException if {@code snapshotVersion < 1}, {@code snapshotPurgeGeneration
   *     < 0}, or the state's type name is longer than {@value #MAX_STATE_TYPE_LENGTH} characters
   * @throws EventStoreException if the state's type does not resolve in the store's registry as its
   *     own class
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
   * Reads events from the global stream starting after the given offset. Each event is read as Read
   * path in the class javadoc describes.
   *
   * <p><b>Offsets.</b> An append assigns each event a global offset. Offsets are distinct and
   * increase in append order: within one append they follow the order of its events, and an append
   * that starts after another has returned successfully assigns greater offsets than that one. They
   * are not promised to be contiguous: an offset may be held by no readable event, permanently.
   *
   * <p><b>Contract: nothing appears behind a returned event.</b> Once this method has returned an
   * event at offset {@code X}, no event with an offset in ({@code afterOffset}, {@code X}] becomes
   * readable later. An offset that is assigned and not readable yet (an append in progress, a
   * transaction that has not committed) therefore bounds what a call returns: a store whose offsets
   * can become readable out of order returns no event above an offset that may still become
   * readable, as {@link #lastGlobalOffset()} reports no head above one.
   *
   * <p>A reader that keeps the offset of the last event it received as its checkpoint and reads on
   * after it therefore misses no event, whatever the holes. A polling subscription ({@code
   * PollingEventSubscription} in {@code streamrune-runtime}) relies on it: it reads page after page
   * from its checkpoint and ends a catch-up on an empty read, never on a short page, since a page
   * may stop short of a hole.
   *
   * @param afterOffset read events with global_offset strictly greater than this value
   * @param maxCount maximum number of events to return
   * @return list of event envelopes ordered by global offset, empty if none available
   */
  List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount);

  /**
   * Reads events from a specific stream starting after the given version. Each event is read as
   * Read path in the class javadoc describes.
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
   * already claimed by a prior call, nothing is appended and the recorded offsets and version are
   * returned with {@code alreadyApplied() == true}. An empty {@code events} list still claims the
   * key so a zero-event command is not re-run on redelivery; like an empty {@link #append} it does
   * not consult the stream's head, and it records {@code expectedVersion} as its final version.
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
   * <p>{@code commandType} is required and at most {@value CommandInbox#MAX_COMMAND_TYPE_LENGTH}
   * characters (code points), the width of the inbox column; implementations check it with {@link
   * CommandInbox#requireCommandType} before they write anything, so a longer one claims no key and
   * stores no event.
   *
   * @throws IllegalArgumentException if {@code idempotencyKey} is already recorded against a
   *     different command type or a different stream, or if {@code commandType} is longer than
   *     {@value CommandInbox#MAX_COMMAND_TYPE_LENGTH} characters
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
   * <p>The value is an identity token: it is compared with {@code equals} and never inspected.
   *
   * <p>Defaults to {@code null}. A decorator forwards it to the store it wraps (see the class
   * javadoc).
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
