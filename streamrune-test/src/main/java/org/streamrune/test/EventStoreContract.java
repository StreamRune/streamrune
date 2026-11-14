package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventSchemaAheadException;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStore.AppendResult;
import org.streamrune.core.EventStore.IdempotentAppendResult;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdConstraints;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;

/**
 * The contract every {@link EventStore} must honour, so callers can switch backends without a
 * change in behaviour. Run by {@code InMemoryEventStoreContractTest}, {@code
 * InMemoryEventStoreWithoutInboxContractTest}, {@code MinimalEventStoreContractTest} and {@code
 * PostgresEventStoreContractTest}. To prove a custom store, extend it and implement the two
 * abstract hooks:
 *
 * <ul>
 *   <li>{@link #newStore(Setup)} — a store over empty storage, built with a {@link Setup}: the type
 *       registry, crypto engine, event upcasters and snapshot migrations a deployment configures.
 *   <li>{@link #reopen(EventStore, Setup)} — a new store over the same data as an earlier one,
 *       built with a possibly different setup: what a deployment restarted with other upcasters,
 *       migrations or crypto sees of the data stored before.
 * </ul>
 *
 * <p>and override, for a store that leaves a capability out, its {@code supports…()} hook, and, for
 * a store that keeps a purge record, {@link #purgeSnapshot(StreamId)}.
 *
 * <p>The promises, as the {@link EventStore} javadoc states them:
 *
 * <ol>
 *   <li><b>Version assignment.</b> An append stores its {@code n} events as versions {@code
 *       expectedVersion + 1 .. expectedVersion + n} in list order, whatever versions the envelopes
 *       carry; a new stream starts at {@link Version#initial()}. The result holds one global offset
 *       per event in the order given, and the final version.
 *   <li><b>Strict conflict detection.</b> An append whose {@code expectedVersion} is not the
 *       stream's head, behind it or beyond it, fails with {@link OptimisticLockException} and
 *       stores nothing, so a stream never has a version gap.
 *   <li><b>Empty appends.</b> An empty event list stores nothing and returns {@code new
 *       AppendResult(List.of(), expectedVersion)} without consulting the head.
 *   <li><b>Atomicity.</b> A multi-event append stores all of its events or none, also when several
 *       race for one version, and its events become readable together: while appends run, no read
 *       returns some events of one append without the others.
 *   <li><b>Concurrent appends.</b> Appends to distinct streams at the same time store every event
 *       once, under an offset no other event has, each stream holding versions {@code 1..n}; within
 *       one append the offsets follow the order of its events.
 *   <li><b>No idempotence by default.</b> Appending an envelope twice stores two events.
 *   <li><b>Reads.</b> {@link EventStore#load} returns a stream's events in version order; {@link
 *       EventStore#readStream} reads one stream strictly after a version, {@link
 *       EventStore#readGlobalStream} every stream strictly after a global offset, in append order;
 *       both return at most {@code maxCount} events. The same id value under another aggregate type
 *       is another stream. A reader that pages through {@link EventStore#readGlobalStream} from the
 *       offset of the last event it read visits every event once and in order, also while appends
 *       run, and {@link EventStore#readStream} pages through a stream of over a thousand events the
 *       same way. Every field of an event's metadata reads back as appended, on all three reads.
 *   <li><b>Identifiers.</b> A stream whose aggregate type and id are as long as {@link
 *       AggregateType} and {@link StreamId} allow, in ASCII, two-byte or supplementary-plane
 *       characters, is stored, read, snapshotted and keyed like any other. So is an event type of
 *       {@value EventType#MAX_LENGTH} characters; a command type of {@value
 *       CommandInbox#MAX_COMMAND_TYPE_LENGTH} characters is recorded and replayed, and a longer one
 *       is refused with {@link IllegalArgumentException} before anything is written.
 *   <li><b>Null arguments.</b> {@code append}, {@code load}, {@code readStream}, {@code
 *       readGlobalStream} and {@code saveSnapshot} refuse a {@code null} stream, event list,
 *       version or offset with {@link NullPointerException}, and {@code saveSnapshot} a {@code
 *       null} state with {@link IllegalArgumentException}; a refused call stores nothing.
 *   <li><b>Read path.</b> On all three reads: an append stores each event at the current version of
 *       the writing store's upcaster for its type ({@code 1} without one), whatever its payload; a
 *       store reads an event stored below its upcaster's version through each step up to it, and
 *       refuses one stored above the version it reaches with {@link EventSchemaAheadException}. A
 *       read that cannot turn a stored event into its envelope fails as a whole, with {@link
 *       UnknownEventTypeException} for a type the registry does not resolve, {@link
 *       EventDeserializationException} for an upcaster that throws or returns data the class cannot
 *       bind and for a tombstone the class rejects, and a plain {@link EventStoreException} for a
 *       key-store outage.
 *   <li><b>The global head.</b> {@link EventStore#lastGlobalOffset()} is {@link
 *       GlobalOffset#initial()} for an empty store and the offset of the last stored event after;
 *       nothing becomes readable at or below a head once it was returned, also while appends run
 *       concurrently.
 *   <li><b>Snapshots.</b> A saved snapshot is loaded with only the events after it; a later one
 *       replaces it; {@code load(streamId, n)} uses it only when its schema version is {@code n},
 *       uses any with {@code 0}, never reads it with {@link EventStore#IGNORE_SNAPSHOT}, and
 *       rejects {@code n} below that with {@link IllegalArgumentException}; the save that names no
 *       schema version stores schema version {@code 1}; a schema version below {@code 1} and a
 *       negative purge generation are rejected; a save at generation {@code 0} of a stream never
 *       purged is stored, and a store without a purge record stores a save at any generation. For a
 *       store with a purge record ({@link #purgeSnapshot(StreamId)}): a purge removes the snapshot
 *       and raises the generation, and a save from a load before the purge writes nothing.
 *   <li><b>Using a stored snapshot.</b> On a schema-version mismatch a complete chain of {@link
 *       SnapshotMigration}s migrates the snapshot, which is used with only the later events. It is
 *       discarded, and every event returned, when no complete chain reaches the expected version, a
 *       step throws, returns {@code null} or another class than its {@code toType()}, its state
 *       does not deserialize, or its stored type does not resolve. A crypto failure while
 *       deserializing or migrating a snapshot the store would use fails the load (a plain {@link
 *       EventStoreException} for a key-store outage, {@link EventDeserializationException} for a
 *       {@link CryptoMappingException}); a snapshot discarded for its type or version is not
 *       deserialized, so it raises none. A save whose state type name does not resolve to the
 *       state's own class is refused with {@link EventStoreException} and stores nothing.
 *   <li><b>Idempotent append.</b> {@link EventStore#appendWithKey(StreamId, List, Version,
 *       IdempotencyKey, String)} stores the events once: a replay of the key returns the recorded
 *       offsets and version with {@code alreadyApplied()} and stores nothing, whatever it carries;
 *       a zero-event command claims its key too, whatever its expected version, and records that
 *       version as its final version; concurrent executions of one key store once (one presented on
 *       another stream is refused rather than answered with the winner's result); a key recorded
 *       for another command type or stream is refused with {@link IllegalArgumentException}; a
 *       keyed append refused for its version leaves the key unclaimed. With no saga origin the
 *       six-argument method is the same append; a store that cannot fence ({@link
 *       EventStore#sagaFenceStorageIdentity()} is {@code null}) refuses an append that names one
 *       with {@link UnsupportedOperationException}. The fence itself is the contract of {@link
 *       SagaForwardFenceContract}.
 *   <li><b>Capabilities.</b> The javadoc lets a store leave four capabilities out, each answered
 *       with {@link UnsupportedOperationException}: the idempotent append, snapshots, snapshot
 *       schema versions ({@code load(streamId, n)} with {@code n >= 1}) and the global head. Each
 *       has a {@code supports…()} hook, {@code true} by default; a store that leaves one out
 *       overrides its hook to return {@code false}, its cases are skipped, and the contract checks
 *       that the store refuses it instead of answering wrongly.
 *   <li><b>Durability across a restart.</b> A store reopened over the same data ({@link
 *       #reopen(EventStore, Setup)}) reads the events, snapshots, purge records and recorded keys
 *       stored before, under the same offsets and versions, and continues every stream and the
 *       global order. It reads them with its own setup: its upcasters, migrations, registry and
 *       crypto engine.
 *   <li><b>Transactional outbox.</b> For a store that {@link #supportsTransactionalOutbox()}: an
 *       append stores the entries of its events with them, and an append that is refused or
 *       replayed stores none.
 *   <li><b>Interrupted writes.</b> For a store that {@link #supportsCrashInjection()}: a write
 *       killed at a {@link CrashPoint} before it is durable leaves nothing of itself for the
 *       restarted store to read (no event, recorded key or outbox entry, the earlier snapshot
 *       kept), the next append takes offsets above the head and paging still visits every event
 *       once; one killed after it is durable is stored whole; a keyed append repeated with its key
 *       after either stores its events once.
 *   <li><b>Several instances.</b> For a store that {@link #supportsMultipleInstances()}: the
 *       concurrency cases hold with their workers split over two instances over the same data, the
 *       second one opened by {@link #reopen(EventStore, Setup)} and used alongside the first: one
 *       version raced, distinct streams, the order of an append's offsets, paging by checkpoint,
 *       atomic visibility, the global head and one key raced.
 * </ol>
 *
 * <p>The contract appends {@link ContractEvent}s under {@link #EVENT_TYPE} and saves {@link
 * ContractState} snapshots; the registry of {@link Setup#defaults()} resolves both, and a store
 * that resolves types through a registry must use the registry of its {@link Setup}.
 */
public abstract class EventStoreContract {

  /**
   * A new store over empty storage, built with {@code setup}; the suite calls it once before each
   * test with {@link Setup#defaults()} and may call it again with another setup.
   *
   * <p>Empty storage means no stored events, snapshots, purge records or command-inbox rows, and a
   * global offset counter that hands out {@code 1} to the first event appended, as a freshly
   * created database does. The store must resolve stored types through {@code setup.registry()},
   * protect {@code @Encrypted} fields with {@code setup.crypto()} when it is not {@code null},
   * upcast events read back through {@code setup.upcasters()} and migrate snapshots loaded back
   * through {@code setup.migrations()}, exactly as the store's production configuration would. When
   * {@link #supportsIdempotentAppend()} it has a command inbox, so {@code appendWithKey} records
   * its keys.
   */
  protected abstract EventStore newStore(Setup setup);

  /**
   * A new store instance over the same data as {@code store}, built with {@code setup} in place of
   * the setup {@code store} was built with; {@code store} is not used again by the case.
   *
   * <p>It models a deployment restarted with another configuration over the same database: the new
   * store reads every event, snapshot, purge record and command-inbox row {@code store} stored,
   * with the offsets and versions it stored them under, and its next append continues the global
   * order. Nothing stored is rewritten: each event keeps the schema version it was appended with,
   * so the new store's upcasters, migrations, registry and crypto engine apply to it on read as
   * they would in that deployment. It must not empty the storage.
   *
   * <p>When {@link #supportsMultipleInstances()}, an {@code …AcrossInstances} case keeps using
   * {@code store} alongside the new store, as two processes of one deployment over one database:
   * the new store must then share no state with {@code store} but the storage, as a store built in
   * another process would not.
   */
  protected abstract EventStore reopen(EventStore store, Setup setup);

  /** Whether the store implements {@code appendWithKey}; {@code true} by default. */
  protected boolean supportsIdempotentAppend() {
    return true;
  }

  /** Whether the store keeps snapshots; {@code true} by default. */
  protected boolean supportsSnapshots() {
    return true;
  }

  /**
   * Whether the store honours a snapshot schema version ({@code load(streamId, n)} with {@code n >=
   * 1}); {@code true} by default.
   *
   * <p>A store that keeps snapshots overrides {@code load(StreamId, int)} even when it returns
   * {@code false} here: the default of that method sends {@link EventStore#IGNORE_SNAPSHOT} to
   * {@code load(StreamId)}, which returns the stored snapshot, and the javadoc of {@code
   * IGNORE_SNAPSHOT} requires such a store never to read it. Without the override {@code
   * loadIgnoringSnapshotsReadsEveryEvent} fails, as it should.
   */
  protected boolean supportsSnapshotSchemaVersions() {
    return true;
  }

  /** Whether the store reports {@code lastGlobalOffset()}; {@code true} by default. */
  protected boolean supportsLastGlobalOffset() {
    return true;
  }

  /**
   * Whether several instances of the store can run over the same storage, as the processes of one
   * deployment do (Several instances in the {@link EventStore} javadoc); {@code true} by default.
   * The {@code …AcrossInstances} cases then open a second instance with {@link #reopen(EventStore,
   * Setup)} and split the workers of each concurrency case between the two. A store whose storage
   * belongs to the instance that holds it, as an in-process store's does, returns {@code false},
   * and those cases are skipped: there is no second process to race.
   */
  protected boolean supportsMultipleInstances() {
    return true;
  }

  /**
   * Whether the store writes outbox entries with its appends (Transactional outbox in the {@link
   * EventStore} javadoc); {@code false} by default, since no method of the SPI reaches the outbox
   * and the suite cannot see the entries without {@link #outboxOf(EventStore)}. A store that
   * returns {@code true} is built by {@link #newStore(Setup)} and {@link #reopen(EventStore,
   * Setup)} with {@link #OUTBOX_MAPPER}, over empty outbox storage in the case of {@code newStore}.
   */
  protected boolean supportsTransactionalOutbox() {
    return false;
  }

  /**
   * Whether the suite can make a write of the store die at each {@link CrashPoint} with {@link
   * #crashAt(EventStore, CrashPoint)}; {@code false} by default, since dying the way a killed
   * process dies takes a hook into the store's storage that only its own test can give. A store
   * that returns {@code true} provides Interrupted writes of the {@link EventStore} javadoc, and
   * the crash cases check it.
   */
  protected boolean supportsCrashInjection() {
    return false;
  }

  /**
   * Arms {@code store} so that its next write that reaches {@code point} dies there as it would if
   * the process running it were killed: the storage loses the write's session, as it loses a dead
   * process's, and the call throws. Only that write dies; the suite then opens the store a
   * restarted process would, with {@link #reopen(EventStore, Setup)}, and checks what the write
   * left; it does not judge what the call throws. Called only when {@link
   * #supportsCrashInjection()}, and only for {@link CrashPoint#AFTER_FIRST_OUTBOX_WRITE} when
   * {@link #supportsTransactionalOutbox()}.
   */
  protected void crashAt(EventStore store, CrashPoint point) {
    throw new UnsupportedOperationException("the suite cannot crash a write of the store");
  }

  /**
   * A point inside a write of a store at which the process running it can die. A store maps each to
   * the moment of its own write that matches the description.
   */
  public enum CrashPoint {
    /** A keyed append has recorded its key; nothing else of it is written. */
    AFTER_KEY_CLAIM,
    /** An append has taken the global offsets of its events; none of its events is written. */
    AFTER_OFFSET_RESERVATION,
    /** An append has written the first of its events and not the others. */
    AFTER_FIRST_EVENT_WRITE,
    /** An append has written its events and the first of its outbox entries, and is not durable. */
    AFTER_FIRST_OUTBOX_WRITE,
    /** An append is durable; its caller never learns so and sees the call fail. */
    AFTER_COMMIT,
    /** A snapshot save is writing the snapshot and is not durable. */
    DURING_SNAPSHOT_SAVE,
    /** A snapshot save is durable; its caller never learns so and sees the call fail. */
    AFTER_SNAPSHOT_COMMIT
  }

  /**
   * The {@link OutboxStore} the relay reads the entries {@code store} writes through; called only
   * when {@link #supportsTransactionalOutbox()}. The suite reads it with {@link
   * OutboxStore#findByStatus}, which claims nothing.
   */
  protected OutboxStore outboxOf(EventStore store) {
    throw new UnsupportedOperationException("the store writes no outbox entries");
  }

  /**
   * Purges the snapshot of {@code streamId} as the store's {@code SnapshotPurger} does, raising the
   * stream's purge generation, and returns {@code true}. The default returns {@code false}: the
   * store keeps no purge record, so the purge cases are skipped and the case of a store without a
   * purge record runs instead. The suite also calls it on a stream with no snapshot to learn which
   * of the two the store is.
   */
  protected boolean purgeSnapshot(StreamId streamId) {
    return false;
  }

  /**
   * How a store is configured: what {@link #newStore(Setup)} and {@link #reopen(EventStore, Setup)}
   * build it with.
   *
   * @param registry resolves the stored event types by {@link EventType} name and snapshot state
   *     types by simple class name (required)
   * @param crypto the crypto engine of {@code @Encrypted} fields; {@code null} for none
   * @param upcasters the event upcasters, applied on read; empty for none
   * @param migrations the snapshot migrations, applied on load; empty for none
   */
  public record Setup(
      EventTypeRegistry registry,
      CryptoEngine crypto,
      List<EventUpcaster> upcasters,
      List<SnapshotMigration> migrations) {

    private static final EventTypeRegistry CONTRACT_REGISTRY =
        SimpleEventTypeRegistry.builder()
            .registerEvent(EVENT_TYPE, ContractEvent.class)
            .registerState(ContractState.class.getSimpleName(), ContractState.class)
            .build();

    public Setup {
      Objects.requireNonNull(registry, "registry is required");
      upcasters = List.copyOf(Objects.requireNonNull(upcasters, "upcasters is required"));
      migrations = List.copyOf(Objects.requireNonNull(migrations, "migrations is required"));
    }

    /**
     * The setup of every case that names none: a registry resolving {@link ContractEvent} under
     * {@link #EVENT_TYPE} and {@link ContractState} under its simple class name, no crypto engine,
     * no upcasters and no migrations.
     */
    public static Setup defaults() {
      return new Setup(CONTRACT_REGISTRY, null, List.of(), List.of());
    }

    /** This setup with {@code registry} in place of its registry. */
    public Setup withRegistry(EventTypeRegistry registry) {
      return new Setup(registry, crypto, upcasters, migrations);
    }

    /** This setup with {@code crypto} in place of its crypto engine; {@code null} for none. */
    public Setup withCrypto(CryptoEngine crypto) {
      return new Setup(registry, crypto, upcasters, migrations);
    }

    /** This setup with {@code upcasters} in place of its upcasters. */
    public Setup withUpcasters(List<EventUpcaster> upcasters) {
      return new Setup(registry, crypto, upcasters, migrations);
    }

    /** This setup with {@code migrations} in place of its snapshot migrations. */
    public Setup withMigrations(List<SnapshotMigration> migrations) {
      return new Setup(registry, crypto, upcasters, migrations);
    }
  }

  /** The event the contract appends. */
  public record ContractEvent(String value) implements DomainEvent {
    @JsonCreator
    public ContractEvent(@JsonProperty("value") String value) {
      this.value = value;
    }
  }

  /** The snapshot state the contract saves. */
  public record ContractState(String marker) implements AggregateState {
    @JsonCreator
    public ContractState(@JsonProperty("marker") String marker) {
      this.marker = marker;
    }
  }

  /**
   * The outbox mapper of a store that {@link #supportsTransactionalOutbox()}: one pending entry per
   * event, under the event's id, with the event's type as its payload type and the event's stream.
   */
  public static final OutboxEventMapper OUTBOX_MAPPER =
      event ->
          List.of(
              OutboxEntry.pending(
                  OutboxEntryId.of(event.metadata().eventId().value()),
                  "{}",
                  event.eventType().value(),
                  event.streamId()));

  /** The event type name the contract's events are stored under. */
  public static final EventType EVENT_TYPE = new EventType("ContractEvent");

  protected static final AggregateType ORDER = AggregateType.of("contract_order");
  protected static final StreamId A = StreamId.of(ORDER, AggregateId.of("order-A"));
  protected static final StreamId B = StreamId.of(ORDER, AggregateId.of("order-B"));

  /** The same id value as {@link #A}, under another aggregate type: a different stream. */
  protected static final StreamId A_OTHER_TYPE =
      StreamId.of(AggregateType.of("contract_invoice"), AggregateId.of("order-A"));

  protected static final String COMMAND_TYPE = "com.example.contract.PlaceOrder";
  protected static final String OTHER_COMMAND_TYPE = "com.example.contract.CancelOrder";

  protected EventStore store;

  /**
   * The second instance an {@code …AcrossInstances} case runs half of its workers on, opened over
   * the data of {@link #store} by {@link #withSecondInstance()}; {@link #store} itself otherwise.
   */
  private EventStore peer;

  @BeforeEach
  void freshStore() {
    store = newStore(Setup.defaults());
    peer = store;
  }

  /**
   * The store the {@code n}th worker of a concurrency case calls: {@link #store} for an even {@code
   * n}, the second instance for an odd one. Readers of a case call the second instance.
   */
  private EventStore instance(int n) {
    return n % 2 == 0 ? store : peer;
  }

  /**
   * Opens a second instance over the data of {@link #store}, used alongside it as another process
   * of the deployment would, or skips the case for a store whose storage belongs to one instance.
   */
  private void withSecondInstance() {
    assumeTrue(supportsMultipleInstances(), "the store's storage belongs to one instance");
    peer = reopen(store, Setup.defaults());
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────────────────────

  /** The envelope of one contract event, its payload naming the stream and {@code version}. */
  protected static EventEnvelope event(StreamId streamId, long version) {
    return EventStoreFixture.event(
        streamId,
        new Version(version),
        EVENT_TYPE,
        new ContractEvent(streamId.aggregateId().value() + "@" + version));
  }

  /** The envelopes of {@code count} events carrying versions {@code from .. from + count - 1}. */
  protected static List<EventEnvelope> events(StreamId streamId, long from, int count) {
    return LongStream.range(from, from + count).mapToObj(v -> event(streamId, v)).toList();
  }

  protected List<EventEnvelope> stored(StreamId streamId) {
    return store.readStream(streamId, Version.initial(), 10_000);
  }

  protected List<EventEnvelope> global() {
    return store.readGlobalStream(GlobalOffset.initial(), 10_000);
  }

  /**
   * The ids of the pending outbox entries of {@code store}, in the order the store returns them.
   */
  protected List<String> outboxIds(EventStore store) {
    return outboxOf(store).findByStatus(OutboxStatus.PENDING, 10_000).stream()
        .map(entry -> entry.id().value())
        .toList();
  }

  /** The ids of {@code events}, as their outbox entries carry them. */
  protected static List<String> entryIds(List<EventEnvelope> events) {
    return events.stream().map(e -> e.metadata().eventId().value()).toList();
  }

  protected static List<Long> versions(List<EventEnvelope> events) {
    return events.stream().map(e -> e.version().value()).toList();
  }

  protected static List<GlobalOffset> offsets(List<EventEnvelope> events) {
    return events.stream().map(EventEnvelope::globalOffset).toList();
  }

  protected static List<String> values(List<EventEnvelope> events) {
    return events.stream().map(e -> ((ContractEvent) e.event()).value()).toList();
  }

  protected static void assertStrictlyIncreasing(List<GlobalOffset> offsets) {
    for (int i = 1; i < offsets.size(); i++) {
      assertTrue(
          offsets.get(i).value() > offsets.get(i - 1).value(),
          "global offsets must increase: " + offsets);
    }
  }

  protected IdempotentAppendResult appendWithKey(
      StreamId streamId, long head, int count, IdempotencyKey key, String commandType) {
    return store.appendWithKey(
        streamId, events(streamId, head + 1, count), new Version(head), key, commandType);
  }

  /**
   * Releases {@code workers} together, each on a thread of its own, and waits for all of them,
   * calling {@code poll} on the calling thread in the meantime when it is not {@code null}. Fails
   * when they have not finished within 60 seconds and rethrows the first failure of a worker; the
   * threads are stopped either way.
   */
  private static void runConcurrently(List<Callable<?>> workers, Runnable poll) throws Exception {
    var start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(workers.size());
    try {
      List<Future<?>> done = new ArrayList<>();
      for (Callable<?> worker : workers) {
        done.add(
            pool.submit(
                () -> {
                  start.await();
                  return worker.call();
                }));
      }
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      start.countDown();
      if (poll != null) {
        while (!done.stream().allMatch(Future::isDone)) {
          if (System.nanoTime() > deadline) {
            fail("the workers did not finish within 60 seconds");
          }
          poll.run();
        }
      }
      for (var worker : done) {
        worker.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
  }

  private static List<EventId> eventIds(List<EventEnvelope> events) {
    return events.stream().map(e -> e.metadata().eventId()).toList();
  }

  // ── Version assignment ──────────────────────────────────────────────────────────────────────

  @Test
  public void aFirstAppendStoresVersionsFromOneInListOrder() {
    AppendResult result = store.append(A, events(A, 1, 3), Version.initial());

    assertEquals(new Version(3), result.finalVersion());
    assertEquals(List.of(1L, 2L, 3L), versions(stored(A)));
    assertEquals(List.of("order-A@1", "order-A@2", "order-A@3"), values(stored(A)));
  }

  @Test
  public void versionsContinueFromTheExpectedVersion() {
    store.append(A, events(A, 1, 2), Version.initial());

    AppendResult result = store.append(A, events(A, 3, 2), new Version(2));

    assertEquals(new Version(4), result.finalVersion());
    assertEquals(List.of(1L, 2L, 3L, 4L), versions(stored(A)));
  }

  @Test
  public void theVersionsTheEnvelopesCarryAreNotConsulted() {
    var misnumbered = List.of(event(A, 7), event(A, 7), event(A, 42));

    AppendResult result = store.append(A, misnumbered, Version.initial());

    assertEquals(new Version(3), result.finalVersion());
    assertEquals(List.of(1L, 2L, 3L), versions(stored(A)));
  }

  @Test
  public void anAppendReturnsOneGlobalOffsetPerEventInOrder() {
    AppendResult result = store.append(A, events(A, 1, 3), Version.initial());

    assertEquals(3, result.globalOffsets().size());
    assertStrictlyIncreasing(result.globalOffsets());
    assertEquals(result.globalOffsets(), offsets(stored(A)));
    assertEquals(result.globalOffsets(), offsets(global()));
  }

  @Test
  public void globalOffsetsIncreaseAcrossStreamsInAppendOrder() {
    AppendResult first = store.append(A, events(A, 1, 2), Version.initial());
    AppendResult second = store.append(B, events(B, 1, 2), Version.initial());

    assertTrue(
        second.globalOffsets().getFirst().value() > first.globalOffsets().getLast().value(),
        "a later append has greater offsets, whichever stream it writes");
  }

  @Test
  public void theOffsetsOfAnAppendAreTheOffsetsItsEventsReadBackWith() {
    AppendResult result = store.append(A, events(A, 1, 2), Version.initial());

    assertEquals(result.globalOffsets(), offsets(store.load(A).events()));
    assertEquals(result.globalOffsets(), offsets(stored(A)));
    assertEquals(result.globalOffsets(), offsets(global()));
  }

  // ── Strict conflict detection ───────────────────────────────────────────────────────────────

  @Test
  public void aStaleExpectedVersionIsRejectedAndStoresNothing() {
    store.append(A, events(A, 1, 2), Version.initial());

    assertThrows(
        OptimisticLockException.class, () -> store.append(A, events(A, 2, 1), new Version(1)));

    assertEquals(List.of(1L, 2L), versions(stored(A)));
    assertEquals(2, global().size());
  }

  @Test
  public void aFutureExpectedVersionIsRejectedAndStoresNothing() {
    store.append(A, events(A, 1, 2), Version.initial());

    assertThrows(
        OptimisticLockException.class, () -> store.append(A, events(A, 4, 1), new Version(3)));

    assertEquals(List.of(1L, 2L), versions(stored(A)));
    assertEquals(2, global().size());
  }

  @Test
  public void aFutureExpectedVersionOnANewStreamIsRejected() {
    assertThrows(
        OptimisticLockException.class, () -> store.append(A, events(A, 2, 1), new Version(1)));

    assertTrue(stored(A).isEmpty());
    assertTrue(global().isEmpty());
  }

  @Test
  public void afterAConflictTheStreamContinuesWithoutAGap() {
    store.append(A, events(A, 1, 1), Version.initial());
    assertThrows(
        OptimisticLockException.class, () -> store.append(A, events(A, 6, 1), new Version(5)));

    store.append(A, events(A, 2, 2), new Version(1));

    assertEquals(List.of(1L, 2L, 3L), versions(stored(A)));
  }

  // ── Empty appends ───────────────────────────────────────────────────────────────────────────

  @Test
  public void anEmptyAppendStoresNothingAndReturnsTheExpectedVersion() {
    AppendResult result = store.append(A, List.of(), new Version(5));

    assertEquals(new AppendResult(List.of(), new Version(5)), result);
    assertTrue(stored(A).isEmpty());
    assertTrue(global().isEmpty());
  }

  @Test
  public void anEmptyAppendToAnExistingStreamStoresNothing() {
    store.append(A, events(A, 1, 2), Version.initial());

    AppendResult result = store.append(A, List.of(), new Version(2));

    assertEquals(new AppendResult(List.of(), new Version(2)), result);
    assertEquals(2, stored(A).size());
  }

  // ── Atomicity ───────────────────────────────────────────────────────────────────────────────

  @Test
  public void aRejectedMultiEventAppendStoresNoneOfItsEvents() {
    store.append(A, events(A, 1, 1), Version.initial());

    assertThrows(
        OptimisticLockException.class, () -> store.append(A, events(A, 1, 3), Version.initial()));

    assertEquals(List.of("order-A@1"), values(stored(A)));
    assertEquals(1, global().size());
  }

  /**
   * {@link #multiEventAppendsRacingForOneVersionStoreExactlyOneWholeBatch()} with its workers split
   * over two instances of one store.
   */
  @Test
  public void multiEventAppendsRacingForOneVersionStoreExactlyOneWholeBatchAcrossInstances()
      throws Exception {
    withSecondInstance();
    multiEventAppendsRacingForOneVersionStoreExactlyOneWholeBatch();
  }

  @Test
  public void multiEventAppendsRacingForOneVersionStoreExactlyOneWholeBatch() throws Exception {
    int racers = 8;
    var start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(racers);
    try {
      List<Future<Boolean>> outcomes = new ArrayList<>();
      for (int r = 0; r < racers; r++) {
        String batch = "racer-" + r;
        EventStore racer = instance(r);
        outcomes.add(
            pool.submit(
                () -> {
                  start.await();
                  var envelopes =
                      LongStream.rangeClosed(1, 3)
                          .mapToObj(
                              v ->
                                  EventStoreFixture.event(
                                      A,
                                      new Version(v),
                                      EVENT_TYPE,
                                      new ContractEvent(batch + "#" + v)))
                          .toList();
                  try {
                    racer.append(A, envelopes, Version.initial());
                    return true;
                  } catch (OptimisticLockException e) {
                    return false;
                  }
                }));
      }
      start.countDown();
      int winners = 0;
      for (var outcome : outcomes) {
        if (outcome.get(60, TimeUnit.SECONDS)) {
          winners++;
        }
      }

      assertEquals(1, winners, "exactly one append wins the version");
      List<EventEnvelope> stream = stored(A);
      assertEquals(List.of(1L, 2L, 3L), versions(stream));
      String winner = values(stream).getFirst().split("#")[0];
      assertEquals(
          List.of(winner + "#1", winner + "#2", winner + "#3"),
          values(stream),
          "the stream holds the whole batch of the winner and nothing of the others");
      assertEquals(3, global().size());
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * {@link #noReadReturnsPartOfAnAppendWhileMultiEventAppendsRun()} with its workers split over two
   * instances of one store.
   */
  @Test
  public void noReadReturnsPartOfAnAppendWhileMultiEventAppendsRunAcrossInstances()
      throws Exception {
    withSecondInstance();
    noReadReturnsPartOfAnAppendWhileMultiEventAppendsRun();
  }

  /**
   * Writers append batches of three events to streams of their own while readers read through
   * {@link EventStore#readGlobalStream}, {@link EventStore#readStream} and {@link EventStore#load}
   * as fast as they can. An append's events become readable together, so no read returns the first
   * event of a batch without the other two. A read cut short by its {@code maxCount} is not judged,
   * and neither is a batch whose first event lies before where the read started; the verdict never
   * depends on timing, only how likely a store that publishes a batch piecemeal is to be caught.
   */
  @Test
  public void noReadReturnsPartOfAnAppendWhileMultiEventAppendsRun() throws Exception {
    int writers = 4;
    int batchesEach = 100;
    int batchSize = 3;
    int maxCount = 1_000;
    var writing = new CountDownLatch(writers);
    Queue<String> violations = new ConcurrentLinkedQueue<>();
    List<StreamId> streams = new ArrayList<>();
    List<Callable<?>> work = new ArrayList<>();
    for (int w = 0; w < writers; w++) {
      StreamId stream = StreamId.of(ORDER, AggregateId.of("batches-" + w));
      EventStore writer = instance(w);
      streams.add(stream);
      work.add(
          () -> {
            try {
              for (int b = 0; b < batchesEach; b++) {
                String batch = stream.aggregateId().value() + "/" + b;
                long head = (long) b * batchSize;
                var envelopes =
                    LongStream.rangeClosed(1, batchSize)
                        .mapToObj(
                            i ->
                                EventStoreFixture.event(
                                    stream,
                                    new Version(head + i),
                                    EVENT_TYPE,
                                    new ContractEvent(batch + "#" + i)))
                        .toList();
                writer.append(stream, envelopes, new Version(head));
              }
            } finally {
              writing.countDown();
            }
            return null;
          });
    }
    work.add(
        () -> {
          GlobalOffset cursor = GlobalOffset.initial();
          while (writing.getCount() > 0 && !Thread.currentThread().isInterrupted()) {
            List<EventEnvelope> read = peer.readGlobalStream(cursor, maxCount);
            if (read.size() < maxCount) {
              recordPartialBatches("readGlobalStream", read, batchSize, violations);
            }
            if (!read.isEmpty()) {
              cursor = read.getLast().globalOffset();
            }
          }
          return null;
        });
    work.add(
        () -> {
          Map<StreamId, Version> cursors = new HashMap<>();
          for (int n = 0; writing.getCount() > 0 && !Thread.currentThread().isInterrupted(); n++) {
            StreamId stream = streams.get(n % streams.size());
            List<EventEnvelope> read =
                peer.readStream(stream, cursors.getOrDefault(stream, Version.initial()), maxCount);
            if (read.size() < maxCount) {
              recordPartialBatches("readStream", read, batchSize, violations);
            }
            if (!read.isEmpty()) {
              cursors.put(stream, read.getLast().version());
            }
          }
          return null;
        });
    work.add(
        () -> {
          for (int n = 0; writing.getCount() > 0 && !Thread.currentThread().isInterrupted(); n++) {
            recordPartialBatches(
                "load", peer.load(streams.get(n % streams.size())).events(), batchSize, violations);
          }
          return null;
        });

    runConcurrently(work, null);

    assertEquals(List.of(), List.copyOf(violations), "a read returned part of an append");
    assertEquals(writers * batchesEach * batchSize, global().size());
  }

  /**
   * Records in {@code violations} every batch of {@code read} whose first event it holds without
   * all {@code batchSize} of its events; a contract event's value is {@code <batch>#<index>}.
   */
  private static void recordPartialBatches(
      String path, List<EventEnvelope> read, int batchSize, Queue<String> violations) {
    Map<String, List<Integer>> indexes = new HashMap<>();
    for (String value : values(read)) {
      int hash = value.lastIndexOf('#');
      indexes
          .computeIfAbsent(value.substring(0, hash), k -> new ArrayList<>())
          .add(Integer.parseInt(value.substring(hash + 1)));
    }
    List<Integer> whole = IntStream.rangeClosed(1, batchSize).boxed().toList();
    indexes.forEach(
        (batch, seen) -> {
          if (seen.contains(1) && !seen.equals(whole)) {
            violations.add(path + " returned " + batch + " as " + seen);
          }
        });
  }

  // ── Concurrent appends ──────────────────────────────────────────────────────────────────────

  /**
   * {@link #concurrentAppendsToDistinctStreamsStoreEveryEventOnceUnderItsOwnOffset()} with its
   * workers split over two instances of one store.
   */
  @Test
  public void concurrentAppendsToDistinctStreamsStoreEveryEventOnceAcrossInstances()
      throws Exception {
    withSecondInstance();
    concurrentAppendsToDistinctStreamsStoreEveryEventOnceUnderItsOwnOffset();
  }

  /**
   * Writers append to streams of their own at the same time. Every event is stored once, under an
   * offset no other event has, and each stream holds its events as versions {@code 1..n} in the
   * order its writer appended them, under the offsets its appends returned.
   */
  @Test
  public void concurrentAppendsToDistinctStreamsStoreEveryEventOnceUnderItsOwnOffset()
      throws Exception {
    int writers = 8;
    int appendsEach = 20;
    Map<StreamId, List<AppendResult>> results = new ConcurrentHashMap<>();
    List<Callable<?>> work = new ArrayList<>();
    int expected = 0;
    for (int w = 0; w < writers; w++) {
      StreamId stream = StreamId.of(ORDER, AggregateId.of("distinct-" + w));
      EventStore writer = instance(w);
      int size = 1 + w % 3;
      expected += size * appendsEach;
      work.add(
          () -> {
            List<AppendResult> mine = new ArrayList<>();
            for (int i = 0; i < appendsEach; i++) {
              long head = (long) i * size;
              mine.add(writer.append(stream, events(stream, head + 1, size), new Version(head)));
            }
            results.put(stream, mine);
            return null;
          });
    }

    runConcurrently(work, null);

    List<EventEnvelope> all = global();
    assertEquals(expected, all.size());
    assertEquals(all.size(), new HashSet<>(offsets(all)).size(), "every offset is distinct");
    assertEquals(all.size(), new HashSet<>(eventIds(all)).size(), "every event is read once");
    assertStrictlyIncreasing(offsets(all));
    assertEquals(writers, results.size());
    for (var entry : results.entrySet()) {
      StreamId stream = entry.getKey();
      List<EventEnvelope> read = stored(stream);
      int n = read.size();
      assertEquals(LongStream.rangeClosed(1, n).boxed().toList(), versions(read));
      assertEquals(
          LongStream.rangeClosed(1, n)
              .mapToObj(v -> stream.aggregateId().value() + "@" + v)
              .toList(),
          values(read));
      assertEquals(
          entry.getValue().stream().flatMap(r -> r.globalOffsets().stream()).toList(),
          offsets(read),
          "the stream's events carry the offsets its appends returned");
    }
  }

  /**
   * {@link #theOffsetsOfAnAppendFollowTheOrderOfItsEventsWhileOthersAppend()} with its workers
   * split over two instances of one store.
   */
  @Test
  public void theOffsetsOfAnAppendFollowTheOrderOfItsEventsAcrossInstances() throws Exception {
    withSecondInstance();
    theOffsetsOfAnAppendFollowTheOrderOfItsEventsWhileOthersAppend();
  }

  /**
   * Within one append offsets follow the order of its events, also while other appends run: the
   * event read at each offset an append returned is the event given at that position. That the
   * offsets of one append are contiguous is not promised, so nothing is asserted about other
   * appends' offsets lying between them.
   */
  @Test
  public void theOffsetsOfAnAppendFollowTheOrderOfItsEventsWhileOthersAppend() throws Exception {
    int writers = 4;
    int appendsEach = 10;
    int size = 4;
    record Appended(List<EventEnvelope> given, AppendResult result) {}
    Queue<Appended> appended = new ConcurrentLinkedQueue<>();
    List<Callable<?>> work = new ArrayList<>();
    for (int w = 0; w < writers; w++) {
      StreamId stream = StreamId.of(ORDER, AggregateId.of("ordered-" + w));
      EventStore writer = instance(w);
      work.add(
          () -> {
            for (int i = 0; i < appendsEach; i++) {
              long head = (long) i * size;
              List<EventEnvelope> given = events(stream, head + 1, size);
              appended.add(new Appended(given, writer.append(stream, given, new Version(head))));
            }
            return null;
          });
    }

    runConcurrently(work, null);

    Map<GlobalOffset, EventId> byOffset = new HashMap<>();
    global().forEach(e -> byOffset.put(e.globalOffset(), e.metadata().eventId()));
    assertEquals(writers * appendsEach, appended.size());
    for (Appended append : appended) {
      List<GlobalOffset> offsets = append.result().globalOffsets();
      assertEquals(size, offsets.size());
      assertStrictlyIncreasing(offsets);
      for (int i = 0; i < size; i++) {
        assertEquals(
            append.given().get(i).metadata().eventId(),
            byOffset.get(offsets.get(i)),
            "the event at the append's offset " + i + " is the one given at that position");
      }
    }
  }

  // ── No idempotence by default ───────────────────────────────────────────────────────────────

  @Test
  public void appendingTheSameEnvelopeTwiceStoresTwoEvents() {
    EventEnvelope envelope = event(A, 1);
    store.append(A, List.of(envelope), Version.initial());

    store.append(A, List.of(envelope), new Version(1));

    List<EventEnvelope> stream = stored(A);
    assertEquals(List.of(1L, 2L), versions(stream));
    assertEquals(envelope.metadata().eventId(), stream.get(0).metadata().eventId());
    assertEquals(envelope.metadata().eventId(), stream.get(1).metadata().eventId());
    assertNotEquals(stream.get(0).globalOffset(), stream.get(1).globalOffset());
  }

  // ── Reads ───────────────────────────────────────────────────────────────────────────────────

  @Test
  public void loadOfAnUnknownStreamIsEmpty() {
    AggregateHistory history = store.load(A);

    assertNull(history.snapshotState());
    assertTrue(history.events().isEmpty());
    assertEquals(Version.initial(), history.version());
    assertEquals(Version.initial(), history.lastSnapshotVersion());
    assertEquals(0, history.snapshotPurgeGeneration());
  }

  @Test
  public void loadReturnsEveryEventInVersionOrder() {
    store.append(A, events(A, 1, 2), Version.initial());
    store.append(A, events(A, 3, 1), new Version(2));

    AggregateHistory history = store.load(A);

    assertNull(history.snapshotState());
    assertEquals(List.of(1L, 2L, 3L), versions(history.events()));
    assertEquals(List.of("order-A@1", "order-A@2", "order-A@3"), values(history.events()));
    assertEquals(new Version(3), history.version());
  }

  @Test
  public void theSameIdUnderAnotherAggregateTypeIsAnotherStream() {
    store.append(A, events(A, 1, 2), Version.initial());

    store.append(A_OTHER_TYPE, events(A_OTHER_TYPE, 1, 1), Version.initial());

    assertEquals(2, store.load(A).events().size());
    assertEquals(1, store.load(A_OTHER_TYPE).events().size());
    assertEquals(A_OTHER_TYPE, stored(A_OTHER_TYPE).getFirst().streamId());
  }

  @Test
  public void aReadEventCarriesItsStreamTypePayloadAndMetadata() {
    EventEnvelope envelope = event(A, 1);
    store.append(A, List.of(envelope), Version.initial());

    EventEnvelope read = stored(A).getFirst();

    assertEquals(A, read.streamId());
    assertEquals(EVENT_TYPE, read.eventType());
    assertEquals(envelope.event(), read.event());
    assertEquals(envelope.metadata().eventId(), read.metadata().eventId());
    assertEquals(envelope.metadata().commandId(), read.metadata().commandId());
    assertEquals(envelope.metadata().correlationId(), read.metadata().correlationId());
  }

  @Test
  public void readStreamReadsStrictlyAfterTheGivenVersion() {
    store.append(A, events(A, 1, 5), Version.initial());

    assertEquals(List.of(3L, 4L, 5L), versions(store.readStream(A, new Version(2), 100)));
    assertTrue(store.readStream(A, new Version(5), 100).isEmpty());
  }

  @Test
  public void readStreamReturnsAtMostMaxCountEvents() {
    store.append(A, events(A, 1, 5), Version.initial());

    assertEquals(List.of(1L, 2L), versions(store.readStream(A, Version.initial(), 2)));
    assertEquals(List.of(4L), versions(store.readStream(A, new Version(3), 1)));
  }

  @Test
  public void readStreamReadsOnlyTheGivenStream() {
    store.append(A, events(A, 1, 2), Version.initial());
    store.append(B, events(B, 1, 1), Version.initial());

    assertEquals(List.of("order-A@1", "order-A@2"), values(stored(A)));
    assertTrue(
        store
            .readStream(StreamId.of(ORDER, AggregateId.of("unknown")), Version.initial(), 10)
            .isEmpty());
  }

  @Test
  public void readGlobalStreamReturnsEveryStreamInAppendOrder() {
    store.append(A, events(A, 1, 2), Version.initial());
    store.append(B, events(B, 1, 1), Version.initial());
    store.append(A, events(A, 3, 1), new Version(2));

    List<EventEnvelope> all = global();

    assertEquals(List.of("order-A@1", "order-A@2", "order-B@1", "order-A@3"), values(all));
    assertStrictlyIncreasing(offsets(all));
  }

  @Test
  public void readGlobalStreamReadsStrictlyAfterTheGivenOffset() {
    store.append(A, events(A, 1, 2), Version.initial());
    store.append(B, events(B, 1, 2), Version.initial());
    GlobalOffset second = global().get(1).globalOffset();

    assertEquals(List.of("order-B@1", "order-B@2"), values(store.readGlobalStream(second, 100)));
  }

  @Test
  public void readGlobalStreamReturnsAtMostMaxCountEvents() {
    store.append(A, events(A, 1, 5), Version.initial());

    assertEquals(
        List.of("order-A@1", "order-A@2"),
        values(store.readGlobalStream(GlobalOffset.initial(), 2)));
  }

  @Test
  public void readGlobalStreamPastTheLastEventIsEmpty() {
    AppendResult result = store.append(A, events(A, 1, 2), Version.initial());

    assertTrue(store.readGlobalStream(result.globalOffsets().getLast(), 100).isEmpty());
  }

  @Test
  public void readGlobalStreamOfAnEmptyStoreIsEmpty() {
    assertTrue(global().isEmpty());
  }

  @Test
  public void pagingByCheckpointVisitsEveryEventOnceInOrder() {
    List<StreamId> streams = List.of(A, B, A_OTHER_TYPE);
    Map<StreamId, Long> heads = new HashMap<>();
    int total = 0;
    for (int i = 0; i < 30; i++) {
      StreamId stream = streams.get(i % streams.size());
      long head = heads.getOrDefault(stream, 0L);
      int size = 1 + i % 4;
      store.append(stream, events(stream, head + 1, size), new Version(head));
      heads.put(stream, head + size);
      total += size;
    }

    int pageSize = 7;
    List<EventEnvelope> paged = new ArrayList<>();
    GlobalOffset checkpoint = GlobalOffset.initial();
    for (List<EventEnvelope> page = store.readGlobalStream(checkpoint, pageSize);
        !page.isEmpty();
        page = store.readGlobalStream(checkpoint, pageSize)) {
      assertTrue(page.size() <= pageSize, "a page holds at most maxCount events");
      paged.addAll(page);
      checkpoint = page.getLast().globalOffset();
      assertTrue(paged.size() <= total, "paging reads no event twice and ends");
    }

    assertEquals(total, paged.size());
    assertEquals(total, new HashSet<>(eventIds(paged)).size(), "every event is visited once");
    assertStrictlyIncreasing(offsets(paged));
    List<EventEnvelope> all = global();
    assertEquals(offsets(all), offsets(paged));
    assertEquals(values(all), values(paged));
  }

  /**
   * {@link #aReaderPagingByCheckpointWhileWritersAppendReadsEveryEventOnce()} with its workers
   * split over two instances of one store.
   */
  @Test
  public void aReaderPagingByCheckpointReadsEveryEventOnceAcrossInstances() throws Exception {
    withSecondInstance();
    aReaderPagingByCheckpointWhileWritersAppendReadsEveryEventOnce();
  }

  /**
   * A reader pages through {@link EventStore#readGlobalStream} from its checkpoint, the offset of
   * the last event it read, in small pages while writers append, and drains to an empty read once
   * they are done: it reads every appended event exactly once, in increasing offset order.
   */
  @Test
  public void aReaderPagingByCheckpointWhileWritersAppendReadsEveryEventOnce() throws Exception {
    int writers = 4;
    int appendsEach = 25;
    int pageSize = 7;
    Set<EventId> appended = ConcurrentHashMap.newKeySet();
    List<Callable<?>> work = new ArrayList<>();
    for (int w = 0; w < writers; w++) {
      StreamId stream = StreamId.of(ORDER, AggregateId.of("paged-" + w));
      EventStore writer = instance(w);
      work.add(
          () -> {
            for (int i = 0; i < appendsEach; i++) {
              List<EventEnvelope> given = events(stream, 2L * i + 1, 2);
              writer.append(stream, given, new Version(2L * i));
              appended.addAll(eventIds(given));
            }
            return null;
          });
    }
    List<EventEnvelope> read = new ArrayList<>();
    GlobalOffset[] checkpoint = {GlobalOffset.initial()};
    Runnable readPage =
        () -> {
          List<EventEnvelope> page = peer.readGlobalStream(checkpoint[0], pageSize);
          if (!page.isEmpty()) {
            read.addAll(page);
            checkpoint[0] = page.getLast().globalOffset();
          }
        };

    runConcurrently(work, readPage);
    int before;
    do {
      before = read.size();
      readPage.run();
      assertTrue(read.size() <= writers * appendsEach * 2, "paging reads no event twice and ends");
    } while (read.size() > before);

    assertEquals(writers * appendsEach * 2, appended.size());
    assertStrictlyIncreasing(offsets(read));
    assertEquals(read.size(), new HashSet<>(eventIds(read)).size(), "no event is read twice");
    assertEquals(appended, new HashSet<>(eventIds(read)), "every appended event is read");
    assertEquals(offsets(global()), offsets(read));
  }

  @Test
  public void aStreamOfElevenHundredEventsLoadsWholeAndInOrder() {
    int count = 1_100;
    store.append(A, events(A, 1, count), Version.initial());
    List<Long> expectedVersions = LongStream.rangeClosed(1, count).boxed().toList();
    List<String> expectedValues =
        LongStream.rangeClosed(1, count).mapToObj(v -> "order-A@" + v).toList();

    AggregateHistory history = store.load(A);

    assertEquals(expectedVersions, versions(history.events()));
    assertEquals(expectedValues, values(history.events()));
    assertEquals(new Version(count), history.version());
    List<EventEnvelope> paged = new ArrayList<>();
    Version after = Version.initial();
    for (List<EventEnvelope> page = store.readStream(A, after, 250);
        !page.isEmpty();
        page = store.readStream(A, after, 250)) {
      assertTrue(page.size() <= 250, "a page holds at most maxCount events");
      paged.addAll(page);
      after = page.getLast().version();
      assertTrue(paged.size() <= count, "paging reads no event twice and ends");
    }
    assertEquals(expectedVersions, versions(paged));
    assertEquals(expectedValues, values(paged));
  }

  @Test
  public void everyMetadataFieldRoundTripsOnEveryReadPath() {
    SagaOrigin oneHopFromASaga =
        SagaEnvelopes.metadataOfContinuation(
                SagaEnvelopes.causedBy(
                    SagaForwardFenceContract.FenceState.class,
                    SagaId.of("saga-full"),
                    B,
                    new ContractEvent("cause")))
            .sagaOrigin();
    EventMetadata full =
        new EventMetadata(
            EventId.of("evt-full"),
            CommandId.of("cmd-full"),
            TraceId.of("trace-full"),
            CorrelationId.of("corr-full"),
            CausationId.of("cause-full"),
            UserId.of("user-full"),
            Instant.parse("2026-03-04T05:06:07.123456789Z"),
            Map.of("tenant", "t-1", "region", "eu-west"),
            oneHopFromASaga,
            SpanLink.ofW3c("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"));
    EventMetadata sparse =
        new EventMetadata(
            EventId.of("evt-sparse"),
            CommandId.of("cmd-sparse"),
            null,
            CorrelationId.of("corr-sparse"),
            null,
            null,
            Instant.parse("2026-03-04T05:06:08Z"),
            null,
            null,
            null);
    assertTrue(full.spanContext().isPresent(), "the full metadata links a span");
    assertEquals(1, oneHopFromASaga.hop());
    store.append(
        A,
        List.of(withMetadata(event(A, 1), full), withMetadata(event(A, 2), sparse)),
        Version.initial());
    List<EventMetadata> expected = List.of(full, sparse);

    assertEquals(expected, metadata(store.load(A).events()), "load");
    assertEquals(expected, metadata(stored(A)), "readStream");
    assertEquals(expected, metadata(global()), "readGlobalStream");
  }

  private static EventEnvelope withMetadata(EventEnvelope envelope, EventMetadata metadata) {
    return new EventEnvelope(
        envelope.globalOffset(),
        envelope.streamId(),
        envelope.version(),
        envelope.eventType(),
        envelope.event(),
        metadata);
  }

  private static List<EventMetadata> metadata(List<EventEnvelope> events) {
    return events.stream().map(EventEnvelope::metadata).toList();
  }

  // ── Identifier lengths ─────────────────────────────────────────────────────────────────────

  /**
   * Streams whose aggregate type and id are as long as the core types allow: a type of {@value
   * AggregateType#MAX_LENGTH} characters, and ids of {@value IdConstraints#MAX_LENGTH} characters
   * that are ASCII, two bytes each in UTF-8, or supplementary-plane characters of two UTF-16 code
   * units each (which {@link StreamId} counts as one character each, as the {@code aggregate_id}
   * columns do, and which only {@link AggregateId}'s decode door builds). Each is appended, read on
   * every path, snapshotted and, with an inbox, keyed.
   */
  @Test
  public void idsAsLongAsTheCoreTypesAllowAreStoredReadAndSnapshotted() {
    AggregateType longestType =
        AggregateType.of("ids_" + "t".repeat(AggregateType.MAX_LENGTH - "ids_".length()));
    int longest = IdConstraints.MAX_LENGTH;
    List<StreamId> streams =
        List.of(
            StreamId.of(longestType, AggregateId.of("a".repeat(longest))),
            StreamId.of(longestType, AggregateId.of("é".repeat(longest))),
            StreamId.of(longestType, new AggregateId("😀".repeat(longest))));

    for (StreamId stream : streams) {
      String shown = stream.aggregateId().value().substring(0, 2);
      AppendResult appended = store.append(stream, events(stream, 1, 2), Version.initial());

      AggregateHistory history = store.load(stream);
      assertEquals(List.of(1L, 2L), versions(history.events()), shown);
      assertEquals(appended.globalOffsets(), offsets(history.events()), shown);
      history.events().forEach(e -> assertEquals(stream, e.streamId(), shown));
      stored(stream).forEach(e -> assertEquals(stream, e.streamId(), shown));
      List<EventEnvelope> fromGlobal =
          store.readGlobalStream(
              GlobalOffset.of(appended.globalOffsets().getFirst().value() - 1), 2);
      fromGlobal.forEach(e -> assertEquals(stream, e.streamId(), shown));
      assertEquals(appended.globalOffsets(), offsets(fromGlobal), shown);
      if (supportsSnapshots()) {
        store.saveSnapshot(stream, new Version(2), new ContractState(shown));
        assertEquals(new ContractState(shown), store.load(stream).snapshotState(), shown);
      }
      if (supportsIdempotentAppend()) {
        IdempotencyKey key = IdempotencyKey.of("key-long-" + streams.indexOf(stream));
        assertFalse(appendWithKey(stream, 2, 1, key, COMMAND_TYPE).alreadyApplied(), shown);
        assertTrue(appendWithKey(stream, 2, 1, key, COMMAND_TYPE).alreadyApplied(), shown);
        assertEquals(List.of(1L, 2L, 3L), versions(stored(stream)), shown);
      }
    }
  }

  /**
   * Events whose type is as long as {@link EventType} allows, {@value EventType#MAX_LENGTH}
   * characters: ASCII, and supplementary-plane characters (two UTF-16 code units each, one
   * character as the {@code event_type} columns count it). Each is appended and read on every path.
   */
  @Test
  public void eventTypesAsLongAsTheCoreTypeAllowsAreStoredAndRead() {
    EventType ascii = new EventType("t".repeat(EventType.MAX_LENGTH));
    EventType supplementary = new EventType("😀".repeat(EventType.MAX_LENGTH));
    store =
        newStore(
            Setup.defaults()
                .withRegistry(
                    SimpleEventTypeRegistry.builder()
                        .registerEvent(ascii, ContractEvent.class)
                        .registerEvent(supplementary, ContractEvent.class)
                        .registerState(ContractState.class.getSimpleName(), ContractState.class)
                        .build()));
    List<EventType> types = List.of(ascii, supplementary);

    for (EventType type : types) {
      StreamId stream = StreamId.of(ORDER, AggregateId.of("long-type-" + types.indexOf(type)));
      List<EventEnvelope> batch =
          List.of(
              EventStoreFixture.event(stream, new Version(1), type, new ContractEvent("one")),
              EventStoreFixture.event(stream, new Version(2), type, new ContractEvent("two")));
      AppendResult appended = store.append(stream, batch, Version.initial());

      List<EventEnvelope> loaded = store.load(stream).events();
      assertEquals(List.of("one", "two"), values(loaded));
      loaded.forEach(e -> assertEquals(type, e.eventType()));
      stored(stream).forEach(e -> assertEquals(type, e.eventType()));
      List<EventEnvelope> fromGlobal =
          store.readGlobalStream(
              GlobalOffset.of(appended.globalOffsets().getFirst().value() - 1), 2);
      fromGlobal.forEach(e -> assertEquals(type, e.eventType()));
      assertEquals(appended.globalOffsets(), offsets(fromGlobal));
    }
  }

  /**
   * A command type of exactly {@value CommandInbox#MAX_COMMAND_TYPE_LENGTH} characters, ASCII or
   * supplementary-plane, is recorded and replayed; one character more is refused with {@link
   * IllegalArgumentException} before anything is written, claims no key and stores no event.
   */
  @Test
  public void aCommandTypeIsBoundedAtTheColumnWidthAndARefusedOneStoresNothing() {
    assumeTrue(supportsIdempotentAppend(), "the store does not implement appendWithKey");
    List<String> accepted =
        List.of(
            "c".repeat(CommandInbox.MAX_COMMAND_TYPE_LENGTH),
            "😀".repeat(CommandInbox.MAX_COMMAND_TYPE_LENGTH));

    for (String commandType : accepted) {
      StreamId stream =
          StreamId.of(ORDER, AggregateId.of("long-command-" + accepted.indexOf(commandType)));
      IdempotencyKey key = IdempotencyKey.of("key-long-command-" + accepted.indexOf(commandType));
      assertFalse(appendWithKey(stream, 0, 2, key, commandType).alreadyApplied());
      assertTrue(appendWithKey(stream, 0, 2, key, commandType).alreadyApplied());
      assertEquals(List.of(1L, 2L), versions(stored(stream)));
    }

    String tooLong = "c".repeat(CommandInbox.MAX_COMMAND_TYPE_LENGTH + 1);
    IdempotencyKey key = IdempotencyKey.of("key-too-long-command");
    var refused =
        assertThrows(IllegalArgumentException.class, () -> appendWithKey(A, 0, 1, key, tooLong));
    assertFalse(refused.getMessage().contains(tooLong), "the value is not echoed");
    assertTrue(stored(A).isEmpty(), "nothing is stored");

    assertFalse(
        appendWithKey(A, 0, 1, key, COMMAND_TYPE).alreadyApplied(),
        "the refused call claimed no key");
    assertEquals(List.of(1L), versions(stored(A)));
  }

  // ── Null arguments ─────────────────────────────────────────────────────────────────────────

  @Test
  public void appendRefusesANullArgumentAndStoresNothing() {
    assertThrows(
        NullPointerException.class, () -> store.append(null, events(A, 1, 1), Version.initial()));
    assertThrows(NullPointerException.class, () -> store.append(A, null, Version.initial()));
    assertThrows(NullPointerException.class, () -> store.append(A, events(A, 1, 1), null));

    assertTrue(global().isEmpty());
  }

  @Test
  public void loadRefusesANullStream() {
    assertThrows(NullPointerException.class, () -> store.load(null));
    assertThrows(NullPointerException.class, () -> store.load(null, 0));
    assertThrows(NullPointerException.class, () -> store.load(null, EventStore.IGNORE_SNAPSHOT));
  }

  @Test
  public void readStreamRefusesANullArgumentOnAnEmptyAndAFilledStore() {
    assertThrows(NullPointerException.class, () -> store.readStream(null, Version.initial(), 10));
    assertThrows(NullPointerException.class, () -> store.readStream(A, null, 10));
    store.append(A, events(A, 1, 2), Version.initial());

    assertThrows(NullPointerException.class, () -> store.readStream(null, Version.initial(), 10));
    assertThrows(NullPointerException.class, () -> store.readStream(A, null, 10));
  }

  @Test
  public void readGlobalStreamRefusesANullOffsetOnAnEmptyAndAFilledStore() {
    assertThrows(NullPointerException.class, () -> store.readGlobalStream(null, 10));
    store.append(A, events(A, 1, 2), Version.initial());

    assertThrows(NullPointerException.class, () -> store.readGlobalStream(null, 10));
  }

  // ── The global head ─────────────────────────────────────────────────────────────────────────

  @Test
  public void theHeadOfAnEmptyStoreIsTheInitialOffset() {
    assumeTrue(supportsLastGlobalOffset(), "the store does not report its head");

    assertEquals(GlobalOffset.initial(), store.lastGlobalOffset());
  }

  @Test
  public void theHeadIsTheOffsetOfTheLastStoredEvent() {
    assumeTrue(supportsLastGlobalOffset(), "the store does not report its head");
    store.append(A, events(A, 1, 2), Version.initial());
    AppendResult last = store.append(B, events(B, 1, 3), Version.initial());

    assertEquals(last.globalOffsets().getLast(), store.lastGlobalOffset());
  }

  @Test
  public void aRejectedAppendDoesNotMoveTheHead() {
    assumeTrue(supportsLastGlobalOffset(), "the store does not report its head");
    store.append(A, events(A, 1, 1), Version.initial());
    GlobalOffset head = store.lastGlobalOffset();

    assertThrows(
        OptimisticLockException.class, () -> store.append(A, events(A, 1, 2), Version.initial()));

    assertEquals(head, store.lastGlobalOffset());
  }

  @Test
  public void eventsStoredAfterTheHeadWasReadLieAboveIt() {
    assumeTrue(supportsLastGlobalOffset(), "the store does not report its head");
    store.append(A, events(A, 1, 2), Version.initial());
    GlobalOffset head = store.lastGlobalOffset();

    AppendResult later = store.append(B, events(B, 1, 2), Version.initial());

    later.globalOffsets().forEach(o -> assertTrue(o.value() > head.value(), "above " + head));
    assertEquals(later.globalOffsets(), offsets(store.readGlobalStream(head, 100)));
  }

  /**
   * {@link #nothingBecomesReadableAtOrBelowAHeadWhileAppendsRun()} with its workers split over two
   * instances of one store.
   */
  @Test
  public void nothingBecomesReadableAtOrBelowAHeadAcrossInstances() throws Exception {
    withSecondInstance();
    nothingBecomesReadableAtOrBelowAHeadWhileAppendsRun();
  }

  /**
   * Writers append to their own streams while a reader repeatedly reads the head and then every
   * readable event at or below it. Once all writers are done, the events at or below each head the
   * reader saw must be exactly the ones it read right after: none became readable later. A store
   * fast enough to finish the appends before the first sample passes trivially; the case is
   * meaningful for a store whose appends take long enough to overlap the reads.
   */
  @Test
  public void nothingBecomesReadableAtOrBelowAHeadWhileAppendsRun() throws Exception {
    assumeTrue(supportsLastGlobalOffset(), "the store does not report its head");
    int writers = 4;
    int appendsEach = 25;
    var start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      List<Future<?>> done = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        StreamId stream = StreamId.of(ORDER, AggregateId.of("writer-" + w));
        EventStore writer = instance(w);
        done.add(
            pool.submit(
                () -> {
                  start.await();
                  for (int i = 0; i < appendsEach; i++) {
                    writer.append(stream, events(stream, i + 1, 1), new Version(i));
                  }
                  return null;
                }));
      }
      record Sample(long head, List<Long> readable) {}
      List<Sample> samples = new ArrayList<>();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      start.countDown();
      while (samples.isEmpty() || !done.stream().allMatch(Future::isDone)) {
        if (System.nanoTime() > deadline) {
          fail("the writers did not finish within 60 seconds");
        }
        long head = peer.lastGlobalOffset().value();
        List<Long> readable =
            global().stream().map(e -> e.globalOffset().value()).filter(o -> o <= head).toList();
        samples.add(new Sample(head, readable));
      }
      for (var writer : done) {
        writer.get(60, TimeUnit.SECONDS);
      }

      List<Long> all = global().stream().map(e -> e.globalOffset().value()).toList();
      assertEquals(writers * appendsEach, all.size());
      for (Sample sample : samples) {
        assertEquals(
            all.stream().filter(o -> o <= sample.head()).toList(),
            sample.readable(),
            "an event became readable at or below head " + sample.head() + " after it was read");
      }
      assertEquals(all.getLast(), store.lastGlobalOffset().value());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void aStoreWithoutAHeadRefusesToReportOne() {
    assumeFalse(supportsLastGlobalOffset(), "the store reports its head");

    assertThrows(UnsupportedOperationException.class, () -> store.lastGlobalOffset());
  }

  // ── Snapshots ───────────────────────────────────────────────────────────────────────────────

  @Test
  public void aSavedSnapshotIsLoadedWithOnlyTheEventsAfterIt() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 3), Version.initial());

    store.saveSnapshot(A, new Version(2), new ContractState("at-2"));

    AggregateHistory history = store.load(A);
    assertEquals(new ContractState("at-2"), history.snapshotState());
    assertEquals(new Version(2), history.lastSnapshotVersion());
    assertEquals(List.of(3L), versions(history.events()));
    assertEquals(new Version(3), history.version());
  }

  @Test
  public void aSnapshotAtTheHeadIsLoadedWithNoEvents() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 2), Version.initial());

    store.saveSnapshot(A, new Version(2), new ContractState("at-head"));

    AggregateHistory history = store.load(A);
    assertEquals(new ContractState("at-head"), history.snapshotState());
    assertTrue(history.events().isEmpty());
    assertEquals(new Version(2), history.version());
  }

  @Test
  public void aLaterSnapshotReplacesTheEarlierOne() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 4), Version.initial());
    store.saveSnapshot(A, new Version(1), new ContractState("at-1"));

    store.saveSnapshot(A, new Version(3), new ContractState("at-3"));

    AggregateHistory history = store.load(A);
    assertEquals(new ContractState("at-3"), history.snapshotState());
    assertEquals(List.of(4L), versions(history.events()));
  }

  @Test
  public void aSnapshotBelongsToItsStreamOnly() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 2), Version.initial());
    store.append(A_OTHER_TYPE, events(A_OTHER_TYPE, 1, 2), Version.initial());

    store.saveSnapshot(A, new Version(2), new ContractState("of-A"));

    assertNull(store.load(A_OTHER_TYPE).snapshotState());
    assertEquals(2, store.load(A_OTHER_TYPE).events().size());
  }

  @Test
  public void aSaveAtGenerationZeroOfANeverPurgedStreamIsStored() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 2), Version.initial());

    assertTrue(store.saveSnapshot(A, new Version(2), new ContractState("gen-0"), 1, 0));

    AggregateHistory history = store.load(A);
    assertEquals(new ContractState("gen-0"), history.snapshotState());
    assertEquals(0, history.snapshotPurgeGeneration());
  }

  @Test
  public void aStoreWithoutAPurgeRecordStoresASaveAtAnyGeneration() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    assumeFalse(purgeSnapshot(B), "the store keeps a purge record");
    store.append(A, events(A, 1, 2), Version.initial());

    assertTrue(store.saveSnapshot(A, new Version(2), new ContractState("gen-7"), 1, 7));

    assertEquals(new ContractState("gen-7"), store.load(A).snapshotState());
  }

  @Test
  public void aSnapshotSchemaVersionBelowOneIsRejected() {
    store.append(A, events(A, 1, 1), Version.initial());

    assertThrows(
        IllegalArgumentException.class,
        () -> store.saveSnapshot(A, new Version(1), new ContractState("v0"), 0));
    if (supportsSnapshots()) {
      assertThrows(
          IllegalArgumentException.class,
          () -> store.saveSnapshot(A, new Version(1), new ContractState("v0"), 0, 0));
      assertNull(store.load(A).snapshotState());
    }
  }

  @Test
  public void aNegativePurgeGenerationIsRejected() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 1), Version.initial());

    assertThrows(
        IllegalArgumentException.class,
        () -> store.saveSnapshot(A, new Version(1), new ContractState("neg"), 1, -1));
    assertNull(store.load(A).snapshotState());
  }

  @Test
  public void loadWithTheSnapshotsSchemaVersionUsesIt() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    store.append(A, events(A, 1, 3), Version.initial());
    store.saveSnapshot(A, new Version(2), new ContractState("schema-2"), 2);

    AggregateHistory history = store.load(A, 2);

    assertEquals(new ContractState("schema-2"), history.snapshotState());
    assertEquals(List.of(3L), versions(history.events()));
  }

  @Test
  public void loadWithAnotherSchemaVersionIgnoresTheSnapshot() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    store.append(A, events(A, 1, 3), Version.initial());
    store.saveSnapshot(A, new Version(2), new ContractState("schema-2"), 2);

    AggregateHistory history = store.load(A, 3);

    assertNull(history.snapshotState());
    assertEquals(Version.initial(), history.lastSnapshotVersion());
    assertEquals(List.of(1L, 2L, 3L), versions(history.events()));
    assertEquals(new Version(3), history.version());
  }

  @Test
  public void loadWithZeroUsesAnySnapshot() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 3), Version.initial());
    store.saveSnapshot(A, new Version(2), new ContractState("any"), 5);

    AggregateHistory history = store.load(A, 0);

    assertEquals(new ContractState("any"), history.snapshotState());
    assertEquals(List.of(3L), versions(history.events()));
  }

  @Test
  public void loadIgnoringSnapshotsReadsEveryEvent() {
    store.append(A, events(A, 1, 3), Version.initial());
    if (supportsSnapshots()) {
      store.saveSnapshot(A, new Version(2), new ContractState("never-read"));
    }

    AggregateHistory history = store.load(A, EventStore.IGNORE_SNAPSHOT);

    assertNull(history.snapshotState());
    assertEquals(Version.initial(), history.lastSnapshotVersion());
    assertEquals(List.of(1L, 2L, 3L), versions(history.events()));
    assertEquals(new Version(3), history.version());
  }

  @Test
  public void loadWithZeroWorksOnEveryStore() {
    store.append(A, events(A, 1, 2), Version.initial());

    AggregateHistory history = store.load(A, 0);

    assertEquals(List.of(1L, 2L), versions(history.events()));
  }

  @Test
  public void aSchemaVersionBelowIgnoreSnapshotIsRejected() {
    store.append(A, events(A, 1, 1), Version.initial());
    if (supportsSnapshots()) {
      store.saveSnapshot(A, new Version(1), new ContractState("stored"));
    }

    assertThrows(
        IllegalArgumentException.class, () -> store.load(A, EventStore.IGNORE_SNAPSHOT - 1));
  }

  @Test
  public void aStoreWithoutSnapshotsRefusesToSaveOne() {
    assumeFalse(supportsSnapshots(), "the store keeps snapshots");
    store.append(A, events(A, 1, 1), Version.initial());

    assertThrows(
        UnsupportedOperationException.class,
        () -> store.saveSnapshot(A, new Version(1), new ContractState("none"), 1, 0));
    assertNull(store.load(A).snapshotState());
  }

  @Test
  public void aStoreWithoutSchemaVersionsRefusesToMatchOne() {
    assumeFalse(supportsSnapshotSchemaVersions(), "the store has snapshot schema versions");

    assertThrows(UnsupportedOperationException.class, () -> store.load(A, 1));
  }

  @Test
  public void theSaveWithoutASchemaVersionStoresSchemaVersionOne() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    store.append(A, events(A, 1, 3), Version.initial());

    store.saveSnapshot(A, new Version(2), new ContractState("default-schema"));

    AggregateHistory atOne = store.load(A, 1);
    assertEquals(new ContractState("default-schema"), atOne.snapshotState());
    assertEquals(List.of(3L), versions(atOne.events()));
    AggregateHistory atTwo = store.load(A, 2);
    assertNull(atTwo.snapshotState());
    assertEquals(List.of(1L, 2L, 3L), versions(atTwo.events()));
  }

  @Test
  public void aSaveWithANullStateIsRejectedWithAnIllegalArgumentException() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 2), Version.initial());

    assertThrows(
        IllegalArgumentException.class, () -> store.saveSnapshot(A, new Version(2), null, 1, 0));
    assertThrows(IllegalArgumentException.class, () -> store.saveSnapshot(A, new Version(2), null));

    assertNull(store.load(A).snapshotState(), "nothing is stored");
  }

  @Test
  public void aSaveWithANullStreamOrVersionIsRejectedWithANullPointerException() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 2), Version.initial());

    assertThrows(
        NullPointerException.class,
        () -> store.saveSnapshot(null, new Version(2), new ContractState("no-stream"), 1, 0));
    assertThrows(
        NullPointerException.class,
        () -> store.saveSnapshot(A, null, new ContractState("no-version"), 1, 0));

    assertNull(store.load(A).snapshotState(), "nothing is stored");
  }

  // ── Snapshot purge ──────────────────────────────────────────────────────────────────────────

  /** A stream of three events with a snapshot at version 2, then purged; returns the old load. */
  private AggregateHistory snapshottedThenPurged() {
    store.append(A, events(A, 1, 3), Version.initial());
    store.saveSnapshot(A, new Version(2), new ContractState("before-purge"));
    AggregateHistory beforePurge = store.load(A);
    assumeTrue(purgeSnapshot(A), "the store keeps no purge record");
    return beforePurge;
  }

  @Test
  public void aPurgeRemovesTheSnapshotAndRaisesTheGeneration() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    AggregateHistory beforePurge = snapshottedThenPurged();

    AggregateHistory afterPurge = store.load(A);

    assertNull(afterPurge.snapshotState());
    assertEquals(List.of(1L, 2L, 3L), versions(afterPurge.events()));
    assertTrue(
        afterPurge.snapshotPurgeGeneration() > beforePurge.snapshotPurgeGeneration(),
        "a purge raises the stream's generation");
  }

  @Test
  public void aSaveFromALoadBeforeThePurgeWritesNothing() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    AggregateHistory beforePurge = snapshottedThenPurged();

    boolean stored =
        store.saveSnapshot(
            A,
            new Version(3),
            new ContractState("stale"),
            1,
            beforePurge.snapshotPurgeGeneration());

    assertFalse(stored);
    assertNull(store.load(A).snapshotState(), "the purged snapshot stays purged");
  }

  @Test
  public void aSaveFromALoadAfterThePurgeIsStored() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    snapshottedThenPurged();
    AggregateHistory afterPurge = store.load(A);

    boolean stored =
        store.saveSnapshot(
            A, new Version(3), new ContractState("fresh"), 1, afterPurge.snapshotPurgeGeneration());

    assertTrue(stored);
    assertEquals(new ContractState("fresh"), store.load(A).snapshotState());
  }

  @Test
  public void theGenerationlessSaveOfAPurgedStreamIsRefused() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    snapshottedThenPurged();

    assertThrows(
        IllegalStateException.class,
        () -> store.saveSnapshot(A, new Version(3), new ContractState("gen-0")));
    assertNull(store.load(A).snapshotState());
  }

  // ── Idempotent append ───────────────────────────────────────────────────────────────────────

  @Test
  public void aFirstKeyedAppendStoresItsEvents() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");

    var result = appendWithKey(A, 0, 2, IdempotencyKey.of("key-first"), COMMAND_TYPE);

    assertFalse(result.alreadyApplied());
    assertEquals(new Version(2), result.finalVersion());
    assertEquals(2, result.globalOffsets().size());
    assertEquals(result.globalOffsets(), offsets(stored(A)));
    assertEquals(List.of(1L, 2L), versions(stored(A)));
  }

  @Test
  public void aReplayReturnsTheRecordedResultAndStoresNothing() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-replayed");
    var first = appendWithKey(A, 0, 2, key, COMMAND_TYPE);

    var replay = appendWithKey(A, 0, 2, key, COMMAND_TYPE);

    assertTrue(replay.alreadyApplied());
    assertEquals(first.globalOffsets(), replay.globalOffsets());
    assertEquals(first.finalVersion(), replay.finalVersion());
    assertEquals(2, stored(A).size());
    assertEquals(2, global().size());
  }

  @Test
  public void aReplayIsAnsweredFromTheRecordWhateverItCarries() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-replay-moved-on");
    var first = appendWithKey(A, 0, 1, key, COMMAND_TYPE);
    store.append(A, events(A, 2, 2), new Version(1));

    var replay = appendWithKey(A, 0, 3, key, COMMAND_TYPE);

    assertTrue(replay.alreadyApplied(), "a stale version and other events are still a replay");
    assertEquals(first.globalOffsets(), replay.globalOffsets());
    assertEquals(first.finalVersion(), replay.finalVersion());
    assertEquals(3, stored(A).size());
  }

  @Test
  public void aZeroEventCommandRecordsTheExpectedVersionAsItsFinalVersion() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    store.append(A, events(A, 1, 2), Version.initial());
    IdempotencyKey key = IdempotencyKey.of("key-zero-events-at-2");

    var first = store.appendWithKey(A, List.of(), new Version(2), key, COMMAND_TYPE);
    var replay = store.appendWithKey(A, List.of(), new Version(2), key, COMMAND_TYPE);

    assertEquals(new Version(2), first.finalVersion());
    assertTrue(first.globalOffsets().isEmpty());
    assertTrue(replay.alreadyApplied());
    assertEquals(new Version(2), replay.finalVersion());
    assertEquals(2, stored(A).size());
  }

  @Test
  public void aZeroEventCommandDoesNotConsultTheHead() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-zero-events-future");

    var first = store.appendWithKey(A, List.of(), new Version(5), key, COMMAND_TYPE);

    assertFalse(first.alreadyApplied(), "an empty list never fails with OptimisticLockException");
    assertEquals(new Version(5), first.finalVersion());
    assertTrue(
        store.appendWithKey(A, List.of(), new Version(5), key, COMMAND_TYPE).alreadyApplied());
    assertTrue(stored(A).isEmpty());
  }

  @Test
  public void aZeroEventCommandClaimsItsKey() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-zero-events");

    var first = store.appendWithKey(A, List.of(), Version.initial(), key, COMMAND_TYPE);
    var replay = store.appendWithKey(A, List.of(), Version.initial(), key, COMMAND_TYPE);

    assertFalse(first.alreadyApplied());
    assertTrue(first.globalOffsets().isEmpty());
    assertEquals(Version.initial(), first.finalVersion());
    assertTrue(replay.alreadyApplied(), "the zero-event command is not re-run on redelivery");
    assertTrue(stored(A).isEmpty());
    assertTrue(global().isEmpty());
  }

  @Test
  public void aKeyRecordedForAnotherCommandTypeIsRefused() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-other-command");
    appendWithKey(A, 0, 1, key, COMMAND_TYPE);

    assertThrows(
        IllegalArgumentException.class, () -> appendWithKey(A, 1, 1, key, OTHER_COMMAND_TYPE));

    assertEquals(1, stored(A).size());
  }

  @Test
  public void aKeyRecordedForAnotherStreamIsRefused() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-other-stream");
    appendWithKey(A, 0, 1, key, COMMAND_TYPE);

    assertThrows(IllegalArgumentException.class, () -> appendWithKey(B, 0, 1, key, COMMAND_TYPE));

    assertTrue(stored(B).isEmpty());
  }

  @Test
  public void aKeyRecordedForTheSameIdUnderAnotherAggregateTypeIsRefused() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-other-type");
    appendWithKey(A, 0, 1, key, COMMAND_TYPE);

    assertThrows(
        IllegalArgumentException.class, () -> appendWithKey(A_OTHER_TYPE, 0, 1, key, COMMAND_TYPE));

    assertTrue(stored(A_OTHER_TYPE).isEmpty());
  }

  @Test
  public void aKeyedAppendRefusedForAStaleVersionLeavesTheKeyUnclaimed() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    store.append(A, events(A, 1, 1), Version.initial());
    IdempotencyKey key = IdempotencyKey.of("key-stale");

    assertThrows(OptimisticLockException.class, () -> appendWithKey(A, 0, 1, key, COMMAND_TYPE));
    assertEquals(1, stored(A).size());

    var retried = appendWithKey(A, 1, 1, key, COMMAND_TYPE);
    assertFalse(retried.alreadyApplied(), "the refused append left its key unclaimed");
    assertEquals(List.of(1L, 2L), versions(stored(A)));
  }

  @Test
  public void aKeyedAppendWithAFutureVersionIsRefused() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");

    assertThrows(
        OptimisticLockException.class,
        () -> appendWithKey(A, 3, 1, IdempotencyKey.of("key-future"), COMMAND_TYPE));

    assertTrue(stored(A).isEmpty());
  }

  @Test
  public void distinctKeysAppendIndependently() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");

    var first = appendWithKey(A, 0, 1, IdempotencyKey.of("key-one"), COMMAND_TYPE);
    var second = appendWithKey(A, 1, 1, IdempotencyKey.of("key-two"), COMMAND_TYPE);

    assertFalse(first.alreadyApplied());
    assertFalse(second.alreadyApplied());
    assertEquals(new Version(2), second.finalVersion());
    assertEquals(List.of(1L, 2L), versions(stored(A)));
  }

  /** One racer's outcome: its result, or the {@link IllegalArgumentException} that refused it. */
  private record KeyedOutcome(
      StreamId streamId, IdempotentAppendResult result, IllegalArgumentException refusal) {}

  /**
   * Runs {@code streams.size()} keyed appends of {@code key}, each of {@code events} events at
   * version 0 of its stream, released together.
   */
  private List<KeyedOutcome> raceOneKey(IdempotencyKey key, List<StreamId> streams, int events)
      throws Exception {
    var start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(streams.size());
    try {
      List<Future<KeyedOutcome>> futures = new ArrayList<>();
      for (int r = 0; r < streams.size(); r++) {
        StreamId stream = streams.get(r);
        EventStore racer = instance(r);
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  try {
                    return new KeyedOutcome(
                        stream,
                        racer.appendWithKey(
                            stream,
                            events(stream, 1, events),
                            Version.initial(),
                            key,
                            COMMAND_TYPE),
                        null);
                  } catch (IllegalArgumentException e) {
                    return new KeyedOutcome(stream, null, e);
                  }
                }));
      }
      start.countDown();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      List<KeyedOutcome> outcomes = new ArrayList<>();
      for (var future : futures) {
        outcomes.add(future.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS));
      }
      return outcomes;
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * {@link #concurrentKeyedAppendsOfOneKeyStoreOnceAndRefuseAnotherStream()} with its workers split
   * over two instances of one store.
   */
  @Test
  public void concurrentKeyedAppendsOfOneKeyStoreOnceAndRefuseAnotherStreamAcrossInstances()
      throws Exception {
    withSecondInstance();
    concurrentKeyedAppendsOfOneKeyStoreOnceAndRefuseAnotherStream();
  }

  /**
   * Concurrent executions of one key all miss a caller's look-up of the inbox, so the store alone
   * decides: exactly one stores its events; one presented on the winner's stream is answered with
   * the winner's result, one presented on another stream is refused, and nothing is stored twice.
   */
  @Test
  public void concurrentKeyedAppendsOfOneKeyStoreOnceAndRefuseAnotherStream() throws Exception {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    List<StreamId> streams = new ArrayList<>();
    for (int r = 0; r < 8; r++) {
      streams.add(r % 2 == 0 ? A : B);
    }

    List<KeyedOutcome> outcomes = raceOneKey(IdempotencyKey.of("key-raced"), streams, 2);

    List<KeyedOutcome> winners =
        outcomes.stream().filter(o -> o.result() != null && !o.result().alreadyApplied()).toList();
    assertEquals(1, winners.size(), "exactly one execution of the key stores its events");
    KeyedOutcome winner = winners.getFirst();
    for (KeyedOutcome outcome : outcomes) {
      if (outcome == winner) {
        continue;
      }
      if (outcome.streamId().equals(winner.streamId())) {
        assertTrue(
            outcome.result() != null && outcome.result().alreadyApplied(),
            "an execution on the winner's stream is a replay");
        assertEquals(winner.result().globalOffsets(), outcome.result().globalOffsets());
        assertEquals(winner.result().finalVersion(), outcome.result().finalVersion());
      } else {
        assertTrue(
            outcome.refusal() != null,
            "an execution on another stream is refused, never answered with the winner's result");
      }
    }
    assertEquals(2, global().size(), "only the winner's events are stored");
    assertEquals(winner.result().globalOffsets(), offsets(stored(winner.streamId())));
  }

  /**
   * {@link #concurrentKeyedAppendsOfOneKeyOnOneStreamStoreOnce()} with its workers split over two
   * instances of one store.
   */
  @Test
  public void concurrentKeyedAppendsOfOneKeyOnOneStreamStoreOnceAcrossInstances() throws Exception {
    withSecondInstance();
    concurrentKeyedAppendsOfOneKeyOnOneStreamStoreOnce();
  }

  @Test
  public void concurrentKeyedAppendsOfOneKeyOnOneStreamStoreOnce() throws Exception {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");

    List<KeyedOutcome> outcomes =
        raceOneKey(IdempotencyKey.of("key-raced-one-stream"), List.of(A, A, A, A, A, A), 2);

    assertTrue(outcomes.stream().allMatch(o -> o.result() != null), "no execution is refused");
    assertEquals(
        1,
        outcomes.stream().filter(o -> !o.result().alreadyApplied()).count(),
        "exactly one execution stores its events");
    assertEquals(
        1,
        outcomes.stream().map(o -> o.result().globalOffsets()).distinct().count(),
        "every execution reports the same offsets");
    assertEquals(List.of(1L, 2L), versions(stored(A)));
    assertEquals(2, global().size());
  }

  @Test
  public void withoutASagaOriginTheSixArgumentAppendIsTheKeyedAppend() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-no-origin");

    var first = store.appendWithKey(A, events(A, 1, 1), Version.initial(), key, COMMAND_TYPE, null);
    var replay = appendWithKey(A, 0, 1, key, COMMAND_TYPE);

    assertFalse(first.alreadyApplied());
    assertTrue(replay.alreadyApplied(), "both methods share one inbox");
    assertEquals(first.globalOffsets(), replay.globalOffsets());
    assertEquals(1, stored(A).size());
  }

  @Test
  public void aStoreThatCannotFenceRefusesAFencedAppend() {
    assumeTrue(
        store.sagaFenceStorageIdentity() == null, "the store fences: see SagaForwardFenceContract");
    SagaOrigin forwardOf =
        SagaEnvelopes.metadataCausedBy(
                SagaForwardFenceContract.FenceState.class, SagaId.of("saga-1"))
            .sagaOrigin();

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            store.appendWithKey(
                A,
                events(A, 1, 1),
                Version.initial(),
                IdempotencyKey.of("key-fenced"),
                COMMAND_TYPE,
                forwardOf));

    assertTrue(stored(A).isEmpty(), "a store that cannot fence must not append unfenced");
  }

  @Test
  public void aStoreWithoutAnInboxRefusesAKeyedAppend() {
    assumeFalse(supportsIdempotentAppend(), "the store has a command inbox");

    assertThrows(
        UnsupportedOperationException.class,
        () -> appendWithKey(A, 0, 1, IdempotencyKey.of("key-unsupported"), COMMAND_TYPE));

    assertTrue(stored(A).isEmpty());
  }

  // ── Interrupted writes ──────────────────────────────────────────────────────────────────────

  @Test
  public void anAppendKilledAfterTakingItsOffsetsLeavesNothing() {
    assertAnAppendKilledBeforeItIsDurableLeavesNothing(CrashPoint.AFTER_OFFSET_RESERVATION, null);
  }

  @Test
  public void anAppendKilledAfterItsFirstEventLeavesNothing() {
    assertAnAppendKilledBeforeItIsDurableLeavesNothing(CrashPoint.AFTER_FIRST_EVENT_WRITE, null);
  }

  @Test
  public void anAppendKilledAfterItsFirstOutboxEntryLeavesNothing() {
    assumeTrue(supportsTransactionalOutbox(), "the store writes no outbox entries");
    assertAnAppendKilledBeforeItIsDurableLeavesNothing(CrashPoint.AFTER_FIRST_OUTBOX_WRITE, null);
  }

  @Test
  public void aKeyedAppendKilledAfterClaimingItsKeyLeavesNothingAndItsRetryStoresOnce() {
    assertAnAppendKilledBeforeItIsDurableLeavesNothing(
        CrashPoint.AFTER_KEY_CLAIM, IdempotencyKey.of("key-killed-claim"));
  }

  @Test
  public void aKeyedAppendKilledAfterTakingItsOffsetsLeavesNothingAndItsRetryStoresOnce() {
    assertAnAppendKilledBeforeItIsDurableLeavesNothing(
        CrashPoint.AFTER_OFFSET_RESERVATION, IdempotencyKey.of("key-killed-offsets"));
  }

  @Test
  public void aKeyedAppendKilledAfterItsFirstEventLeavesNothingAndItsRetryStoresOnce() {
    assertAnAppendKilledBeforeItIsDurableLeavesNothing(
        CrashPoint.AFTER_FIRST_EVENT_WRITE, IdempotencyKey.of("key-killed-event"));
  }

  @Test
  public void aKeyedAppendKilledAfterItsFirstOutboxEntryLeavesNothingAndItsRetryStoresOnce() {
    assumeTrue(supportsTransactionalOutbox(), "the store writes no outbox entries");
    assertAnAppendKilledBeforeItIsDurableLeavesNothing(
        CrashPoint.AFTER_FIRST_OUTBOX_WRITE, IdempotencyKey.of("key-killed-outbox"));
  }

  @Test
  public void anAppendKilledAfterItIsDurableIsStoredWhole() {
    assumeTrue(supportsCrashInjection(), "the suite cannot crash a write of the store");
    store.append(A, events(A, 1, 1), Version.initial());

    killed(CrashPoint.AFTER_COMMIT, () -> store.append(A, events(A, 2, 3), new Version(1)));

    assertStoredWhole();
  }

  @Test
  public void aKeyedAppendKilledAfterItIsDurableIsStoredWholeAndItsRetryIsAReplay() {
    assumeTrue(supportsCrashInjection(), "the suite cannot crash a write of the store");
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-killed-durable");
    store.append(A, events(A, 1, 1), Version.initial());

    killed(
        CrashPoint.AFTER_COMMIT,
        () -> store.appendWithKey(A, events(A, 2, 3), new Version(1), key, COMMAND_TYPE));

    assertStoredWhole();
    List<GlobalOffset> stored = offsets(stored(A)).subList(1, 4);
    var retry = store.appendWithKey(A, events(A, 2, 3), new Version(1), key, COMMAND_TYPE);
    assertTrue(retry.alreadyApplied(), "the key was recorded with the events");
    assertEquals(stored, retry.globalOffsets());
    assertEquals(new Version(4), retry.finalVersion());
    assertStoredWhole();
  }

  @Test
  public void aSnapshotSaveKilledBeforeItIsDurableKeepsTheEarlierSnapshot() {
    assumeTrue(supportsCrashInjection(), "the suite cannot crash a write of the store");
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 2), Version.initial());
    store.saveSnapshot(A, new Version(1), new ContractState("at-1"));

    killed(
        CrashPoint.DURING_SNAPSHOT_SAVE,
        () -> store.saveSnapshot(A, new Version(2), new ContractState("at-2")));

    AggregateHistory history = store.load(A);
    assertEquals(new ContractState("at-1"), history.snapshotState());
    assertEquals(List.of(2L), versions(history.events()));
    store.saveSnapshot(A, new Version(2), new ContractState("at-2"));
    assertEquals(new ContractState("at-2"), store.load(A).snapshotState());
    assertTrue(store.load(A).events().isEmpty());
  }

  @Test
  public void aFirstSnapshotSaveKilledBeforeItIsDurableLeavesNone() {
    assumeTrue(supportsCrashInjection(), "the suite cannot crash a write of the store");
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 2), Version.initial());

    killed(
        CrashPoint.DURING_SNAPSHOT_SAVE,
        () -> store.saveSnapshot(A, new Version(2), new ContractState("at-2")));

    AggregateHistory history = store.load(A);
    assertNull(history.snapshotState());
    assertEquals(List.of(1L, 2L), versions(history.events()));
  }

  @Test
  public void aSnapshotSaveKilledAfterItIsDurableIsUsedAndSavesAgain() {
    assumeTrue(supportsCrashInjection(), "the suite cannot crash a write of the store");
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 2), Version.initial());

    killed(
        CrashPoint.AFTER_SNAPSHOT_COMMIT,
        () -> store.saveSnapshot(A, new Version(2), new ContractState("at-2")));

    assertEquals(new ContractState("at-2"), store.load(A).snapshotState());
    assertTrue(store.load(A).events().isEmpty());
    store.saveSnapshot(A, new Version(2), new ContractState("at-2"));
    assertEquals(new ContractState("at-2"), store.load(A).snapshotState());
  }

  /**
   * Runs {@code write} on {@link #store} armed to die at {@code point}, checks that the call fails,
   * and replaces {@link #store} with the store a restarted process opens over the same data. What
   * the call throws is not judged: the caller of a killed process sees nothing at all, and a driver
   * whose connection dies mid-statement may raise an {@link Error}.
   */
  private void killed(CrashPoint point, Runnable write) {
    crashAt(store, point);
    assertThrows(Throwable.class, write::run, "the write died at " + point);
    store = reopen(store, Setup.defaults());
    peer = store;
  }

  /**
   * Over a stream holding one event, an append of three more events, keyed with {@code key} when it
   * is not {@code null}, dies at {@code point}: the restarted store reads none of the three, has no
   * outbox entry and no recorded key for them, keeps its head, takes the offsets of the next append
   * above it and pages through every event once; the append run again stores the three once, and a
   * keyed one run a third time is a replay.
   */
  private void assertAnAppendKilledBeforeItIsDurableLeavesNothing(
      CrashPoint point, IdempotencyKey key) {
    assumeTrue(supportsCrashInjection(), "the suite cannot crash a write of the store");
    assumeTrue(key == null || supportsIdempotentAppend(), "the store has no command inbox");
    AppendResult first = store.append(A, events(A, 1, 1), Version.initial());
    GlobalOffset head = first.globalOffsets().getLast();
    Runnable append =
        key == null
            ? () -> store.append(A, events(A, 2, 3), new Version(1))
            : () -> store.appendWithKey(A, events(A, 2, 3), new Version(1), key, COMMAND_TYPE);

    killed(point, append);

    assertEquals(List.of("order-A@1"), values(stored(A)), "readStream returns none of the three");
    assertEquals(List.of("order-A@1"), values(store.load(A).events()), "load returns none");
    assertEquals(first.globalOffsets(), offsets(global()), "readGlobalStream returns none");
    if (supportsLastGlobalOffset()) {
      assertEquals(head, store.lastGlobalOffset(), "the head did not move");
    }
    if (supportsTransactionalOutbox()) {
      assertEquals(entryIds(stored(A)), outboxIds(store), "no outbox entry of the three");
    }

    AppendResult other = store.append(B, events(B, 1, 1), Version.initial());
    assertTrue(
        other.globalOffsets().getFirst().value() > head.value(), "the next offsets lie above");
    if (key == null) {
      store.append(A, events(A, 2, 3), new Version(1));
    } else {
      var retry = store.appendWithKey(A, events(A, 2, 3), new Version(1), key, COMMAND_TYPE);
      assertFalse(retry.alreadyApplied(), "the key was not recorded");
      assertEquals(offsets(stored(A)).subList(1, 4), retry.globalOffsets());
      var replay = store.appendWithKey(A, events(A, 2, 3), new Version(1), key, COMMAND_TYPE);
      assertTrue(replay.alreadyApplied());
      assertEquals(retry.globalOffsets(), replay.globalOffsets());
    }
    assertEquals(List.of(1L, 2L, 3L, 4L), versions(stored(A)), "the rerun stored the three once");
    assertEquals(5, global().size());
    assertEquals(offsets(global()), pagedByCheckpoint(), "paging visits every event once");
    if (supportsTransactionalOutbox()) {
      assertEquals(Set.copyOf(entryIds(global())), Set.copyOf(outboxIds(store)));
      assertEquals(5, outboxIds(store).size(), "each event has one outbox entry");
    }
  }

  /**
   * After an append of three events over a stream of one died once it was durable: the restarted
   * store reads the four events, and the outbox holds one entry for each.
   */
  private void assertStoredWhole() {
    assertEquals(
        List.of("order-A@1", "order-A@2", "order-A@3", "order-A@4"),
        values(stored(A)),
        "the durable append is stored whole");
    assertEquals(4, global().size());
    if (supportsLastGlobalOffset()) {
      assertEquals(offsets(global()).getLast(), store.lastGlobalOffset());
    }
    if (supportsTransactionalOutbox()) {
      assertEquals(Set.copyOf(entryIds(stored(A))), Set.copyOf(outboxIds(store)));
      assertEquals(4, outboxIds(store).size(), "each event has one outbox entry");
    }
  }

  /** The offsets a reader paging by checkpoint, two events a page, reads until a page is empty. */
  private List<GlobalOffset> pagedByCheckpoint() {
    List<GlobalOffset> read = new ArrayList<>();
    GlobalOffset checkpoint = GlobalOffset.initial();
    for (List<EventEnvelope> page = store.readGlobalStream(checkpoint, 2);
        !page.isEmpty();
        page = store.readGlobalStream(checkpoint, 2)) {
      read.addAll(offsets(page));
      checkpoint = page.getLast().globalOffset();
    }
    return read;
  }

  // ── Transactional outbox ────────────────────────────────────────────────────────────────────

  @Test
  public void anAppendStoresAnOutboxEntryForEachOfItsEvents() {
    assumeTrue(supportsTransactionalOutbox(), "the store writes no outbox entries");

    store.append(A, events(A, 1, 2), Version.initial());
    store.append(B, events(B, 1, 1), Version.initial());

    assertEquals(Set.copyOf(entryIds(global())), Set.copyOf(outboxIds(store)));
    assertEquals(3, outboxIds(store).size());
  }

  @Test
  public void anAppendRefusedForItsVersionStoresNoOutboxEntry() {
    assumeTrue(supportsTransactionalOutbox(), "the store writes no outbox entries");
    store.append(A, events(A, 1, 1), Version.initial());

    assertThrows(
        OptimisticLockException.class, () -> store.append(A, events(A, 1, 2), Version.initial()));

    assertEquals(entryIds(stored(A)), outboxIds(store));
  }

  @Test
  public void aReplayedKeyStoresNoOutboxEntry() {
    assumeTrue(supportsTransactionalOutbox(), "the store writes no outbox entries");
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-outbox");
    appendWithKey(A, 0, 2, key, COMMAND_TYPE);

    assertTrue(appendWithKey(A, 0, 2, key, COMMAND_TYPE).alreadyApplied());

    assertEquals(Set.copyOf(entryIds(stored(A))), Set.copyOf(outboxIds(store)));
    assertEquals(2, outboxIds(store).size());
  }

  // ── Reopening over the same data ────────────────────────────────────────────────────────────

  @Test
  public void aReopenedStoreSeesTheStoredEvents() {
    AppendResult first = store.append(A, events(A, 1, 2), Version.initial());
    AppendResult second = store.append(B, events(B, 1, 1), Version.initial());

    EventStore reopened = reopen(store, Setup.defaults());

    AggregateHistory history = reopened.load(A);
    assertEquals(List.of(1L, 2L), versions(history.events()));
    assertEquals(List.of("order-A@1", "order-A@2"), values(history.events()));
    assertEquals(first.globalOffsets(), offsets(history.events()));
    List<EventEnvelope> all = reopened.readGlobalStream(GlobalOffset.initial(), 100);
    assertEquals(List.of("order-A@1", "order-A@2", "order-B@1"), values(all));
    assertEquals(
        List.of(
            first.globalOffsets().get(0),
            first.globalOffsets().get(1),
            second.globalOffsets().getFirst()),
        offsets(all));
  }

  @Test
  public void aReopenedStoreContinuesTheStreamAndTheGlobalOrder() {
    AppendResult before = store.append(A, events(A, 1, 2), Version.initial());

    EventStore reopened = reopen(store, Setup.defaults());
    assertThrows(
        OptimisticLockException.class,
        () -> reopened.append(A, events(A, 1, 1), Version.initial()),
        "the reopened store knows the stream's head");
    AppendResult after = reopened.append(A, events(A, 3, 1), new Version(2));

    assertEquals(new Version(3), after.finalVersion());
    assertTrue(
        after.globalOffsets().getFirst().value() > before.globalOffsets().getLast().value(),
        "an append after the reopen lies above every offset stored before it");
    assertEquals(List.of(1L, 2L, 3L), versions(reopened.load(A).events()));
  }

  @Test
  public void aReopenedStoreSeesTheStoredSnapshot() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 3), Version.initial());
    store.saveSnapshot(A, new Version(2), new ContractState("before-reopen"));

    AggregateHistory history = reopen(store, Setup.defaults()).load(A);

    assertEquals(new ContractState("before-reopen"), history.snapshotState());
    assertEquals(new Version(2), history.lastSnapshotVersion());
    assertEquals(List.of(3L), versions(history.events()));
  }

  @Test
  public void aReopenedStoreSeesTheRecordedKeys() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-before-reopen");
    var first = appendWithKey(A, 0, 2, key, COMMAND_TYPE);

    EventStore reopened = reopen(store, Setup.defaults());
    var replay = reopened.appendWithKey(A, events(A, 1, 2), Version.initial(), key, COMMAND_TYPE);

    assertTrue(replay.alreadyApplied(), "the key recorded before the reopen is a replay after it");
    assertEquals(first.globalOffsets(), replay.globalOffsets());
    assertEquals(2, reopened.readStream(A, Version.initial(), 100).size());
  }

  @Test
  public void aReopenedStoreKeepsThePurgeRecord() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    AggregateHistory beforePurge = snapshottedThenPurged();

    EventStore reopened = reopen(store, Setup.defaults());

    assertNull(reopened.load(A).snapshotState(), "the purged snapshot stays purged");
    assertFalse(
        reopened.saveSnapshot(
            A,
            new Version(3),
            new ContractState("stale"),
            1,
            beforePurge.snapshotPurgeGeneration()),
        "a save from a load before the purge writes nothing after the reopen either");
  }

  // ── Read-path fixtures ──────────────────────────────────────────────────────────────────────

  /** An event with an {@code @Encrypted} field. */
  private record SecretEvent(String subject, @Encrypted(subjectId = "subject") String value)
      implements DomainEvent {
    @JsonCreator
    SecretEvent(@JsonProperty("subject") String subject, @JsonProperty("value") String value) {
      this.subject = subject;
      this.value = value;
    }
  }

  /**
   * An event with an {@code @Encrypted} field its constructor validates, so it rejects the
   * tombstone the field of an erased subject reads as.
   */
  private record ValidatedEvent(String subject, @Encrypted(subjectId = "subject") String email)
      implements DomainEvent {
    @JsonCreator
    ValidatedEvent(@JsonProperty("subject") String subject, @JsonProperty("email") String email) {
      if (email != null && !email.contains("@")) {
        throw new IllegalArgumentException("not an email address");
      }
      this.subject = subject;
      this.email = email;
    }
  }

  /** An event of a type a later deployment does not register. */
  private record RetiredEvent(String value) implements DomainEvent {
    @JsonCreator
    RetiredEvent(@JsonProperty("value") String value) {
      this.value = value;
    }
  }

  /** A snapshot state with an {@code @Encrypted} field. */
  private record SecretState(String subject, @Encrypted(subjectId = "subject") String value)
      implements AggregateState {
    @JsonCreator
    SecretState(@JsonProperty("subject") String subject, @JsonProperty("value") String value) {
      this.subject = subject;
      this.value = value;
    }
  }

  /**
   * A snapshot state with an {@code @Encrypted} field its constructor validates, so it rejects the
   * tombstone the field of an erased subject reads as.
   */
  private record ValidatedState(String subject, @Encrypted(subjectId = "subject") String email)
      implements AggregateState {
    @JsonCreator
    ValidatedState(@JsonProperty("subject") String subject, @JsonProperty("email") String email) {
      if (email != null && !email.contains("@")) {
        throw new IllegalArgumentException("not an email address");
      }
      this.subject = subject;
      this.email = email;
    }
  }

  /** A snapshot state of a class a later deployment does not register. */
  private record RetiredState(String marker) implements AggregateState {
    @JsonCreator
    RetiredState(@JsonProperty("marker") String marker) {
      this.marker = marker;
    }
  }

  /**
   * {@link ContractState} with its field renamed: registered under {@code ContractState}'s name, a
   * stored {@code ContractState} does not deserialize as it.
   */
  private record RenamedFieldState(String label) implements AggregateState {
    @JsonCreator
    RenamedFieldState(@JsonProperty("label") String label) {
      this.label = label;
    }
  }

  /** A state class the registry does not know. */
  private record UnregisteredState(String marker) implements AggregateState {}

  /**
   * Holds a state class whose simple name is the name {@link ContractState} is registered under.
   */
  private static final class Shadowing {
    private record ContractState(String marker) implements AggregateState {}
  }

  /** Encrypts to the plaintext and fails every decrypt as a key-store outage would. */
  private static final class UnavailableKeyStore implements CryptoEngine {
    @Override
    public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
      return plaintext.clone();
    }

    @Override
    public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
      throw new CryptoOperationException("key store unavailable");
    }

    @Override
    public void deleteKey(SubjectId subjectId) {}

    @Override
    public boolean isKeyAvailable(SubjectId subjectId) {
      return true;
    }
  }

  private static final EventType SECRET_EVENT = new EventType("ContractSecretEvent");
  private static final EventType VALIDATED_EVENT = new EventType("ContractValidatedEvent");
  private static final EventType RETIRED_EVENT = new EventType("ContractRetiredEvent");
  private static final EventType OTHER_EVENT = new EventType("ContractOtherEvent");
  private static final String SUBJECT = "contract-subject-1";

  /** The types of {@link Setup#defaults()}, to register more on. */
  private static SimpleEventTypeRegistry.Builder contractTypes() {
    return SimpleEventTypeRegistry.builder()
        .registerEvent(EVENT_TYPE, ContractEvent.class)
        .registerState(ContractState.class.getSimpleName(), ContractState.class);
  }

  /**
   * The default setup with the types holding {@code @Encrypted} fields registered too; a store may
   * refuse such a registry without a crypto engine, so it is always given one.
   */
  private static Setup secretTypes(CryptoEngine crypto) {
    return Setup.defaults()
        .withRegistry(
            contractTypes()
                .registerEvent(SECRET_EVENT, SecretEvent.class)
                .registerEvent(VALIDATED_EVENT, ValidatedEvent.class)
                .registerState(SecretState.class.getSimpleName(), SecretState.class)
                .registerState(ValidatedState.class.getSimpleName(), ValidatedState.class)
                .build())
        .withCrypto(crypto);
  }

  /**
   * The default setup with {@link RetiredEvent} and {@link RetiredState} registered too; {@link
   * Setup#defaults()} is the setup of a later deployment that retired them.
   */
  private static Setup retiredTypes() {
    return Setup.defaults()
        .withRegistry(
            contractTypes()
                .registerEvent(RETIRED_EVENT, RetiredEvent.class)
                .registerState(RetiredState.class.getSimpleName(), RetiredState.class)
                .build());
  }

  /**
   * An upcaster of {@code type} at {@code currentVersion} whose step from version {@code n} appends
   * {@code "+v(n+1)"} to the {@code value} field.
   */
  private static EventUpcaster tagging(EventType type, int currentVersion) {
    return upcaster(
        type,
        currentVersion,
        (data, from) -> {
          data.put("value", data.get("value") + "+v" + (from + 1));
          return data;
        });
  }

  private interface Step {
    Map<String, Object> apply(Map<String, Object> data, int fromVersion);
  }

  private static EventUpcaster upcaster(EventType type, int currentVersion, Step step) {
    return new EventUpcaster() {
      @Override
      public EventType eventType() {
        return type;
      }

      @Override
      public int currentVersion() {
        return currentVersion;
      }

      @Override
      public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
        return step.apply(eventData, fromVersion);
      }
    };
  }

  private static SnapshotMigration migration(
      int from,
      int to,
      UnaryOperator<AggregateState> migrate,
      Class<? extends AggregateState> toType) {
    return new SnapshotMigration() {
      @Override
      public int fromVersion() {
        return from;
      }

      @Override
      public int toVersion() {
        return to;
      }

      @Override
      public AggregateState migrate(AggregateState state) {
        return migrate.apply(state);
      }

      @Override
      public Class<? extends AggregateState> toType() {
        return toType;
      }
    };
  }

  /** A step that appends {@code ">to"} to a {@link ContractState}'s marker. */
  private static SnapshotMigration marking(int from, int to) {
    return migration(
        from,
        to,
        state -> new ContractState(((ContractState) state).marker() + ">" + to),
        ContractState.class);
  }

  /** A step that fails with {@code failure}. */
  private static SnapshotMigration failing(int from, int to, RuntimeException failure) {
    return migration(
        from,
        to,
        state -> {
          throw failure;
        },
        null);
  }

  /** Three events on {@link #A} and a snapshot of {@code state} at version 2, schema {@code 1}. */
  private void threeEventsAndASnapshotAtTwo(AggregateState state) {
    store.append(A, events(A, 1, 3), Version.initial());
    store.saveSnapshot(A, new Version(2), state, 1);
  }

  /** The snapshot was discarded: no state, and every event of the three-event stream. */
  private static void assertDiscarded(AggregateHistory history) {
    assertNull(history.snapshotState(), "the snapshot is discarded");
    assertEquals(Version.initial(), history.lastSnapshotVersion());
    assertEquals(List.of(1L, 2L, 3L), versions(history.events()), "every event is replayed");
    assertEquals(new Version(3), history.version());
  }

  /** The snapshot at version 2 was used, as {@code state}, with only the third event. */
  private static void assertUsed(AggregateHistory history, AggregateState state) {
    assertEquals(state, history.snapshotState());
    assertEquals(new Version(2), history.lastSnapshotVersion());
    assertEquals(List.of(3L), versions(history.events()), "only the events after it");
    assertEquals(new Version(3), history.version());
  }

  /** {@code load}, {@code readStream} and {@code readGlobalStream} over {@link #A} all read. */
  private static List<List<EventEnvelope>> everyReadOfA(EventStore reader) {
    return List.of(
        reader.load(A).events(),
        reader.readStream(A, Version.initial(), 100),
        reader.readGlobalStream(GlobalOffset.initial(), 100));
  }

  /** {@code load}, {@code readStream} and {@code readGlobalStream} over {@link #A} all fail. */
  private static <T extends EventStoreException> List<T> everyReadOfAFails(
      EventStore reader, Class<T> type) {
    return List.of(
        assertThrows(type, () -> reader.load(A), "load"),
        assertThrows(type, () -> reader.readStream(A, Version.initial(), 100), "readStream"),
        assertThrows(
            type, () -> reader.readGlobalStream(GlobalOffset.initial(), 100), "readGlobalStream"));
  }

  private static void assertPlainEventStoreException(EventStoreException thrown) {
    assertSame(
        EventStoreException.class,
        thrown.getClass(),
        "a failure a retry may heal is a plain EventStoreException, none of its subtypes: "
            + thrown);
  }

  private static EventEnvelope secret(StreamId streamId, String value) {
    return EventStoreFixture.event(
        streamId, new Version(1), SECRET_EVENT, new SecretEvent(SUBJECT, value));
  }

  // ── Read path: schema version and upcasting ────────────────────────────────────────────────

  @Test
  public void anEventStoredBelowTheCurrentVersionIsUpcastOnEveryReadPath() {
    store.append(A, events(A, 1, 2), Version.initial());

    EventStore upcasting =
        reopen(store, Setup.defaults().withUpcasters(List.of(tagging(EVENT_TYPE, 2))));

    for (List<EventEnvelope> read : everyReadOfA(upcasting)) {
      assertEquals(List.of("order-A@1+v2", "order-A@2+v2"), values(read));
    }
  }

  @Test
  public void anEventIsUpcastOneVersionAtATimeUpToTheCurrentVersion() {
    store.append(A, events(A, 1, 1), Version.initial());

    EventStore upcasting =
        reopen(store, Setup.defaults().withUpcasters(List.of(tagging(EVENT_TYPE, 4))));

    for (List<EventEnvelope> read : everyReadOfA(upcasting)) {
      assertEquals(List.of("order-A@1+v2+v3+v4"), values(read));
    }
  }

  @Test
  public void anEventAppendedBeforeItsUpcasterExistedIsStoredAtVersionOneWhateverItsPayload() {
    store.append(A, events(A, 1, 1), Version.initial());
    List<Integer> fromVersions = new ArrayList<>();
    EventUpcaster recording =
        upcaster(
            EVENT_TYPE,
            2,
            (data, from) -> {
              fromVersions.add(from);
              return data;
            });

    EventStore upcasting = reopen(store, Setup.defaults().withUpcasters(List.of(recording)));

    assertEquals(List.of("order-A@1"), values(upcasting.readStream(A, Version.initial(), 100)));
    assertEquals(List.of(1), fromVersions, "the payload already binds, and is still upcast from 1");
  }

  @Test
  public void anEventIsStoredAtTheVersionOfTheWritersUpcaster() {
    store = newStore(Setup.defaults().withUpcasters(List.of(tagging(EVENT_TYPE, 2))));
    store.append(A, events(A, 1, 1), Version.initial());

    for (List<EventEnvelope> read : everyReadOfA(store)) {
      assertEquals(List.of("order-A@1"), values(read), "stored at 2, read as stored");
    }
    EventStore later =
        reopen(store, Setup.defaults().withUpcasters(List.of(tagging(EVENT_TYPE, 3))));
    for (List<EventEnvelope> read : everyReadOfA(later)) {
      assertEquals(List.of("order-A@1+v3"), values(read), "upcast from 2, not from 1");
    }
  }

  @Test
  public void anEventStoredAboveAReaderWithoutAnUpcasterIsRefusedAsSchemaAhead() {
    store = newStore(Setup.defaults().withUpcasters(List.of(tagging(EVENT_TYPE, 2))));
    store.append(A, events(A, 1, 1), Version.initial());

    EventStore older = reopen(store, Setup.defaults());

    everyReadOfAFails(older, EventSchemaAheadException.class);
  }

  @Test
  public void anEventStoredAboveTheReadersUpcasterIsRefusedAsSchemaAhead() {
    store = newStore(Setup.defaults().withUpcasters(List.of(tagging(EVENT_TYPE, 3))));
    store.append(A, events(A, 1, 1), Version.initial());

    EventStore older =
        reopen(store, Setup.defaults().withUpcasters(List.of(tagging(EVENT_TYPE, 2))));

    everyReadOfAFails(older, EventSchemaAheadException.class);
  }

  @Test
  public void upcastersForOtherTypesDoNotRaiseTheVersionAReaderReaches() {
    store = newStore(Setup.defaults().withUpcasters(List.of(tagging(EVENT_TYPE, 2))));
    store.append(A, events(A, 1, 1), Version.initial());

    EventStore older =
        reopen(store, Setup.defaults().withUpcasters(List.of(tagging(OTHER_EVENT, 5))));

    everyReadOfAFails(older, EventSchemaAheadException.class);
  }

  // ── Read path: read failures ───────────────────────────────────────────────────────────────

  @Test
  public void anUpcasterThatThrowsFailsTheReadWithAnEventDeserializationException() {
    store.append(A, events(A, 1, 1), Version.initial());
    EventUpcaster throwing =
        upcaster(
            EVENT_TYPE,
            2,
            (data, from) -> {
              throw new IllegalStateException("upcaster bug");
            });

    EventStore upcasting = reopen(store, Setup.defaults().withUpcasters(List.of(throwing)));

    everyReadOfAFails(upcasting, EventDeserializationException.class);
  }

  @Test
  public void anUpcasterReturningDataTheClassCannotBindFailsWithAnEventDeserializationException() {
    store.append(A, events(A, 1, 1), Version.initial());
    EventUpcaster garbling =
        upcaster(
            EVENT_TYPE,
            2,
            (data, from) -> {
              Map<String, Object> garbage = new HashMap<>();
              garbage.put("value", Map.of("nested", List.of(1, 2, 3)));
              return garbage;
            });

    EventStore upcasting = reopen(store, Setup.defaults().withUpcasters(List.of(garbling)));

    everyReadOfAFails(upcasting, EventDeserializationException.class);
  }

  @Test
  public void aKeyStoreOutageOnReadIsAPlainEventStoreException() {
    store = newStore(secretTypes(new InMemoryCryptoEngine()));
    store.append(A, List.of(secret(A, "top-secret")), Version.initial());

    EventStore outage = reopen(store, secretTypes(new UnavailableKeyStore()));

    everyReadOfAFails(outage, EventStoreException.class)
        .forEach(EventStoreContract::assertPlainEventStoreException);
  }

  @Test
  public void anErasedSubjectsTombstoneTheClassRejectsIsAnEventDeserializationException() {
    var crypto = new InMemoryCryptoEngine();
    store = newStore(secretTypes(crypto));
    store.append(
        A,
        List.of(
            EventStoreFixture.event(
                A, new Version(1), VALIDATED_EVENT, new ValidatedEvent(SUBJECT, "a@example.com"))),
        Version.initial());

    crypto.deleteKey(SubjectId.of(SUBJECT));

    everyReadOfAFails(store, EventDeserializationException.class);
  }

  @Test
  public void anEventOfATypeTheRegistryDoesNotResolveIsAnUnknownEventTypeException() {
    store = newStore(retiredTypes());
    store.append(
        A,
        List.of(
            EventStoreFixture.event(A, new Version(1), RETIRED_EVENT, new RetiredEvent("retired"))),
        Version.initial());

    EventStore later = reopen(store, Setup.defaults());

    everyReadOfAFails(later, UnknownEventTypeException.class);
  }

  @Test
  public void aReadThatCannotReadAnEventFailsAsAWholeAndSkipsNothing() {
    store = newStore(retiredTypes());
    store.append(A, events(A, 1, 1), Version.initial());
    store.append(
        A,
        List.of(EventStoreFixture.event(A, new Version(2), RETIRED_EVENT, new RetiredEvent("bad"))),
        new Version(1));
    store.append(A, events(A, 3, 1), new Version(2));

    EventStore later = reopen(store, Setup.defaults());

    everyReadOfAFails(later, UnknownEventTypeException.class);
    assertThrows(
        UnknownEventTypeException.class,
        () -> later.readStream(A, new Version(1), 100),
        "a read starting right before the bad event fails too");
  }

  // ── Snapshot use, migration and discard ────────────────────────────────────────────────────

  @Test
  public void aMismatchedSnapshotWithACompleteMigrationChainIsMigratedAndUsed() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    threeEventsAndASnapshotAtTwo(new ContractState("m"));

    EventStore migrating = reopen(store, Setup.defaults().withMigrations(List.of(marking(1, 2))));

    assertUsed(migrating.load(A, 2), new ContractState("m>2"));
  }

  @Test
  public void aSnapshotIsMigratedThroughEveryStepOfTheChain() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    threeEventsAndASnapshotAtTwo(new ContractState("m"));

    EventStore migrating =
        reopen(
            store,
            Setup.defaults().withMigrations(List.of(marking(1, 2), marking(2, 3), marking(3, 4))));

    assertUsed(migrating.load(A, 4), new ContractState("m>2>3>4"));
    assertUsed(migrating.load(A, 3), new ContractState("m>2>3"));
  }

  @Test
  public void aSnapshotWithAGapInItsMigrationChainIsDiscarded() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    threeEventsAndASnapshotAtTwo(new ContractState("m"));

    EventStore migrating =
        reopen(store, Setup.defaults().withMigrations(List.of(marking(1, 2), marking(3, 4))));

    assertDiscarded(migrating.load(A, 4));
  }

  @Test
  public void aSnapshotWhoseMigrationStepGoesPastTheExpectedVersionIsDiscarded() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    threeEventsAndASnapshotAtTwo(new ContractState("m"));

    EventStore migrating = reopen(store, Setup.defaults().withMigrations(List.of(marking(1, 3))));

    assertDiscarded(migrating.load(A, 2));
  }

  @Test
  public void aSnapshotStoredAboveTheExpectedVersionIsDiscarded() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    store.append(A, events(A, 1, 3), Version.initial());
    store.saveSnapshot(A, new Version(2), new ContractState("m"), 3);

    EventStore migrating =
        reopen(store, Setup.defaults().withMigrations(List.of(marking(1, 2), marking(2, 3))));

    assertDiscarded(migrating.load(A, 2));
  }

  @Test
  public void aMismatchedSnapshotOfAStoreWithoutMigrationsIsDiscarded() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    threeEventsAndASnapshotAtTwo(new ContractState("m"));

    EventStore withoutMigrations = reopen(store, Setup.defaults());

    assertDiscarded(withoutMigrations.load(A, 2));
  }

  @Test
  public void aSnapshotWhoseMigrationStepThrowsIsDiscarded() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    threeEventsAndASnapshotAtTwo(new ContractState("m"));

    EventStore migrating =
        reopen(
            store,
            Setup.defaults()
                .withMigrations(List.of(failing(1, 2, new IllegalStateException("step bug")))));

    assertDiscarded(migrating.load(A, 2));
  }

  @Test
  public void aSnapshotWhoseMigrationStepReturnsNullIsDiscarded() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    threeEventsAndASnapshotAtTwo(new ContractState("m"));

    EventStore migrating =
        reopen(
            store,
            Setup.defaults()
                .withMigrations(List.of(marking(1, 2), migration(2, 3, state -> null, null))));

    assertDiscarded(migrating.load(A, 3));
  }

  @Test
  public void aSnapshotWhoseMigrationStepReturnsAnotherClassThanItsToTypeIsDiscarded() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    threeEventsAndASnapshotAtTwo(new ContractState("m"));

    EventStore migrating =
        reopen(
            store,
            Setup.defaults()
                .withMigrations(List.of(migration(1, 2, state -> state, RenamedFieldState.class))));

    assertDiscarded(migrating.load(A, 2));
  }

  @Test
  public void aSnapshotWhoseStateDoesNotDeserializeIsDiscarded() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    threeEventsAndASnapshotAtTwo(new ContractState("m"));
    Setup renamed =
        Setup.defaults()
            .withRegistry(
                SimpleEventTypeRegistry.builder()
                    .registerEvent(EVENT_TYPE, ContractEvent.class)
                    .registerState(ContractState.class.getSimpleName(), RenamedFieldState.class)
                    .build());

    EventStore later = reopen(store, renamed);

    assertDiscarded(later.load(A));
    if (supportsSnapshotSchemaVersions()) {
      assertDiscarded(later.load(A, 1));
      assertDiscarded(reopen(later, renamed.withMigrations(List.of(marking(1, 2)))).load(A, 2));
    }
  }

  @Test
  public void aSnapshotOfARetiredStateTypeIsDiscarded() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store = newStore(retiredTypes());
    threeEventsAndASnapshotAtTwo(new RetiredState("m"));

    EventStore later = reopen(store, Setup.defaults());

    assertDiscarded(later.load(A));
    if (supportsSnapshotSchemaVersions()) {
      assertDiscarded(later.load(A, 1));
      assertDiscarded(
          reopen(later, Setup.defaults().withMigrations(List.of(marking(1, 2)))).load(A, 2));
    }
  }

  // ── Snapshot crypto failures ───────────────────────────────────────────────────────────────

  @Test
  public void aKeyStoreOutageWhileDeserializingAUsableSnapshotFailsTheLoadPlainly() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store = newStore(secretTypes(new InMemoryCryptoEngine()));
    threeEventsAndASnapshotAtTwo(new SecretState(SUBJECT, "top-secret"));

    EventStore outage = reopen(store, secretTypes(new UnavailableKeyStore()));

    assertPlainEventStoreException(assertThrows(EventStoreException.class, () -> outage.load(A)));
    if (supportsSnapshotSchemaVersions()) {
      assertPlainEventStoreException(
          assertThrows(EventStoreException.class, () -> outage.load(A, 1)));
      EventStore migrating =
          reopen(
              outage,
              secretTypes(new UnavailableKeyStore())
                  .withMigrations(List.of(migration(1, 2, state -> state, null))));
      assertPlainEventStoreException(
          assertThrows(EventStoreException.class, () -> migrating.load(A, 2)));
    }
  }

  @Test
  public void anErasedSubjectsTombstoneAUsableSnapshotRejectsFailsTheLoad() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    var crypto = new InMemoryCryptoEngine();
    store = newStore(secretTypes(crypto));
    threeEventsAndASnapshotAtTwo(new ValidatedState(SUBJECT, "a@example.com"));

    crypto.deleteKey(SubjectId.of(SUBJECT));

    assertThrows(EventDeserializationException.class, () -> store.load(A));
  }

  @Test
  public void aKeyStoreOutageInAMigrationStepFailsTheLoadPlainly() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    threeEventsAndASnapshotAtTwo(new ContractState("m"));
    var outage =
        new IllegalStateException(
            "step failed", new CryptoOperationException("key store unavailable"));

    EventStore migrating =
        reopen(store, Setup.defaults().withMigrations(List.of(failing(1, 2, outage))));

    assertPlainEventStoreException(
        assertThrows(EventStoreException.class, () -> migrating.load(A, 2)));
  }

  @Test
  public void aCryptoMappingFailureInAMigrationStepIsAnEventDeserializationException() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    threeEventsAndASnapshotAtTwo(new ContractState("m"));
    var refusal = new CryptoMappingException("the step cannot read the erased subject's field");

    EventStore migrating =
        reopen(store, Setup.defaults().withMigrations(List.of(failing(1, 2, refusal))));

    assertThrows(EventDeserializationException.class, () -> migrating.load(A, 2));
  }

  @Test
  public void aSnapshotDiscardedForItsVersionIsNotDeserialized() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    store = newStore(secretTypes(new InMemoryCryptoEngine()));
    threeEventsAndASnapshotAtTwo(new SecretState(SUBJECT, "top-secret"));

    EventStore outage = reopen(store, secretTypes(new UnavailableKeyStore()));

    assertDiscarded(outage.load(A, 2));
    EventStore gapped =
        reopen(
            outage,
            secretTypes(new UnavailableKeyStore())
                .withMigrations(List.of(migration(1, 2, state -> state, null))));
    assertDiscarded(gapped.load(A, 3));
  }

  @Test
  public void aSnapshotDiscardedForItsTypeIsNotDeserialized() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store = newStore(secretTypes(new InMemoryCryptoEngine()));
    threeEventsAndASnapshotAtTwo(new SecretState(SUBJECT, "top-secret"));
    Setup secretStateRetired = Setup.defaults().withCrypto(new UnavailableKeyStore());

    EventStore later = reopen(store, secretStateRetired);

    assertDiscarded(later.load(A));
    if (supportsSnapshotSchemaVersions()) {
      assertDiscarded(later.load(A, 1));
      EventStore migrating =
          reopen(
              later,
              secretStateRetired.withMigrations(List.of(migration(1, 2, state -> state, null))));
      assertDiscarded(migrating.load(A, 2));
    }
  }

  // ── Snapshot state type ────────────────────────────────────────────────────────────────────

  @Test
  public void aSnapshotOfAStateTypeTheRegistryDoesNotResolveIsRefused() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 2), Version.initial());

    assertThrows(
        EventStoreException.class,
        () -> store.saveSnapshot(A, new Version(2), new UnregisteredState("x"), 1, 0));

    assertNull(store.load(A).snapshotState(), "nothing is stored");
  }

  @Test
  public void aSnapshotWhoseTypeNameResolvesToAnotherClassIsRefused() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 2), Version.initial());

    assertThrows(
        EventStoreException.class,
        () -> store.saveSnapshot(A, new Version(2), new Shadowing.ContractState("x"), 1, 0));

    assertNull(store.load(A).snapshotState(), "nothing is stored");
  }
}
