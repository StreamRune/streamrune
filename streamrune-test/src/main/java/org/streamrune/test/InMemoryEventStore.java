package org.streamrune.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventSchemaAheadException;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.SnapshotMigrationChain;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.core.upcasting.UpcasterChain;
import org.streamrune.crypto.CryptoShreddingModule;

/**
 * In-memory implementation of {@link EventStore} for testing and development.
 *
 * <p>This implementation is a faithful stand-in for {@code PostgresEventStore} and mirrors its
 * observable semantics:
 *
 * <ul>
 *   <li><b>Optimistic locking — strict</b>: an append succeeds only when {@code expectedVersion}
 *       equals the stream's actual current head exactly, checked under {@code appendLock}
 *       (mirroring {@code PostgresEventStore}'s post-lock head read — see its {@code insertEvents}
 *       javadoc). A stale (behind-head) or future (beyond-head) {@code expectedVersion} both fail
 *       identically with {@link OptimisticLockException} naming both the expected and the actual
 *       head. The exception message matches {@code PostgresEventStore}'s wording, so message
 *       assertions transfer to integration tests. A documented zero-event append (see {@link
 *       EventStore#append}) is exempt: it carries no events to order against the stream.
 *   <li><b>Global ordering</b>: appends across all streams are serialized — Postgres serializes on
 *       the single-row {@code global_offset_sequence} counter's row lock, held from offset
 *       reservation until the append commits; this class uses a single in-process lock — and {@link
 *       #readGlobalStream(GlobalOffset, int)} returns events strictly ordered by global offset. The
 *       consequent single-writer append-throughput ceiling is a real production property (see
 *       {@code PostgresEventStore}'s class javadoc), not modeled by this in-memory double.
 *   <li><b>Snapshot versioning</b>: snapshots are tagged with a schema version; {@link
 *       #load(StreamId, int)} ignores a snapshot whose stored schema version does not match and
 *       replays all events instead.
 *   <li><b>Append notifications</b>: after a successful append, registered {@link AppendListener}s
 *       receive the last assigned global offset — mirroring the post-commit {@code
 *       pg_notify('streamrune_events', lastOffset)} that wakes push-based subscriptions. Like
 *       {@code pg_notify}, delivery is best-effort: a throwing listener never fails the append.
 *       {@link InMemoryEventSubscription} builds on this hook.
 * </ul>
 *
 * <p><b>Serialization modes.</b> The no-arg constructor stores events and snapshot state by
 * reference — fast, but events that Jackson cannot serialize, {@code @Encrypted} field handling,
 * and snapshot type registration are never exercised. {@link #serializing(EventTypeRegistry,
 * CryptoEngine)} (or the {@link #InMemoryEventStore(ObjectMapper, EventTypeRegistry) ObjectMapper
 * constructor}) enables an opt-in serializing mode that mirrors {@code PostgresEventStore}'s
 * store-the-ciphertext / transform-on-read model: every event payload, event metadata, and snapshot
 * state is SERIALIZED to JSON (encrypting {@code @Encrypted} fields) on append/save, and
 * DESERIALIZED — decrypting, {@linkplain #withUpcasters(List) upcasting}, and re-resolving the
 * stored type through the registry — on every read path ({@link #load}, {@link #readStream}, {@link
 * #readGlobalStream}). Because deserialization is deferred to read, the double faithfully
 * reproduces the read-time behaviors a Postgres-backed app would see, so unit tests do not silently
 * lie:
 *
 * <ul>
 *   <li><b>Crypto-shredding on replay</b>: after {@code cryptoEngine.deleteKey(subject)},
 *       re-reading an event or snapshot whose {@code @Encrypted} field belonged to that subject
 *       yields {@link CryptoShreddingModule#REDACTED} rather than resurfacing the original
 *       plaintext PII.
 *   <li><b>Upcasting on read</b>: an event stored at an older schema version is upcast through the
 *       registered {@link #withUpcasters(List) upcaster chain} when it is read back.
 *   <li><b>Retired-type discard</b>: a snapshot whose stored state type is not registered is
 *       discarded (full replay) instead of crashing the load.
 * </ul>
 *
 * <p>Serialization failures surface where Postgres raises them: a payload Jackson cannot serialize
 * (or a {@code @Encrypted} field with no resolvable subject) fails at append; an unregistered or
 * shape-incompatible event type, and a crypto-shredded field, surface at read.
 *
 * <p><b>Tracing.</b> In serializing mode a store given a tracing ({@link
 * #withTracing(StreamRuneTracing)}) starts the event-store spans {@code PostgresEventStore} starts,
 * with the same names, attributes and nesting: {@value TraceAttributes#EVENT_STORE_APPEND} with
 * {@value TraceAttributes#EVENT_STORE_APPEND_SERIALIZE}, {@value TraceAttributes#EVENT_STORE_LOAD}
 * with {@value TraceAttributes#EVENT_STORE_SNAPSHOT_LOAD} and {@value
 * TraceAttributes#EVENT_STORE_LOAD_DESERIALIZE}, {@value
 * TraceAttributes#EVENT_STORE_SNAPSHOT_SAVE}, and {@value TraceAttributes#EVENT_STORE_READ} for a
 * read that returns events under a span current on its thread. Its outbox entry count is always
 * {@code 0}: this store writes no outbox. In by-reference mode it starts no span: it neither
 * serializes nor does any I/O, so a span would time nothing a deployment has.
 *
 * <p>Thread-safe: appends are serialized by a single lock, reads operate on copy-on-write lists,
 * and an append publishes its events to each list in one step, so a read returns all of them or
 * none.
 */
public final class InMemoryEventStore implements EventStore {

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  /**
   * Receives the post-append notification carrying only the last assigned global offset —
   * NOTIFY-payload semantics: no events, no count, just the latest offset. Listeners must read the
   * events themselves (e.g. via {@link #readGlobalStream(GlobalOffset, int)}).
   */
  @FunctionalInterface
  public interface AppendListener {

    /**
     * Called after each successful append, outside the append lock.
     *
     * @param lastGlobalOffset the highest global offset assigned by the append
     */
    void onAppend(long lastGlobalOffset);
  }

  private static final Logger log = LoggerFactory.getLogger(InMemoryEventStore.class);

  // The stored data and the lock that orders its appends; shared by a store reopened over it.
  private final List<StoredEvent> globalStream;
  private final Map<StreamId, List<StoredEvent>> streams;
  private final Map<StreamId, Snapshot> snapshots;
  private final ReentrantLock appendLock;
  private final AtomicLong globalOffset;
  private final List<AppendListener> appendListeners = new CopyOnWriteArrayList<>();

  private volatile InMemoryCommandInbox commandInbox;

  /** The saga rows a forward command is fenced against; null until {@link #withSagaStore}. */
  private volatile InMemorySagaStore sagaStore;

  /** Test seam: run once inside the forward fence, before the fenced append. */
  private volatile Runnable afterFenceRead;

  /**
   * Registered snapshot migrations, applied on load to upgrade a stored snapshot to the expected
   * schema version before falling back to discard-and-replay. Empty by default. Validated once when
   * set (see {@link #withMigrations}), mirroring {@code PostgresEventStore}.
   */
  private volatile SnapshotMigrationChain migrations = SnapshotMigrationChain.of(null);

  /**
   * Event upcaster chain applied on the read path in serializing mode, mirroring {@code
   * PostgresEventStore}. {@link UpcasterChain#empty()} (no upcasting) by default; set via {@link
   * #withUpcasters}. An event's stored schema version is stamped at append time from this chain, so
   * upcasting is exercised by appending under an empty/older chain and reading under a chain with a
   * higher current version.
   */
  private volatile UpcasterChain upcasterChain = UpcasterChain.empty();

  /** The tracing of the serializing mode's spans; set by {@link #withTracing}. */
  private volatile StreamRuneTracing tracing = StreamRuneTracing.NOOP;

  /** Whether this store starts its spans: serializing mode, with the event-store area on. */
  private volatile boolean traced;

  /** Both null in by-reference mode, both set in serializing mode. */
  private final ObjectMapper objectMapper;

  private final EventTypeRegistry typeRegistry;

  /** Creates a store that keeps events and snapshot state by reference (no serialization). */
  public InMemoryEventStore() {
    this(null, null, null);
  }

  /**
   * Creates a store in serializing mode: every event payload, event metadata, and snapshot state is
   * serialized through the given {@link ObjectMapper} on append/save and deserialized on read, so
   * Jackson serialization failures — and the read-time crypto-shred / upcast / retired-type
   * behaviors — surface in unit tests exactly where {@code PostgresEventStore} would raise them.
   *
   * @param objectMapper the mapper to round-trip with; must be configured like the production one
   *     (see {@link #serializing(EventTypeRegistry, CryptoEngine)} for the default setup)
   * @param typeRegistry resolves event and state classes on the deserialization leg, exactly as
   *     {@code PostgresEventStore} does on read
   */
  public InMemoryEventStore(ObjectMapper objectMapper, EventTypeRegistry typeRegistry) {
    this(
        Objects.requireNonNull(objectMapper, "objectMapper is required"),
        Objects.requireNonNull(typeRegistry, "typeRegistry is required"),
        null);
  }

  /**
   * A store over the stored data of {@code shared}, or over empty storage when it is {@code null}.
   */
  private InMemoryEventStore(
      ObjectMapper objectMapper, EventTypeRegistry typeRegistry, InMemoryEventStore shared) {
    this.objectMapper = objectMapper;
    this.typeRegistry = typeRegistry;
    if (shared == null) {
      this.globalStream = new CopyOnWriteArrayList<>();
      this.streams = new ConcurrentHashMap<>();
      this.snapshots = new ConcurrentHashMap<>();
      this.appendLock = new ReentrantLock();
      this.globalOffset = new AtomicLong(0);
    } else {
      this.globalStream = shared.globalStream;
      this.streams = shared.streams;
      this.snapshots = shared.snapshots;
      this.appendLock = shared.appendLock;
      this.globalOffset = shared.globalOffset;
      this.commandInbox = shared.commandInbox;
    }
  }

  /**
   * Creates a store in serializing mode with the same {@link ObjectMapper} configuration as {@code
   * PostgresEventStore.create()}: {@link JavaTimeModule} always, {@link CryptoShreddingModule} when
   * a crypto engine is given.
   *
   * @param typeRegistry resolves event and state classes on the deserialization leg (required)
   * @param cryptoEngine crypto engine for {@code @Encrypted} field handling (optional, may be null)
   * @return store that serializes all payloads on append and deserializes them on read
   */
  public static InMemoryEventStore serializing(
      EventTypeRegistry typeRegistry, CryptoEngine cryptoEngine) {
    return new InMemoryEventStore(serializingMapper(cryptoEngine), typeRegistry);
  }

  /**
   * A new serializing store over the same stored data as {@code source} — its events, snapshots and
   * global offset counter, and the command inbox wired into it — as a deployment restarted with
   * another registry or crypto engine sees the data. Appends through either store are ordered by
   * one lock. The new store starts with no upcasters, migrations, saga store, tracing or append
   * listeners; wire them as on a fresh store. Stored events keep the schema versions they were
   * stamped with, so the upcaster chain of the new store reads them as a restarted deployment's
   * chain would.
   *
   * @param source a store in serializing mode
   * @param typeRegistry resolves event and state classes on read (required)
   * @param cryptoEngine crypto engine for {@code @Encrypted} fields (optional, may be null)
   * @throws IllegalArgumentException if {@code source} keeps its data by reference
   */
  static InMemoryEventStore sharingStorageOf(
      InMemoryEventStore source, EventTypeRegistry typeRegistry, CryptoEngine cryptoEngine) {
    Objects.requireNonNull(source, "source is required");
    if (source.objectMapper == null) {
      throw new IllegalArgumentException(
          "a store that keeps its data by reference cannot be reopened in serializing mode");
    }
    return new InMemoryEventStore(
        serializingMapper(cryptoEngine),
        Objects.requireNonNull(typeRegistry, "typeRegistry is required"),
        source);
  }

  /** The mapper {@code PostgresEventStore.create()} configures, for the serializing mode. */
  private static ObjectMapper serializingMapper(CryptoEngine cryptoEngine) {
    var mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    if (cryptoEngine != null) {
      mapper.registerModule(new CryptoShreddingModule(cryptoEngine));
    }
    return mapper;
  }

  /** Wires the inbox so {@link #appendWithKey} can dedup. Tests call this after construction. */
  public InMemoryEventStore withCommandInbox(InMemoryCommandInbox inbox) {
    this.commandInbox = inbox;
    return this;
  }

  /**
   * Wires the saga rows a saga's forward command is fenced against, so {@link #appendWithKey(
   * StreamId, List, Version, IdempotencyKey, String, SagaOrigin)} with a {@link SagaOrigin} refuses
   * a command of a saga that has left its forward phase, as {@code PostgresEventStore} does. Wire
   * the same {@link InMemorySagaStore} the saga runner keeps its rows in; {@link
   * #sagaFenceStorageIdentity()} then equals that store's {@link
   * InMemorySagaStore#storageIdentity()}. Tests call this after construction.
   *
   * @param sagaStore the saga store whose entries the fence reads (required)
   * @return this store, for chaining
   */
  public InMemoryEventStore withSagaStore(InMemorySagaStore sagaStore) {
    this.sagaStore = Objects.requireNonNull(sagaStore, "sagaStore is required");
    return this;
  }

  /**
   * The {@link InMemorySagaStore#storageIdentity()} of the store wired by {@link #withSagaStore};
   * {@code null} (cannot fence) until one is wired.
   */
  @Override
  public Object sagaFenceStorageIdentity() {
    InMemorySagaStore rows = sagaStore;
    return rows == null ? null : rows.storageIdentity();
  }

  /**
   * Test seam: {@code hook} runs once, on the appending thread, inside the next forward fence —
   * after the saga entry is checked and while its read lock is held, before the events are stored.
   */
  void afterFenceRead(Runnable hook) {
    this.afterFenceRead = hook;
  }

  /**
   * Registers snapshot migrations applied on load, mirroring {@code
   * PostgresEventStore.Builder.migrations(...)}. When {@link #load(StreamId, int)} finds a snapshot
   * whose schema version differs from the expected version, a complete chain among these migrations
   * upgrades it in place (only later events replay); a gapped chain discards the snapshot and
   * replays from events, exactly as a plain version mismatch does. Validated here (no duplicate
   * {@code fromVersion}; {@code toVersion > fromVersion}).
   *
   * @param migrations the migration steps (may be {@code null} or empty for no migrations)
   * @return this store, for chaining
   * @throws IllegalArgumentException if the migration set is invalid
   */
  public InMemoryEventStore withMigrations(List<SnapshotMigration> migrations) {
    this.migrations = SnapshotMigrationChain.of(migrations);
    return this;
  }

  /**
   * Registers event upcasters applied on the read path in serializing mode, mirroring {@code
   * PostgresEventStore.Builder.upcasters(...)}. An event is stamped with its schema version at
   * append time (from the chain then in effect); on read, an event stored below the chain's current
   * version for its type is deserialized to a map, upcast, and rebound to the resolved class. To
   * exercise upcasting against this double, append the old-shape event <em>before</em> calling this
   * (so it is stamped at the old version) and register the upcaster afterwards, then load.
   *
   * @param upcasters the upcaster steps (may be {@code null} or empty for no upcasting)
   * @return this store, for chaining
   * @throws IllegalStateException if two upcasters are registered for the same event type
   */
  public InMemoryEventStore withUpcasters(List<EventUpcaster> upcasters) {
    this.upcasterChain = upcasters == null ? UpcasterChain.empty() : new UpcasterChain(upcasters);
    return this;
  }

  /**
   * Starts the event-store spans through {@code tracing} when its {@link TracingArea#EVENT_STORE}
   * area is on, in serializing mode; see the class javadoc. A by-reference store starts none.
   *
   * @param tracing the tracing; {@link StreamRuneTracing#NOOP} traces nothing
   * @return this store, for chaining
   */
  public InMemoryEventStore withTracing(StreamRuneTracing tracing) {
    this.tracing = Objects.requireNonNull(tracing, "tracing is required");
    this.traced = objectMapper != null && tracing.enabled(TracingArea.EVENT_STORE);
    return this;
  }

  private boolean traced() {
    return traced;
  }

  /**
   * Runs {@code work} inside the event-store span {@code spanName}; {@code work} sets the
   * attributes known at its end on the scope it is handed.
   */
  private <T> T inSpan(String spanName, SpanAttributes attributes, Function<TraceScope, T> work) {
    TraceScope scope =
        tracing.start(TracingArea.EVENT_STORE, spanName, SpanKind.INTERNAL, attributes);
    try {
      T result = work.apply(scope);
      scope.recordSuccess();
      return result;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  /** The attributes {@code stream.id} and {@code aggregate.type} of a span about one stream. */
  private static SpanAttributes streamAttributes(StreamId streamId) {
    return SpanAttributes.builder()
        .put(TraceAttributes.STREAM_ID, LogSanitizer.sanitizeForTrace(streamId.value()))
        .put(TraceAttributes.AGGREGATE_TYPE, streamId.aggregateType().value())
        .build();
  }

  /**
   * Runs {@code append} inside the span {@value TraceAttributes#EVENT_STORE_APPEND}; a version
   * conflict is its outcome {@code CONFLICT}, a refusal of the saga forward fence its outcome
   * {@code FENCED}, neither an error.
   */
  private <T> T tracedAppend(
      StreamId streamId,
      int eventCount,
      Version expectedVersion,
      java.util.function.Supplier<T> append,
      Function<T, String> outcome) {
    TraceScope scope =
        tracing.start(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_APPEND,
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put(TraceAttributes.STREAM_ID, LogSanitizer.sanitizeForTrace(streamId.value()))
                .put(TraceAttributes.AGGREGATE_TYPE, streamId.aggregateType().value())
                .put(TraceAttributes.EVENT_COUNT, (long) eventCount)
                .put(TraceAttributes.EXPECTED_VERSION, expectedVersion.value())
                .put(TraceAttributes.OUTBOX_ENTRY_COUNT, 0L)
                .build());
    try {
      T result = append.get();
      scope.setAttribute(TraceAttributes.APPEND_OUTCOME, outcome.apply(result));
      scope.recordSuccess();
      return result;
    } catch (OptimisticLockException conflict) {
      scope.setAttribute(TraceAttributes.APPEND_OUTCOME, "CONFLICT");
      throw conflict;
    } catch (SagaForwardFencedException fenced) {
      scope.setAttribute(TraceAttributes.APPEND_OUTCOME, "FENCED");
      throw fenced;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  /** {@link #toEnvelope} over {@code events}, inside the load's deserialize span when traced. */
  private List<EventEnvelope> deserialize(Stream<StoredEvent> events, boolean traced) {
    if (!traced) {
      return events.map(this::toEnvelope).toList();
    }
    List<StoredEvent> stored = events.toList();
    UpcasterChain chain = upcasterChain;
    long upcasts =
        chain.isEmpty()
            ? 0
            : stored.stream()
                .filter(e -> e.schemaVersion() < chain.currentVersion(e.eventType()))
                .count();
    return inSpan(
        TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE,
        SpanAttributes.builder()
            .put(TraceAttributes.EVENT_COUNT, (long) stored.size())
            .put(TraceAttributes.UPCAST_COUNT, upcasts)
            .build(),
        scope -> stored.stream().map(this::toEnvelope).toList());
  }

  /**
   * {@link #toEnvelope} over a read's {@code events}, inside the read span when {@code traced} and
   * there are any; the span starts at {@code started}, when the read started.
   */
  private List<EventEnvelope> readEvents(
      Stream<StoredEvent> events, String readKind, boolean traced, Instant started) {
    if (!traced) {
      return events.map(this::toEnvelope).toList();
    }
    List<StoredEvent> stored = events.toList();
    if (stored.isEmpty()) {
      return List.of();
    }
    TraceScope scope =
        tracing.startChild(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_READ,
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put(TraceAttributes.EVENT_COUNT, (long) stored.size())
                .put(TraceAttributes.READ_KIND, readKind)
                .build(),
            started);
    try {
      List<EventEnvelope> read = stored.stream().map(this::toEnvelope).toList();
      scope.recordSuccess();
      return read;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  /**
   * {@link #prepareForAppend}, inside the append's serialize span when {@code traced}: the append
   * runs inside its own span.
   */
  private List<StoredEvent> serialize(
      StreamId streamId, List<EventEnvelope> events, boolean traced) {
    if (!traced) {
      return prepareForAppend(streamId, events);
    }
    return inSpan(
        TraceAttributes.EVENT_STORE_APPEND_SERIALIZE,
        SpanAttributes.builder().put(TraceAttributes.EVENT_COUNT, (long) events.size()).build(),
        scope -> prepareForAppend(streamId, events));
  }

  /**
   * Registers a listener notified after each successful append. Used by {@link
   * InMemoryEventSubscription}; tests may register their own to observe append activity.
   */
  public void addAppendListener(AppendListener listener) {
    Objects.requireNonNull(listener, "listener is required");
    appendListeners.add(listener);
  }

  /** Unregisters a registered append listener. Unknown listeners are ignored. */
  public void removeAppendListener(AppendListener listener) {
    Objects.requireNonNull(listener, "listener is required");
    appendListeners.remove(listener);
  }

  @Override
  public AggregateHistory load(StreamId streamId) {
    return load(streamId, 0);
  }

  @Override
  public AggregateHistory load(StreamId streamId, int expectedSnapshotVersion) {
    Objects.requireNonNull(streamId, "streamId is required");
    if (expectedSnapshotVersion < EventStore.IGNORE_SNAPSHOT) {
      throw new IllegalArgumentException(
          "expectedSnapshotVersion must be >= "
              + EventStore.IGNORE_SNAPSHOT
              + " (IGNORE_SNAPSHOT skips reading any snapshot, 0 skips the version check), got "
              + expectedSnapshotVersion);
    }
    if (!traced()) {
      return loadHistory(streamId, expectedSnapshotVersion, false);
    }
    return inSpan(
        TraceAttributes.EVENT_STORE_LOAD,
        streamAttributes(streamId),
        scope -> {
          AggregateHistory history = loadHistory(streamId, expectedSnapshotVersion, true);
          scope.setAttribute(TraceAttributes.EVENT_COUNT, (long) history.events().size());
          scope.setAttribute(TraceAttributes.SNAPSHOT_USED, history.snapshotState() != null);
          return history;
        });
  }

  /**
   * The load; {@code traced} says whether it runs inside its span, so it starts the snapshot and
   * deserialize spans under it.
   */
  private AggregateHistory loadHistory(
      StreamId streamId, int expectedSnapshotVersion, boolean traced) {
    AggregateState state = null;
    Version snapshotVersion = Version.initial();

    // EventStore.IGNORE_SNAPSHOT (the read-side counterpart of SnapshotPolicy.never())
    // never even looks the snapshot up — a stored snapshot left over from before the policy
    // switched to never() must not silently keep hydrating a possibly-stale schema. (The one-time
    // "legacy snapshot row found" diagnostic is production-only, on PostgresEventStore — this
    // in-memory test double has no logging dependency and the diagnostic has little value in
    // tests.)
    Snapshot snapshot =
        expectedSnapshotVersion == EventStore.IGNORE_SNAPSHOT ? null : snapshots.get(streamId);
    if (snapshot != null) {
      // Deserialize (decrypt, resolve, migrate) the stored snapshot on READ — mirroring
      // PostgresEventStore. A crypto-shredded @Encrypted field yields [REDACTED]; a retired stored
      // type is discarded (full replay); a version mismatch is migrated up or discarded.
      ResolvedSnapshot resolved =
          traced
              ? resolveSnapshotTraced(streamId, snapshot, expectedSnapshotVersion)
              : resolveSnapshot(snapshot, expectedSnapshotVersion);
      state = resolved.state();
      snapshotVersion = resolved.version();
    }

    long afterVersion = snapshotVersion.value();
    List<EventEnvelope> events =
        deserialize(
            streams.getOrDefault(streamId, List.<StoredEvent>of()).stream()
                .filter(e -> e.version().value() > afterVersion)
                .sorted(Comparator.comparingLong(e -> e.version().value())),
            traced);

    Version version = events.isEmpty() ? snapshotVersion : events.getLast().version();
    return new AggregateHistory(state, events, version, snapshotVersion);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Parity note: the empty-events early return below is the plain {@link EventStore} no-op and
   * drops nothing, because this store has no transactional-outbox overload — it never receives
   * outbox entries, so there is no second side effect for the no-op to skip. The sibling {@code
   * PostgresEventStore.append(StreamId, List, Version, List)} does take entries and therefore
   * REFUSES non-empty entries with an empty events list instead of returning success. Keep that
   * asymmetry in mind if an outbox-aware overload is ever added here.
   */
  @Override
  public AppendResult append(
      StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(events, "events is required");
    Objects.requireNonNull(expectedVersion, "expectedVersion is required");
    if (events.isEmpty()) {
      return new AppendResult(List.of(), expectedVersion);
    }
    if (!traced()) {
      return appendEvents(streamId, events, expectedVersion, false);
    }
    return tracedAppend(
        streamId,
        events.size(),
        expectedVersion,
        () -> appendEvents(streamId, events, expectedVersion, true),
        result -> "APPENDED");
  }

  private AppendResult appendEvents(
      StreamId streamId, List<EventEnvelope> events, Version expectedVersion, boolean traced) {
    // In serializing mode, serialize payloads BEFORE touching any state: a Jackson failure on the
    // third event must leave nothing appended, mirroring Postgres' transaction rollback.
    List<StoredEvent> toAppend = serialize(streamId, events, traced);

    AppendResult result;
    // Serialize appends across ALL streams so events become visible in strict global_offset
    // order, mirroring PostgresEventStore, whose appends serialize on the row lock of the
    // single-row global_offset_sequence counter, held until the append commits.
    appendLock.lock();
    try {
      result = appendLocked(streamId, toAppend, expectedVersion);
    } finally {
      appendLock.unlock();
    }

    // Notify after releasing the lock — Postgres sends pg_notify after commit, when the events
    // are already visible to readers. Best-effort like pg_notify: a throwing listener must not
    // fail an append that already happened, and must not prevent other listeners from waking.
    if (!result.globalOffsets().isEmpty()) {
      long lastOffset = result.globalOffsets().getLast().value();
      for (AppendListener listener : appendListeners) {
        try {
          listener.onAppend(lastOffset);
        } catch (RuntimeException _) {
          // Best-effort, mirroring PostgresEventStore's post-commit NOTIFY: subscribers that miss
          // a wakeup fall back to polling.
        }
      }
    }
    return result;
  }

  /**
   * The body of an append that runs <em>while holding {@code appendLock}</em>. Called by both
   * {@link #append} and {@link #appendWithKey} so claim+append is atomic under the same lock. Must
   * be called with {@code appendLock} already held; must not acquire it again.
   *
   * @param streamId target stream (not null, pre-validated by callers)
   * @param toAppend prepared events to append — carrying serialized JSON in serializing mode, or
   *     the event/metadata objects by reference otherwise; their global offset and version are
   *     placeholders, stamped here
   * @param expectedVersion caller's expected stream head
   * @return the result carrying assigned global offsets and the new stream head
   */
  private AppendResult appendLocked(
      StreamId streamId, List<StoredEvent> toAppend, Version expectedVersion) {
    var streamEvents = streams.computeIfAbsent(streamId, k -> new CopyOnWriteArrayList<>());

    if (!toAppend.isEmpty()) {
      // Strict optimistic-concurrency head check, mirroring
      // PostgresEventStore's post-lock head read (see its insertEvents javadoc) — appendLock
      // (held by both callers of this method) already serializes ALL appends, to every stream, so
      // by the time we reach here no concurrent append can be mid-flight and this read is
      // race-free. Guarded on !toAppend.isEmpty() so the documented zero-event no-op (see
      // EventStore#append) is unchanged: it carries no events to order against the stream. Before
      // this check a conflict was detected only when a computed version {@code expectedVersion + 1
      // .. expectedVersion + n} already existed (the (aggregate_type, aggregate_id, version)
      // unique-constraint model), so an expectedVersion BEYOND the current head passed silently
      // and left a version gap; global vs per-stream replay order then diverged on a later, lower
      // append.
      long actualHead = streamEvents.isEmpty() ? 0L : streamEvents.getLast().version().value();
      if (expectedVersion.value() != actualHead) {
        throw new OptimisticLockException(
            "Version conflict on stream '"
                + LogSanitizer.sanitizeForLog(streamId.value())
                + "': expected version "
                + expectedVersion.value()
                + " but actual head is "
                + actualHead);
      }
    }

    List<GlobalOffset> globalOffsets = new ArrayList<>();
    List<StoredEvent> batch = new ArrayList<>(toAppend.size());
    long version = expectedVersion.value();
    for (var template : toAppend) {
      version++;
      GlobalOffset offset = GlobalOffset.of(globalOffset.incrementAndGet());
      batch.add(
          new StoredEvent(
              offset,
              streamId,
              new Version(version),
              template.eventType(),
              template.event(),
              template.metadata(),
              template.payloadJson(),
              template.metadataJson(),
              template.schemaVersion()));
      globalOffsets.add(offset);
    }
    // Publish the whole append in one step on each list, as a PostgreSQL commit makes all of an
    // append's rows visible at once: a copy-on-write list replaces its array once per addAll, so a
    // read (which iterates one array) sees every event of the append or none of them. Adding the
    // events one at a time let a concurrent load, readStream or readGlobalStream return the first
    // events of an append without the rest.
    streamEvents.addAll(batch);
    globalStream.addAll(batch);

    return new AppendResult(List.copyOf(globalOffsets), new Version(version));
  }

  @Override
  public IdempotentAppendResult appendWithKey(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      IdempotencyKey idempotencyKey,
      String commandType) {
    return appendWithKey(streamId, events, expectedVersion, idempotencyKey, commandType, null);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Here the fence reads the entry of the {@link InMemorySagaStore} wired by {@link
   * #withSagaStore} under that saga's read lock, taken after this store's append lock and held
   * until the events and the inbox row are stored.
   *
   * @throws UnsupportedOperationException if no command inbox is wired (see {@link
   *     #withCommandInbox}), or if {@code forwardOf} is given and no saga store is wired
   */
  @Override
  public IdempotentAppendResult appendWithKey(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      IdempotencyKey idempotencyKey,
      String commandType,
      SagaOrigin forwardOf) {
    // A store without an inbox does not support the keyed append: refused first, before the
    // arguments are checked, as PostgresEventStore built without a command inbox refuses it.
    if (commandInbox == null) {
      throw new UnsupportedOperationException(
          "No InMemoryCommandInbox wired; call withCommandInbox(...)");
    }
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(events, "events is required");
    Objects.requireNonNull(expectedVersion, "expectedVersion is required");
    Objects.requireNonNull(idempotencyKey, "idempotencyKey is required");
    CommandInbox.requireCommandType(commandType);

    InMemorySagaStore rows = sagaStore;
    if (forwardOf != null && rows == null) {
      throw new UnsupportedOperationException(
          "No InMemorySagaStore wired; call withSagaStore(...)");
    }
    if (events.isEmpty() || !traced()) {
      return appendEventsWithKey(
          streamId, events, expectedVersion, idempotencyKey, commandType, forwardOf, rows, false);
    }
    return tracedAppend(
        streamId,
        events.size(),
        expectedVersion,
        () ->
            appendEventsWithKey(
                streamId,
                events,
                expectedVersion,
                idempotencyKey,
                commandType,
                forwardOf,
                rows,
                true),
        result -> result.alreadyApplied() ? "DUPLICATE" : "APPENDED");
  }

  private IdempotentAppendResult appendEventsWithKey(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      IdempotencyKey idempotencyKey,
      String commandType,
      SagaOrigin forwardOf,
      InMemorySagaStore rows,
      boolean traced) {
    // In serializing mode, serialize payloads BEFORE taking the lock (same as append):
    // a Jackson failure on the third event must leave nothing appended.
    List<StoredEvent> toAppend = serialize(streamId, events, traced);

    AppendResult result;
    appendLock.lock();
    try {
      var existing = commandInbox.find(idempotencyKey);
      if (existing.isPresent()) {
        var r = existing.get();
        // Enforce the same key-collision contract PostgresEventStore
        // does. A test double that silently returns another command's offsets makes every
        // in-memory test a false witness to a defect that discards a command in production.
        r.requireBoundTo(commandType, streamId);
        return new IdempotentAppendResult(r.globalOffsets(), r.finalVersion(), true);
      }
      if (forwardOf == null) {
        result = appendAndRecord(streamId, toAppend, expectedVersion, idempotencyKey, commandType);
      } else {
        result =
            rows.underForwardFence(
                forwardOf,
                () -> {
                  Runnable hook = afterFenceRead;
                  if (hook != null) {
                    afterFenceRead = null;
                    hook.run();
                  }
                  return appendAndRecord(
                      streamId, toAppend, expectedVersion, idempotencyKey, commandType);
                });
      }
    } finally {
      appendLock.unlock();
    }

    // Fire append listeners after releasing the lock, exactly as append() does.
    if (!result.globalOffsets().isEmpty()) {
      long lastOffset = result.globalOffsets().getLast().value();
      for (AppendListener listener : appendListeners) {
        try {
          listener.onAppend(lastOffset);
        } catch (RuntimeException _) {
          // Best-effort.
        }
      }
    }
    return new IdempotentAppendResult(result.globalOffsets(), result.finalVersion(), false);
  }

  /** Stores the events and records the key in the inbox; {@code appendLock} must be held. */
  private AppendResult appendAndRecord(
      StreamId streamId,
      List<StoredEvent> toAppend,
      Version expectedVersion,
      IdempotencyKey idempotencyKey,
      String commandType) {
    AppendResult result = appendLocked(streamId, toAppend, expectedVersion);
    commandInbox.record(
        new CommandInbox.InboxResult(
            idempotencyKey,
            commandType,
            streamId,
            result.finalVersion(),
            result.globalOffsets(),
            Instant.now()));
    return result;
  }

  /**
   * Prepares events for storage. In by-reference mode carries the event and metadata objects
   * directly; in serializing mode SERIALIZES each payload and metadata to JSON (the same
   * store-the-ciphertext model as {@code PostgresEventStore}) and stamps the current schema version
   * — deserialization, decryption, and upcasting are deferred to the read paths. A Jackson
   * serialization failure (an unmappable payload, or a {@code @Encrypted} field with no resolvable
   * subject) is raised here, before any state changes, so a failure on the third event leaves
   * nothing appended. Returned templates carry placeholder offset/version, stamped in {@link
   * #appendLocked}.
   */
  private List<StoredEvent> prepareForAppend(StreamId streamId, List<EventEnvelope> events) {
    List<StoredEvent> templates = new ArrayList<>(events.size());
    for (var envelope : events) {
      if (objectMapper == null) {
        templates.add(
            new StoredEvent(
                envelope.globalOffset(),
                streamId,
                envelope.version(),
                envelope.eventType(),
                envelope.event(),
                envelope.metadata(),
                null,
                null,
                0));
      } else {
        String payloadJson;
        String metadataJson;
        try {
          payloadJson = objectMapper.writeValueAsString(envelope.event());
          metadataJson = objectMapper.writeValueAsString(envelope.metadata());
        } catch (JsonProcessingException e) {
          throw new EventStoreException(
              "Failed to serialize event for stream: "
                  + LogSanitizer.sanitizeForLog(streamId.value()),
              e);
        }
        int schemaVersion = upcasterChain.currentVersion(envelope.eventType());
        templates.add(
            new StoredEvent(
                envelope.globalOffset(),
                streamId,
                envelope.version(),
                envelope.eventType(),
                null,
                null,
                payloadJson,
                metadataJson,
                schemaVersion));
      }
    }
    return templates;
  }

  /**
   * Materializes a stored event into an {@link EventEnvelope}. By-reference mode returns the stored
   * objects directly. Serializing mode DESERIALIZES the stored JSON on every read — resolving the
   * event class through the registry (so an unregistered type surfaces here, not at append),
   * upcasting a below-current-version payload through the {@link #withUpcasters upcaster chain},
   * and decrypting {@code @Encrypted} fields (so a crypto-shredded subject yields {@link
   * CryptoShreddingModule#REDACTED}) — mirroring {@code PostgresEventStore.toEnvelope}.
   */
  private EventEnvelope toEnvelope(StoredEvent stored) {
    if (objectMapper == null) {
      return new EventEnvelope(
          stored.globalOffset(),
          stored.streamId(),
          stored.version(),
          stored.eventType(),
          stored.event(),
          stored.metadata());
    }
    EventType eventType = stored.eventType();
    Class<?> eventClass = typeRegistry.resolveEventType(eventType);
    try {
      Object event;
      // A reader with no upcasters understands only v1 — the write path stamps schema_version = 1
      // with the empty chain (see the append path above) — and the empty chain reports
      // currentVersion = 1, so a stored schema_version >= 2 meets the guard below instead of
      // falling through to a plain readValue that would silently mis-bind the payload. Mirrors
      // PostgresEventStore so the double cannot mask the divergence.
      int currentVersion = upcasterChain.currentVersion(eventType);
      if (stored.schemaVersion() > currentVersion) {
        // A newer writer stamped it (a replica of the next release, or a store with upcasters
        // this one lacks). Reading on would silently mis-bind the payload. Fail closed with the
        // dedicated retry-later type, mirroring PostgresEventStore.
        throw new EventSchemaAheadException(
            "Event type "
                + LogSanitizer.sanitizeForLog(eventType.value())
                + " for stream "
                + LogSanitizer.sanitizeForLog(stored.streamId().value())
                + " is stored at schema_version "
                + stored.schemaVersion()
                + " but this event store's upcaster chain only reaches version "
                + currentVersion
                + ". A newer writer stamped it: either a replica of a newer release of this"
                + " application, or an event store wired with upcasters this one lacks. Reading"
                + " it would silently mis-bind the payload, so the read is refused.",
            eventType,
            stored.schemaVersion(),
            currentVersion);
      }
      // A store without upcasters binds every row directly, a schema_version below 1 included.
      if (!upcasterChain.isEmpty() && stored.schemaVersion() < currentVersion) {
        // Deserialize to map, upcast, then bind to the target class. convertValue() binds the
        // already-parsed map directly — no intermediate JSON string — mirroring
        // PostgresEventStore.
        Map<String, Object> rawData = objectMapper.readValue(stored.payloadJson(), MAP_TYPE);
        Map<String, Object> upcasted =
            upcasterChain.upcast(eventType, rawData, stored.schemaVersion());
        event = objectMapper.convertValue(upcasted, eventClass);
      } else {
        event = objectMapper.readValue(stored.payloadJson(), eventClass);
      }
      EventMetadata metadata = objectMapper.readValue(stored.metadataJson(), EventMetadata.class);
      return new EventEnvelope(
          stored.globalOffset(),
          stored.streamId(),
          stored.version(),
          eventType,
          (DomainEvent) event,
          metadata);
    } catch (IOException | RuntimeException e) {
      // Parity with PostgresEventStore: the catch is deliberately wider than
      // JsonProcessingException, mirroring PostgresEventStore.toEnvelope. The upcast branch runs
      // USER upcaster code bare and binds its output via objectMapper.convertValue (unchecked
      // IllegalArgumentException), and a ROOT-level @Encrypted failure (bare
      // CryptoOperationException / CryptoMappingException from the tree-stage decrypt) propagates
      // out of readValue unwrapped — all escaped the old catch raw, so a unit test against this
      // double witnessed classification behavior production does not have.
      if (e instanceof EventStoreException ese) {
        // Already typed by the store itself (e.g. the fail-closed throw above) — never
        // double-wrap.
        throw ese;
      }
      if (hasTransientCryptoCause(e)) {
        // Engine-raised transient key-store evidence: stays the PLAIN EventStoreException so
        // read-path classifiers retry instead of terminally halting (the read-path retry contract).
        throw new EventStoreException(
            "Failed to deserialize event data for stream: "
                + LogSanitizer.sanitizeForLog(stored.streamId().value()),
            e);
      }
      throw new EventDeserializationException(
          "Failed to deserialize event data for stream: "
              + LogSanitizer.sanitizeForLog(stored.streamId().value()),
          e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>This store has no {@code SnapshotPurger} and keeps no purge record: every load reports
   * generation {@code 0}, and the snapshot is stored whatever {@code snapshotPurgeGeneration} says,
   * so this method always returns {@code true}. The purge guard is exercised against {@code
   * PostgresEventStore}.
   */
  @Override
  public boolean saveSnapshot(
      StreamId streamId,
      Version version,
      AggregateState state,
      int snapshotVersion,
      long snapshotPurgeGeneration) {
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(version, "version is required");
    if (state == null) {
      throw new IllegalArgumentException("state must not be null");
    }
    if (snapshotVersion < 1) {
      throw new IllegalArgumentException("snapshotVersion must be >= 1");
    }
    if (snapshotPurgeGeneration < 0) {
      throw new IllegalArgumentException(
          "snapshotPurgeGeneration must not be negative, got " + snapshotPurgeGeneration);
    }
    if (objectMapper == null) {
      snapshots.put(streamId, new Snapshot(version, snapshotVersion, state, null, null));
      return true;
    }
    if (!traced()) {
      return writeSnapshot(streamId, version, state, snapshotVersion);
    }
    return inSpan(
        TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE,
        SpanAttributes.builder()
            .put(TraceAttributes.STREAM_ID, LogSanitizer.sanitizeForTrace(streamId.value()))
            .put(TraceAttributes.STREAM_VERSION, version.value())
            .build(),
        scope -> {
          boolean saved = writeSnapshot(streamId, version, state, snapshotVersion);
          scope.setAttribute(TraceAttributes.SNAPSHOT_OUTCOME, saved ? "SAVED" : "SKIPPED");
          return saved;
        });
  }

  /** The serializing mode's snapshot write. */
  private boolean writeSnapshot(
      StreamId streamId, Version version, AggregateState state, int snapshotVersion) {
    // Serializing mode: validate the simple-name registry contract and store the SERIALIZED form
    // (JSON / ciphertext), exactly like PostgresEventStore.saveSnapshot. Deserialization —
    // including @Encrypted decryption and retired-type resolution — is deferred to load().
    String stateType = state.getClass().getSimpleName();
    Class<?> resolved;
    try {
      resolved = typeRegistry.resolveStateType(stateType);
    } catch (RuntimeException e) {
      throw new EventStoreException(
          "Snapshot state type '"
              + stateType
              + "' does not resolve in the EventTypeRegistry — register "
              + state.getClass().getName()
              + " under its simple name so the snapshot can be rehydrated on load",
          e);
    }
    if (resolved != state.getClass()) {
      throw new EventStoreException(
          "Snapshot state type '"
              + stateType
              + "' resolves to "
              + resolved.getName()
              + " in the EventTypeRegistry, but the state being saved is "
              + state.getClass().getName()
              + " — snapshots are stored by simple class name, so two state classes sharing a"
              + " simple name would rehydrate as the wrong type");
    }
    String stateJson;
    try {
      stateJson = objectMapper.writeValueAsString(state);
    } catch (JsonProcessingException e) {
      throw new EventStoreException(
          "Failed to serialize snapshot for stream: "
              + LogSanitizer.sanitizeForLog(streamId.value()),
          e);
    }
    snapshots.put(streamId, new Snapshot(version, snapshotVersion, null, stateJson, stateType));
    return true;
  }

  /**
   * {@link #resolveSnapshot} of the stream's stored {@code snapshot}, inside the span {@value
   * TraceAttributes#EVENT_STORE_SNAPSHOT_LOAD}, with the outcome {@code HIT} or {@code DISCARDED}.
   */
  private ResolvedSnapshot resolveSnapshotTraced(
      StreamId streamId, Snapshot snapshot, int expectedSnapshotVersion) {
    return inSpan(
        TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD,
        SpanAttributes.builder()
            .put(TraceAttributes.STREAM_ID, LogSanitizer.sanitizeForTrace(streamId.value()))
            .build(),
        scope -> {
          ResolvedSnapshot resolved = resolveSnapshot(snapshot, expectedSnapshotVersion);
          scope.setAttribute(
              TraceAttributes.SNAPSHOT_OUTCOME, resolved.state() != null ? "HIT" : "DISCARDED");
          scope.setAttribute(TraceAttributes.STREAM_VERSION, resolved.version().value());
          return resolved;
        });
  }

  /**
   * Resolves a stored snapshot on the read path. In serializing mode the stored JSON is
   * deserialized to its ORIGINAL (stored) runtime class — re-running {@code @Encrypted} decryption
   * (a crypto-shredded subject yields {@link CryptoShreddingModule#REDACTED}) and resolving the
   * stored {@code state_type} through the registry via the NON-throwing resolver (a retired/renamed
   * type is discarded → full replay, never a crash). A version mismatch is migrated up through the
   * registered chain; one the chain cannot bridge is discarded before the state is deserialized,
   * and so is a retired stored type — mirroring {@code PostgresEventStore.load}.
   */
  private ResolvedSnapshot resolveSnapshot(Snapshot snapshot, int expectedSnapshotVersion) {
    boolean compatible =
        expectedSnapshotVersion == 0 || snapshot.snapshotVersion() == expectedSnapshotVersion;
    if (!compatible && !migrations.canReach(snapshot.snapshotVersion(), expectedSnapshotVersion)) {
      // A version mismatch no complete migration chain bridges is discarded before the stored
      // state is read, as PostgresEventStore decides it: a snapshot the load will not use is never
      // deserialized (nor decrypted), so a crypto failure in it cannot fail the load, and no
      // migration step runs on it.
      return ResolvedSnapshot.discard();
    }

    AggregateState storedState;
    if (objectMapper == null) {
      storedState = snapshot.state();
    } else {
      Class<?> resolved = resolveStateTypeOrNull(snapshot.stateType());
      if (resolved == null) {
        // Retired/renamed stored type: undeserializable → discard and replay from the beginning.
        return ResolvedSnapshot.discard();
      }
      try {
        storedState = deserializeSnapshotState(snapshot.stateJson(), resolved);
      } catch (EventStoreException e) {
        // Parity with PostgresEventStore: a plain Jackson/IO deserialize failure (a field
        // renamed/removed without a snapshotVersion bump) means the derived snapshot CACHE is
        // unreadable — discard it and replay all events from the beginning rather than crashing the
        // load. A crypto decrypt failure (CryptoOperationException, possibly wrapped by Jackson
        // into a JsonMappingException) is fail-closed and must still propagate.
        if (hasCryptoCause(e)) {
          throw e;
        }
        log.error(
            "Snapshot is undeserializable; discarding the derived snapshot cache and replaying all"
                + " events from the beginning (bump the snapshot version when changing"
                + " aggregate-state shape)",
            e);
        return ResolvedSnapshot.discard();
      }
    }

    if (compatible) {
      return new ResolvedSnapshot(storedState, snapshot.version());
    }
    // Version mismatch with a complete chain (checked above): migrate the stored state up to the
    // expected schema version, so only later events replay. The empty result below stays for
    // safety; a chain canReach accepted lands on the expected version or throws.
    //
    // SnapshotMigrationChain.migrate() can also THROW — NullPointerException when a
    // step returns null, or IllegalStateException when a step's result does not match its
    // declared toType() — a programming error IN THE STEP, not a chain configuration gap.
    // Uncaught, that propagated straight out of load() and wedged the aggregate exactly like the
    // undeserializable-snapshot case below is designed to avoid. A migration step is
    // registered CODE, and a bug in it makes the stored snapshot exactly as untrustworthy as an
    // unreadable payload, so it gets the same discard-and-replay treatment — except a crypto
    // failure, which must stay fatal (mirrors PostgresEventStore.load / hasCryptoCause below):
    // discarding would risk silently dropping a GDPR-erased subject's redaction, and replaying
    // from events would hit the identical crypto failure on the same encrypted fields anyway.
    try {
      return migrations
          .migrate(storedState, snapshot.snapshotVersion(), expectedSnapshotVersion)
          .map(migrated -> new ResolvedSnapshot(migrated, snapshot.version()))
          .orElseGet(ResolvedSnapshot::discard);
    } catch (RuntimeException e) {
      if (hasCryptoCause(e)) {
        // Parity with PostgresEventStore: the crypto-caused migration failure
        // still PROPAGATES (fail-closed — never discarded as a stale cache), but the frame is
        // typed with the same two-way routing as the deserialize path: transient key-store
        // evidence stays the plain EventStoreException; the module's deterministic
        // CryptoMappingException gets the typed EventDeserializationException so classifiers see
        // a per-aggregate stored-data fact, not infrastructure.
        if (hasTransientCryptoCause(e)) {
          throw new EventStoreException("Snapshot migration failed", e);
        }
        throw new EventDeserializationException("Snapshot migration failed", e);
      }
      log.error(
          "Snapshot migration failed; discarding the derived snapshot cache and replaying all"
              + " events from the beginning. This means a registered SnapshotMigration step has a"
              + " bug — inspect the migration chain for the version this snapshot was stored at.",
          e);
      return ResolvedSnapshot.discard();
    }
  }

  private AggregateState deserializeSnapshotState(String stateJson, Class<?> stateClass) {
    try {
      return (AggregateState) objectMapper.readValue(stateJson, stateClass);
    } catch (IOException | RuntimeException e) {
      // Parity with PostgresEventStore: wider than JsonProcessingException, mirroring
      // PostgresEventStore's snapshot deserialize catch — a ROOT-level failure (corrupt @Encrypted
      // Base64, a bare CryptoOperationException, or the deterministic CryptoMappingException from
      // a constructor rejecting the post-forget [REDACTED] tombstone) propagates out of readValue
      // unwrapped and escaped the old catch raw. The frame is typed with the same two-way crypto
      // routing; propagate-vs-discard is decided by the caller (resolveSnapshot), which rethrows
      // any crypto-caused EventStoreException (fail-closed) and discards the rest.
      if (hasTransientCryptoCause(e)) {
        throw new EventStoreException("Failed to deserialize snapshot state", e);
      }
      if (hasCryptoCause(e)) {
        throw new EventDeserializationException("Failed to deserialize snapshot state", e);
      }
      throw new EventStoreException("Failed to deserialize snapshot state", e);
    }
  }

  /**
   * Resolves the class for a stored snapshot {@code state_type}, returning {@code null} instead of
   * throwing when the type is not registered — so a snapshot written under a since-retired state
   * class is discarded (full replay) rather than crashing the load, exactly like {@code
   * PostgresEventStore.resolveStateTypeOrNull}.
   */
  private Class<?> resolveStateTypeOrNull(String stateType) {
    try {
      return typeRegistry.resolveStateType(stateType);
    } catch (UnknownEventTypeException _) {
      return null;
    }
  }

  /**
   * True when {@code t} or any exception in its cause chain is a {@link CryptoOperationException} —
   * so the snapshot-discard fallback keeps the crypto fail-closed contract (a wrapped decrypt
   * failure propagates, never silently discarded), mirroring {@code PostgresEventStore}.
   */
  static boolean hasCryptoCause(Throwable t) {
    // Bounded walk (depth 50) so a self-referential/cyclic cause chain terminates
    // instead of spinning forever, matching VirtualThreadCommandBus.hasSubjectForgottenCause.
    Throwable c = t;
    for (int depth = 0; c != null && depth < 50; depth++, c = c.getCause()) {
      if (c instanceof CryptoOperationException) {
        return true;
      }
    }
    return false;
  }

  /**
   * True when {@code t}'s chain carries a bare {@link CryptoOperationException} that is NOT the
   * deterministic {@link CryptoMappingException} subtype — read as engine-raised transient
   * key-store evidence — mirroring {@code PostgresEventStore.hasTransientCryptoCause}. {@code
   * CryptoShreddingModule} never wraps engine exceptions, so every bare {@code
   * CryptoOperationException} in a chain came from a {@code CryptoEngine} backend — and every
   * shipped engine (filesystem, PostgreSQL, Vault, AWS KMS, and {@link InMemoryCryptoEngine})
   * raises the deterministic subtype for every blob-property verdict — decided on the data at rest
   * before any backend call (ciphertext too short, unsupported format, key or envelope version,
   * malformed Vault ciphertext shape, a local AEAD tag mismatch) or returned BY the backend as a
   * verdict on the blob itself (Vault's HTTP 400 {@code "invalid ciphertext ..."} family — EXCEPT
   * {@code "version is too new"}, handled below — and its AEAD failure text if forwarded; KMS's
   * {@code InvalidCiphertextException}) — so for the shipped engines a bare one IS outage-or-policy
   * evidence. Deliberately bare, because an operator (or, for the version-too-new case,
   * time/replication) can reverse them: a Vault 400 for a version refused by the key's {@code
   * min_decryption_version} policy, a Vault 400 for a ciphertext version newer than THIS node
   * currently knows about ({@code "invalid ciphertext: version is too new"} — a lagging secondary,
   * a restore from an older snapshot, or key material temporarily missing, not a verdict on the
   * blob), and every KMS key-state / wrong-key / throttling failure. A third-party engine that
   * raises the bare type for a deterministic condition is classified transient here and retried
   * indefinitely — engine authors must raise {@code CryptoMappingException} for conditions a retry
   * can never fix.
   */
  static boolean hasTransientCryptoCause(Throwable t) {
    Throwable c = t;
    for (int depth = 0; c != null && depth < 50; depth++, c = c.getCause()) {
      if (c instanceof CryptoOperationException && !(c instanceof CryptoMappingException)) {
        return true;
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * <p>The head of the completed appends: read under the append lock, so it never names an offset
   * an append in progress has assigned and not stored yet. An event that becomes readable after
   * this method returned has a greater offset, as with {@code PostgresEventStore}, whose head is
   * the highest committed offset.
   */
  @Override
  public GlobalOffset lastGlobalOffset() {
    appendLock.lock();
    try {
      return GlobalOffset.of(globalOffset.get());
    } finally {
      appendLock.unlock();
    }
  }

  @Override
  public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
    Objects.requireNonNull(afterOffset, "afterOffset is required");
    boolean traced = traced();
    Instant started = traced ? Instant.now() : null;
    return readEvents(
        globalStream.stream()
            .filter(e -> e.globalOffset().value() > afterOffset.value())
            .sorted(Comparator.comparingLong(e -> e.globalOffset().value()))
            .limit(maxCount),
        "GLOBAL",
        traced,
        started);
  }

  @Override
  public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(afterVersion, "afterVersion is required");
    boolean traced = traced();
    Instant started = traced ? Instant.now() : null;
    return readEvents(
        streams.getOrDefault(streamId, List.<StoredEvent>of()).stream()
            .filter(e -> e.version().value() > afterVersion.value())
            .sorted(Comparator.comparingLong(e -> e.version().value()))
            .limit(maxCount),
        "STREAM",
        traced,
        started);
  }

  /** Whether this store serializes its events: it has an {@link EventTypeRegistry} to read with. */
  boolean serializes() {
    return objectMapper != null;
  }

  /**
   * The mapper this store serializes with, {@code null} when it keeps its events by reference. For
   * {@link EventStreamFixtures}, which writes the payloads of a serializing store with a copy.
   */
  ObjectMapper objectMapper() {
    return objectMapper;
  }

  /**
   * The schema version this store reads an event of {@code eventType} at: the current version of
   * its upcaster chain, {@code 1} without upcasters. For {@link EventStreamFixtures}.
   */
  int schemaVersionOf(EventType eventType) {
    return upcasterChain.currentVersion(eventType);
  }

  /**
   * Binds an event in the stored form of the serializing mode — its payload JSON, its metadata JSON
   * and the schema version of the payload — the way a read binds a stored event: the registry
   * resolves the type, the upcaster chain upcasts a payload below its current version, and a
   * payload above it is refused. For {@link EventStreamFixtures}, whose files hold events in that
   * form.
   *
   * @throws IllegalStateException if this store keeps its events by reference
   */
  EventEnvelope bindStoredForm(
      StreamId streamId,
      Version version,
      EventType eventType,
      int schemaVersion,
      String payloadJson,
      String metadataJson) {
    if (!serializes()) {
      throw new IllegalStateException(
          "this InMemoryEventStore keeps its events by reference and has no EventTypeRegistry");
    }
    return toEnvelope(
        new StoredEvent(
            GlobalOffset.initial(),
            streamId,
            version,
            eventType,
            null,
            null,
            payloadJson,
            metadataJson,
            schemaVersion));
  }

  /**
   * One stored event. In by-reference mode {@code event}/{@code metadata} hold the objects and the
   * JSON/schema fields are unused; in serializing mode {@code payloadJson}/{@code
   * metadataJson}/{@code schemaVersion} hold the serialized form and {@code event}/{@code metadata}
   * are null — the same store-the-raw-row / deserialize-on-read split {@code PostgresEventStore}
   * uses.
   */
  private record StoredEvent(
      GlobalOffset globalOffset,
      StreamId streamId,
      Version version,
      EventType eventType,
      DomainEvent event,
      EventMetadata metadata,
      String payloadJson,
      String metadataJson,
      int schemaVersion) {}

  /**
   * One stored snapshot. By-reference mode holds {@code state}; serializing mode holds {@code
   * stateJson} + {@code stateType} (the stored simple name) and deserializes on read.
   */
  private record Snapshot(
      Version version,
      int snapshotVersion,
      AggregateState state,
      String stateJson,
      String stateType) {}

  /**
   * The outcome of resolving a stored snapshot on read: the state to seed replay from (null to
   * discard) and the version after which events replay.
   */
  private record ResolvedSnapshot(AggregateState state, Version version) {
    static ResolvedSnapshot discard() {
      return new ResolvedSnapshot(null, Version.initial());
    }
  }
}
