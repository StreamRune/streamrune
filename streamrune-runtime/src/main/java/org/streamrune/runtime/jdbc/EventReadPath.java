package org.streamrune.runtime.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventSchemaAheadException;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.Internal;
import org.streamrune.core.SnapshotMigrationChain;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.UpcasterChain;

/**
 * The read path of an event store, shared by the backends: decoding the raw columns of an event row
 * into an {@link EventEnvelope}, deciding whether a load starts from the stream's snapshot,
 * resolving and migrating that snapshot, assembling the load's {@link AggregateHistory}, and the
 * contiguity guards of a load and of a global read, with the spans, logs and metrics of each step.
 *
 * <p><b>Rows first, decoding after.</b> A store reads raw columns ({@link #readRawRow}, {@link
 * #decideSnapshot}) on its own connection and returns that connection before it calls {@link
 * #assembleHistory}, {@link #historyIgnoringSnapshot} or {@link #readEvents}: deserialization, and
 * any decrypt of an {@code @Encrypted} field it triggers, never runs while the store holds a pooled
 * connection. A load whose snapshot is discarded re-reads the whole stream through the store's
 * {@code reRead} function, on a connection of the store's own.
 *
 * <p><b>Failures.</b> A stored event this reader cannot read is classified the same on every
 * backend: {@link EventSchemaAheadException} for a row stored above the reader's schema version, a
 * plain {@link EventStoreException} for a transient crypto failure (a bare {@link
 * CryptoOperationException}), {@link EventDeserializationException} for every other failure to bind
 * the stored row. An unregistered event type is the registry's own {@link
 * UnknownEventTypeException}, raised before any of these. A snapshot that cannot be deserialized or
 * migrated is discarded and the stream replays from its start, except for a crypto failure, which
 * is never discarded.
 *
 * <p><b>Helper rules.</b> It works on the caller's result set and never opens, commits, rolls back
 * or closes a connection, and lets every {@link SQLException} propagate unwrapped. It logs through
 * the store's {@link Logger}, so log lines keep the store's category, and traces through the
 * store's already-guarded {@link StreamRuneTracing} under the store's {@code traced} flag. Its one
 * per-instance state, the streams whose version-gap warning was logged, belongs to the store
 * instance that built it: a store builds exactly one.
 *
 * @hidden
 */
@Internal
public final class EventReadPath {

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  /**
   * What {@link #resolveSnapshot} returns for a stored snapshot the load discards: the whole stream
   * replays from its start.
   */
  private static final Object DISCARDED_SNAPSHOT = new Object();

  private final ObjectMapper objectMapper;
  private final EventTypeRegistry typeRegistry;
  private final UpcasterChain upcasterChain;
  private final SnapshotMigrationChain snapshotMigrations;
  private final StreamRuneMetrics metrics;
  private final StreamRuneTracing tracing;
  private final boolean traced;
  private final Logger log;
  private final Function<RawEventRow, GlobalOffset> offsetBeforeTypeResolution;

  // Streams whose non-contiguity WARN fired once for this instance.
  private final Set<String> gapWarnedStreams = ConcurrentHashMap.newKeySet();

  /**
   * One event row's raw columns, read while the connection is open; decoded after it closed.
   *
   * @param globalOffset the {@code global_offset} column
   * @param aggregateType the {@code aggregate_type} column
   * @param aggregateId the {@code aggregate_id} column
   * @param version the {@code version} column
   * @param eventType the {@code event_type} column
   * @param payloadJson the {@code payload} column
   * @param metadataJson the {@code metadata} column
   * @param schemaVersion the {@code schema_version} column
   */
  public record RawEventRow(
      long globalOffset,
      String aggregateType,
      String aggregateId,
      long version,
      String eventType,
      String payloadJson,
      String metadataJson,
      int schemaVersion) {

    /**
     * The stored stream's {@code type:id} text, sanitized for an exception or log message.
     *
     * @return the sanitized stream text
     */
    public String streamText() {
      return LogSanitizer.sanitizeForLog(aggregateType + StreamId.SEPARATOR + aggregateId);
    }
  }

  /**
   * What the load read of a stream's snapshot row, before any deserialization.
   *
   * @param stored whether the stream has a stored snapshot, usable or not: only then does a traced
   *     load start the snapshot span
   * @param stateJson the state to deserialize, or {@code null} when the load does not start from a
   *     snapshot
   * @param stateClass the class the stored state type resolves to, or {@code null}
   * @param version the stream version the snapshot was taken at; {@link Version#initial()} when the
   *     load replays every event
   * @param migrateFromVersion the stored snapshot version to migrate from, or {@code -1} when no
   *     migration is pending
   * @param purgeGeneration the purge generation of the stream's snapshot row; {@code 0} when there
   *     is none
   */
  public record StoredSnapshot(
      boolean stored,
      String stateJson,
      Class<?> stateClass,
      Version version,
      int migrateFromVersion,
      long purgeGeneration) {

    /** No snapshot row: every event replays. */
    public static final StoredSnapshot NONE =
        new StoredSnapshot(false, null, null, Version.initial(), -1, 0);
  }

  /**
   * The contiguity guard of one global read: the read returns only a contiguous run of offsets. The
   * run starts at the first row above the read's checkpoint, so a leading hole is paged over, and
   * stops at the first hole inside the page, which it logs once at WARN. Not thread-safe; one per
   * read, from {@link #globalContiguity}.
   */
  public final class GlobalContiguity {

    private final GlobalOffset afterOffset;
    private long expectedNext = -1;

    private GlobalContiguity(GlobalOffset afterOffset) {
      this.afterOffset = afterOffset;
    }

    /**
     * Whether the row at {@code globalOffset} continues the run: the first row always does; a later
     * row only at the offset after the one before it. A row that does not is logged at WARN and the
     * read stops before it.
     *
     * @param globalOffset the {@code global_offset} of the next row the query returned
     * @return {@code true} to read the row, {@code false} to stop the read before it
     */
    public boolean admits(long globalOffset) {
      if (expectedNext == -1) {
        expectedNext = globalOffset;
      } else if (globalOffset != expectedNext) {
        log.warn(
            "non-contiguous global_offset observed after {}: expected {}, got {}",
            afterOffset,
            expectedNext,
            globalOffset);
        return false;
      }
      expectedNext++;
      return true;
    }
  }

  /**
   * The read path of one store instance.
   *
   * @param objectMapper the mapper of events and snapshot states
   * @param typeRegistry resolves stored event and state types
   * @param upcasterChain the upcasters applied on read
   * @param snapshotMigrations the snapshot migrations applied on load
   * @param metrics records discarded snapshots
   * @param guardedTracing the store's tracing, already guarded; never guarded here
   * @param traced whether the store's {@link TracingArea#EVENT_STORE} area is traced
   * @param log the store's logger, so every log line keeps the store's category
   * @param offsetBeforeTypeResolution {@code null} to parse a row's global offset inside the
   *     decoding's failure handling, when the envelope is built, so a negative offset is an {@link
   *     EventDeserializationException}; otherwise the function that parses it first, before the
   *     event type is resolved and outside that handling, so a negative offset is the raw {@link
   *     IllegalArgumentException}. It keeps each backend's placement until the backends align it,
   *     and is removed then.
   */
  public EventReadPath(
      ObjectMapper objectMapper,
      EventTypeRegistry typeRegistry,
      UpcasterChain upcasterChain,
      SnapshotMigrationChain snapshotMigrations,
      StreamRuneMetrics metrics,
      StreamRuneTracing guardedTracing,
      boolean traced,
      Logger log,
      Function<RawEventRow, GlobalOffset> offsetBeforeTypeResolution) {
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper is required");
    this.typeRegistry = Objects.requireNonNull(typeRegistry, "typeRegistry is required");
    this.upcasterChain = Objects.requireNonNull(upcasterChain, "upcasterChain is required");
    this.snapshotMigrations =
        Objects.requireNonNull(snapshotMigrations, "snapshotMigrations is required");
    this.metrics = Objects.requireNonNull(metrics, "metrics is required");
    this.tracing = Objects.requireNonNull(guardedTracing, "guardedTracing is required");
    this.traced = traced;
    this.log = Objects.requireNonNull(log, "log is required");
    this.offsetBeforeTypeResolution = offsetBeforeTypeResolution;
  }

  // ── Rows ──────────────────────────────────────────────────────────────────────────────────────

  /**
   * Reads the current row's raw event columns by name. Touches no deserialization: safe while a
   * pooled connection is held.
   *
   * @param rs the caller's result set, positioned on an event row
   * @return the raw row
   * @throws SQLException if a read fails
   */
  public static RawEventRow readRawRow(ResultSet rs) throws SQLException {
    return new RawEventRow(
        rs.getLong("global_offset"),
        rs.getString("aggregate_type"),
        rs.getString("aggregate_id"),
        rs.getLong("version"),
        rs.getString("event_type"),
        rs.getString("payload"),
        rs.getString("metadata"),
        rs.getInt("schema_version"));
  }

  /**
   * Decides, from the current snapshot row and without deserializing it, whether the load can start
   * from it: compatible and of a resolvable type, or migratable by a complete chain from a
   * resolvable stored type. Otherwise the load replays every event. Reads {@code purge_generation},
   * {@code state_type} and {@code snapshot_version}, and {@code state_payload} and {@code version}
   * only when the snapshot is usable. A purged row has no state and is no snapshot; its purge
   * generation is read all the same. The caller handles a stream without a row as {@link
   * StoredSnapshot#NONE}.
   *
   * @param row the caller's result set, positioned on the stream's snapshot row
   * @param expectedSnapshotVersion the snapshot version the load expects; {@code 0} accepts any
   * @return what the load starts from
   * @throws SQLException if a read fails
   */
  public StoredSnapshot decideSnapshot(ResultSet row, int expectedSnapshotVersion)
      throws SQLException {
    long purgeGeneration = row.getLong("purge_generation");
    String stateType = row.getString("state_type");
    if (stateType == null) {
      return new StoredSnapshot(false, null, null, Version.initial(), -1, purgeGeneration);
    }
    int storedVersion = row.getInt("snapshot_version");
    // A retired or renamed state type is undeserializable: discard, never crash the load.
    Class<?> stateClass = resolveStateTypeOrNull(stateType);
    boolean compatible = expectedSnapshotVersion == 0 || storedVersion == expectedSnapshotVersion;
    boolean usable =
        stateClass != null
            && (compatible || snapshotMigrations.canReach(storedVersion, expectedSnapshotVersion));
    if (!usable) {
      return new StoredSnapshot(true, null, null, Version.initial(), -1, purgeGeneration);
    }
    return new StoredSnapshot(
        true,
        row.getString("state_payload"),
        stateClass,
        new Version(row.getLong("version")),
        compatible ? -1 : storedVersion,
        purgeGeneration);
  }

  // ── Load ──────────────────────────────────────────────────────────────────────────────────────

  /**
   * The load of a stream: checks the arguments, then runs {@code loadHistory} inside the span
   * {@value TraceAttributes#EVENT_STORE_LOAD} when traced, recording the event count and whether a
   * snapshot was used.
   *
   * @param streamId the stream (required)
   * @param expectedSnapshotVersion the snapshot version the load expects, at least {@link
   *     EventStore#IGNORE_SNAPSHOT}
   * @param loadHistory the store's load
   * @return what {@code loadHistory} returned
   * @throws NullPointerException if {@code streamId} is {@code null}
   * @throws IllegalArgumentException if {@code expectedSnapshotVersion} is below {@link
   *     EventStore#IGNORE_SNAPSHOT}
   */
  public AggregateHistory load(
      StreamId streamId, int expectedSnapshotVersion, Supplier<AggregateHistory> loadHistory) {
    Objects.requireNonNull(streamId, "streamId is required");
    if (expectedSnapshotVersion < EventStore.IGNORE_SNAPSHOT) {
      throw new IllegalArgumentException(
          "expectedSnapshotVersion must be >= "
              + EventStore.IGNORE_SNAPSHOT
              + " (IGNORE_SNAPSHOT skips reading any snapshot, 0 skips the version check), got "
              + expectedSnapshotVersion);
    }
    if (!traced) {
      return loadHistory.get();
    }
    TraceScope scope =
        tracing.start(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_LOAD,
            SpanKind.INTERNAL,
            streamAttributes(streamId));
    try {
      AggregateHistory history = loadHistory.get();
      scope.setAttribute(TraceAttributes.EVENT_COUNT, (long) history.events().size());
      scope.setAttribute(TraceAttributes.SNAPSHOT_USED, history.snapshotState() != null);
      scope.recordSuccess();
      return history;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  private static SpanAttributes streamAttributes(StreamId streamId) {
    return SpanAttributes.builder()
        .put(TraceAttributes.STREAM_ID, LogSanitizer.sanitizeForTrace(streamId.value()))
        .put(TraceAttributes.AGGREGATE_TYPE, streamId.aggregateType().value())
        .build();
  }

  /**
   * The history of a load from what the store read, after its connection was returned: resolves the
   * snapshot (inside the span {@value TraceAttributes#EVENT_STORE_SNAPSHOT_LOAD} when one is stored
   * and the load is traced). When the snapshot is discarded, the rows were narrowed to the versions
   * after it, so the whole stream is read again through {@code reRead} and replayed, with the
   * snapshot row's purge generation. Otherwise the rows are deserialized, a version gap is warned
   * about once per stream, and the history starts from the snapshot.
   *
   * @param streamId the stream
   * @param snapshot what {@link #decideSnapshot} decided, or {@link StoredSnapshot#NONE}
   * @param rawRows the stream's rows after {@code snapshot.version()}
   * @param expectedSnapshotVersion the snapshot version the load expects
   * @param reRead reads the stream's rows after the given version on a connection of its own
   * @return the history
   */
  public AggregateHistory assembleHistory(
      StreamId streamId,
      StoredSnapshot snapshot,
      List<RawEventRow> rawRows,
      int expectedSnapshotVersion,
      Function<Version, List<RawEventRow>> reRead) {
    Object snapshotState =
        snapshot.stored() && traced
            ? resolveSnapshotTraced(streamId, snapshot, expectedSnapshotVersion)
            : resolveSnapshot(streamId, snapshot, expectedSnapshotVersion);
    if (snapshotState == DISCARDED_SNAPSHOT) {
      // The event read was narrowed to the versions after the discarded snapshot: read the whole
      // stream again.
      List<EventEnvelope> replayEvents = deserialize(reRead.apply(Version.initial()));
      Version replayLast =
          replayEvents.isEmpty() ? Version.initial() : replayEvents.getLast().version();
      return new AggregateHistory(
          null, replayEvents, replayLast, Version.initial(), snapshot.purgeGeneration());
    }
    List<EventEnvelope> events = deserialize(rawRows);
    warnIfNonContiguous(streamId, snapshot.version(), events);
    Version lastVersion = events.isEmpty() ? snapshot.version() : events.getLast().version();
    return new AggregateHistory(
        (AggregateState) snapshotState,
        events,
        lastVersion,
        snapshot.version(),
        snapshot.purgeGeneration());
  }

  /**
   * The history of an {@link EventStore#IGNORE_SNAPSHOT} load: every event, never a snapshot, a
   * version gap warned about once per stream.
   *
   * @param streamId the stream
   * @param rawRows every row of the stream
   * @return the history
   */
  public AggregateHistory historyIgnoringSnapshot(StreamId streamId, List<RawEventRow> rawRows) {
    List<EventEnvelope> events = deserialize(rawRows);
    warnIfNonContiguous(streamId, Version.initial(), events);
    Version version = events.isEmpty() ? Version.initial() : events.getLast().version();
    return new AggregateHistory(null, events, version, Version.initial());
  }

  private Object resolveSnapshotTraced(
      StreamId streamId, StoredSnapshot snapshot, int expectedSnapshotVersion) {
    TraceScope scope =
        tracing.start(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD,
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put(TraceAttributes.STREAM_ID, LogSanitizer.sanitizeForTrace(streamId.value()))
                .build());
    try {
      Object state = resolveSnapshot(streamId, snapshot, expectedSnapshotVersion);
      boolean hit = snapshot.stateJson() != null && state != DISCARDED_SNAPSHOT;
      scope.setAttribute(TraceAttributes.SNAPSHOT_OUTCOME, hit ? "HIT" : "DISCARDED");
      scope.setAttribute(TraceAttributes.STREAM_VERSION, hit ? snapshot.version().value() : 0L);
      scope.recordSuccess();
      return state;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  /**
   * The state of the snapshot the load read, deserialized and, when pending, migrated: {@code null}
   * when there is no usable snapshot, {@link #DISCARDED_SNAPSHOT} when the stored one cannot be
   * used. A crypto failure is never discarded: a transient one is a plain {@link
   * EventStoreException}, a deterministic one an {@link EventDeserializationException}.
   */
  private Object resolveSnapshot(
      StreamId streamId, StoredSnapshot snapshot, int expectedSnapshotVersion) {
    if (snapshot.stateJson() == null) {
      return null;
    }
    AggregateState deserialized;
    try {
      deserialized =
          (AggregateState) objectMapper.readValue(snapshot.stateJson(), snapshot.stateClass());
    } catch (IOException | RuntimeException e) {
      // Wider than IOException: a root-level property failure (a corrupt Base64 ciphertext) is a
      // raw RuntimeException and must reach the discard below too.
      if (hasCryptoCause(e)) {
        if (hasTransientCryptoCause(e)) {
          throw new EventStoreException(
              "Failed to deserialize snapshot state for stream: " + streamText(streamId), e);
        }
        throw new EventDeserializationException(
            "Failed to deserialize snapshot state for stream: " + streamText(streamId), e);
      }
      log.error(
          "Snapshot for stream {} is undeserializable ({}); discarding the derived snapshot cache"
              + " and replaying all events from the beginning. This usually means an"
              + " aggregate-state field was renamed or removed without bumping the snapshot"
              + " version — bump SnapshotPolicy's snapshotVersion so fresh snapshots are written.",
          LogSanitizer.sanitizeForLog(streamId.value()),
          e.toString(),
          e);
      recordSnapshotDiscarded("deserialize_failure", streamId);
      return DISCARDED_SNAPSHOT;
    }
    if (snapshot.migrateFromVersion() < 0) {
      return deserialized;
    }
    int from = snapshot.migrateFromVersion();
    Optional<AggregateState> migrated;
    try {
      migrated = snapshotMigrations.migrate(deserialized, from, expectedSnapshotVersion);
    } catch (RuntimeException e) {
      if (hasCryptoCause(e)) {
        if (hasTransientCryptoCause(e)) {
          throw new EventStoreException(
              "Snapshot migration failed for stream: " + streamText(streamId), e);
        }
        throw new EventDeserializationException(
            "Snapshot migration failed for stream: " + streamText(streamId), e);
      }
      log.error(
          "Snapshot migration for stream {} failed ({}); discarding the derived snapshot"
              + " cache and replaying all events from the beginning. This means a registered"
              + " SnapshotMigration step has a bug — inspect the migration chain for the"
              + " version this stream's snapshot was stored at.",
          LogSanitizer.sanitizeForLog(streamId.value()),
          e.toString(),
          e);
      recordSnapshotDiscarded("migration_failure", streamId);
      return DISCARDED_SNAPSHOT;
    }
    // canReach held when the row was read; a chain that then fails to apply is a bug of the chain
    // itself, a hard failure rather than a silent discard.
    return migrated.orElseThrow(
        () ->
            new EventStoreException(
                "Snapshot migration chain reported reachable from version "
                    + from
                    + " to "
                    + expectedSnapshotVersion
                    + " but failed to apply for stream: "
                    + streamText(streamId)));
  }

  /** Records a discarded snapshot; a failing metrics backend must not wedge the load. */
  private void recordSnapshotDiscarded(String reason, StreamId streamId) {
    try {
      metrics.recordSnapshotDiscarded(reason);
    } catch (RuntimeException e) {
      log.warn(
          "Metrics recordSnapshotDiscarded({}) failed for stream {}",
          reason,
          LogSanitizer.sanitizeForLog(streamId.value()),
          e);
    }
  }

  /**
   * The events of a load, deserialized from {@code rows} inside the span {@value
   * TraceAttributes#EVENT_STORE_LOAD_DESERIALIZE} when traced. Package-private: stores reach it
   * through {@link #assembleHistory} and {@link #historyIgnoringSnapshot}.
   */
  List<EventEnvelope> deserialize(List<RawEventRow> rows) {
    if (!traced) {
      return toEnvelopes(rows);
    }
    TraceScope scope =
        tracing.start(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE,
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put(TraceAttributes.EVENT_COUNT, (long) rows.size())
                .put(TraceAttributes.UPCAST_COUNT, upcastCount(rows))
                .build());
    try {
      List<EventEnvelope> events = toEnvelopes(rows);
      scope.recordSuccess();
      return events;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  private long upcastCount(List<RawEventRow> rows) {
    if (upcasterChain.isEmpty()) {
      return 0;
    }
    long count = 0;
    for (RawEventRow row : rows) {
      if (row.schemaVersion() < upcasterChain.currentVersion(new EventType(row.eventType()))) {
        count++;
      }
    }
    return count;
  }

  private List<EventEnvelope> toEnvelopes(List<RawEventRow> rows) {
    return rows.stream().map(this::toEnvelope).toList();
  }

  /**
   * Once per stream and instance, a WARN when a loaded stream's versions are not contiguous: rows
   * written, deleted or restored outside the store. Never renumbers.
   */
  private void warnIfNonContiguous(
      StreamId streamId, Version baseline, List<EventEnvelope> events) {
    long expected = baseline.value();
    for (EventEnvelope event : events) {
      long actual = event.version().value();
      if (actual != expected + 1) {
        if (gapWarnedStreams.add(streamId.value())) {
          log.warn(
              "Stream '{}' has non-contiguous versions (expected {} but found {}, right after"
                  + " version {}) — rows were written, deleted or restored outside this store."
                  + " This store never renumbers events automatically; reconstructing the intended"
                  + " history is a data-recovery decision. Logged once per stream per process.",
              LogSanitizer.sanitizeForLog(streamId.value()),
              expected + 1,
              actual,
              expected);
        }
        return;
      }
      expected = actual;
    }
  }

  private Class<?> resolveStateTypeOrNull(String stateType) {
    try {
      return typeRegistry.resolveStateType(stateType);
    } catch (UnknownEventTypeException _) {
      return null;
    }
  }

  // ── Reads ─────────────────────────────────────────────────────────────────────────────────────

  /**
   * The events of a global or stream read, deserialized from {@code rows}; inside the span {@value
   * TraceAttributes#EVENT_STORE_READ} when traced, the read returned at least one row and a span is
   * current on this thread, whose child it is. The span starts at {@code started}, when the read's
   * query started, so it covers the query and the deserialization.
   *
   * @param rows the rows the read returned
   * @param readKind the {@value TraceAttributes#READ_KIND} attribute ({@code GLOBAL}, {@code
   *     STREAM})
   * @param started when the read's query started; {@code null} when not traced
   * @return the events, in the order of {@code rows}
   */
  public List<EventEnvelope> readEvents(List<RawEventRow> rows, String readKind, Instant started) {
    if (!traced || rows.isEmpty()) {
      return toEnvelopes(rows);
    }
    TraceScope scope =
        tracing.startChild(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_READ,
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put(TraceAttributes.EVENT_COUNT, (long) rows.size())
                .put(TraceAttributes.READ_KIND, readKind)
                .build(),
            started);
    try {
      List<EventEnvelope> events = toEnvelopes(rows);
      scope.recordSuccess();
      return events;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  /**
   * A new contiguity guard for one global read.
   *
   * @param afterOffset the read's checkpoint, named in the WARN of a hole
   * @return the guard; one per read
   */
  public GlobalContiguity globalContiguity(GlobalOffset afterOffset) {
    return new GlobalContiguity(afterOffset);
  }

  /**
   * Refuses a negative {@code maxCount} before the query, with the {@link IllegalArgumentException}
   * the {@link EventStore} javadoc promises.
   *
   * @param maxCount the most events a read returns
   * @throws IllegalArgumentException if {@code maxCount} is negative
   */
  public static void requireMaxCount(int maxCount) {
    if (maxCount < 0) {
      throw new IllegalArgumentException("maxCount must not be negative, got " + maxCount);
    }
  }

  // ── Decoding ──────────────────────────────────────────────────────────────────────────────────

  /**
   * The message of the refusal to read an event stored at a {@code schema_version} above this
   * reader's: names the type, the stream and both versions, and the two ways it arises.
   *
   * @param eventType the event type, sanitized
   * @param stream the stream text, sanitized
   * @param storedVersion the row's {@code schema_version}
   * @param readerVersion the version this reader's upcaster chain reaches
   * @return the message
   */
  public static String schemaAheadMessage(
      String eventType, String stream, int storedVersion, int readerVersion) {
    return "Event type "
        + eventType
        + " for stream "
        + stream
        + " is stored at schema_version "
        + storedVersion
        + " but this event store's upcaster chain only reaches version "
        + readerVersion
        + ". A newer writer stamped it: either a replica of a newer release of this application"
        + " (a rolling deployment that bumps this event's schema is in progress, and this replica"
        + " reads it once it is upgraded), or an event store wired with upcasters this one lacks."
        + " Reading it would silently mis-bind the payload, so the read is refused; register the"
        + " same upcasters on every event store that reads this database.";
  }

  /**
   * Decodes one raw row, upcasting it when it is stored below the reader's version: {@link
   * EventSchemaAheadException} for a row stored above the reader's version, a plain {@link
   * EventStoreException} for a transient crypto failure, {@link EventDeserializationException} for
   * every other failure to bind the stored row; an {@link EventStoreException} raised inside passes
   * through unwrapped. The event type is resolved before that handling, so an unregistered type is
   * the registry's {@link UnknownEventTypeException}. Package-private: stores reach it through
   * {@link #readEvents}, {@link #assembleHistory} and {@link #historyIgnoringSnapshot}.
   */
  EventEnvelope toEnvelope(RawEventRow row) {
    GlobalOffset offsetParsedFirst =
        offsetBeforeTypeResolution == null ? null : offsetBeforeTypeResolution.apply(row);
    EventType eventType = new EventType(row.eventType());
    int schemaVersion = row.schemaVersion();
    Class<?> eventClass = typeRegistry.resolveEventType(eventType);
    try {
      int currentVersion = upcasterChain.currentVersion(eventType);
      if (schemaVersion > currentVersion) {
        throw new EventSchemaAheadException(
            schemaAheadMessage(
                LogSanitizer.sanitizeForLog(eventType.value()),
                row.streamText(),
                schemaVersion,
                currentVersion),
            eventType,
            schemaVersion,
            currentVersion);
      }
      Object event;
      if (!upcasterChain.isEmpty() && schemaVersion < currentVersion) {
        Map<String, Object> rawData = objectMapper.readValue(row.payloadJson(), MAP_TYPE);
        Map<String, Object> upcasted = upcasterChain.upcast(eventType, rawData, schemaVersion);
        event = objectMapper.convertValue(upcasted, eventClass);
      } else {
        event = objectMapper.readValue(row.payloadJson(), eventClass);
      }
      EventMetadata metadata = objectMapper.readValue(row.metadataJson(), EventMetadata.class);
      return new EventEnvelope(
          offsetBeforeTypeResolution != null
              ? offsetParsedFirst
              : GlobalOffset.of(row.globalOffset()),
          StreamId.of(new AggregateType(row.aggregateType()), new AggregateId(row.aggregateId())),
          new Version(row.version()),
          eventType,
          (DomainEvent) event,
          metadata);
    } catch (IOException | RuntimeException e) {
      if (e instanceof EventStoreException ese) {
        throw ese;
      }
      if (hasTransientCryptoCause(e)) {
        throw new EventStoreException(
            "Failed to deserialize event data for stream: " + row.streamText(), e);
      }
      throw new EventDeserializationException(
          "Failed to deserialize event data for stream: " + row.streamText(), e);
    }
  }

  /**
   * Whether {@code t}'s cause chain (bounded, see {@link Causes}) holds a {@link
   * CryptoOperationException}.
   *
   * @param t the throwable to walk
   * @return {@code true} if a link is a crypto failure of either kind
   */
  public static boolean hasCryptoCause(Throwable t) {
    return Causes.find(t, CryptoOperationException.class) != null;
  }

  /**
   * Whether {@code t}'s cause chain holds a bare {@link CryptoOperationException}, not the
   * deterministic {@link CryptoMappingException}: engine-raised key-store evidence, retried rather
   * than quarantined.
   *
   * @param t the throwable to walk
   * @return {@code true} if a link is a transient crypto failure
   */
  public static boolean hasTransientCryptoCause(Throwable t) {
    return Causes.anyMatch(
        t, c -> c instanceof CryptoOperationException && !(c instanceof CryptoMappingException));
  }

  private static String streamText(StreamId streamId) {
    return LogSanitizer.sanitizeForLog(streamId.value());
  }
}
