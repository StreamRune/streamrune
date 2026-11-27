package org.streamrune.runtime.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventSchemaAheadException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.SnapshotMigrationChain;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.core.upcasting.UpcasterChain;
import org.streamrune.runtime.jdbc.EventReadPath.GlobalContiguity;
import org.streamrune.runtime.jdbc.EventReadPath.RawEventRow;
import org.streamrune.runtime.jdbc.EventReadPath.StoredSnapshot;
import org.streamrune.test.RecordingStreamRuneMetrics;

class EventReadPathTest {

  /** An event; stored at schema version 1 with a {@code title}, upcast to {@code name} at 2. */
  public record Opened(String name) implements DomainEvent {}

  /** A snapshot state at snapshot version 1. */
  public record CounterState(int count) implements AggregateState {}

  /** The snapshot state a migration from version 1 to 2 produces. */
  public record CounterStateV2(int count, String label) implements AggregateState {}

  /** A state whose construction fails the way a decrypt does: transient or permanent. */
  public record CryptoState(String mode) implements AggregateState {
    public CryptoState {
      if ("transient".equals(mode)) {
        throw new CryptoOperationException("key store unavailable");
      }
      if ("permanent".equals(mode)) {
        throw new CryptoMappingException("tombstone rejected");
      }
    }
  }

  private static final String CATEGORY = "test.store";
  private static final StreamId STREAM =
      StreamId.of(new AggregateType("Account"), new AggregateId("a-1"));
  private static final String SHOWN = "Account:a-1";
  private static final ObjectMapper MAPPER =
      StoreObjectMappers.eventsAndSagas(null, StreamRuneMetrics.NOOP);
  private static final EventTypeRegistry REGISTRY =
      SimpleEventTypeRegistry.builder()
          .registerEvent("Opened", Opened.class)
          .registerState("CounterState", CounterState.class)
          .registerState("CryptoState", CryptoState.class)
          .build();
  private static final String META = metadataJson();

  private static final String EVENT_TABLE =
      "CREATE TABLE event_stream (schema_version INTEGER, metadata TEXT, payload TEXT,"
          + " event_type TEXT, version INTEGER, aggregate_id TEXT, aggregate_type TEXT,"
          + " global_offset INTEGER PRIMARY KEY)";
  private static final String SNAPSHOT_TABLE =
      "CREATE TABLE snapshot_store (aggregate_type TEXT, aggregate_id TEXT, version INTEGER,"
          + " state_type TEXT, state_payload TEXT, snapshot_version INTEGER,"
          + " purge_generation INTEGER)";

  @TempDir Path dir;

  private final RecordingStreamRuneMetrics metrics = new RecordingStreamRuneMetrics();
  private Capture store;
  private Capture own;

  @BeforeEach
  void attach() {
    store = Capture.attach(CATEGORY);
    own = Capture.attach(EventReadPath.class.getName());
  }

  @AfterEach
  void detach() {
    store.close();
    own.close();
  }

  // ── Raw rows and snapshot decisions ────────────────────────────────────────────────────────────

  @Test
  void readRawRow_readsEveryColumnByName() throws Exception {
    var db = JdbcTestDatabase.create(dir, EVENT_TABLE);
    try (Connection c = db.connection()) {
      try (PreparedStatement ps =
          c.prepareStatement(
              "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version,"
                  + " event_type, payload, metadata, schema_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
        ps.setLong(1, 42);
        ps.setString(2, "Account");
        ps.setString(3, "a-1");
        ps.setLong(4, 7);
        ps.setString(5, "Opened");
        ps.setString(6, "{\"name\":\"n\"}");
        ps.setString(7, META);
        ps.setInt(8, 3);
        ps.executeUpdate();
      }
      try (PreparedStatement ps = c.prepareStatement("SELECT * FROM event_stream");
          ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        RawEventRow row = EventReadPath.readRawRow(rs);
        assertThat(row)
            .isEqualTo(
                new RawEventRow(42, "Account", "a-1", 7, "Opened", "{\"name\":\"n\"}", META, 3));
        assertThat(row.streamText()).isEqualTo(SHOWN);
      }
    }
  }

  @Test
  void rawEventRow_streamTextIsSanitized() {
    var row = new RawEventRow(1, "Account", "a-1\nforged", 1, "Opened", "{}", META, 1);

    assertThat(row.streamText()).doesNotContain("\n").startsWith("Account:a-1");
  }

  @Test
  void storedSnapshotNone_isNotStoredAndStartsFromTheBeginning() {
    assertThat(StoredSnapshot.NONE)
        .isEqualTo(new StoredSnapshot(false, null, null, Version.initial(), -1, 0));
  }

  @Test
  void decideSnapshot_noStateType_isNotStoredAndKeepsThePurgeGeneration() throws Exception {
    var db = JdbcTestDatabase.create(dir, SNAPSHOT_TABLE);
    insertSnapshot(db, null, null, 0, 0, 7);

    assertThat(decide(db, path(), 0))
        .isEqualTo(new StoredSnapshot(false, null, null, Version.initial(), -1, 7));
  }

  @Test
  void decideSnapshot_compatible_readsPayloadAndVersion() throws Exception {
    var db = JdbcTestDatabase.create(dir, SNAPSHOT_TABLE);
    insertSnapshot(db, "CounterState", "{\"count\":5}", 5, 2, 3);

    StoredSnapshot exact = decide(db, path(), 2);
    StoredSnapshot unchecked = decide(db, path(), 0);

    var expected =
        new StoredSnapshot(true, "{\"count\":5}", CounterState.class, new Version(5), -1, 3);
    assertThat(exact).isEqualTo(expected);
    assertThat(unchecked).isEqualTo(expected);
  }

  @Test
  void decideSnapshot_incompatibleButReachable_migratesFromTheStoredVersion() throws Exception {
    var db = JdbcTestDatabase.create(dir, SNAPSHOT_TABLE);
    insertSnapshot(db, "CounterState", "{\"count\":5}", 5, 1, 0);
    EventReadPath path = path(UpcasterChain.empty(), chain(toV2()), metrics, null, false, null);

    assertThat(decide(db, path, 2))
        .isEqualTo(
            new StoredSnapshot(true, "{\"count\":5}", CounterState.class, new Version(5), 1, 0));
  }

  @Test
  void decideSnapshot_unusable_neverReadsThePayload() throws Exception {
    var unusable = new StoredSnapshot(true, null, null, Version.initial(), -1, 9);

    // A state type the registry no longer knows.
    assertThat(path().decideSnapshot(rowFailingOnPayload(9, "Retired", 1), 0)).isEqualTo(unusable);
    // A known type stored at a version no migration chain reaches.
    assertThat(path().decideSnapshot(rowFailingOnPayload(9, "CounterState", 1), 3))
        .isEqualTo(unusable);
  }

  // ── Decoding one row ───────────────────────────────────────────────────────────────────────────

  @Test
  void toEnvelope_decodesTheRow() {
    EventEnvelope envelope = path().toEnvelope(row(5, 2));

    assertThat(envelope.globalOffset()).isEqualTo(GlobalOffset.of(5));
    assertThat(envelope.streamId()).isEqualTo(STREAM);
    assertThat(envelope.version()).isEqualTo(new Version(2));
    assertThat(envelope.eventType()).isEqualTo(new EventType("Opened"));
    assertThat(envelope.event()).isEqualTo(new Opened("n2"));
    assertThat(envelope.metadata().eventId()).isEqualTo(EventId.of("e-1"));
  }

  @Test
  void toEnvelope_unknownTypeIsRaw() {
    var row = new RawEventRow(1, "Account", "a-1", 1, "Nope", "{not json", META, 1);

    var e = assertThrows(UnknownEventTypeException.class, () -> path().toEnvelope(row));

    assertThat(e).isExactlyInstanceOf(UnknownEventTypeException.class);
  }

  @Test
  void toEnvelope_schemaAhead_isEventSchemaAheadExceptionWithTheMessage() {
    var row = new RawEventRow(1, "Account", "a-1", 1, "Opened", "{\"name\":\"n\"}", META, 3);

    var e = assertThrows(EventSchemaAheadException.class, () -> path().toEnvelope(row));

    assertThat(e.getMessage()).isEqualTo(EventReadPath.schemaAheadMessage("Opened", SHOWN, 3, 1));
    assertThat(e.getMessage())
        .startsWith(
            "Event type Opened for stream Account:a-1 is stored at schema_version 3 but this event"
                + " store's upcaster chain only reaches version 1. A newer writer stamped it:")
        .endsWith(" register the same upcasters on every event store that reads this database.");
  }

  @Test
  void toEnvelope_upcastsOlderRows() {
    EventReadPath path = path(chainOf(titleToName()), SnapshotMigrationChain.of(null));

    EventEnvelope envelope = path.toEnvelope(oldRow(3, 1));

    assertThat(envelope.event()).isEqualTo(new Opened("t1"));
  }

  @Test
  void toEnvelope_corruptPayload_isEventDeserializationException() {
    var row = new RawEventRow(1, "Account", "a-1", 1, "Opened", "{not json", META, 1);

    var e = assertThrows(EventDeserializationException.class, () -> path().toEnvelope(row));

    assertThat(e.getMessage()).isEqualTo("Failed to deserialize event data for stream: " + SHOWN);
    assertThat(e.getCause()).isNotNull();
  }

  @Test
  void toEnvelope_transientCryptoCause_isEventStoreException() {
    EventReadPath transientPath =
        path(chainOf(throwingUpcaster(new CryptoOperationException("kms down"))), noMigrations());
    EventReadPath permanentPath =
        path(chainOf(throwingUpcaster(new CryptoMappingException("bad blob"))), noMigrations());

    var transientFailure =
        assertThrows(EventStoreException.class, () -> transientPath.toEnvelope(oldRow(1, 1)));
    var permanentFailure =
        assertThrows(EventStoreException.class, () -> permanentPath.toEnvelope(oldRow(1, 1)));

    assertThat(transientFailure).isExactlyInstanceOf(EventStoreException.class);
    assertThat(transientFailure.getMessage())
        .isEqualTo("Failed to deserialize event data for stream: " + SHOWN);
    assertThat(transientFailure.getCause()).isInstanceOf(CryptoOperationException.class);
    assertThat(permanentFailure).isExactlyInstanceOf(EventDeserializationException.class);
  }

  @Test
  void toEnvelope_eventStoreExceptionPassesThrough() {
    var typed = new EventStoreException("already typed");
    EventReadPath path = path(chainOf(throwingUpcaster(typed)), noMigrations());

    var e = assertThrows(EventStoreException.class, () -> path.toEnvelope(oldRow(1, 1)));

    assertSame(typed, e);
  }

  @Test
  void toEnvelope_negativeOffsetInsideTheTry_isEventDeserializationException() {
    var row = new RawEventRow(-1, "Account", "a-1", 1, "Opened", "{\"name\":\"n\"}", META, 1);

    var e = assertThrows(EventDeserializationException.class, () -> path().toEnvelope(row));

    assertThat(e.getCause())
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("globalOffset must be >= 0");
  }

  @Test
  void toEnvelope_negativeOffsetParsedFirst_isRawIllegalArgumentBeforeTypeResolution() {
    Function<RawEventRow, GlobalOffset> parseFirst = r -> GlobalOffset.of(r.globalOffset());
    EventReadPath path =
        path(UpcasterChain.empty(), noMigrations(), metrics, null, false, parseFirst);
    var row = new RawEventRow(-1, "Account", "a-1", 1, "Nope", "{\"name\":\"n\"}", META, 1);

    var e = assertThrows(IllegalArgumentException.class, () -> path.toEnvelope(row));

    assertThat(e).hasMessage("globalOffset must be >= 0");
  }

  @Test
  void toEnvelope_offsetParsedFirst_isTheOffsetOfTheEnvelope() {
    Function<RawEventRow, GlobalOffset> parseFirst = r -> GlobalOffset.of(r.globalOffset());
    EventReadPath path =
        path(UpcasterChain.empty(), noMigrations(), metrics, null, false, parseFirst);

    assertThat(path.toEnvelope(row(8, 1)).globalOffset()).isEqualTo(GlobalOffset.of(8));
  }

  // ── Assembling a load's history ────────────────────────────────────────────────────────────────

  @Test
  void assembleHistory_noSnapshot_replaysTheRows() {
    AggregateHistory history =
        path()
            .assembleHistory(
                STREAM, StoredSnapshot.NONE, List.of(row(1, 1), row(2, 2)), 0, noReRead());

    assertThat(history.snapshotState()).isNull();
    assertThat(history.events()).extracting(EventEnvelope::version).containsExactly(v(1), v(2));
    assertThat(history.version()).isEqualTo(v(2));
    assertThat(history.lastSnapshotVersion()).isEqualTo(Version.initial());
    assertThat(history.snapshotPurgeGeneration()).isZero();
  }

  @Test
  void assembleHistory_compatibleSnapshot_startsFromIt() {
    var snapshot = new StoredSnapshot(true, "{\"count\":2}", CounterState.class, v(2), -1, 6);

    AggregateHistory history = path().assembleHistory(STREAM, snapshot, List.of(), 0, noReRead());

    assertThat(history.snapshotState()).isEqualTo(new CounterState(2));
    assertThat(history.events()).isEmpty();
    assertThat(history.version()).isEqualTo(v(2));
    assertThat(history.lastSnapshotVersion()).isEqualTo(v(2));
    assertThat(history.snapshotPurgeGeneration()).isEqualTo(6);
  }

  @Test
  void assembleHistory_pendingMigration_migratesTheState() {
    var snapshot = new StoredSnapshot(true, "{\"count\":2}", CounterState.class, v(2), 1, 0);
    EventReadPath path = path(UpcasterChain.empty(), chain(toV2()));

    AggregateHistory history =
        path.assembleHistory(STREAM, snapshot, List.of(row(3, 3)), 2, noReRead());

    assertThat(history.snapshotState()).isEqualTo(new CounterStateV2(2, "migrated"));
    assertThat(history.version()).isEqualTo(v(3));
    assertThat(history.lastSnapshotVersion()).isEqualTo(v(2));
  }

  @Test
  void assembleHistory_undeserializableSnapshot_discards_logsError_counts_reReads() {
    var snapshot = new StoredSnapshot(true, "{not json", CounterState.class, v(2), -1, 4);
    List<Version> reReads = new ArrayList<>();

    AggregateHistory history =
        path()
            .assembleHistory(
                STREAM,
                snapshot,
                List.of(row(3, 3)),
                0,
                after -> {
                  reReads.add(after);
                  return List.of(row(1, 1), row(2, 2), row(3, 3));
                });

    assertThat(reReads).containsExactly(Version.initial());
    assertThat(history.snapshotState()).isNull();
    assertThat(history.events()).hasSize(3);
    assertThat(history.version()).isEqualTo(v(3));
    assertThat(history.lastSnapshotVersion()).isEqualTo(Version.initial());
    assertThat(history.snapshotPurgeGeneration()).isEqualTo(4);
    List<ILoggingEvent> errors = store.events(Level.ERROR);
    assertThat(errors).hasSize(1);
    assertThat(errors.getFirst().getMessage())
        .isEqualTo(
            "Snapshot for stream {} is undeserializable ({}); discarding the derived snapshot cache"
                + " and replaying all events from the beginning. This usually means an"
                + " aggregate-state field was renamed or removed without bumping the snapshot"
                + " version — bump SnapshotPolicy's snapshotVersion so fresh snapshots are"
                + " written.");
    assertThat(errors.getFirst().getArgumentArray()[0]).isEqualTo(SHOWN);
    assertThat(errors.getFirst().getThrowableProxy()).isNotNull();
    assertThat(
            metrics.count(
                MetricNames.SNAPSHOTS_DISCARDED, MetricNames.TAG_REASON, "deserialize_failure"))
        .isEqualTo(1);
  }

  @Test
  void assembleHistory_emptyReRead_isAnEmptyHistory() {
    var snapshot = new StoredSnapshot(true, "{not json", CounterState.class, v(2), -1, 1);

    AggregateHistory history =
        path().assembleHistory(STREAM, snapshot, List.of(), 0, after -> List.of());

    assertThat(history.events()).isEmpty();
    assertThat(history.version()).isEqualTo(Version.initial());
    assertThat(history.snapshotPurgeGeneration()).isEqualTo(1);
  }

  @Test
  void assembleHistory_migrationFailure_discardsWithReasonMigrationFailure() {
    var snapshot = new StoredSnapshot(true, "{\"count\":2}", CounterState.class, v(2), 1, 0);
    EventReadPath path =
        path(UpcasterChain.empty(), chain(failingStep(new IllegalStateException("step bug"))));
    List<Version> reReads = new ArrayList<>();

    AggregateHistory history =
        path.assembleHistory(
            STREAM,
            snapshot,
            List.of(),
            2,
            after -> {
              reReads.add(after);
              return List.of(row(1, 1));
            });

    assertThat(reReads).containsExactly(Version.initial());
    assertThat(history.snapshotState()).isNull();
    assertThat(history.version()).isEqualTo(v(1));
    assertThat(store.events(Level.ERROR)).hasSize(1);
    assertThat(store.events(Level.ERROR).getFirst().getMessage())
        .startsWith(
            "Snapshot migration for stream {} failed ({}); discarding the derived snapshot");
    assertThat(
            metrics.count(
                MetricNames.SNAPSHOTS_DISCARDED, MetricNames.TAG_REASON, "migration_failure"))
        .isEqualTo(1);
  }

  @Test
  void
      assembleHistory_cryptoCause_transientIsEventStoreException_permanentIsEventDeserializationException() {
    var transientState =
        new StoredSnapshot(true, "{\"mode\":\"transient\"}", CryptoState.class, v(2), -1, 0);
    var permanentState =
        new StoredSnapshot(true, "{\"mode\":\"permanent\"}", CryptoState.class, v(2), -1, 0);
    String deserializeMessage = "Failed to deserialize snapshot state for stream: " + SHOWN;

    assertThat(failure(path(), transientState, 0))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(deserializeMessage);
    assertThat(failure(path(), permanentState, 0))
        .isExactlyInstanceOf(EventDeserializationException.class)
        .hasMessage(deserializeMessage);

    var pending = new StoredSnapshot(true, "{\"count\":2}", CounterState.class, v(2), 1, 0);
    EventReadPath transientStep =
        path(UpcasterChain.empty(), chain(failingStep(new CryptoOperationException("kms down"))));
    EventReadPath permanentStep =
        path(UpcasterChain.empty(), chain(failingStep(new CryptoMappingException("bad blob"))));
    String migrationMessage = "Snapshot migration failed for stream: " + SHOWN;

    assertThat(failure(transientStep, pending, 2))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(migrationMessage);
    assertThat(failure(permanentStep, pending, 2))
        .isExactlyInstanceOf(EventDeserializationException.class)
        .hasMessage(migrationMessage);
    assertThat(store.events(Level.ERROR)).isEmpty();
    assertThat(metrics.count(MetricNames.SNAPSHOTS_DISCARDED)).isZero();
  }

  @Test
  void assembleHistory_migrationReachableButEmpty_isEventStoreExceptionWithTheMessage() {
    var snapshot = new StoredSnapshot(true, "{\"count\":2}", CounterState.class, v(2), 1, 0);

    assertThat(failure(path(), snapshot, 3))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(
            "Snapshot migration chain reported reachable from version 1 to 3 but failed to apply"
                + " for stream: "
                + SHOWN);
  }

  @Test
  void metricFailureIsWarnedNotThrown() {
    var failingMetrics =
        new RecordingStreamRuneMetrics() {
          @Override
          public void recordSnapshotDiscarded(String reason) {
            throw new IllegalStateException("registry closed");
          }
        };
    EventReadPath path =
        path(UpcasterChain.empty(), noMigrations(), failingMetrics, null, false, null);
    var snapshot = new StoredSnapshot(true, "{not json", CounterState.class, v(2), -1, 0);

    AggregateHistory history =
        path.assembleHistory(STREAM, snapshot, List.of(), 0, after -> List.of(row(1, 1)));

    assertThat(history.version()).isEqualTo(v(1));
    List<ILoggingEvent> warnings = store.events(Level.WARN);
    assertThat(warnings).hasSize(1);
    assertThat(warnings.getFirst().getMessage())
        .isEqualTo("Metrics recordSnapshotDiscarded({}) failed for stream {}");
    assertThat(warnings.getFirst().getArgumentArray())
        .containsExactly("deserialize_failure", SHOWN);
    assertThat(warnings.getFirst().getThrowableProxy().getMessage()).isEqualTo("registry closed");
  }

  @Test
  void historyIgnoringSnapshot_replaysEveryRowWithoutASnapshot() {
    AggregateHistory history =
        path().historyIgnoringSnapshot(STREAM, List.of(row(1, 1), row(2, 2)));

    assertThat(history.snapshotState()).isNull();
    assertThat(history.version()).isEqualTo(v(2));
    assertThat(history.lastSnapshotVersion()).isEqualTo(Version.initial());
    assertThat(history.snapshotPurgeGeneration()).isZero();
    assertThat(path().historyIgnoringSnapshot(STREAM, List.of()).version())
        .isEqualTo(Version.initial());
  }

  @Test
  void gapWarning_oncePerStreamPerInstance() {
    List<RawEventRow> gapped = List.of(row(1, 1), row(2, 2), row(3, 4));
    EventReadPath first = path();
    EventReadPath second = path();

    first.historyIgnoringSnapshot(STREAM, gapped);
    first.historyIgnoringSnapshot(STREAM, gapped);
    second.historyIgnoringSnapshot(STREAM, gapped);

    List<ILoggingEvent> warnings = store.events(Level.WARN);
    assertThat(warnings).hasSize(2);
    assertThat(warnings.getFirst().getMessage())
        .isEqualTo(
            "Stream '{}' has non-contiguous versions (expected {} but found {}, right after"
                + " version {}) — rows were written, deleted or restored outside this store."
                + " This store never renumbers events automatically; reconstructing the intended"
                + " history is a data-recovery decision. Logged once per stream per process.");
    assertThat(warnings.getFirst().getArgumentArray()).containsExactly(SHOWN, 3L, 4L, 2L);
  }

  @Test
  void gapWarning_afterASnapshotCountsFromItsVersion() {
    var snapshot = new StoredSnapshot(true, "{\"count\":2}", CounterState.class, v(2), -1, 0);

    path().assembleHistory(STREAM, snapshot, List.of(row(3, 4)), 0, noReRead());

    assertThat(store.events(Level.WARN)).hasSize(1);
    assertThat(store.events(Level.WARN).getFirst().getArgumentArray())
        .containsExactly(SHOWN, 3L, 4L, 2L);
  }

  @Test
  void globalContiguity_stopsAtTheFirstGapAndWarnsOnce() {
    GlobalContiguity contiguity = path().globalContiguity(GlobalOffset.of(10));
    List<Long> admitted = new ArrayList<>();

    for (long offset : new long[] {12, 13, 15, 16}) {
      if (!contiguity.admits(offset)) {
        break;
      }
      admitted.add(offset);
    }

    // A leading hole after the checkpoint is paged over; the first hole inside the page stops it.
    assertThat(admitted).containsExactly(12L, 13L);
    List<ILoggingEvent> warnings = store.events(Level.WARN);
    assertThat(warnings).hasSize(1);
    assertThat(warnings.getFirst().getMessage())
        .isEqualTo("non-contiguous global_offset observed after {}: expected {}, got {}");
    assertThat(warnings.getFirst().getArgumentArray())
        .containsExactly(GlobalOffset.of(10), 14L, 15L);
    // Each read gets its own guard, seeded by its own first row.
    assertThat(path().globalContiguity(GlobalOffset.of(10)).admits(15)).isTrue();
  }

  // ── Spans ──────────────────────────────────────────────────────────────────────────────────────

  @Test
  void spans_namesAttributesAndChildTiming() {
    var tracing = new RecordingTracing();
    EventReadPath path = path(chainOf(titleToName()), noMigrations(), metrics, tracing, true, null);

    // A stored snapshot the load starts from: HIT.
    var hit = new StoredSnapshot(true, "{\"count\":2}", CounterState.class, v(2), -1, 0);
    path.load(
        STREAM, 0, () -> path.assembleHistory(STREAM, hit, List.of(oldRow(3, 3)), 0, noReRead()));

    assertThat(tracing.names())
        .containsExactly(
            TraceAttributes.EVENT_STORE_LOAD,
            TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD,
            TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    RecordingTracing.RecordedSpan load = tracing.only(TraceAttributes.EVENT_STORE_LOAD);
    assertThat(load.attributes())
        .containsEntry(TraceAttributes.STREAM_ID, SHOWN)
        .containsEntry(TraceAttributes.AGGREGATE_TYPE, "Account")
        .containsEntry(TraceAttributes.EVENT_COUNT, 1L)
        .containsEntry(TraceAttributes.SNAPSHOT_USED, true);
    assertThat(load.ok).isTrue();
    assertThat(load.closed).isTrue();
    RecordingTracing.RecordedSpan snapshotLoad =
        tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD);
    assertThat(snapshotLoad.parent).isSameAs(load);
    assertThat(snapshotLoad.attributes())
        .containsEntry(TraceAttributes.STREAM_ID, SHOWN)
        .containsEntry(TraceAttributes.SNAPSHOT_OUTCOME, "HIT")
        .containsEntry(TraceAttributes.STREAM_VERSION, 2L);
    assertThat(snapshotLoad.ok).isTrue();
    RecordingTracing.RecordedSpan deserialize =
        tracing.only(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    assertThat(deserialize.parent).isSameAs(load);
    assertThat(deserialize.attributes())
        .containsEntry(TraceAttributes.EVENT_COUNT, 1L)
        .containsEntry(TraceAttributes.UPCAST_COUNT, 1L);
    assertThat(deserialize.ok).isTrue();

    // A stored snapshot the load discards: DISCARDED, version 0, the replay deserialized.
    tracing.clear();
    var discarded = new StoredSnapshot(true, "{not json", CounterState.class, v(2), -1, 0);
    path.load(
        STREAM,
        0,
        () ->
            path.assembleHistory(
                STREAM, discarded, List.of(), 0, after -> List.of(oldRow(1, 1), oldRow(2, 2))));

    assertThat(tracing.names())
        .containsExactly(
            TraceAttributes.EVENT_STORE_LOAD,
            TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD,
            TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD).attributes())
        .containsEntry(TraceAttributes.SNAPSHOT_OUTCOME, "DISCARDED")
        .containsEntry(TraceAttributes.STREAM_VERSION, 0L);
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE).attributes())
        .containsEntry(TraceAttributes.EVENT_COUNT, 2L)
        .containsEntry(TraceAttributes.UPCAST_COUNT, 2L);
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_LOAD).attributes())
        .containsEntry(TraceAttributes.SNAPSHOT_USED, false);

    // No stored snapshot: no snapshot span.
    tracing.clear();
    path.load(
        STREAM,
        0,
        () -> path.assembleHistory(STREAM, StoredSnapshot.NONE, List.of(), 0, noReRead()));
    assertThat(tracing.names())
        .containsExactly(
            TraceAttributes.EVENT_STORE_LOAD, TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);

    // A read: the child of the current span, started at the given instant.
    tracing.clear();
    Instant started = Instant.parse("2026-11-27T10:00:00Z");
    RecordingTracing.RecordedSpan parent =
        tracing.inside("command", () -> path.readEvents(List.of(row(4, 4)), "GLOBAL", started));
    RecordingTracing.RecordedSpan read = tracing.only(TraceAttributes.EVENT_STORE_READ);
    assertThat(read.parent).isSameAs(parent);
    assertThat(read.startTime).isEqualTo(started);
    assertThat(read.attributes())
        .containsEntry(TraceAttributes.EVENT_COUNT, 1L)
        .containsEntry(TraceAttributes.READ_KIND, "GLOBAL");
    assertThat(read.ok).isTrue();

    // A read that returned no row starts none, and one without a current span neither.
    tracing.clear();
    tracing.inside("command", () -> path.readEvents(List.of(), "STREAM", started));
    assertThat(path.readEvents(List.of(row(4, 4)), "STREAM", started)).hasSize(1);
    assertThat(tracing.names()).containsExactly("command");

    // Not traced: nothing at all, though the tracing records every area.
    tracing.clear();
    EventReadPath untraced =
        path(chainOf(titleToName()), noMigrations(), metrics, tracing, false, null);
    untraced.load(
        STREAM,
        0,
        () -> untraced.assembleHistory(STREAM, hit, List.of(oldRow(3, 3)), 0, noReRead()));
    tracing.inside("command", () -> untraced.readEvents(List.of(row(4, 4)), "GLOBAL", started));
    assertThat(tracing.names()).containsExactly("command");
  }

  @Test
  void spans_recordTheErrorAndRethrow() {
    var tracing = new RecordingTracing();
    EventReadPath path = path(UpcasterChain.empty(), noMigrations(), metrics, tracing, true, null);
    var corrupt = new RawEventRow(1, "Account", "a-1", 1, "Opened", "{not json", META, 1);
    var transientState =
        new StoredSnapshot(true, "{\"mode\":\"transient\"}", CryptoState.class, v(2), -1, 0);

    assertThrows(
        EventDeserializationException.class,
        () ->
            path.load(
                STREAM,
                0,
                () ->
                    path.assembleHistory(
                        STREAM, StoredSnapshot.NONE, List.of(corrupt), 0, noReRead())));
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_LOAD).error)
        .isInstanceOf(EventDeserializationException.class);
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_LOAD).closed).isTrue();
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE).error)
        .isInstanceOf(EventDeserializationException.class);

    tracing.clear();
    assertThrows(
        EventStoreException.class,
        () -> path.assembleHistory(STREAM, transientState, List.of(), 0, noReRead()));
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD).error)
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD).closed).isTrue();

    tracing.clear();
    tracing.inside(
        "command",
        () ->
            assertThrows(
                EventDeserializationException.class,
                () -> path.readEvents(List.of(corrupt), "GLOBAL", Instant.now())));
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_READ).error)
        .isInstanceOf(EventDeserializationException.class);
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_READ).closed).isTrue();
  }

  // ── Arguments, logger, statics ─────────────────────────────────────────────────────────────────

  @Test
  void load_rejectsANullStreamAndAVersionBelowIgnoreSnapshot() {
    var nullStream =
        assertThrows(
            NullPointerException.class, () -> path().load(null, 0, () -> AggregateHistory.empty()));
    var belowIgnore =
        assertThrows(
            IllegalArgumentException.class,
            () -> path().load(STREAM, -2, () -> AggregateHistory.empty()));

    assertThat(nullStream).hasMessage("streamId is required");
    assertThat(belowIgnore)
        .hasMessage(
            "expectedSnapshotVersion must be >= -1 (IGNORE_SNAPSHOT skips reading any snapshot, 0"
                + " skips the version check), got -2");
    AggregateHistory given = AggregateHistory.empty();
    assertSame(given, path().load(STREAM, -1, () -> given));
  }

  @Test
  void logsOnlyThroughTheGivenLogger() {
    var failingMetrics =
        new RecordingStreamRuneMetrics() {
          @Override
          public void recordSnapshotDiscarded(String reason) {
            throw new IllegalStateException("registry closed");
          }
        };
    EventReadPath path =
        path(UpcasterChain.empty(), noMigrations(), failingMetrics, null, false, null);
    var discarded = new StoredSnapshot(true, "{not json", CounterState.class, v(2), -1, 0);

    path.assembleHistory(STREAM, discarded, List.of(), 0, after -> List.of(row(1, 1)));
    path.historyIgnoringSnapshot(STREAM, List.of(row(1, 1), row(2, 3)));
    GlobalContiguity contiguity = path.globalContiguity(GlobalOffset.of(0));
    contiguity.admits(1);
    contiguity.admits(3);

    assertThat(store.events(Level.ERROR)).hasSize(1);
    assertThat(store.events(Level.WARN)).hasSize(3);
    assertThat(own.all()).isEmpty();
  }

  @Test
  void requireMaxCount_refusesOnlyANegativeCount() {
    EventReadPath.requireMaxCount(0);

    var e = assertThrows(IllegalArgumentException.class, () -> EventReadPath.requireMaxCount(-1));

    assertThat(e).hasMessage("maxCount must not be negative, got -1");
  }

  @Test
  void cryptoCauses_areFoundInTheCauseChain() {
    var transientCause = new RuntimeException(new CryptoOperationException("kms down"));
    var permanentCause = new RuntimeException(new CryptoMappingException("bad blob"));

    assertThat(EventReadPath.hasCryptoCause(transientCause)).isTrue();
    assertThat(EventReadPath.hasCryptoCause(permanentCause)).isTrue();
    assertThat(EventReadPath.hasCryptoCause(new RuntimeException("plain"))).isFalse();
    assertThat(EventReadPath.hasTransientCryptoCause(transientCause)).isTrue();
    assertThat(EventReadPath.hasTransientCryptoCause(permanentCause)).isFalse();
  }

  // ── Fixtures ───────────────────────────────────────────────────────────────────────────────────

  private EventReadPath path() {
    return path(UpcasterChain.empty(), noMigrations());
  }

  private EventReadPath path(UpcasterChain upcasters, SnapshotMigrationChain migrations) {
    return path(upcasters, migrations, metrics, null, false, null);
  }

  private static EventReadPath path(
      UpcasterChain upcasters,
      SnapshotMigrationChain migrations,
      StreamRuneMetrics metrics,
      StreamRuneTracing tracing,
      boolean traced,
      Function<RawEventRow, GlobalOffset> offsetBeforeTypeResolution) {
    return new EventReadPath(
        MAPPER,
        REGISTRY,
        upcasters,
        migrations,
        metrics,
        tracing == null ? StreamRuneTracing.NOOP : tracing,
        traced,
        LoggerFactory.getLogger(CATEGORY),
        offsetBeforeTypeResolution);
  }

  private static Version v(long value) {
    return new Version(value);
  }

  /** A current row of {@code Opened}, schema version 1, named {@code n<version>}. */
  private static RawEventRow row(long offset, long version) {
    return new RawEventRow(
        offset, "Account", "a-1", version, "Opened", "{\"name\":\"n" + version + "\"}", META, 1);
  }

  /** A row stored at schema version 1 with a {@code title}, for a chain at version 2. */
  private static RawEventRow oldRow(long offset, long version) {
    return new RawEventRow(
        offset, "Account", "a-1", version, "Opened", "{\"title\":\"t" + version + "\"}", META, 1);
  }

  private static Function<Version, List<RawEventRow>> noReRead() {
    return after -> {
      throw new AssertionError("no re-read expected, got one after " + after);
    };
  }

  private static RuntimeException failure(
      EventReadPath path, StoredSnapshot snapshot, int expectedSnapshotVersion) {
    return assertThrows(
        RuntimeException.class,
        () ->
            path.assembleHistory(STREAM, snapshot, List.of(), expectedSnapshotVersion, noReRead()));
  }

  private static UpcasterChain chainOf(EventUpcaster upcaster) {
    return new UpcasterChain(List.of(upcaster));
  }

  private static SnapshotMigrationChain noMigrations() {
    return SnapshotMigrationChain.of(null);
  }

  private static SnapshotMigrationChain chain(SnapshotMigration step) {
    return SnapshotMigrationChain.of(List.of(step));
  }

  private static EventUpcaster titleToName() {
    return new EventUpcaster() {
      @Override
      public EventType eventType() {
        return new EventType("Opened");
      }

      @Override
      public int currentVersion() {
        return 2;
      }

      @Override
      public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
        eventData.put("name", eventData.remove("title"));
        return eventData;
      }
    };
  }

  private static EventUpcaster throwingUpcaster(RuntimeException failure) {
    return new EventUpcaster() {
      @Override
      public EventType eventType() {
        return new EventType("Opened");
      }

      @Override
      public int currentVersion() {
        return 2;
      }

      @Override
      public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
        throw failure;
      }
    };
  }

  private static SnapshotMigration toV2() {
    return new SnapshotMigration() {
      @Override
      public int fromVersion() {
        return 1;
      }

      @Override
      public int toVersion() {
        return 2;
      }

      @Override
      public AggregateState migrate(AggregateState state) {
        return new CounterStateV2(((CounterState) state).count(), "migrated");
      }
    };
  }

  private static SnapshotMigration failingStep(RuntimeException failure) {
    return new SnapshotMigration() {
      @Override
      public int fromVersion() {
        return 1;
      }

      @Override
      public int toVersion() {
        return 2;
      }

      @Override
      public AggregateState migrate(AggregateState state) {
        throw failure;
      }
    };
  }

  private static void insertSnapshot(
      JdbcTestDatabase db,
      String stateType,
      String payload,
      long version,
      int snapshotVersion,
      long purgeGeneration)
      throws SQLException {
    try (Connection c = db.connection();
        PreparedStatement ps =
            c.prepareStatement(
                "INSERT INTO snapshot_store (aggregate_type, aggregate_id, version, state_type,"
                    + " state_payload, snapshot_version, purge_generation)"
                    + " VALUES ('Account', 'a-1', ?, ?, ?, ?, ?)")) {
      ps.setLong(1, version);
      ps.setString(2, stateType);
      ps.setString(3, payload);
      ps.setInt(4, snapshotVersion);
      ps.setLong(5, purgeGeneration);
      ps.executeUpdate();
    }
  }

  private static StoredSnapshot decide(JdbcTestDatabase db, EventReadPath path, int expected)
      throws SQLException {
    try (Connection c = db.connection();
        PreparedStatement ps =
            c.prepareStatement(
                "SELECT version, state_type, state_payload, snapshot_version, purge_generation"
                    + " FROM snapshot_store WHERE aggregate_type = 'Account' AND aggregate_id ="
                    + " 'a-1'");
        ResultSet rs = ps.executeQuery()) {
      assertThat(rs.next()).isTrue();
      return path.decideSnapshot(rs, expected);
    }
  }

  /**
   * A snapshot row that answers only {@code purge_generation}, {@code state_type} and {@code
   * snapshot_version}; any other read, {@code state_payload} and {@code version} among them, fails
   * the test.
   */
  private static ResultSet rowFailingOnPayload(
      long purgeGeneration, String stateType, int snapshotVersion) {
    return (ResultSet)
        Proxy.newProxyInstance(
            ResultSet.class.getClassLoader(),
            new Class<?>[] {ResultSet.class},
            (proxy, method, args) -> {
              String read = method.getName() + "(" + (args == null ? "" : args[0]) + ")";
              return switch (read) {
                case "getLong(purge_generation)" -> purgeGeneration;
                case "getString(state_type)" -> stateType;
                case "getInt(snapshot_version)" -> snapshotVersion;
                default -> throw new AssertionError("unexpected read " + read);
              };
            });
  }

  private static String metadataJson() {
    try {
      return MAPPER.writeValueAsString(
          new EventMetadata(
              EventId.of("e-1"),
              CommandId.of("c-1"),
              null,
              CorrelationId.of("k-1"),
              null,
              null,
              Instant.parse("2026-11-27T09:00:00Z")));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * The events of one logger category: a logback {@link ListAppender} on the logger of that name,
   * at TRACE and not additive while attached.
   */
  private static final class Capture implements AutoCloseable {
    private final Logger logger;
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private final Level previousLevel;
    private final boolean previousAdditivity;

    private Capture(Logger logger) {
      this.logger = logger;
      this.previousLevel = logger.getLevel();
      this.previousAdditivity = logger.isAdditive();
      appender.start();
      logger.addAppender(appender);
      logger.setLevel(Level.TRACE);
      logger.setAdditive(false);
    }

    static Capture attach(String name) {
      return new Capture((Logger) LoggerFactory.getLogger(name));
    }

    List<ILoggingEvent> events(Level level) {
      return all().stream().filter(e -> e.getLevel() == level).toList();
    }

    List<ILoggingEvent> all() {
      synchronized (appender) {
        return List.copyOf(appender.list);
      }
    }

    @Override
    public void close() {
      logger.setLevel(previousLevel);
      logger.setAdditive(previousAdditivity);
      logger.detachAppender(appender);
      appender.stop();
    }
  }
}
