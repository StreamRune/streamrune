package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdConstraints;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresEventStoreTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  static PGSimpleDataSource dataSource;
  static ObjectMapper objectMapper;
  static EventTypeRegistry typeRegistry;
  PostgresEventStore store;

  record TestEvent(String value) implements DomainEvent {}

  /** V2 of TestEvent with an additional field. */
  record TestEventV2(String value, String extra) implements DomainEvent {}

  record TestState(String value) implements AggregateState {}

  /** Distinct state class for the snapshot simple-name collision test. */
  record OtherState(String value) implements AggregateState {}

  @BeforeAll
  static void initSchema() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());

    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return TestEvent.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return TestState.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return java.util.List.of(TestEvent.class, TestState.class);
          }
        };

    // Run Flyway migrations via factory
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void setUp() throws Exception {
    store = PostgresEventStore.builder().dataSource(dataSource).typeRegistry(typeRegistry).build();
    // Clean tables between tests
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("DELETE FROM snapshot_store");
      stmt.execute("DELETE FROM event_audit_log");
      // Deterministic 1-based offsets per test: a plain DELETE on event_stream does not reset
      // global_offset_sequence, and readGlobalStream's contiguous-prefix guard requires
      // callers to pass an accurate checkpoint — tests that read from GlobalOffset.initial() (0)
      // and expect freshly appended events starting at offset 1 need the counter reset too.
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
  }

  private EventMetadata testMetadata() {
    return new EventMetadata(
        IdGenerator.generateEventId(),
        IdGenerator.generateCommandId(),
        null,
        null,
        CorrelationId.of("corr-1"),
        null,
        null,
        Instant.now());
  }

  private EventEnvelope envelope(StreamId streamId, long version, TestEvent value) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        new Version(version),
        new EventType("TestEvent"),
        value,
        testMetadata());
  }

  @Test
  void appendAndLoadEvents() {
    StreamId streamId = TestStreams.stream("stream-1");
    store.append(
        streamId, List.of(envelope(streamId, 1, new TestEvent("hello"))), Version.initial());

    AggregateHistory history = store.load(streamId);

    assertEquals(1, history.version().value());
    assertEquals(1, history.events().size());
    assertNull(history.snapshotState());

    EventEnvelope loaded = history.events().getFirst();
    assertEquals(TestStreams.stream("stream-1"), loaded.streamId());
    assertEquals(1, loaded.version().value());
    assertEquals("TestEvent", loaded.eventType().name());
    assertInstanceOf(TestEvent.class, loaded.event());
    assertEquals("hello", ((TestEvent) loaded.event()).value());
  }

  /**
   * {@code AggregateId.of} bounds an id at {@link IdConstraints#MAX_LENGTH} UTF-16 code units
   * because {@code event_stream.aggregate_id} and {@code snapshot_store.aggregate_id} are {@code
   * VARCHAR(255)}, which PostgreSQL measures in characters (code points, in the UTF8 database
   * encoding). Every id the factory accepts must therefore be storable: the largest ASCII id, a
   * supplementary-plane id at the bound (128 code points in 255 code units) and a
   * two-byte-per-character id at the bound append, snapshot and load back verbatim. One character
   * more cannot even form a stream: the stream bounds its id by the column's rule, so the server's
   * SQLState 22001 is unreachable.
   */
  @Test
  void anAggregateIdAtTheIngressBoundIsStorableAndOneCharacterMoreIsNot() {
    String supplementary = "\uD83D\uDE00";
    for (String value :
        List.of(
            "a".repeat(IdConstraints.MAX_LENGTH),
            supplementary.repeat(127) + "a",
            "\u00E9".repeat(IdConstraints.MAX_LENGTH))) {
      StreamId streamId = StreamId.of(TestStreams.TYPE, AggregateId.of(value));
      store.append(
          streamId, List.of(envelope(streamId, 1, new TestEvent("at-bound"))), Version.initial());

      AggregateHistory history = store.load(streamId);
      assertEquals(1, history.events().size());
      assertEquals(streamId, history.events().getFirst().streamId());

      store.saveSnapshot(streamId, new Version(1), new TestState("at-bound"));
      AggregateHistory fromSnapshot = store.load(streamId);
      assertInstanceOf(TestState.class, fromSnapshot.snapshotState());
      assertEquals("at-bound", ((TestState) fromSnapshot.snapshotState()).value());
      assertEquals(1, fromSnapshot.version().value());
    }

    assertThrows(
        IllegalArgumentException.class,
        () -> TestStreams.stream("a".repeat(IdConstraints.MAX_LENGTH + 1)),
        "one character more cannot form a stream, so it never reaches the column");
  }

  @Test
  void loadReturnsLargeStreamsCompleteAndOrderedAcrossFetchBatches() {
    // 1100 events > 2x the driver fetch size (500): the result set spans multiple driver
    // fetch batches on the streaming load path. Everything must arrive, in version order.
    StreamId streamId = TestStreams.stream("large-stream");
    int count = 1100;
    var envelopes = new java.util.ArrayList<EventEnvelope>(count);
    for (int i = 1; i <= count; i++) {
      envelopes.add(envelope(streamId, i, new TestEvent("v" + i)));
    }
    store.append(streamId, envelopes, Version.initial());

    AggregateHistory history = store.load(streamId);

    assertEquals(count, history.events().size());
    assertEquals(count, history.version().value());
    assertEquals("v1", ((TestEvent) history.events().getFirst().event()).value());
    assertEquals("v" + count, ((TestEvent) history.events().getLast().event()).value());
    for (int i = 0; i < count; i++) {
      assertEquals(i + 1, history.events().get(i).version().value());
    }
  }

  @Test
  void optimisticLockFailsOnVersionConflict() {
    StreamId streamId = TestStreams.stream("stream-1");
    store.append(
        streamId, List.of(envelope(streamId, 1, new TestEvent("first"))), Version.initial());

    assertThrows(
        OptimisticLockException.class,
        () ->
            store.append(
                streamId,
                List.of(envelope(streamId, 1, new TestEvent("conflict"))),
                Version.initial()));
  }

  // CONTRACT CHANGE: before this fix, an expectedVersion beyond the
  // current head succeeded and left a permanent version gap (the store only detected a conflict
  // via the (aggregate_type, aggregate_id, version) unique constraint, which a future version never
  // collides with).
  // insertEvents now reads the actual head — under the SAME global_offset_sequence counter lock
  // that already serializes every append — and rejects any mismatch, naming both values.
  @Test
  void optimisticLockFailsOnFutureVersion() {
    StreamId streamId = TestStreams.stream("stream-future");
    store.append(
        streamId, List.of(envelope(streamId, 1, new TestEvent("first"))), Version.initial());

    OptimisticLockException ex =
        assertThrows(
            OptimisticLockException.class,
            () ->
                store.append(
                    streamId,
                    List.of(envelope(streamId, 6, new TestEvent("future"))),
                    new Version(5)));
    assertTrue(ex.getMessage().contains("expected version 5"), ex.getMessage());
    assertTrue(ex.getMessage().contains("actual head is 1"), ex.getMessage());

    // No gap was left: the stream is exactly as it was before the rejected attempt.
    AggregateHistory history = store.load(streamId);
    assertEquals(1, history.events().size());
    assertEquals(1, history.version().value());
  }

  @Test
  void staleVersionIsRejected_actualHeadNamedInMessage() {
    StreamId streamId = TestStreams.stream("stream-stale");
    store.append(
        streamId,
        List.of(
            envelope(streamId, 1, new TestEvent("e1")), envelope(streamId, 2, new TestEvent("e2"))),
        Version.initial());

    OptimisticLockException ex =
        assertThrows(
            OptimisticLockException.class,
            () ->
                store.append(
                    streamId,
                    List.of(envelope(streamId, 2, new TestEvent("stale"))),
                    Version.initial()));
    assertTrue(ex.getMessage().contains("expected version 0"), ex.getMessage());
    assertTrue(ex.getMessage().contains("actual head is 2"), ex.getMessage());
  }

  @Test
  @Timeout(30)
  void concurrentAppendersWithSameExpectedVersion_exactlyOneWins() throws Exception {
    StreamId streamId = TestStreams.stream("stream-concurrent-same-version");
    store.append(
        streamId, List.of(envelope(streamId, 1, new TestEvent("seed"))), Version.initial());

    int attempts = 8;
    var start = new CountDownLatch(1);
    var succeeded = new AtomicInteger();
    var conflicted = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(attempts);
    try {
      List<Future<?>> futures = new java.util.ArrayList<>();
      for (int i = 0; i < attempts; i++) {
        int idx = i;
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  try {
                    store.append(
                        streamId,
                        List.of(envelope(streamId, 2, new TestEvent("racer-" + idx))),
                        new Version(1));
                    succeeded.incrementAndGet();
                  } catch (OptimisticLockException _) {
                    conflicted.incrementAndGet();
                  }
                  return null;
                }));
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(20, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdown();
    }

    assertEquals(
        1, succeeded.get(), "exactly one concurrent appender at the same expectedVersion must win");
    assertEquals(attempts - 1, conflicted.get());

    // Per-stream and global order are consistent: exactly one event landed at version 2.
    AggregateHistory history = store.load(streamId);
    assertEquals(2, history.events().size());
    assertEquals(2, history.version().value());
  }

  @Test
  void eventStreamCreatedAtIsNotNull() throws Exception {
    // INSERT_EVENT leaves created_at to its DEFAULT and nothing updates it, so the baseline
    // declares it NOT NULL: a reader (the Spring health indicator's lastEventTimestamp) never
    // meets a NULL commit instant.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT is_nullable, column_default FROM information_schema.columns"
                    + " WHERE table_schema = current_schema() AND table_name = 'event_stream'"
                    + " AND column_name = 'created_at'")) {
      assertTrue(rs.next(), "event_stream.created_at must exist");
      assertEquals("NO", rs.getString("is_nullable"));
      assertEquals("now()", rs.getString("column_default"));
    }
  }

  @Test
  void nonContiguousStreamWarnsOnceOnLoad() throws Exception {
    // append/appendWithKey cannot create a gap, but this store must
    // still behave sanely against rows written outside it — seed one directly via SQL, bypassing
    // PostgresEventStore entirely.
    StreamId streamId = TestStreams.stream("stream-out-of-band-gap");
    store.append(streamId, List.of(envelope(streamId, 1, new TestEvent("e1"))), Version.initial());
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version,"
                    + " event_type, payload, metadata, schema_version) VALUES (999, ?, ?, 5,"
                    + " 'TestEvent', ?::jsonb, ?::jsonb, 1)")) {
      ps.setString(1, streamId.aggregateType().value());
      ps.setString(2, streamId.aggregateId().value());
      ps.setString(3, objectMapper.writeValueAsString(new TestEvent("out-of-band")));
      ps.setString(4, objectMapper.writeValueAsString(testMetadata()));
      ps.executeUpdate();
    }

    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(PostgresEventStore.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      store.load(streamId); // first load: must warn
      store.load(streamId); // second load, same stream: must NOT warn again
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }

    long warnings =
        appender.list.stream()
            .filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
            .filter(e -> e.getFormattedMessage().contains("non-contiguous"))
            .count();
    assertEquals(
        1,
        warnings,
        "the non-contiguous-stream diagnostic must fire exactly once per stream per process");

    // No automatic renumbering: both the original and the seeded row are still exactly as loaded.
    AggregateHistory history = store.load(streamId);
    assertEquals(2, history.events().size());
    assertEquals(1, history.events().getFirst().version().value());
    assertEquals(5, history.events().getLast().version().value());
  }

  @Test
  void snapshotReducesEventReplay() {
    // Append 3 events
    StreamId streamId = TestStreams.stream("stream-1");
    store.append(
        streamId,
        List.of(
            envelope(streamId, 1, new TestEvent("e1")),
            envelope(streamId, 2, new TestEvent("e2")),
            envelope(streamId, 3, new TestEvent("e3"))),
        Version.initial());

    // Save snapshot at version 2
    store.saveSnapshot(streamId, new Version(2), new TestState("snapshot-state"));

    // Load should return snapshot + only event at version 3
    AggregateHistory history = store.load(streamId);

    assertNotNull(history.snapshotState());
    assertInstanceOf(TestState.class, history.snapshotState());
    assertEquals("snapshot-state", ((TestState) history.snapshotState()).value());
    assertEquals(3, history.version().value());
    assertEquals(1, history.events().size());
    assertEquals(3, history.events().getFirst().version().value());
  }

  @Test
  void ignoreSnapshotNeverUsesTheSnapshot() {
    // EventStore.IGNORE_SNAPSHOT (the read-side counterpart of SnapshotPolicy.never())
    // must skip the snapshot entirely — unlike expectedSnapshotVersion == 0 ("skip the version
    // check, use anything"), which would still hand back this exact snapshot.
    StreamId streamId = TestStreams.stream("stream-1");
    store.append(
        streamId,
        List.of(
            envelope(streamId, 1, new TestEvent("e1")),
            envelope(streamId, 2, new TestEvent("e2")),
            envelope(streamId, 3, new TestEvent("e3"))),
        Version.initial());
    store.saveSnapshot(streamId, new Version(2), new TestState("snapshot-state"));

    AggregateHistory history = store.load(streamId, EventStore.IGNORE_SNAPSHOT);

    assertNull(history.snapshotState(), "IGNORE_SNAPSHOT must never hand back a snapshot state");
    assertEquals(
        3, history.events().size(), "every event must replay, not just the post-snapshot ones");
    assertEquals(3, history.version().value());
    assertEquals(0, history.lastSnapshotVersion().value());
  }

  @Test
  void ignoreSnapshotWarnsOnceWhenALegacySnapshotRowIsFound() {
    // A leftover snapshot_store row under a never() policy is harmless but permanent —
    // this is a one-time nudge for an operator to notice and, if they choose, purge it. Two
    // affected streams must produce exactly one WARN, not one per stream nor one per load.
    StreamId streamA = TestStreams.stream("legacy-a");
    StreamId streamB = TestStreams.stream("legacy-b");
    store.append(streamA, List.of(envelope(streamA, 1, new TestEvent("e1"))), Version.initial());
    store.append(streamB, List.of(envelope(streamB, 1, new TestEvent("e1"))), Version.initial());
    store.saveSnapshot(streamA, new Version(1), new TestState("stale-a"));
    store.saveSnapshot(streamB, new Version(1), new TestState("stale-b"));

    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(PostgresEventStore.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      store.load(streamA, EventStore.IGNORE_SNAPSHOT);
      store.load(streamA, EventStore.IGNORE_SNAPSHOT); // same stream again
      store.load(streamB, EventStore.IGNORE_SNAPSHOT); // a different affected stream
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }

    long warnings =
        appender.list.stream()
            .filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
            .filter(e -> e.getFormattedMessage().contains("snapshot_store"))
            .count();
    assertEquals(1, warnings, "the legacy-snapshot nudge must be logged exactly once per process");
  }

  @Test
  void ignoreSnapshotDoesNotWarnWhenNoSnapshotRowExists() {
    StreamId streamId = TestStreams.stream("no-snapshot-here");
    store.append(streamId, List.of(envelope(streamId, 1, new TestEvent("e1"))), Version.initial());

    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(PostgresEventStore.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      store.load(streamId, EventStore.IGNORE_SNAPSHOT);
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }

    assertTrue(
        appender.list.stream()
            .noneMatch(
                e ->
                    e.getLevel() == ch.qos.logback.classic.Level.WARN
                        && e.getFormattedMessage().contains("snapshot_store")),
        "no snapshot row exists for this stream — nothing to warn about");
  }

  @Test
  void ignoreSnapshotProbesSnapshotStoreAtMostOncePerInstance_evenAcrossManyLoadsWithNoRows() {
    // The legacy-snapshot probe used to re-run its DB round trip on
    // EVERY load under IGNORE_SNAPSHOT whenever snapshot_store has no rows at all — the only
    // early-return (legacySnapshotWarned) fired only once a row was FOUND, so the never()-since-
    // day-one deployment (no snapshot rows, EventStore.IGNORE_SNAPSHOT's default population) paid
    // an extra pool checkout + query on every single command forever. The regression is silent (no
    // WARN either way, unlike ignoreSnapshotDoesNotWarnWhenNoSnapshotRowExists above), so this pins
    // the actual CONNECTION COUNT via FaultInjectingDataSource's getConnection() counter, not a log
    // line.
    StreamId streamId = TestStreams.stream("no-snapshot-repeat-loads");
    store.append(streamId, List.of(envelope(streamId, 1, new TestEvent("e1"))), Version.initial());

    var countingDataSource = new FaultInjectingDataSource(dataSource);
    var probedStore =
        PostgresEventStore.builder()
            .dataSource(countingDataSource)
            .typeRegistry(typeRegistry)
            .build();
    int beforeAnyLoad = countingDataSource.connectionAttempts();

    probedStore.load(streamId, EventStore.IGNORE_SNAPSHOT);
    int firstLoadCost = countingDataSource.connectionAttempts() - beforeAnyLoad;

    probedStore.load(streamId, EventStore.IGNORE_SNAPSHOT);
    int secondLoadCost = countingDataSource.connectionAttempts() - beforeAnyLoad - firstLoadCost;

    assertEquals(
        2,
        firstLoadCost,
        "the FIRST load under IGNORE_SNAPSHOT pays for the one-shot legacy-snapshot probe (1"
            + " connection) plus readRawEvents' own connection (1)");
    assertEquals(
        1,
        secondLoadCost,
        "a SECOND (and every later) load under IGNORE_SNAPSHOT must cost only readRawEvents' own"
            + " connection — the legacy-snapshot probe must not run again for the life of this"
            + " store instance");
  }

  @Test
  void emptyStreamReturnsEmptyHistory() {
    AggregateHistory history = store.load(TestStreams.stream("nonexistent"));

    assertEquals(0, history.version().value());
    assertTrue(history.events().isEmpty());
    assertNull(history.snapshotState());
  }

  @Test
  void readGlobalStreamReturnsEventsAfterOffset() {
    StreamId streamId1 = TestStreams.stream("stream-1");
    StreamId streamId2 = TestStreams.stream("stream-2");
    store.append(
        streamId1,
        List.of(
            envelope(streamId1, 1, new TestEvent("a")), envelope(streamId1, 2, new TestEvent("b"))),
        Version.initial());
    store.append(streamId2, List.of(envelope(streamId2, 1, new TestEvent("c"))), Version.initial());

    // Read all from offset 0
    List<EventEnvelope> all = store.readGlobalStream(GlobalOffset.initial(), 100);
    assertEquals(3, all.size());

    // Read after first event
    GlobalOffset firstOffset = all.getFirst().globalOffset();
    List<EventEnvelope> afterFirst = store.readGlobalStream(firstOffset, 100);
    assertEquals(2, afterFirst.size());

    // Read after last event
    GlobalOffset lastOffset = all.getLast().globalOffset();
    List<EventEnvelope> afterLast = store.readGlobalStream(lastOffset, 100);
    assertTrue(afterLast.isEmpty());
  }

  @Test
  void readGlobalStreamRespectsMaxCount() {
    StreamId streamId = TestStreams.stream("stream-1");
    store.append(
        streamId,
        List.of(
            envelope(streamId, 1, new TestEvent("a")),
            envelope(streamId, 2, new TestEvent("b")),
            envelope(streamId, 3, new TestEvent("c"))),
        Version.initial());

    List<EventEnvelope> limited = store.readGlobalStream(GlobalOffset.initial(), 2);
    assertEquals(2, limited.size());
  }

  @Test
  void readGlobalStreamReturnsEmptyWhenNoEvents() {
    List<EventEnvelope> result = store.readGlobalStream(GlobalOffset.initial(), 100);
    assertTrue(result.isEmpty());
  }

  @Test
  void lastGlobalOffsetReturnsZeroWhenEmpty() {
    assertEquals(GlobalOffset.of(0), store.lastGlobalOffset());
  }

  @Test
  void lastGlobalOffsetReturnsHighestAssignedOffset() {
    StreamId streamId = TestStreams.stream("stream-head");
    var result =
        store.append(
            streamId,
            List.of(
                envelope(streamId, 1, new TestEvent("a")),
                envelope(streamId, 2, new TestEvent("b"))),
            Version.initial());

    GlobalOffset expectedHead = result.globalOffsets().getLast();
    assertEquals(expectedHead, store.lastGlobalOffset());
  }

  @Test
  void appendMultipleEventsInBatch() {
    StreamId streamId = TestStreams.stream("stream-1");
    store.append(
        streamId,
        List.of(
            envelope(streamId, 1, new TestEvent("a")),
            envelope(streamId, 2, new TestEvent("b")),
            envelope(streamId, 3, new TestEvent("c"))),
        Version.initial());

    AggregateHistory history = store.load(streamId);

    assertEquals(3, history.version().value());
    assertEquals(3, history.events().size());
    assertEquals("a", ((TestEvent) history.events().get(0).event()).value());
    assertEquals("b", ((TestEvent) history.events().get(1).event()).value());
    assertEquals("c", ((TestEvent) history.events().get(2).event()).value());
  }

  @Test
  void upcastsV1EventToV2OnLoad() throws SQLException {
    // Insert a V1 event directly into the DB (schema_version=1, no "extra" field).
    // global_offset has no default — reserve one from the same counter the production append path
    // uses.
    try (var conn = dataSource.getConnection()) {
      long offset;
      try (var res =
              conn.prepareStatement(
                  "UPDATE global_offset_sequence SET next_value = next_value + 1 WHERE id = 1"
                      + " RETURNING next_value");
          var rs = res.executeQuery()) {
        rs.next();
        offset = rs.getLong(1);
      }
      try (var ps =
          conn.prepareStatement(
              "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version,"
                  + " event_type, payload, metadata, schema_version)"
                  + " VALUES (?, 'test', ?, ?, ?, ?::jsonb, ?::jsonb, ?)")) {
        ps.setLong(1, offset);
        ps.setString(2, "stream-upcast");
        ps.setLong(3, 1);
        ps.setString(4, "TestEvent");
        ps.setString(5, "{\"value\": \"original\"}");
        ps.setString(
            6,
            "{\"eventId\":\"evt_test123\",\"commandId\":\"cmd_test123\",\"traceId\":null,\"spanId\":null,"
                + "\"correlationId\":\"corr-1\",\"causationId\":null,\"userId\":null,\"timestamp\":\"2025-01-01T00:00:00Z\"}");
        ps.setInt(7, 1);
        ps.executeUpdate();
      }
    }

    // Create an upcaster that adds "extra" field when upgrading V1 -> V2
    var upcaster =
        new EventUpcaster() {
          @Override
          public EventType eventType() {
            return new EventType("TestEvent");
          }

          @Override
          public int currentVersion() {
            return 2;
          }

          @Override
          public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
            if (fromVersion == 1) {
              var result = new HashMap<>(eventData);
              result.put("extra", "added-by-upcaster");
              return result;
            }
            return eventData;
          }
        };

    // Create a type registry that resolves TestEvent to TestEventV2
    var v2TypeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return TestEventV2.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return TestState.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return List.of(TestEventV2.class, TestState.class);
          }
        };

    var storeWithUpcasting =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(v2TypeRegistry)
            .upcasters(List.of(upcaster))
            .build();

    AggregateHistory history = storeWithUpcasting.load(TestStreams.stream("stream-upcast"));

    assertEquals(1, history.events().size());
    var loaded = history.events().getFirst().event();
    assertInstanceOf(TestEventV2.class, loaded);

    var v2Event = (TestEventV2) loaded;
    assertEquals("original", v2Event.value());
    assertEquals("added-by-upcaster", v2Event.extra());
  }

  @Test
  void schemaVersionRecordsTheWritersChainNotThePayloadShape() throws SQLException {
    // A store with no upcaster for a type stamps schema_version 1 even when the event already has
    // the shape a later upcaster produces: one release adds the field, a later one registers the
    // upcaster. A reader with that upcaster then runs its 1 -> 2 step on a payload that already
    // carries the field, so the step has to keep the value it finds.
    var v2TypeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return TestEventV2.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return TestState.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return List.of(TestEventV2.class, TestState.class);
          }
        };
    StreamId streamId = TestStreams.stream("stream-stamped-before-chain");
    var writerWithoutUpcaster =
        PostgresEventStore.builder().dataSource(dataSource).typeRegistry(v2TypeRegistry).build();
    writerWithoutUpcaster.append(
        streamId,
        List.of(
            new EventEnvelope(
                GlobalOffset.initial(),
                streamId,
                new Version(1),
                new EventType("TestEvent"),
                new TestEventV2("value", "real"),
                testMetadata())),
        Version.initial());
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT schema_version FROM event_stream WHERE aggregate_type = ? AND aggregate_id = ?")) {
      ps.setString(1, streamId.aggregateType().value());
      ps.setString(2, streamId.aggregateId().value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    }

    var stepInputs = new ArrayList<Map<String, Object>>();
    var defaultingUpcaster =
        new EventUpcaster() {
          @Override
          public EventType eventType() {
            return new EventType("TestEvent");
          }

          @Override
          public int currentVersion() {
            return 2;
          }

          @Override
          public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
            stepInputs.add(Map.copyOf(eventData));
            if (fromVersion == 1) {
              eventData.putIfAbsent("extra", "default");
            }
            return eventData;
          }
        };
    var readerWithUpcaster =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(v2TypeRegistry)
            .upcasters(List.of(defaultingUpcaster))
            .build();

    var loaded = (TestEventV2) readerWithUpcaster.load(streamId).events().getFirst().event();

    assertEquals(List.of(Map.of("value", "value", "extra", "real")), stepInputs);
    assertEquals("real", loaded.extra());
  }

  @Test
  void readFailsClosedWhenStoredSchemaVersionExceedsReaderChain() throws SQLException {
    // A stored schema_version ABOVE the reader's current upcaster version is never
    // legitimate — it proves this store is mis-wired (missing the upcasters the type was written
    // with, so currentVersion reads 2 for a row persisted at 3) or has been downgraded. Reading on
    // would silently mis-bind the payload. Insert a row stamped v3 and read it with a chain that
    // only reaches v2: the read must FAIL CLOSED with an EventStoreException naming the type.
    try (var conn = dataSource.getConnection()) {
      long offset;
      try (var res =
              conn.prepareStatement(
                  "UPDATE global_offset_sequence SET next_value = next_value + 1 WHERE id = 1"
                      + " RETURNING next_value");
          var rs = res.executeQuery()) {
        rs.next();
        offset = rs.getLong(1);
      }
      try (var ps =
          conn.prepareStatement(
              "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version,"
                  + " event_type, payload, metadata, schema_version)"
                  + " VALUES (?, 'test', ?, ?, ?, ?::jsonb, ?::jsonb, ?)")) {
        ps.setLong(1, offset);
        ps.setString(2, "stream-ahead");
        ps.setLong(3, 1);
        ps.setString(4, "TestEvent");
        ps.setString(5, "{\"value\": \"ahead\"}");
        ps.setString(
            6,
            "{\"eventId\":\"evt_ahead\",\"commandId\":\"cmd_ahead\",\"traceId\":null,\"spanId\":null,"
                + "\"correlationId\":\"corr-1\",\"causationId\":null,\"userId\":null,\"timestamp\":\"2025-01-01T00:00:00Z\"}");
        ps.setInt(7, 3); // stamped ABOVE the reader's chain currentVersion (2)
        ps.executeUpdate();
      }
    }

    var v2Upcaster =
        new EventUpcaster() {
          @Override
          public EventType eventType() {
            return new EventType("TestEvent");
          }

          @Override
          public int currentVersion() {
            return 2;
          }

          @Override
          public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
            return eventData; // identity — the guard fires before any upcast would run
          }
        };

    var storeWithChain =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .upcasters(List.of(v2Upcaster))
            .build();

    var ex =
        assertThrows(
            EventStoreException.class,
            () -> storeWithChain.load(TestStreams.stream("stream-ahead")));
    assertTrue(ex.getMessage().contains("upcaster chain only reaches"), ex.getMessage());
    assertTrue(ex.getMessage().contains("TestEvent"), ex.getMessage());
  }

  @Test
  void readFailsClosedWhenStoredSchemaVersionExceedsChainlessReader() throws SQLException {
    // The original guard only branched when upcasterChain was
    // non-null. A store built WITHOUT .upcasters(...) — a supported construction — skipped it, so a
    // stored schema_version >= 2 fell through to a plain readValue and silently mis-bound the
    // payload. A chainless reader understands only v1 (the write path stamps schema_version = 1 for
    // a chainless store); a stored v2 must fail closed, naming the type, as the chain-present path.
    try (var conn = dataSource.getConnection()) {
      long offset;
      try (var res =
              conn.prepareStatement(
                  "UPDATE global_offset_sequence SET next_value = next_value + 1 WHERE id = 1"
                      + " RETURNING next_value");
          var rs = res.executeQuery()) {
        rs.next();
        offset = rs.getLong(1);
      }
      try (var ps =
          conn.prepareStatement(
              "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version,"
                  + " event_type, payload, metadata, schema_version)"
                  + " VALUES (?, 'test', ?, ?, ?, ?::jsonb, ?::jsonb, ?)")) {
        ps.setLong(1, offset);
        ps.setString(2, "stream-ahead-chainless");
        ps.setLong(3, 1);
        ps.setString(4, "TestEvent");
        ps.setString(5, "{\"value\": \"ahead\"}");
        ps.setString(
            6,
            "{\"eventId\":\"evt_ahead2\",\"commandId\":\"cmd_ahead2\",\"traceId\":null,\"spanId\":null,"
                + "\"correlationId\":\"corr-1\",\"causationId\":null,\"userId\":null,\"timestamp\":\"2025-01-01T00:00:00Z\"}");
        ps.setInt(7, 2); // stamped ABOVE a chainless reader's understood version (1)
        ps.executeUpdate();
      }
    }

    // A store built WITHOUT any upcaster chain — the null-chain sibling path.
    var chainlessStore =
        PostgresEventStore.builder().dataSource(dataSource).typeRegistry(typeRegistry).build();

    var ex =
        assertThrows(
            EventStoreException.class,
            () -> chainlessStore.load(TestStreams.stream("stream-ahead-chainless")));
    assertTrue(ex.getMessage().contains("upcaster chain only reaches"), ex.getMessage());
    assertTrue(ex.getMessage().contains("TestEvent"), ex.getMessage());
  }

  @Test
  void appendWritesEventAuditEntriesInSameTransaction() throws Exception {
    var auditStore = new PostgresEventAuditStore(dataSource);
    var eventStore =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .eventAuditStore(auditStore)
            .build();

    StreamId streamId = TestStreams.stream("audit-test-1");
    var metadata =
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("corr-audit-1"),
            org.streamrune.core.types.CausationId.of("cmd-audit-1"),
            null,
            Instant.now());
    var envelope =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId,
            new Version(1),
            new EventType("TestEvent"),
            new TestEvent("data"),
            metadata);

    eventStore.append(streamId, List.of(envelope), Version.initial());

    // Verify event_audit_log has the entry
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT event_id FROM event_audit_log WHERE event_id = ?")) {
      ps.setString(1, metadata.eventId().value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next(), "Event audit entry should be written in same transaction");
      }
    }
  }

  @Test
  void auditRowsMirrorEventStreamWhenEnvelopeFieldsDiverge() throws Exception {
    var auditStore = new PostgresEventAuditStore(dataSource);
    var eventStore =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .eventAuditStore(auditStore)
            .build();

    // A direct append caller may pass envelopes whose streamId/version fields disagree with the
    // append parameters. The inserted events derive both from the parameters — audit rows must
    // mirror the inserted events, not the envelope fields.
    StreamId streamId = TestStreams.stream("audit-divergence-test");
    var metadata = testMetadata();
    var divergent =
        new EventEnvelope(
            GlobalOffset.initial(),
            TestStreams.stream("some-other-stream"),
            new Version(99),
            new EventType("TestEvent"),
            new TestEvent("data"),
            metadata);

    eventStore.append(streamId, List.of(divergent), Version.initial());

    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT aggregate_type, aggregate_id, version FROM event_audit_log"
                    + " WHERE event_id = ?")) {
      ps.setString(1, metadata.eventId().value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next(), "audit entry must exist");
        assertEquals("test", rs.getString("aggregate_type"));
        assertEquals("audit-divergence-test", rs.getString("aggregate_id"));
        assertEquals(1, rs.getLong("version"));
      }
    }
  }

  @Test
  void appendRejectsEventPayloadContainingNulCharacter() {
    StreamId streamId = TestStreams.stream("nul-payload-test");
    var ex =
        assertThrows(
            EventStoreException.class,
            () ->
                store.append(
                    streamId,
                    List.of(envelope(streamId, 1, new TestEvent("bad\0value"))),
                    Version.initial()));
    assertTrue(ex.getMessage().contains("NUL"), ex.getMessage());
    assertTrue(store.load(streamId).events().isEmpty(), "nothing may be committed");
  }

  @Test
  void appendAcceptsLiteralBackslashU0000Text() {
    // The six characters backslash-u-0-0-0-0 are ordinary data (Jackson serializes the backslash
    // escaped), not a NUL character — they must not trip the fail-fast check.
    StreamId streamId = TestStreams.stream("nul-literal-test");
    store.append(
        streamId,
        List.of(envelope(streamId, 1, new TestEvent("literal \\u0000 text"))),
        Version.initial());

    var loaded = store.load(streamId);
    assertEquals("literal \\u0000 text", ((TestEvent) loaded.events().getFirst().event()).value());
  }

  @Test
  void saveSnapshotRejectsStateContainingNulCharacter() {
    StreamId streamId = TestStreams.stream("nul-snapshot-test");
    var ex =
        assertThrows(
            EventStoreException.class,
            () -> store.saveSnapshot(streamId, new Version(1), new TestState("bad\0state")));
    assertTrue(ex.getMessage().contains("NUL"), ex.getMessage());
  }

  @Test
  void saveSnapshotRejectsStateTypeThatDoesNotResolveInRegistry() {
    var strictRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return TestEvent.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            throw new IllegalArgumentException("Unknown state type: " + stateType);
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return List.of(TestEvent.class);
          }
        };
    var strictStore =
        PostgresEventStore.builder().dataSource(dataSource).typeRegistry(strictRegistry).build();

    var ex =
        assertThrows(
            EventStoreException.class,
            () ->
                strictStore.saveSnapshot(
                    TestStreams.stream("s1"), new Version(1), new TestState("x")));
    assertTrue(ex.getMessage().contains("does not resolve"), ex.getMessage());
  }

  @Test
  void saveSnapshotRejectsSimpleNameResolvingToDifferentClass() {
    // Simulates two state classes sharing a simple name: the registry resolves "TestState" to a
    // different class than the one being saved — rehydration would produce the wrong type.
    var collidingRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return TestEvent.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return OtherState.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return List.of(TestEvent.class, OtherState.class);
          }
        };
    var collidingStore =
        PostgresEventStore.builder().dataSource(dataSource).typeRegistry(collidingRegistry).build();

    var ex =
        assertThrows(
            EventStoreException.class,
            () ->
                collidingStore.saveSnapshot(
                    TestStreams.stream("s1"), new Version(1), new TestState("x")));
    assertTrue(ex.getMessage().contains("simple"), ex.getMessage());
  }

  @Test
  void appendWithOutboxEntriesCommitsBothAtomically() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM outbox_events");
    }
    var outboxStore = new PostgresOutboxStore(dataSource);
    // builder requires outboxStore + outboxEventMapper together; for the manual-append test we wire
    // them directly via the canonical 7-arg ctor (package-accessible within the module)
    var outboxEnabled =
        new PostgresEventStore(
            dataSource, objectMapper, typeRegistry, null, null, outboxStore, null);

    StreamId streamId = TestStreams.stream("outbox-test");
    var entry =
        org.streamrune.core.outbox.OutboxEntry.pending(
            org.streamrune.core.outbox.OutboxEntryId.of("outbox-entry-1"),
            "{\"orderId\":\"o-1\"}",
            "OrderPlaced",
            streamId);

    outboxEnabled.append(
        streamId,
        List.of(envelope(streamId, 1, new TestEvent("with-outbox"))),
        Version.initial(),
        List.of(entry));

    assertEquals(1, outboxEnabled.readGlobalStream(GlobalOffset.initial(), 10).size());
    assertEquals(1, countOutboxRows());

    // Atomicity: a failing append (version conflict) must roll back its outbox entries too
    var conflictEntry =
        org.streamrune.core.outbox.OutboxEntry.pending(
            org.streamrune.core.outbox.OutboxEntryId.of("outbox-entry-2"),
            "{\"orderId\":\"o-2\"}",
            "OrderPlaced",
            streamId);
    assertThrows(
        OptimisticLockException.class,
        () ->
            outboxEnabled.append(
                streamId,
                List.of(envelope(streamId, 1, new TestEvent("conflict"))),
                Version.initial(),
                List.of(conflictEntry)));

    assertEquals(1, countOutboxRows(), "rolled-back outbox entry must not exist");
  }

  /**
   * The {@code events.isEmpty()} early return fired BEFORE any transaction was opened, so {@code
   * outboxStore.saveAll} never ran: caller-supplied outbox entries were silently discarded while
   * the call returned a normal success {@code AppendResult} — no exception, no log, no DLQ, no
   * metric, and nothing converges later because no row exists to retry. The store-presence guard
   * two lines above already throws loudly for the OTHER misuse of the same parameter, proving
   * {@code outboxEntries} is first-class input rather than a hint that may be dropped.
   *
   * <p>Entries are now refused instead. {@code outbox_events} rows are self-contained (no event or
   * offset reference), so they CAN stand alone — but not through this method, whose whole contract
   * is atomic co-commit with the events. Persisting them here in a private transaction would
   * silently hand the caller a weaker guarantee than the one the method promises; the supported
   * standalone path is {@code OutboxStore.save}/{@code saveAll} in the caller's own transaction.
   */
  @Test
  void appendRefusesOutboxEntriesWithoutEvents() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM outbox_events");
    }
    var outboxEnabled =
        new PostgresEventStore(
            dataSource,
            objectMapper,
            typeRegistry,
            null,
            null,
            new PostgresOutboxStore(dataSource),
            null);
    StreamId streamId = TestStreams.stream("outbox-empty-append");
    var entry =
        org.streamrune.core.outbox.OutboxEntry.pending(
            org.streamrune.core.outbox.OutboxEntryId.of("outbox-orphan-1"),
            "{\"orderId\":\"o-9\"}",
            "OrderPlaced",
            streamId);

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> outboxEnabled.append(streamId, List.of(), Version.initial(), List.of(entry)),
            "entries passed with an empty events list must be refused, never silently dropped");
    assertTrue(
        ex.getMessage().contains("outboxEntries"),
        "the failure must name the offending parameter, got: " + ex.getMessage());
    assertEquals(0, countOutboxRows(), "nothing may be persisted by the refused call");

    // Unchanged: an entirely empty append is still the documented no-op.
    var noop = outboxEnabled.append(streamId, List.of(), Version.initial(), List.of());
    assertTrue(noop.globalOffsets().isEmpty());
    assertEquals(Version.initial(), noop.finalVersion());
    assertEquals(0, countOutboxRows());

    // Unchanged: the normal path still commits events and entries together.
    outboxEnabled.append(
        streamId,
        List.of(envelope(streamId, 1, new TestEvent("with-outbox"))),
        Version.initial(),
        List.of(entry));
    assertEquals(1, countOutboxRows(), "the same entry commits once it accompanies an event");
  }

  private int countOutboxRows() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT COUNT(*) FROM outbox_events")) {
      rs.next();
      return rs.getInt(1);
    }
  }

  @Test
  void readStream_aNullStream_isRefusedRatherThanReadAsAnEmptyStream() {
    var e =
        assertThrows(
            NullPointerException.class, () -> store.readStream(null, Version.initial(), 10));
    assertEquals("streamId is required", e.getMessage());
  }

  @Test
  void appendWithOutboxEntriesRequiresConfiguredOutboxStore() {
    StreamId streamId = TestStreams.stream("outbox-unconfigured");
    var entry =
        org.streamrune.core.outbox.OutboxEntry.pending(
            org.streamrune.core.outbox.OutboxEntryId.of("outbox-entry-3"),
            "{}",
            "OrderPlaced",
            streamId);

    assertThrows(
        IllegalStateException.class,
        () ->
            store.append(
                streamId,
                List.of(envelope(streamId, 1, new TestEvent("x"))),
                Version.initial(),
                List.of(entry)));
  }

  private static final AggregateType PRODUCT = AggregateType.of("product");
  private static final AggregateType INVENTORY = AggregateType.of("inventory");

  @Test
  void eventStreamIsKeyedByTypeAndId_withNoComposedColumn() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      var columns = new java.util.HashSet<String>();
      try (var rs =
          stmt.executeQuery(
              "SELECT column_name FROM information_schema.columns WHERE table_name = 'event_stream'"
                  + " AND table_schema = current_schema()")) {
        while (rs.next()) {
          columns.add(rs.getString(1));
        }
      }
      assertTrue(
          columns.containsAll(java.util.Set.of("aggregate_type", "aggregate_id")),
          columns.toString());
      assertFalse(columns.contains("stream_id"), "no table stores a composed stream id");
      try (var rs =
          stmt.executeQuery(
              "SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                  + " WHERE conname = 'event_stream_stream_version_key'")) {
        assertTrue(rs.next(), "the unique constraint is named");
        assertEquals("UNIQUE (aggregate_type, aggregate_id, version)", rs.getString(1));
      }
      try (var rs =
          stmt.executeQuery(
              "SELECT 1 FROM pg_constraint WHERE conname = 'event_stream_aggregate_type_syntax'")) {
        assertTrue(rs.next(), "the syntax CHECK exists");
      }
    }
  }

  @Test
  void twoTypesSharingAnIdValue_haveIndependentStreams() {
    var product = StreamId.of(PRODUCT, AggregateId.of("p-1"));
    var inventory = StreamId.of(INVENTORY, AggregateId.of("p-1"));
    store.append(
        product, List.of(envelope(product, 1, new TestEvent("product"))), Version.initial());
    store.append(
        inventory, List.of(envelope(inventory, 1, new TestEvent("stock-1"))), Version.initial());
    store.append(
        inventory, List.of(envelope(inventory, 2, new TestEvent("stock-2"))), new Version(1));

    var productHistory = store.load(product);
    var inventoryHistory = store.load(inventory);
    assertEquals(1, productHistory.events().size());
    assertEquals(2, inventoryHistory.events().size());
    assertEquals(PRODUCT, productHistory.events().getFirst().aggregateType());
    assertEquals(AggregateId.of("p-1"), productHistory.events().getFirst().aggregateId());
    assertEquals(inventory, inventoryHistory.events().getLast().streamId());
    // The same version on the same pair still conflicts.
    assertThrows(
        OptimisticLockException.class,
        () ->
            store.append(
                product, List.of(envelope(product, 1, new TestEvent("again"))), Version.initial()));
    // The global read rebuilds the typed pair from the two columns.
    var global = store.readGlobalStream(GlobalOffset.initial(), 10);
    assertEquals(
        List.of(product, inventory, inventory),
        global.stream().map(EventEnvelope::streamId).toList());
  }

  @Test
  void twoTypesSharingAnIdValue_haveIndependentSnapshots() throws Exception {
    var product = StreamId.of(PRODUCT, AggregateId.of("p-1"));
    var inventory = StreamId.of(INVENTORY, AggregateId.of("p-1"));
    store.append(
        product, List.of(envelope(product, 1, new TestEvent("product"))), Version.initial());
    store.append(
        inventory, List.of(envelope(inventory, 1, new TestEvent("stock"))), Version.initial());
    store.saveSnapshot(product, new Version(1), new TestState("product-state"));
    store.saveSnapshot(inventory, new Version(1), new TestState("inventory-state"));

    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery("SELECT count(*) FROM snapshot_store WHERE aggregate_id = 'p-1'")) {
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1), "one snapshot row per stream, not per id value");
    }
    assertEquals(
        new TestState("product-state"), store.load(product).snapshotState(), "product snapshot");
    assertEquals(
        new TestState("inventory-state"),
        store.load(inventory).snapshotState(),
        "inventory snapshot");
  }

  @Test
  void aHandWrittenRowWithATypeOutsideTheSyntax_isRefusedByTheCheck() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      var ex =
          assertThrows(
              java.sql.SQLException.class,
              () ->
                  stmt.execute(
                      "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id,"
                          + " version, event_type, payload, metadata)"
                          + " VALUES (999, 'Order', 'o-1', 1, 'X', '{}', '{}')"));
      assertEquals("23514", ex.getSQLState());
    }
  }

  @Test
  void loadingOneStream_readsTheUniqueIndexInOrder_withNoSort() throws Exception {
    // Enough rows, over two types sharing id values, and fresh statistics: the planner's choice
    // then reflects the index, not a tiny never-analysed table.
    for (int i = 0; i < 200; i++) {
      for (var type : List.of(PRODUCT, INVENTORY)) {
        var stream = StreamId.of(type, AggregateId.of("p-" + i));
        store.append(
            stream,
            List.of(
                envelope(stream, 1, new TestEvent("a")),
                envelope(stream, 2, new TestEvent("b")),
                envelope(stream, 3, new TestEvent("c"))),
            Version.initial());
      }
    }
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("VACUUM ANALYZE event_stream");
      stmt.execute("SET enable_seqscan = off");
      stmt.execute("SET enable_bitmapscan = off");
      var plan =
          ExplainPlans.explainJson(
              conn,
              "SELECT global_offset, aggregate_type, aggregate_id, version FROM event_stream"
                  + " WHERE aggregate_type = 'product' AND aggregate_id = 'p-7' AND version > 0"
                  + " ORDER BY version");
      assertNotNull(
          ExplainPlans.findIndexScan(plan, "event_stream_stream_version_key"), plan.toString());
      assertFalse(ExplainPlans.containsNodeType(plan, "Sort"), plan.toString());
    }
  }
}
