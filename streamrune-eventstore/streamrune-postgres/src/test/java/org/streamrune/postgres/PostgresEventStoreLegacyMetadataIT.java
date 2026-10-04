package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Read-path tolerance.
 *
 * <p>The 255-character / control-character bound on {@code CorrelationId} and {@code TraceId} is an
 * INGRESS control ({@code RequestContext.fromRequest}): it exists to stop an unauthenticated {@code
 * X-Correlation-Id} header from parking arbitrary content in the append-only event log. The value
 * types' compact constructors deliberately do not enforce it: {@code EventMetadata} is rebuilt
 * through Jackson on every {@code load()}, every projection catch-up page and every DLQ/audit row,
 * and a caller that builds a {@code RequestContext} directly can still persist a longer or
 * control-bearing id. A read-path check would make such an event permanently unreadable — the
 * aggregate could never be loaded again, and the projection reading past that offset would poison
 * with no self-heal (the log is append-only and there is no metadata-rewrite API).
 *
 * <p>These rows are written here with raw SQL: the metadata goes straight into {@code
 * event_stream.metadata} with no bound on the ids.
 */
@Testcontainers
class PostgresEventStoreLegacyMetadataIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("legacy_metadata");

  private static final String OVERSIZED_CORRELATION_ID = "legacy-corr-" + "x".repeat(300);

  static PGSimpleDataSource dataSource;
  static ObjectMapper objectMapper;
  static EventTypeRegistry typeRegistry;

  PostgresEventStore store;

  record TestEvent(String value) implements DomainEvent {}

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return TestEvent.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return TestEvent.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return List.of(TestEvent.class);
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void setUp() throws Exception {
    store =
        new PostgresEventStore(
            dataSource, objectMapper, typeRegistry, null, null, null, null, null);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
  }

  /**
   * Writes one event row with raw SQL: metadata straight into the {@code jsonb} column with no
   * bound on the correlation / trace ids.
   */
  private void insertLegacyRow(StreamId streamId, long globalOffset, String metadataJson)
      throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version,"
                    + " event_type, payload, metadata, schema_version)"
                    + " VALUES (?, 'test', ?, ?, ?, ?::jsonb, ?::jsonb, ?)")) {
      stmt.setLong(1, globalOffset);
      stmt.setString(2, streamId.aggregateId().value());
      stmt.setLong(3, 1L);
      stmt.setString(4, "TestEvent");
      stmt.setString(5, "{\"value\":\"v1\"}");
      stmt.setString(6, metadataJson);
      stmt.setInt(7, 1);
      stmt.executeUpdate();
    }
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "UPDATE global_offset_sequence SET next_value = " + globalOffset + " WHERE id = 1");
    }
  }

  private static String metadataWith(String correlationId, String traceId) {
    return "{\"eventId\":\"evt-1\",\"commandId\":\"cmd-1\",\"traceId\":"
        + (traceId == null ? "null" : "\"" + traceId + "\"")
        + ",\"spanId\":null,\"correlationId\":\""
        + correlationId
        + "\",\"causationId\":null,\"userId\":null,\"timestamp\":\"2026-01-01T00:00:00Z\","
        + "\"baggage\":{}}";
  }

  @Test
  void load_readsAnEventWhoseStoredCorrelationIdExceedsTheIngressBound() throws Exception {
    StreamId streamId = TestStreams.stream("legacy-oversized");
    insertLegacyRow(streamId, 1L, metadataWith(OVERSIZED_CORRELATION_ID, null));

    var history = assertDoesNotThrow(() -> store.load(streamId));

    assertEquals(1, history.events().size());
    assertEquals(
        OVERSIZED_CORRELATION_ID,
        history.events().get(0).metadata().correlationId().value(),
        "the stored value is returned verbatim — reconstruction must never rewrite history");
  }

  @Test
  void readStream_readsTheSameLegacyRow() throws Exception {
    StreamId streamId = TestStreams.stream("legacy-readstream");
    insertLegacyRow(streamId, 1L, metadataWith(OVERSIZED_CORRELATION_ID, null));

    var events = assertDoesNotThrow(() -> store.readStream(streamId, Version.initial(), 10));
    assertEquals(1, events.size());
  }

  @Test
  void readGlobalStream_doesNotPoisonAProjectionAtALegacyOffset() throws Exception {
    // A projection catching up cannot skip the row: a throw here halts the runner at that offset
    // forever (health DOWN), and no operator action short of rewriting the append-only log clears
    // it.
    StreamId streamId = TestStreams.stream("legacy-global");
    insertLegacyRow(streamId, 1L, metadataWith(OVERSIZED_CORRELATION_ID, null));

    var events = assertDoesNotThrow(() -> store.readGlobalStream(GlobalOffset.of(0L), 10));
    assertEquals(1, events.size());
  }

  @Test
  void load_readsAnEventWhoseStoredTraceIdCarriesControlCharacters() throws Exception {
    // CR/LF was accepted by the pre-fix filters and is legal JSON once escaped. (A NUL cannot be
    // stored in a jsonb column at all, so the only control characters reachable in already-written
    // metadata are the ones Postgres accepts.)
    StreamId streamId = TestStreams.stream("legacy-control");
    insertLegacyRow(streamId, 1L, metadataWith("legacy-corr-1", "trace\\r\\n1"));

    var history = assertDoesNotThrow(() -> store.load(streamId));
    assertEquals("trace\r\n1", history.events().get(0).metadata().traceId().value());
  }

  @Test
  void blankIdsAreStillRejectedOnRead() throws Exception {
    // Leniency is bounded: a blank correlation id was never legal to write, so a row carrying one
    // is corrupt rather than merely old, and the record invariant stays.
    StreamId streamId = TestStreams.stream("legacy-blank");
    insertLegacyRow(streamId, 1L, metadataWith("   ", null));

    assertThrows(EventStoreException.class, () -> store.load(streamId));
  }
}
