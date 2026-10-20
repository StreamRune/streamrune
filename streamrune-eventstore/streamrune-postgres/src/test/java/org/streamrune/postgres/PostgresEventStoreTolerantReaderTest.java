package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The event store reads event payloads as a tolerant reader: a stored payload may carry a property
 * the reader's event class does not declare (a newer release added a field), and the property is
 * ignored. {@code strictEventPayloads(true)} restores the strict reader. Snapshot state, which the
 * application reads and saves back, is always read strictly.
 */
@Testcontainers
class PostgresEventStoreTolerantReaderTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  static PGSimpleDataSource dataSource;
  static EventTypeRegistry typeRegistry;

  record Placed(String orderId) implements DomainEvent {}

  record Counter(int count) implements AggregateState {}

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Placed.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Counter.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return List.of(Placed.class, Counter.class);
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void clean() throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("DELETE FROM snapshot_store");
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
  }

  private static PostgresEventStore.Builder builder() {
    return PostgresEventStore.builder().dataSource(dataSource).typeRegistry(typeRegistry);
  }

  @Test
  void aPayloadCarryingAPropertyTheClassLacks_readsByDefault_onLoadAndOnTheGlobalStream()
      throws SQLException {
    insertEvent("o-1", 1, "{\"orderId\": \"o-1\", \"giftWrap\": true}", 1);
    var store = builder().build();

    AggregateHistory history = store.load(TestStreams.stream("o-1"));
    assertEquals(new Placed("o-1"), history.events().getFirst().event());
    assertEquals(
        new Placed("o-1"),
        store.readGlobalStream(GlobalOffset.initial(), 10).getFirst().event(),
        "a projection reads the same event");
  }

  @Test
  void strictEventPayloads_refusesAPropertyTheClassLacks() throws SQLException {
    insertEvent("o-1", 1, "{\"orderId\": \"o-1\", \"giftWrap\": true}", 1);
    var store = builder().strictEventPayloads(true).build();

    assertThrows(EventDeserializationException.class, () -> store.load(TestStreams.stream("o-1")));
  }

  @Test
  void theFactoryPassesStrictEventPayloadsToTheStore() throws SQLException {
    insertEvent("o-1", 1, "{\"orderId\": \"o-1\", \"giftWrap\": true}", 1);
    var strict =
        new PostgresEventStoreFactory(dataSource, typeRegistry).strictEventPayloads(true).create();
    var tolerant = new PostgresEventStoreFactory(dataSource, typeRegistry).create();

    assertThrows(EventDeserializationException.class, () -> strict.load(TestStreams.stream("o-1")));
    assertEquals(
        new Placed("o-1"), tolerant.load(TestStreams.stream("o-1")).events().getFirst().event());
  }

  @Test
  void anUpcastPayloadCarryingAPropertyTheClassLacks_bindsByDefault_andIsRefusedWhenStrict()
      throws SQLException {
    insertEvent("o-1", 1, "{\"orderId\": \"o-1\"}", 1);
    EventUpcaster addsAKeyTheClassLacks =
        new EventUpcaster() {
          @Override
          public EventType eventType() {
            return new EventType("Placed");
          }

          @Override
          public int currentVersion() {
            return 2;
          }

          @Override
          public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
            var next = new HashMap<>(eventData);
            next.putIfAbsent("channel", "web");
            return next;
          }
        };

    var tolerant = builder().upcasters(List.of(addsAKeyTheClassLacks)).build();
    var strict =
        builder().upcasters(List.of(addsAKeyTheClassLacks)).strictEventPayloads(true).build();

    assertEquals(
        new Placed("o-1"), tolerant.load(TestStreams.stream("o-1")).events().getFirst().event());
    assertThrows(EventDeserializationException.class, () -> strict.load(TestStreams.stream("o-1")));
  }

  @Test
  void snapshotStateStaysStrict_aStateCarryingAnUnknownPropertyIsDiscardedAndReplayed()
      throws SQLException {
    // The state is read, evolved and saved back: a tolerant read would drop the unknown property
    // and the next snapshot would persist the loss. The store discards such a snapshot instead and
    // replays the events.
    insertEvent("o-1", 1, "{\"orderId\": \"o-1\"}", 1);
    var store = builder().build();
    StreamId stream = TestStreams.stream("o-1");
    store.saveSnapshot(stream, new Version(1), new Counter(1));
    assertNotNull(store.load(stream).snapshotState(), "precondition: the snapshot is used");

    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "UPDATE snapshot_store SET state_payload = state_payload || '{\"addedLater\": 7}'"
                    + "::jsonb WHERE aggregate_id = 'o-1'")) {
      assertEquals(1, ps.executeUpdate());
    }

    AggregateHistory history = store.load(stream);
    assertNull(history.snapshotState(), "the state was not bound tolerantly");
    assertEquals(1, history.events().size(), "the events replay instead");
    assertInstanceOf(Placed.class, history.events().getFirst().event());
  }

  private static void insertEvent(String aggregateId, long version, String payload, int schema)
      throws SQLException {
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
                  + " VALUES (?, 'test', ?, ?, 'Placed', ?::jsonb, ?::jsonb, ?)")) {
        ps.setLong(1, offset);
        ps.setString(2, aggregateId);
        ps.setLong(3, version);
        ps.setString(4, payload);
        ps.setString(
            5,
            "{\"eventId\":\"evt_"
                + offset
                + "\",\"commandId\":\"cmd_1\",\"traceId\":null,\"spanId\":null,"
                + "\"correlationId\":\"corr-1\",\"causationId\":null,\"userId\":null,"
                + "\"timestamp\":\"2025-01-01T00:00:00Z\"}");
        ps.setInt(6, schema);
        ps.executeUpdate();
      }
    }
  }
}
