package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.types.*;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresEventAuditStoreTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_event_audit_test");

  static PGSimpleDataSource dataSource;
  PostgresEventAuditStore store;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    EventTypeRegistry typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Object.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Object.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            // No @Encrypted-bearing types are exercised through this registry in this test; report
            // none explicitly rather than inheriting the throwing default.
            return java.util.List.of();
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void setUp() throws Exception {
    store = new PostgresEventAuditStore(dataSource);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_audit_log");
    }
  }

  @Test
  void savePersistsEntry() throws Exception {
    var entry =
        new EventAuditEntry(
            EventId.of("evt-1"),
            new EventType("OrderPlaced"),
            TestStreams.stream("order-1"),
            new Version(1),
            CommandId.of("cmd-1"),
            CorrelationId.of("corr-1"),
            CausationId.of("cause-1"),
            UserId.of("user-42"),
            Instant.parse("2026-01-01T10:00:00Z"));

    store.save(entry);

    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT event_id, event_type, aggregate_type, aggregate_id, version, command_id,"
                    + " correlation_id, "
                    + "causation_id, user_id, occurred_at "
                    + "FROM event_audit_log WHERE event_id = ?")) {
      ps.setString(1, "evt-1");
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("evt-1", rs.getString("event_id"));
        assertEquals("OrderPlaced", rs.getString("event_type"));
        assertEquals("test", rs.getString("aggregate_type"));
        assertEquals("order-1", rs.getString("aggregate_id"));
        assertEquals(1L, rs.getLong("version"));
        assertEquals("cmd-1", rs.getString("command_id"));
        assertEquals("corr-1", rs.getString("correlation_id"));
        assertEquals("cause-1", rs.getString("causation_id"));
        assertEquals("user-42", rs.getString("user_id"));
        assertEquals(
            Instant.parse("2026-01-01T10:00:00Z"), rs.getTimestamp("occurred_at").toInstant());
      }
    }
  }

  @Test
  void saveAllPersistsMultipleEntries() throws Exception {
    var now = Instant.now();
    var entries =
        List.of(
            new EventAuditEntry(
                EventId.of("evt-a"),
                new EventType("ItemAdded"),
                TestStreams.stream("cart-1"),
                new Version(1),
                CommandId.of("cmd-a"),
                CorrelationId.of("corr-a"),
                null,
                null,
                now),
            new EventAuditEntry(
                EventId.of("evt-b"),
                new EventType("ItemAdded"),
                TestStreams.stream("cart-1"),
                new Version(2),
                CommandId.of("cmd-a"),
                CorrelationId.of("corr-a"),
                CausationId.of("evt-a"),
                UserId.of("user-1"),
                now.plusSeconds(1)),
            new EventAuditEntry(
                EventId.of("evt-c"),
                new EventType("CartCheckedOut"),
                TestStreams.stream("cart-1"),
                new Version(3),
                CommandId.of("cmd-b"),
                CorrelationId.of("corr-a"),
                CausationId.of("evt-b"),
                UserId.of("user-1"),
                now.plusSeconds(2)));

    store.saveAll(entries);

    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT count(*) FROM event_audit_log")) {
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
      }
    }
  }

  @Test
  void savePersistsNullCausationAndUser() throws Exception {
    var entry =
        new EventAuditEntry(
            EventId.of("evt-null"),
            new EventType("OrderCreated"),
            TestStreams.stream("order-2"),
            Version.initial(),
            CommandId.of("cmd-null"),
            CorrelationId.of("corr-null"),
            null,
            null,
            Instant.now());

    store.save(entry);

    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT causation_id, user_id FROM event_audit_log WHERE event_id = ?")) {
      ps.setString(1, "evt-null");
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertNull(rs.getString("causation_id"));
        assertNull(rs.getString("user_id"));
      }
    }
  }

  @Test
  void saveRejectsNullEntry() {
    assertThrows(NullPointerException.class, () -> store.save(null));
  }

  @Test
  void constructorRejectsNullDatasource() {
    assertThrows(IllegalArgumentException.class, () -> new PostgresEventAuditStore(null));
  }

  @Test
  void duplicateEventIdIsRejected() {
    var entry =
        new EventAuditEntry(
            EventId.of("evt-dup"),
            new EventType("OrderPlaced"),
            TestStreams.stream("order-3"),
            new Version(1),
            CommandId.of("cmd-dup"),
            CorrelationId.of("corr-dup"),
            null,
            null,
            Instant.now());

    store.save(entry);
    assertThrows(EventStoreException.class, () -> store.save(entry));
  }
}
