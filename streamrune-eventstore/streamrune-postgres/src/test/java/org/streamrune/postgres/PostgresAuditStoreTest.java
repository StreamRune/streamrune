package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.UserId;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresAuditStoreTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_audit_test");

  static PGSimpleDataSource dataSource;
  PostgresAuditStore store;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    // Apply the shipped event-store baseline (creates audit_log) via the factory
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
    store = new PostgresAuditStore(dataSource);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM audit_log");
    }
  }

  @Test
  void save_persists_success_entry() throws Exception {
    var cmdId = CommandId.of("cmd-save-success");
    var occurredAt = Instant.parse("2026-01-01T10:00:00Z");
    store.save(
        new AuditEntry(
            cmdId,
            "PlaceOrder",
            TestStreams.TYPE,
            AggregateId.of("order-1"),
            UserId.of("user-42"),
            occurredAt,
            AuditOutcome.SUCCESS,
            null,
            3,
            null));

    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT command_id, command_type, aggregate_id, user_id, outcome, error_message, event_count "
                    + "FROM audit_log WHERE command_id = ?")) {
      ps.setString(1, "cmd-save-success");
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("cmd-save-success", rs.getString("command_id"));
        assertEquals("PlaceOrder", rs.getString("command_type"));
        assertEquals("order-1", rs.getString("aggregate_id"));
        assertEquals("user-42", rs.getString("user_id"));
        assertEquals("SUCCESS", rs.getString("outcome"));
        assertNull(rs.getString("error_message"));
        assertEquals(3, rs.getInt("event_count"));
      }
    }
  }

  @Test
  void save_persists_correlation_id() throws Exception {
    var cmdId = CommandId.of("cmd-corr");
    store.save(
        new AuditEntry(
            cmdId,
            "PlaceOrder",
            TestStreams.TYPE,
            AggregateId.of("order-9"),
            UserId.of("user-1"),
            Instant.parse("2026-01-01T10:00:00Z"),
            AuditOutcome.SUCCESS,
            null,
            1,
            CorrelationId.of("corr-flow-1")));

    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement("SELECT correlation_id FROM audit_log WHERE command_id = ?")) {
      ps.setString(1, "cmd-corr");
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("corr-flow-1", rs.getString("correlation_id"));
      }
    }
  }

  @Test
  void save_persists_query_origin_entry_with_null_command_id() throws Exception {
    // Query-origin entry: no commandId, carries only the correlation token.
    store.save(
        new AuditEntry(
            null,
            "FindOrderById",
            null,
            null,
            UserId.of("user-2"),
            Instant.parse("2026-01-02T10:00:00Z"),
            AuditOutcome.SUCCESS,
            null,
            0,
            CorrelationId.of("corr-flow-2")));

    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT command_id, correlation_id, command_type FROM audit_log "
                    + "WHERE command_type = ?")) {
      ps.setString(1, "FindOrderById");
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertNull(rs.getString("command_id"));
        assertEquals("corr-flow-2", rs.getString("correlation_id"));
      }
    }
  }

  @Test
  void save_persists_failure_entry_with_null_user() throws Exception {
    var cmdId = CommandId.of("cmd-save-failure");
    store.save(
        new AuditEntry(
            cmdId,
            "CancelOrder",
            TestStreams.TYPE,
            AggregateId.of("order-2"),
            null,
            Instant.now(),
            AuditOutcome.FAILURE,
            "access denied",
            0,
            null));

    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT user_id, outcome, error_message FROM audit_log WHERE command_id = ?")) {
      ps.setString(1, "cmd-save-failure");
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertNull(rs.getString("user_id"));
        assertEquals("FAILURE", rs.getString("outcome"));
        assertEquals("access denied", rs.getString("error_message"));
      }
    }
  }

  @Test
  void save_writesTheAggregateTypeColumn_nullForQueryAndSubjectEntries() throws Exception {
    var t = Instant.parse("2026-01-03T10:00:00Z");
    store.save(
        new AuditEntry(
            CommandId.of("cmd-col-typed"),
            "PlaceOrder",
            TestStreams.TYPE,
            AggregateId.of("order-3"),
            null,
            t,
            AuditOutcome.SUCCESS,
            null,
            1,
            null));
    store.save(
        new AuditEntry(
            CommandId.of("gdpr-col"),
            "GDPR_ERASE",
            null,
            AggregateId.of("subject-hash-9"),
            null,
            t,
            AuditOutcome.SUCCESS,
            null,
            0,
            null));
    store.save(
        new AuditEntry(
            null, "FindOrderById", null, null, null, t, AuditOutcome.SUCCESS, null, 0, null));

    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT command_type, aggregate_type, aggregate_id FROM audit_log"
                    + " ORDER BY command_type")) {
      assertTrue(rs.next());
      assertEquals("FindOrderById", rs.getString("command_type"));
      assertNull(rs.getString("aggregate_type"));
      assertNull(rs.getString("aggregate_id"));
      assertTrue(rs.next());
      assertEquals("GDPR_ERASE", rs.getString("command_type"));
      assertNull(rs.getString("aggregate_type"), "a subject entry carries no aggregate type");
      assertEquals("subject-hash-9", rs.getString("aggregate_id"));
      assertTrue(rs.next());
      assertEquals("PlaceOrder", rs.getString("command_type"));
      assertEquals(TestStreams.TYPE.value(), rs.getString("aggregate_type"));
      assertEquals("order-3", rs.getString("aggregate_id"));
    }
  }

  @Test
  void theAggregateIndex_coversTheTypedPair_forEntriesThatHaveAnAggregate() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_audit_aggregate'")) {
      assertTrue(rs.next());
      var definition = rs.getString(1);
      assertTrue(definition.contains("(aggregate_type, aggregate_id)"), definition);
      assertTrue(definition.contains("WHERE (aggregate_id IS NOT NULL)"), definition);
    }
  }

  @Test
  void save_rejects_null_entry() {
    assertThrows(NullPointerException.class, () -> store.save(null));
  }

  @Test
  void constructor_rejects_null_datasource() {
    assertThrows(IllegalArgumentException.class, () -> new PostgresAuditStore(null));
  }

  @Test
  void save_allows_duplicate_command_ids() throws Exception {
    var cmdId = CommandId.of("cmd-dup");
    var entry =
        new AuditEntry(
            cmdId,
            "Cmd",
            TestStreams.TYPE,
            AggregateId.of("agg-1"),
            null,
            Instant.now(),
            AuditOutcome.SUCCESS,
            null,
            0,
            null);
    store.save(entry);
    // Audit log is append-only — duplicate commandIds are allowed (retry scenarios)
    assertDoesNotThrow(() -> store.save(entry));

    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT count(*) FROM audit_log WHERE command_id = ?")) {
      ps.setString(1, "cmd-dup");
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
      }
    }
  }
}
