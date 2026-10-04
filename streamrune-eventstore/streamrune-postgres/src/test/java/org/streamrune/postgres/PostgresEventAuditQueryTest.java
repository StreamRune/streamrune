package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.types.*;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresEventAuditQueryTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_event_audit_query_test");

  static PGSimpleDataSource dataSource;
  PostgresEventAuditStore store;
  PostgresEventAuditQuery query;

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
    query = new PostgresEventAuditQuery(dataSource);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_audit_log");
    }
  }

  @Test
  void findByCorrelationId() {
    var t1 = Instant.parse("2026-01-01T10:00:00Z");
    var t2 = Instant.parse("2026-01-01T10:00:01Z");
    var t3 = Instant.parse("2026-01-01T10:00:02Z");

    store.saveAll(
        List.of(
            entry("e1", "OrderPlaced", "s1", 1, "cmd-1", "corr-A", null, null, t1),
            entry("e2", "OrderConfirmed", "s1", 2, "cmd-2", "corr-A", "e1", "u1", t2),
            entry("e3", "PaymentReceived", "s2", 1, "cmd-3", "corr-B", null, null, t3)));

    List<EventAuditEntry> results = query.findByCorrelationId(CorrelationId.of("corr-A"));

    assertEquals(2, results.size());
    assertEquals("e1", results.get(0).eventId().value());
    assertEquals("e2", results.get(1).eventId().value());
  }

  @Test
  void findByCorrelationIdReturnsEmptyForUnknown() {
    List<EventAuditEntry> results =
        query.findByCorrelationId(CorrelationId.of("non-existent-corr"));
    assertTrue(results.isEmpty());
  }

  @Test
  void findByCommandId() {
    var now = Instant.now();
    store.saveAll(
        List.of(
            entry("e10", "A", "s1", 1, "cmd-X", "corr-1", null, null, now),
            entry("e11", "B", "s1", 2, "cmd-X", "corr-1", "e10", null, now.plusSeconds(1)),
            entry("e12", "C", "s2", 1, "cmd-Y", "corr-2", null, null, now.plusSeconds(2))));

    List<EventAuditEntry> results = query.findByCommandId(CommandId.of("cmd-X"));

    assertEquals(2, results.size());
    assertEquals("e10", results.get(0).eventId().value());
    assertEquals("e11", results.get(1).eventId().value());
  }

  @Test
  void findByCausationId() {
    var now = Instant.now();
    store.saveAll(
        List.of(
            entry("e20", "Root", "s1", 1, "cmd-1", "corr-1", null, null, now),
            entry("e21", "Child1", "s1", 2, "cmd-2", "corr-1", "e20", null, now.plusSeconds(1)),
            entry("e22", "Child2", "s2", 1, "cmd-3", "corr-1", "e20", null, now.plusSeconds(2)),
            entry(
                "e23", "Grandchild", "s2", 2, "cmd-4", "corr-1", "e21", null, now.plusSeconds(3))));

    List<EventAuditEntry> results = query.findByCausationId(CausationId.of("e20"));

    assertEquals(2, results.size());
    assertEquals("e21", results.get(0).eventId().value());
    assertEquals("e22", results.get(1).eventId().value());
  }

  @Test
  void findByStreamId() {
    var t1 = Instant.parse("2026-03-01T10:00:00Z");
    var t2 = Instant.parse("2026-03-01T12:00:00Z");
    var t3 = Instant.parse("2026-03-01T14:00:00Z");
    var t4 = Instant.parse("2026-03-01T16:00:00Z");

    store.saveAll(
        List.of(
            entry("e30", "A", "stream-1", 1, "cmd-1", "corr-1", null, null, t1),
            entry("e31", "B", "stream-1", 2, "cmd-2", "corr-1", null, null, t2),
            entry("e32", "C", "stream-1", 3, "cmd-3", "corr-1", null, null, t3),
            entry("e33", "D", "stream-1", 4, "cmd-4", "corr-1", null, null, t4),
            entry("e34", "E", "stream-2", 1, "cmd-5", "corr-2", null, null, t2)));

    // Query with inclusive time range: t2..t3 should include e31 and e32
    List<EventAuditEntry> results = query.findByStreamId(TestStreams.stream("stream-1"), t2, t3);

    assertEquals(2, results.size());
    assertEquals("e31", results.get(0).eventId().value());
    assertEquals("e32", results.get(1).eventId().value());
  }

  @Test
  void findByStreamId_aNullStream_isRefusedRatherThanReadAsAnEmptyStream() {
    var from = Instant.parse("2026-03-01T10:00:00Z");
    var to = Instant.parse("2026-03-01T12:00:00Z");
    var e = assertThrows(NullPointerException.class, () -> query.findByStreamId(null, from, to));
    assertEquals("streamId is required", e.getMessage());
  }

  @Test
  void aHandWrittenRowWhoseIdExceedsTheStreamColumn_isRefusedByTheDatabase() throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO event_audit_log (event_id, event_type, aggregate_type, aggregate_id,"
                    + " version, command_id, correlation_id, occurred_at)"
                    + " VALUES ('e-long', 'A', 'agg', ?, 1, 'cmd-long', 'corr-long', now())")) {
      ps.setString(1, "x".repeat(256));
      var refused = assertThrows(java.sql.SQLException.class, ps::executeUpdate);
      assertEquals("22001", refused.getSQLState(), refused.getMessage());
    }
  }

  @Test
  void aHandWrittenRowTheIdTypesRefuse_failsAsAnEventStoreException() throws Exception {
    // NOT NULL and VARCHAR(255) admit an empty id; AggregateId refuses a blank one on decode.
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO event_audit_log (event_id, event_type, aggregate_type, aggregate_id,"
                    + " version, command_id, correlation_id, occurred_at)"
                    + " VALUES ('e-empty', 'A', 'agg', '', 1, 'cmd-empty', 'corr-empty', now())")) {
      ps.executeUpdate();
    }

    var e =
        assertThrows(
            org.streamrune.core.EventStoreException.class,
            () -> query.findByCommandId(CommandId.of("cmd-empty")));
    assertInstanceOf(IllegalArgumentException.class, e.getCause());
  }

  @Test
  void aHandWrittenRowWhoseIdFillsTheStreamColumn_isReadBack() throws Exception {
    var id = "x".repeat(255);
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO event_audit_log (event_id, event_type, aggregate_type, aggregate_id,"
                    + " version, command_id, correlation_id, occurred_at)"
                    + " VALUES ('e-full', 'A', 'agg', ?, 1, 'cmd-full', 'corr-full', now())")) {
      ps.setString(1, id);
      ps.executeUpdate();
    }

    var entries = query.findByCommandId(CommandId.of("cmd-full"));

    assertEquals(1, entries.size());
    assertEquals(id, entries.get(0).streamId().aggregateId().value());
  }

  @Test
  void findByStreamId_twoTypesSharingAnIdValue_neverSeeEachOthersEntries() {
    var t = Instant.parse("2026-03-02T10:00:00Z");
    var product = StreamId.of(AggregateType.of("product"), AggregateId.of("x"));
    var inventory = StreamId.of(AggregateType.of("inventory"), AggregateId.of("x"));
    store.saveAll(
        List.of(
            typedEntry("p1", product, 1, t),
            typedEntry("p2", product, 2, t.plusSeconds(1)),
            typedEntry("i1", inventory, 1, t.plusSeconds(2))));

    var productEntries =
        query.findByStreamId(product, t.minusSeconds(60), t.plusSeconds(60)).stream()
            .map(e -> e.eventId().value())
            .toList();
    var inventoryEntries =
        query.findByStreamId(inventory, t.minusSeconds(60), t.plusSeconds(60)).stream()
            .map(e -> e.eventId().value())
            .toList();

    assertEquals(List.of("p1", "p2"), productEntries);
    assertEquals(List.of("i1"), inventoryEntries);
    assertEquals(
        product,
        query.findByStreamId(product, t.minusSeconds(60), t.plusSeconds(60)).getFirst().streamId());
  }

  /**
   * The stream lookup reads its rows from the stream index in order, so there is no sort above it.
   */
  @Test
  void findByStreamId_readsFromTheStreamIndexWithoutASort() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "INSERT INTO event_audit_log (event_id, event_type, aggregate_type, aggregate_id,"
              + " version, command_id, correlation_id, occurred_at)"
              + " SELECT 'seed-' || g, 'Seeded',"
              + " CASE WHEN g % 2 = 0 THEN 'product' ELSE 'inventory' END,"
              + " 'x-' || ((g / 2) % 25), g, 'cmd-' || g, 'corr-' || g,"
              + " now() - g * interval '1 minute' FROM generate_series(1, 2000) g");
      stmt.execute("VACUUM ANALYZE event_audit_log");
      stmt.execute("SET enable_seqscan = off");
      stmt.execute("SET enable_bitmapscan = off");

      var plan =
          ExplainPlans.explainJson(
              conn,
              "SELECT event_id FROM event_audit_log WHERE aggregate_type = 'product'"
                  + " AND aggregate_id = 'x-7' AND occurred_at >= now() - interval '1 day'"
                  + " AND occurred_at <= now() ORDER BY occurred_at");

      assertNotNull(
          ExplainPlans.findIndexScan(plan, "idx_event_audit_stream"), "plan was: " + plan);
      assertFalse(ExplainPlans.containsNodeType(plan, "Sort"), "plan was: " + plan);
    }
  }

  @Test
  void findByUserId() {
    var t1 = Instant.parse("2026-04-01T08:00:00Z");
    var t2 = Instant.parse("2026-04-01T09:00:00Z");
    var t3 = Instant.parse("2026-04-01T10:00:00Z");

    store.saveAll(
        List.of(
            entry("e40", "A", "s1", 1, "cmd-1", "corr-1", null, "alice", t1),
            entry("e41", "B", "s1", 2, "cmd-2", "corr-1", null, "alice", t2),
            entry("e42", "C", "s2", 1, "cmd-3", "corr-2", null, "bob", t2),
            entry("e43", "D", "s1", 3, "cmd-4", "corr-1", null, "alice", t3)));

    List<EventAuditEntry> results = query.findByUserId(UserId.of("alice"), t1, t2);

    assertEquals(2, results.size());
    assertEquals("e40", results.get(0).eventId().value());
    assertEquals("e41", results.get(1).eventId().value());
    // bob's entry at t2 is excluded — different user
  }

  @Test
  void traceFullCausationChain() {
    // Chain: e1 -> e2 -> e4, e1 -> e3 (branch), e5 unrelated
    var t = Instant.parse("2026-05-01T10:00:00Z");
    store.saveAll(
        List.of(
            entry("e1", "Root", "s1", 1, "cmd-1", "corr-1", null, null, t),
            entry("e2", "Child1", "s1", 2, "cmd-2", "corr-1", "e1", null, t.plusSeconds(1)),
            entry("e3", "Child2", "s2", 1, "cmd-3", "corr-1", "e1", null, t.plusSeconds(2)),
            entry("e4", "Grandchild", "s1", 3, "cmd-4", "corr-1", "e2", null, t.plusSeconds(3)),
            entry("e5", "Unrelated", "s3", 1, "cmd-5", "corr-2", null, null, t.plusSeconds(4))));

    List<EventAuditEntry> chain = query.traceFullCausationChain(EventId.of("e1"));

    assertEquals(4, chain.size());
    assertEquals("e1", chain.get(0).eventId().value());
    assertEquals("e2", chain.get(1).eventId().value());
    assertEquals("e3", chain.get(2).eventId().value());
    assertEquals("e4", chain.get(3).eventId().value());
    // e5 is NOT in the chain
  }

  @Test
  void traceFullCausationChainTerminatesOnCausationCycle() {
    // Corrupted data: c1 -> c2 -> c1. The recursive CTE must terminate (UNION discards
    // re-derived rows) and return each event exactly once instead of spinning until timeout.
    var t = Instant.parse("2026-05-02T10:00:00Z");
    store.saveAll(
        List.of(
            entry("c1", "A", "s1", 1, "cmd-1", "corr-1", "c2", null, t),
            entry("c2", "B", "s1", 2, "cmd-2", "corr-1", "c1", null, t.plusSeconds(1))));

    List<EventAuditEntry> chain = query.traceFullCausationChain(EventId.of("c1"));

    assertEquals(2, chain.size());
    assertEquals("c1", chain.get(0).eventId().value());
    assertEquals("c2", chain.get(1).eventId().value());
  }

  @Test
  void traceFullCausationChainTerminatesOnSelfReference() {
    // Corrupted data: an event caused by itself must appear once, not recurse forever.
    store.save(entry("self-1", "Loop", "s1", 1, "cmd-1", "corr-1", "self-1", null, Instant.now()));

    List<EventAuditEntry> chain = query.traceFullCausationChain(EventId.of("self-1"));

    assertEquals(1, chain.size());
    assertEquals("self-1", chain.get(0).eventId().value());
  }

  @Test
  void traceFullCausationChainReturnsEmptyForUnknown() {
    List<EventAuditEntry> chain = query.traceFullCausationChain(EventId.of("non-existent-event"));
    assertTrue(chain.isEmpty());
  }

  @Test
  void traceFullCausationChainSingleEvent() {
    store.save(entry("leaf-1", "LeafEvent", "s1", 1, "cmd-1", "corr-1", null, null, Instant.now()));

    List<EventAuditEntry> chain = query.traceFullCausationChain(EventId.of("leaf-1"));

    assertEquals(1, chain.size());
    assertEquals("leaf-1", chain.get(0).eventId().value());
  }

  private static EventAuditEntry typedEntry(
      String eventId, StreamId streamId, long version, Instant occurredAt) {
    return new EventAuditEntry(
        EventId.of(eventId),
        new EventType("Typed"),
        streamId,
        new Version(version),
        CommandId.of("cmd-" + eventId),
        CorrelationId.of("corr-" + eventId),
        null,
        null,
        occurredAt);
  }

  private static EventAuditEntry entry(
      String eventId,
      String eventType,
      String streamId,
      long version,
      String commandId,
      String correlationId,
      String causationId,
      String userId,
      Instant occurredAt) {
    return new EventAuditEntry(
        EventId.of(eventId),
        new EventType(eventType),
        TestStreams.stream(streamId),
        new Version(version),
        CommandId.of(commandId),
        CorrelationId.of(correlationId),
        causationId != null ? CausationId.of(causationId) : null,
        userId != null ? UserId.of(userId) : null,
        occurredAt);
  }
}
