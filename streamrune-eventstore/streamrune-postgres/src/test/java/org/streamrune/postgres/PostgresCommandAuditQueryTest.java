package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.UserId;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresCommandAuditQueryTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_command_audit_query_test");

  static PGSimpleDataSource dataSource;
  PostgresAuditStore store;
  PostgresCommandAuditQuery query;

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
    store = new PostgresAuditStore(dataSource);
    query = new PostgresCommandAuditQuery(dataSource);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM audit_log");
    }
  }

  @Test
  void findByCommandId_roundTrips() {
    var occurredAt = Instant.parse("2026-01-01T10:00:00Z");
    store.save(
        new AuditEntry(
            CommandId.of("cmd-1"),
            "PlaceOrder",
            TestStreams.TYPE,
            AggregateId.of("order-1"),
            UserId.of("alice"),
            occurredAt,
            AuditOutcome.SUCCESS,
            null,
            3,
            CorrelationId.of("corr-A")));

    Optional<AuditEntry> result = query.findByCommandId(CommandId.of("cmd-1"));

    assertTrue(result.isPresent());
    AuditEntry entry = result.get();
    assertEquals(CommandId.of("cmd-1"), entry.commandId());
    assertEquals("PlaceOrder", entry.commandType());
    assertEquals(AggregateId.of("order-1"), entry.aggregateId());
    assertEquals(UserId.of("alice"), entry.userId());
    assertEquals(occurredAt, entry.occurredAt());
    assertEquals(AuditOutcome.SUCCESS, entry.outcome());
    assertEquals(3, entry.eventCount());
    assertEquals(CorrelationId.of("corr-A"), entry.correlationId());
  }

  /**
   * {@code AggregateId.of} refuses control characters — it is the INGRESS door. This row mapper is
   * the DECODE door: an {@code audit_log.aggregate_id} written for a command routed through the
   * canonical constructor (no charset rule) must read back verbatim, or every audit page reaching
   * that row would throw.
   */
  @Test
  void findByCommandId_aStoredAggregateIdCarryingAControlCharacterReadsBackVerbatim() {
    var raw = "order\r\n\u0085-1";
    store.save(
        new AuditEntry(
            CommandId.of("cmd-ctl"),
            "PlaceOrder",
            TestStreams.TYPE,
            new AggregateId(raw),
            UserId.of("alice"),
            Instant.parse("2026-01-01T10:00:00Z"),
            AuditOutcome.SUCCESS,
            null,
            1,
            CorrelationId.of("corr-ctl")));

    Optional<AuditEntry> result = query.findByCommandId(CommandId.of("cmd-ctl"));

    assertTrue(result.isPresent());
    assertEquals(new AggregateId(raw), result.get().aggregateId());
  }

  @Test
  void findByCommandId_returnsMostRecentOnRetryDuplicates() {
    var t1 = Instant.parse("2026-01-01T10:00:00Z");
    var t2 = Instant.parse("2026-01-01T10:00:05Z");
    store.save(
        new AuditEntry(
            CommandId.of("cmd-retry"),
            "C",
            TestStreams.TYPE,
            AggregateId.of("agg"),
            null,
            t1,
            AuditOutcome.FAILURE,
            "boom",
            0,
            null));
    store.save(
        new AuditEntry(
            CommandId.of("cmd-retry"),
            "C",
            TestStreams.TYPE,
            AggregateId.of("agg"),
            null,
            t2,
            AuditOutcome.SUCCESS,
            null,
            1,
            null));

    Optional<AuditEntry> result = query.findByCommandId(CommandId.of("cmd-retry"));

    assertTrue(result.isPresent());
    assertEquals(AuditOutcome.SUCCESS, result.get().outcome());
    assertEquals(t2, result.get().occurredAt());
  }

  @Test
  void aCommandEntryOfAType_isReturnedByEveryFinderWithItsAggregateType() {
    var t = Instant.parse("2026-07-01T10:00:00Z");
    var order = AggregateType.of("order");
    store.save(
        new AuditEntry(
            CommandId.of("cmd-typed"),
            "PlaceTypedOrder",
            order,
            AggregateId.of("o-1"),
            UserId.of("typed-user"),
            t,
            AuditOutcome.SUCCESS,
            null,
            1,
            null));
    var from = t.minusSeconds(1);
    var to = t.plusSeconds(1);
    var one = PageRequest.of(0, 10);

    assertEquals(
        order, query.findByCommandId(CommandId.of("cmd-typed")).orElseThrow().aggregateType());
    assertEquals(
        order,
        query
            .findByUserId(UserId.of("typed-user"), from, to, one)
            .content()
            .getFirst()
            .aggregateType());
    assertEquals(
        order,
        query
            .findByCommandType("PlaceTypedOrder", from, to, one)
            .content()
            .getFirst()
            .aggregateType());
    var byOutcome =
        query.findByOutcome(AuditOutcome.SUCCESS, from, to, one).content().stream()
            .filter(e -> "cmd-typed".equals(e.commandId().value()))
            .findFirst()
            .orElseThrow();
    assertEquals(order, byOutcome.aggregateType());
    assertEquals(AggregateId.of("o-1"), byOutcome.aggregateId());
  }

  @Test
  void aQueryOriginEntryAndASubjectEntry_readBackWithoutAType_theSubjectEntryKeepsItsId() {
    var t = Instant.parse("2026-07-02T10:00:00Z");
    store.save(
        new AuditEntry(
            null,
            "FindSomething",
            null,
            null,
            UserId.of("q-user"),
            t,
            AuditOutcome.SUCCESS,
            null,
            0,
            null));
    // The shape the erasure audit writes: a subject hash in the id column, no aggregate type.
    store.save(
        new AuditEntry(
            CommandId.of("gdpr-1"),
            "GDPR_ERASE",
            null,
            AggregateId.of("subject-hash-1"),
            UserId.of("dpo"),
            t,
            AuditOutcome.SUCCESS,
            null,
            0,
            null));
    var from = t.minusSeconds(1);
    var to = t.plusSeconds(1);
    var one = PageRequest.of(0, 10);

    var queryEntry = query.findByCommandType("FindSomething", from, to, one).content().getFirst();
    assertNull(queryEntry.aggregateType());
    assertNull(queryEntry.aggregateId());

    var subjectEntry = query.findByCommandId(CommandId.of("gdpr-1")).orElseThrow();
    assertNull(subjectEntry.aggregateType());
    assertEquals(AggregateId.of("subject-hash-1"), subjectEntry.aggregateId());
  }

  @Test
  void findByCommandId_returnsEmptyForUnknown() {
    assertTrue(query.findByCommandId(CommandId.of("nope")).isEmpty());
  }

  @Test
  void findByCommandId_rejectsNull() {
    assertThrows(NullPointerException.class, () -> query.findByCommandId(null));
  }

  @Test
  void findByQueryOriginEntry_hasNullCommandIdAndCarriesCorrelation() {
    var occurredAt = Instant.parse("2026-02-01T10:00:00Z");
    // Query-origin entry written by AuditingQueryBus: null commandId.
    store.save(
        new AuditEntry(
            null,
            "FindOrderById",
            null,
            null,
            UserId.of("bob"),
            occurredAt,
            AuditOutcome.SUCCESS,
            null,
            0,
            CorrelationId.of("corr-Q")));

    Page<AuditEntry> page =
        query.findByCommandType(
            "FindOrderById",
            occurredAt.minusSeconds(1),
            occurredAt.plusSeconds(1),
            PageRequest.of(0, 10));

    assertEquals(1, page.totalElements());
    AuditEntry entry = page.content().get(0);
    assertNull(entry.commandId());
    assertNull(entry.aggregateId(), "a query-origin entry has no aggregate: NULL reads back null");
    assertEquals(CorrelationId.of("corr-Q"), entry.correlationId());
  }

  @Test
  void findByUserId_filtersAndPagesInOccurredAtOrder() {
    var t1 = Instant.parse("2026-03-01T08:00:00Z");
    var t2 = Instant.parse("2026-03-01T09:00:00Z");
    var t3 = Instant.parse("2026-03-01T10:00:00Z");
    store.save(entry("cmd-a", "PlaceOrder", "alice", t1));
    store.save(entry("cmd-b", "CancelOrder", "alice", t2));
    store.save(entry("cmd-c", "PlaceOrder", "bob", t2)); // different user
    store.save(entry("cmd-d", "ShipOrder", "alice", t3));

    Page<AuditEntry> page = query.findByUserId(UserId.of("alice"), t1, t3, PageRequest.of(0, 2));

    assertEquals(3, page.totalElements());
    assertEquals(2, page.totalPages());
    assertEquals(2, page.content().size());
    assertEquals("cmd-a", page.content().get(0).commandId().value());
    assertEquals("cmd-b", page.content().get(1).commandId().value());

    Page<AuditEntry> page2 = query.findByUserId(UserId.of("alice"), t1, t3, PageRequest.of(1, 2));
    assertEquals(1, page2.content().size());
    assertEquals("cmd-d", page2.content().get(0).commandId().value());
  }

  @Test
  void findByUserId_respectsTimeWindow() {
    var t1 = Instant.parse("2026-03-02T08:00:00Z");
    var t2 = Instant.parse("2026-03-02T09:00:00Z");
    var t3 = Instant.parse("2026-03-02T10:00:00Z");
    store.save(entry("cmd-w1", "C", "carol", t1));
    store.save(entry("cmd-w2", "C", "carol", t2));
    store.save(entry("cmd-w3", "C", "carol", t3));

    Page<AuditEntry> page = query.findByUserId(UserId.of("carol"), t1, t2, PageRequest.of(0, 10));

    assertEquals(2, page.totalElements());
    assertEquals("cmd-w1", page.content().get(0).commandId().value());
    assertEquals("cmd-w2", page.content().get(1).commandId().value());
  }

  @Test
  void findByOutcome_filtersOnOutcome() {
    var t1 = Instant.parse("2026-04-01T08:00:00Z");
    var t2 = Instant.parse("2026-04-01T09:00:00Z");
    store.save(
        new AuditEntry(
            CommandId.of("ok-1"),
            "C",
            TestStreams.TYPE,
            AggregateId.of("agg"),
            UserId.of("u"),
            t1,
            AuditOutcome.SUCCESS,
            null,
            1,
            null));
    store.save(
        new AuditEntry(
            CommandId.of("fail-1"),
            "C",
            TestStreams.TYPE,
            AggregateId.of("agg"),
            UserId.of("u"),
            t2,
            AuditOutcome.FAILURE,
            "denied",
            0,
            null));

    Page<AuditEntry> failures =
        query.findByOutcome(AuditOutcome.FAILURE, t1, t2.plusSeconds(1), PageRequest.of(0, 10));

    assertEquals(1, failures.totalElements());
    assertEquals("fail-1", failures.content().get(0).commandId().value());
    assertEquals("denied", failures.content().get(0).errorMessage());
  }

  @Test
  void findByCommandType_filtersOnType() {
    var t1 = Instant.parse("2026-05-01T08:00:00Z");
    var t2 = Instant.parse("2026-05-01T09:00:00Z");
    store.save(entry("p1", "PlaceOrder", "u", t1));
    store.save(entry("c1", "CancelOrder", "u", t2));

    Page<AuditEntry> page =
        query.findByCommandType("PlaceOrder", t1, t2.plusSeconds(1), PageRequest.of(0, 10));

    assertEquals(1, page.totalElements());
    assertEquals("p1", page.content().get(0).commandId().value());
  }

  @Test
  void findByUserId_returnsEmptyPageForNoMatch() {
    var now = Instant.parse("2026-06-01T08:00:00Z");
    Page<AuditEntry> page =
        query.findByUserId(UserId.of("ghost"), now, now.plusSeconds(1), PageRequest.of(0, 10));

    assertTrue(page.isEmpty());
    assertEquals(0, page.totalElements());
  }

  @Test
  void constructor_rejectsNullDataSource() {
    assertThrows(IllegalArgumentException.class, () -> new PostgresCommandAuditQuery(null));
  }

  private static AuditEntry entry(
      String commandId, String commandType, String userId, Instant occurredAt) {
    return new AuditEntry(
        CommandId.of(commandId),
        commandType,
        TestStreams.TYPE,
        AggregateId.of("agg-" + commandId),
        UserId.of(userId),
        occurredAt,
        AuditOutcome.SUCCESS,
        null,
        1,
        CorrelationId.of("corr-" + commandId));
  }
}
