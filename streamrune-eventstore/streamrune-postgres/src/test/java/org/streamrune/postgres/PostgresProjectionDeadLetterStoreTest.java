package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Testcontainers-backed tests for {@link PostgresProjectionDeadLetterStore}. Exercises the full
 * save/read/readAll/discard surface against a real PostgreSQL instance with the event-store
 * baseline applied.
 */
@Testcontainers
class PostgresProjectionDeadLetterStoreTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_proj_dlq_test");

  static PGSimpleDataSource dataSource;
  PostgresProjectionDeadLetterStore store;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    // Apply the shipped event-store baseline (creates projection_dead_letters) via the factory
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
    store = new PostgresProjectionDeadLetterStore(dataSource);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM projection_dead_letters");
    }
  }

  private ProjectionDeadLetterEntry entry(String name, long from, long to) {
    return new ProjectionDeadLetterEntry(
        ProjectionName.of(name),
        GlobalOffset.of(from),
        GlobalOffset.of(to),
        (int) (to - from + 1),
        "java.lang.RuntimeException",
        "connection refused",
        3,
        Instant.now());
  }

  @Test
  void countPending_returns_total_across_all_projections() {
    // Fleet-wide COUNT(*) backing streamrune.projections.dead_letter_backlog.
    assertEquals(0L, store.countPending());
    store.save(entry("OrderView", 1, 10));
    store.save(entry("OrderView", 11, 20));
    store.save(entry("InventoryView", 1, 5));
    assertEquals(3L, store.countPending());

    store.discard(ProjectionName.of("OrderView"), GlobalOffset.of(1));
    assertEquals(2L, store.countPending());
  }

  @Test
  void save_and_read_roundtrip() {
    store.save(entry("OrderView", 1, 10));

    var results = store.read(ProjectionName.of("OrderView"), 10);
    assertEquals(1, results.size());
    var e = results.get(0);
    assertEquals(ProjectionName.of("OrderView"), e.projectionName());
    assertEquals(GlobalOffset.of(1), e.fromOffset());
    assertEquals(GlobalOffset.of(10), e.toOffset());
    assertEquals(10, e.batchSize());
    assertEquals("java.lang.RuntimeException", e.errorType());
    assertEquals("connection refused", e.errorMessage());
    assertEquals(3, e.attempts());
    assertNotNull(e.failedAt());
  }

  @Test
  void read_filters_by_projection_name() {
    store.save(entry("OrderView", 1, 10));
    store.save(entry("InventoryView", 5, 15));

    assertEquals(1, store.read(ProjectionName.of("OrderView"), 10).size());
    assertEquals(1, store.read(ProjectionName.of("InventoryView"), 10).size());
    assertTrue(store.read(ProjectionName.of("Unknown"), 10).isEmpty());
  }

  @Test
  void readAll_returns_across_projections() {
    store.save(entry("OrderView", 1, 10));
    store.save(entry("InventoryView", 5, 15));

    assertEquals(2, store.readAll(10).size());
  }

  @Test
  void readAll_respects_limit() {
    store.save(entry("A", 1, 2));
    store.save(entry("B", 3, 4));
    store.save(entry("C", 5, 6));

    assertEquals(2, store.readAll(2).size());
  }

  private ProjectionDeadLetterEntry entryAt(String name, long from, long to, Instant failedAt) {
    return new ProjectionDeadLetterEntry(
        ProjectionName.of(name),
        GlobalOffset.of(from),
        GlobalOffset.of(to),
        (int) (to - from + 1),
        "java.lang.RuntimeException",
        "connection refused",
        3,
        failedAt);
  }

  @Test
  void read_overLimitBacklog_returnsOldestFirst() {
    // Seed more entries than the read limit, with strictly increasing failed_at, so the oldest
    // range is only ever surfaced if the store reads ascending rather than descending.
    Instant base = Instant.parse("2026-01-01T00:00:00Z");
    for (int i = 0; i < 5; i++) {
      store.save(entryAt("OrderView", i + 1, i + 1, base.plusSeconds(i)));
    }

    // Read a page smaller than the backlog: must be the OLDEST 2, not the newest.
    var page1 = store.read(ProjectionName.of("OrderView"), 2);
    assertEquals(2, page1.size());
    assertEquals(GlobalOffset.of(1), page1.get(0).fromOffset());
    assertEquals(GlobalOffset.of(2), page1.get(1).fromOffset());

    // Repeated replay-and-discard passes drain toward the newest.
    store.discard(ProjectionName.of("OrderView"), page1.get(0).fromOffset());
    store.discard(ProjectionName.of("OrderView"), page1.get(1).fromOffset());
    var page2 = store.read(ProjectionName.of("OrderView"), 2);
    assertEquals(GlobalOffset.of(3), page2.get(0).fromOffset());
    assertEquals(GlobalOffset.of(4), page2.get(1).fromOffset());
  }

  @Test
  void readAll_overLimitBacklog_returnsOldestFirst() {
    Instant base = Instant.parse("2026-01-01T00:00:00Z");
    for (int i = 0; i < 5; i++) {
      store.save(entryAt("P" + i, i + 1, i + 1, base.plusSeconds(i)));
    }

    var page1 = store.readAll(2);
    assertEquals(2, page1.size());
    assertEquals(GlobalOffset.of(1), page1.get(0).fromOffset());
    assertEquals(GlobalOffset.of(2), page1.get(1).fromOffset());
  }

  @Test
  void read_tieBreak_onFromOffsetAscending() {
    Instant same = Instant.parse("2026-01-01T00:00:00Z");
    store.save(entryAt("OrderView", 5, 5, same));
    store.save(entryAt("OrderView", 1, 1, same));
    store.save(entryAt("OrderView", 3, 3, same));

    var results = store.read(ProjectionName.of("OrderView"), 10);
    assertEquals(3, results.size());
    assertEquals(GlobalOffset.of(1), results.get(0).fromOffset());
    assertEquals(GlobalOffset.of(3), results.get(1).fromOffset());
    assertEquals(GlobalOffset.of(5), results.get(2).fromOffset());
  }

  @Test
  void readAll_tieBreak_onFromOffsetAscending() {
    Instant same = Instant.parse("2026-01-01T00:00:00Z");
    store.save(entryAt("A", 5, 5, same));
    store.save(entryAt("B", 1, 1, same));
    store.save(entryAt("C", 3, 3, same));

    var results = store.readAll(10);
    assertEquals(3, results.size());
    assertEquals(GlobalOffset.of(1), results.get(0).fromOffset());
    assertEquals(GlobalOffset.of(3), results.get(1).fromOffset());
    assertEquals(GlobalOffset.of(5), results.get(2).fromOffset());
  }

  @Test
  void discard_removes_entry() {
    store.save(entry("OrderView", 1, 10));
    store.save(entry("OrderView", 11, 20));

    store.discard(ProjectionName.of("OrderView"), GlobalOffset.of(1));

    var results = store.read(ProjectionName.of("OrderView"), 10);
    assertEquals(1, results.size());
    assertEquals(GlobalOffset.of(11), results.get(0).fromOffset());
  }

  @Test
  void discard_is_idempotent() {
    store.save(entry("OrderView", 1, 10));
    store.discard(ProjectionName.of("OrderView"), GlobalOffset.of(1));
    store.discard(ProjectionName.of("OrderView"), GlobalOffset.of(1));
    assertTrue(store.read(ProjectionName.of("OrderView"), 10).isEmpty());
  }

  @Test
  void save_same_batch_twice_updates_instead_of_violating_pk() {
    store.save(entry("OrderView", 1, 10));

    // Same (projection_name, from_offset) — a replay of the batch failed again.
    var second =
        new ProjectionDeadLetterEntry(
            ProjectionName.of("OrderView"),
            GlobalOffset.of(1),
            GlobalOffset.of(10),
            10,
            "java.lang.IllegalStateException",
            "still broken",
            4,
            Instant.now());
    assertDoesNotThrow(() -> store.save(second));

    var results = store.read(ProjectionName.of("OrderView"), 10);
    assertEquals(1, results.size());
    var e = results.get(0);
    assertEquals("java.lang.IllegalStateException", e.errorType());
    assertEquals("still broken", e.errorMessage());
    assertEquals(4, e.attempts());
  }

  @Test
  void save_same_batch_twice_bumps_attempts_even_when_caller_repeats_count() {
    store.save(entry("OrderView", 1, 10)); // attempts = 3
    store.save(entry("OrderView", 1, 10)); // caller repeats attempts = 3

    var e = store.read(ProjectionName.of("OrderView"), 10).get(0);
    assertEquals(4, e.attempts(), "attempts must stay monotonic on repeat failure");
  }

  @Test
  void baseline_migration_applies_cleanly() {
    // If we got here, Flyway applied the baseline successfully in @BeforeAll
    assertNotNull(store);
  }

  @Test
  void constructor_rejects_null_dataSource() {
    assertThrows(IllegalArgumentException.class, () -> new PostgresProjectionDeadLetterStore(null));
  }

  @Test
  void save_rejects_null_entry() {
    assertThrows(IllegalArgumentException.class, () -> store.save(null));
  }

  @Test
  void discard_rejects_null_projectionName() {
    assertThrows(
        IllegalArgumentException.class,
        () -> store.discard((ProjectionName) null, GlobalOffset.of(1)));
  }

  @Test
  void discard_rejects_null_fromOffset() {
    assertThrows(
        IllegalArgumentException.class, () -> store.discard(ProjectionName.of("OrderView"), null));
  }
}
