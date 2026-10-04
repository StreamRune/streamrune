package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

class InMemoryProjectionDeadLetterStoreTest {

  InMemoryProjectionDeadLetterStore store;

  @BeforeEach
  void setUp() {
    store = new InMemoryProjectionDeadLetterStore();
  }

  private ProjectionDeadLetterEntry entry(String name, long from, long to) {
    return new ProjectionDeadLetterEntry(
        ProjectionName.of(name),
        GlobalOffset.of(from),
        GlobalOffset.of(to),
        (int) (to - from + 1),
        "java.lang.RuntimeException",
        "something went wrong",
        1,
        Instant.now());
  }

  @Test
  void save_and_read_by_projection_name() {
    store.save(entry("OrderProjection", 1, 5));
    var results = store.read(ProjectionName.of("OrderProjection"), 10);
    assertEquals(1, results.size());
    assertEquals(ProjectionName.of("OrderProjection"), results.get(0).projectionName());
    assertEquals(GlobalOffset.of(1), results.get(0).fromOffset());
    assertEquals(GlobalOffset.of(5), results.get(0).toOffset());
  }

  @Test
  void read_respects_limit() {
    store.save(entry("OrderProjection", 1, 1));
    store.save(entry("OrderProjection", 2, 2));
    store.save(entry("OrderProjection", 3, 3));
    var results = store.read(ProjectionName.of("OrderProjection"), 2);
    assertEquals(2, results.size());
  }

  @Test
  void read_returns_oldest_first() throws InterruptedException {
    store.save(entry("OrderProjection", 1, 1));
    Thread.sleep(5);
    store.save(entry("OrderProjection", 2, 2));
    var results = store.read(ProjectionName.of("OrderProjection"), 10);
    assertEquals(2, results.size());
    assertTrue(
        results.get(0).failedAt().isBefore(results.get(1).failedAt())
            || results.get(0).failedAt().equals(results.get(1).failedAt()));
    assertEquals(GlobalOffset.of(1), results.get(0).fromOffset());
    assertEquals(GlobalOffset.of(2), results.get(1).fromOffset());
  }

  private ProjectionDeadLetterEntry entryAt(String name, long from, long to, Instant failedAt) {
    return new ProjectionDeadLetterEntry(
        ProjectionName.of(name),
        GlobalOffset.of(from),
        GlobalOffset.of(to),
        (int) (to - from + 1),
        "java.lang.RuntimeException",
        "something went wrong",
        1,
        failedAt);
  }

  @Test
  void read_overLimitBacklog_returnsOldestFirst_andDrainsTowardNewest() {
    Instant base = Instant.parse("2026-01-01T00:00:00Z");
    for (int i = 0; i < 5; i++) {
      store.save(entryAt("OrderProjection", i + 1, i + 1, base.plusSeconds(i)));
    }

    var page1 = store.read(ProjectionName.of("OrderProjection"), 2);
    assertEquals(2, page1.size());
    assertEquals(GlobalOffset.of(1), page1.get(0).fromOffset());
    assertEquals(GlobalOffset.of(2), page1.get(1).fromOffset());

    store.discard(ProjectionName.of("OrderProjection"), page1.get(0).fromOffset());
    store.discard(ProjectionName.of("OrderProjection"), page1.get(1).fromOffset());
    var page2 = store.read(ProjectionName.of("OrderProjection"), 2);
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
    store.save(entryAt("OrderProjection", 5, 5, same));
    store.save(entryAt("OrderProjection", 1, 1, same));
    store.save(entryAt("OrderProjection", 3, 3, same));

    var results = store.read(ProjectionName.of("OrderProjection"), 10);
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
  void readAll_returns_across_projections() {
    store.save(entry("OrderProjection", 1, 1));
    store.save(entry("InventoryProjection", 2, 2));
    var results = store.readAll(10);
    assertEquals(2, results.size());
  }

  @Test
  void readAll_respects_limit() {
    store.save(entry("OrderProjection", 1, 1));
    store.save(entry("OrderProjection", 2, 2));
    store.save(entry("InventoryProjection", 3, 3));
    var results = store.readAll(2);
    assertEquals(2, results.size());
  }

  @Test
  void discard_removes_entry() {
    store.save(entry("OrderProjection", 1, 5));
    store.discard(ProjectionName.of("OrderProjection"), GlobalOffset.of(1));
    assertTrue(store.read(ProjectionName.of("OrderProjection"), 10).isEmpty());
    assertTrue(store.all().isEmpty());
  }

  @Test
  void discard_is_idempotent() {
    store.save(entry("OrderProjection", 1, 5));
    store.discard(ProjectionName.of("OrderProjection"), GlobalOffset.of(1));
    assertDoesNotThrow(
        () -> store.discard(ProjectionName.of("OrderProjection"), GlobalOffset.of(1)));
    assertTrue(store.all().isEmpty());
  }

  @Test
  void read_returns_empty_for_unknown_projection() {
    var results = store.read(ProjectionName.of("NonExistentProjection"), 10);
    assertTrue(results.isEmpty());
  }

  @Test
  void countPending_returns_total_across_all_projections() {
    // Fleet-wide count backing the streamrune.projections.dead_letter_backlog gauge.
    assertEquals(0L, store.countPending());
    store.save(entry("OrderProjection", 1, 1));
    store.save(entry("OrderProjection", 2, 2));
    store.save(entry("InventoryProjection", 3, 3));
    assertEquals(3L, store.countPending());

    store.discard(ProjectionName.of("OrderProjection"), GlobalOffset.of(1));
    assertEquals(2L, store.countPending());
  }

  @Test
  void save_rejects_null_entry() {
    assertThrows(NullPointerException.class, () -> store.save(null));
  }

  @Test
  void discard_rejects_null_projection_name() {
    assertThrows(
        NullPointerException.class, () -> store.discard((ProjectionName) null, GlobalOffset.of(1)));
  }

  @Test
  void discard_rejects_null_offset() {
    assertThrows(
        NullPointerException.class,
        () -> store.discard(ProjectionName.of("OrderProjection"), null));
  }
}
