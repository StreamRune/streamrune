package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * The behaviour every {@link ProjectionDeadLetterStore} must honour, taken from the interface's
 * Javadoc: a saved entry reads back unchanged, {@code read} and {@code readAll} order oldest first
 * ({@code failedAt}, then {@code fromOffset}) and honour their limit, {@code read} is scoped to one
 * projection while {@code readAll} is not, {@code discard} removes the entry named by projection
 * and starting offset, and {@code countPending} counts across projections. Run by {@code
 * InMemoryProjectionDeadLetterStoreContractTest} and {@code
 * PostgresProjectionDeadLetterStoreContractTest}.
 *
 * <p>Deliberately not covered, because the interface leaves it open: saving a second entry for a
 * (projection, fromOffset) that is already stored, and the rejection of null arguments or a
 * non-positive limit.
 */
public abstract class ProjectionDeadLetterStoreContract {

  private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
  private static final ProjectionName ORDERS = ProjectionName.of("orders");
  private static final ProjectionName STOCK = ProjectionName.of("stock");

  /** A fresh, empty store. */
  protected abstract ProjectionDeadLetterStore newStore();

  private ProjectionDeadLetterStore store;

  @BeforeEach
  void freshStore() {
    store = newStore();
  }

  private static ProjectionDeadLetterEntry entry(ProjectionName name, long from, Instant failedAt) {
    return new ProjectionDeadLetterEntry(
        name,
        GlobalOffset.of(from),
        GlobalOffset.of(from + 9),
        10,
        "java.lang.IllegalStateException",
        "boom",
        3,
        failedAt);
  }

  private static List<String> keys(List<ProjectionDeadLetterEntry> entries) {
    return entries.stream()
        .map(e -> e.projectionName().value() + "@" + e.fromOffset().value())
        .toList();
  }

  @Test
  void savedEntryReadsBackUnchanged() {
    ProjectionDeadLetterEntry saved = entry(ORDERS, 100, T0);
    store.save(saved);

    assertEquals(List.of(saved), store.read(ORDERS, 10));
    assertEquals(List.of(saved), store.readAll(10));
  }

  @Test
  void aNullErrorMessageReadsBackAsNull() {
    ProjectionDeadLetterEntry saved =
        new ProjectionDeadLetterEntry(
            ORDERS, GlobalOffset.of(5), GlobalOffset.of(5), 1, "java.lang.Error", null, 0, T0);
    store.save(saved);

    ProjectionDeadLetterEntry read = store.read(ORDERS, 10).getFirst();

    assertNull(read.errorMessage());
    assertEquals(saved, read);
  }

  @Test
  void readsOnAnEmptyStoreAreEmpty() {
    assertTrue(store.read(ORDERS, 10).isEmpty());
    assertTrue(store.readAll(10).isEmpty());
    assertEquals(0, store.countPending());
  }

  @Test
  void readIsScopedToOneProjectionWhileReadAllSpansAll() {
    store.save(entry(ORDERS, 1, T0));
    store.save(entry(STOCK, 2, T0.plusSeconds(1)));
    store.save(entry(ORDERS, 3, T0.plusSeconds(2)));

    assertEquals(List.of("orders@1", "orders@3"), keys(store.read(ORDERS, 10)));
    assertEquals(List.of("stock@2"), keys(store.read(STOCK, 10)));
    assertEquals(List.of("orders@1", "stock@2", "orders@3"), keys(store.readAll(10)));
  }

  @Test
  void readOfAProjectionWithNoEntriesIsEmptyEvenWhenOthersHaveSome() {
    store.save(entry(ORDERS, 1, T0));

    assertTrue(store.read(STOCK, 10).isEmpty());
  }

  @Test
  void readsOrderOldestFailureFirstRegardlessOfSaveOrder() {
    store.save(entry(ORDERS, 30, T0.plusSeconds(30)));
    store.save(entry(ORDERS, 10, T0.plusSeconds(10)));
    store.save(entry(ORDERS, 20, T0.plusSeconds(20)));

    assertEquals(List.of("orders@10", "orders@20", "orders@30"), keys(store.read(ORDERS, 10)));
    assertEquals(List.of("orders@10", "orders@20", "orders@30"), keys(store.readAll(10)));
  }

  @Test
  void entriesFailedAtTheSameInstantOrderByFromOffset() {
    store.save(entry(ORDERS, 300, T0));
    store.save(entry(ORDERS, 100, T0));
    store.save(entry(ORDERS, 200, T0));

    assertEquals(List.of("orders@100", "orders@200", "orders@300"), keys(store.read(ORDERS, 10)));
    assertEquals(List.of("orders@100", "orders@200", "orders@300"), keys(store.readAll(10)));
  }

  @Test
  void aLimitedReadKeepsTheOldestEntries() {
    store.save(entry(ORDERS, 1, T0));
    store.save(entry(STOCK, 2, T0.plusSeconds(1)));
    store.save(entry(ORDERS, 3, T0.plusSeconds(2)));

    assertEquals(List.of("orders@1"), keys(store.read(ORDERS, 1)));
    assertEquals(List.of("orders@1", "stock@2"), keys(store.readAll(2)));
  }

  @Test
  void discardRemovesOnlyTheNamedProjectionAndOffset() {
    store.save(entry(ORDERS, 1, T0));
    store.save(entry(ORDERS, 2, T0.plusSeconds(1)));
    store.save(entry(STOCK, 1, T0.plusSeconds(2)));

    store.discard(ORDERS, GlobalOffset.of(1));

    assertEquals(List.of("orders@2", "stock@1"), keys(store.readAll(10)));
    assertEquals(2, store.countPending());
  }

  @Test
  void discardOfAnEntryThatIsNotThereLeavesTheStoreAsItWas() {
    store.save(entry(ORDERS, 1, T0));

    store.discard(ORDERS, GlobalOffset.of(99));
    store.discard(STOCK, GlobalOffset.of(1));

    assertEquals(List.of("orders@1"), keys(store.readAll(10)));
  }

  @Test
  void countPendingCountsEntriesAcrossAllProjections() {
    store.save(entry(ORDERS, 1, T0));
    store.save(entry(ORDERS, 2, T0));
    store.save(entry(STOCK, 1, T0));

    assertEquals(3, store.countPending());
  }
}
