package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.ProjectionName;

/**
 * What the paged reads of a {@link ProjectionRepository} promise, whatever stores the rows: {@code
 * findAll(name, type, pageRequest)} and {@code findAllBy(name, type, filter, pageRequest)} return
 * the rows in the order of the request's sort, compared as JSON text in code-point order, with the
 * id breaking every tie and a missing field after every value ascending; the filter matches the
 * field's text exactly; the total counts the rows the read selects; a page beyond the last, also
 * past an {@code int} offset, is empty and carries the total; a field outside {@code [A-Za-z0-9_]+}
 * or naming a repository column in a filter is refused; and declaring an index twice is accepted.
 * Run by {@code InMemoryProjectionRepositoryQueryContractTest} (streamrune-test) and {@code
 * JdbcProjectionRepositoryQueryContractTest} (streamrune-postgres), so a test on the in-memory
 * repository sees the order production returns.
 */
public abstract class ProjectionRepositoryContract {

  private static final AtomicInteger NAMES = new AtomicInteger();

  private static final Sort NEWEST_FIRST = Sort.by("createdAt", SortDirection.DESC);

  /** For a subclass that supplies the repository under test through the abstract hooks. */
  protected ProjectionRepositoryContract() {}

  /**
   * The repository under test.
   *
   * @return the repository under test
   */
  protected abstract ProjectionRepository repository();

  /**
   * Declares {@code index} on the repository under test, as the application's wiring does.
   *
   * @param index the index to declare
   */
  protected abstract void declareIndex(ProjectionIndex index);

  /**
   * A projection name no other test in this JVM has used.
   *
   * @return a fresh projection name
   */
  protected ProjectionName freshName() {
    return ProjectionName.of("query_contract_" + NAMES.incrementAndGet());
  }

  /**
   * The read model the contract writes.
   *
   * @param id the row id, also stored in the read model
   * @param customer the field the contract filters on
   * @param createdAt the field the contract sorts on; may be {@code null}
   * @param rank a number field; may be {@code null}
   */
  public record ContractRow(String id, String customer, String createdAt, Integer rank) {
    /**
     * Jackson creator, so a serializing store can read the row back.
     *
     * @param id the row id
     * @param customer the filtered field
     * @param createdAt the sorted field
     * @param rank a number field
     */
    @JsonCreator
    public ContractRow(
        @JsonProperty("id") String id,
        @JsonProperty("customer") String customer,
        @JsonProperty("createdAt") String createdAt,
        @JsonProperty("rank") Integer rank) {
      this.id = id;
      this.customer = customer;
      this.createdAt = createdAt;
      this.rank = rank;
    }
  }

  private ContractRow save(ProjectionName name, String id, String customer, String createdAt) {
    var row = new ContractRow(id, customer, createdAt, null);
    repository().save(name, id, row);
    return row;
  }

  private Page<ContractRow> by(ProjectionName name, String customer, PageRequest request) {
    return repository()
        .findAllBy(name, ContractRow.class, FieldEquals.of("customer", customer), request);
  }

  private Page<ContractRow> all(ProjectionName name, PageRequest request) {
    return repository().findAll(name, ContractRow.class, request);
  }

  private static List<String> ids(Page<ContractRow> page) {
    return page.content().stream().map(ContractRow::id).toList();
  }

  /** The filter selects the rows whose field holds the value, and the total counts only them. */
  @Test
  public void aFilteredPageHoldsTheMatchingRowsAndCountsOnlyThem() {
    var name = freshName();
    save(name, "r1", "alice", "2026-01-01T00:00:01Z");
    save(name, "r2", "bob", "2026-01-01T00:00:02Z");
    save(name, "r3", "alice", "2026-01-01T00:00:03Z");
    save(name, "r4", "alicia", "2026-01-01T00:00:04Z");
    save(name, "r5", "Alice", "2026-01-01T00:00:05Z");

    var page = by(name, "alice", PageRequest.of(0, 10));

    assertEquals(List.of("r1", "r3"), ids(page), "exact text match, unsorted = id order");
    assertEquals(2L, page.totalElements());
    assertFalse(page.hasNext());
  }

  /** A filter no row matches is an empty first page with a zero total. */
  @Test
  public void aFilterThatMatchesNothingIsAnEmptyPage() {
    var name = freshName();
    save(name, "r1", "alice", null);

    var page = by(name, "nobody", PageRequest.of(0, 10));

    assertTrue(page.isEmpty());
    assertEquals(0L, page.totalElements());
    assertFalse(page.hasNext());
  }

  /** A filtered read of a projection with no rows at all is empty too. */
  @Test
  public void aFilteredReadOfAnUntouchedProjectionIsEmpty() {
    var page = by(freshName(), "alice", PageRequest.of(0, 10));

    assertTrue(page.isEmpty());
    assertEquals(0L, page.totalElements());
  }

  /** Pages of the matches follow each other without a gap or a repeat. */
  @Test
  public void aFilteredReadPagesThroughTheMatchesWithoutGapsOrRepeats() {
    var name = freshName();
    for (int i = 1; i <= 5; i++) {
      save(name, "a" + i, "alice", null);
      save(name, "b" + i, "bob", null);
    }

    var first = by(name, "alice", PageRequest.of(0, 2));
    var second = by(name, "alice", PageRequest.of(1, 2));
    var last = by(name, "alice", PageRequest.of(2, 2));
    var beyond = by(name, "alice", PageRequest.of(3, 2));

    assertEquals(List.of("a1", "a2"), ids(first));
    assertTrue(first.hasNext());
    assertEquals(List.of("a3", "a4"), ids(second));
    assertTrue(second.hasNext());
    assertEquals(List.of("a5"), ids(last));
    assertFalse(last.hasNext());
    assertTrue(beyond.isEmpty());
    assertEquals(5L, beyond.totalElements(), "a page beyond the last still carries the total");
    assertFalse(beyond.hasNext());
  }

  /** The filtered read sorts by the request's sort, and the id breaks ties. */
  @Test
  public void aFilteredReadSortsAndBreaksTiesById() {
    var name = freshName();
    save(name, "o3", "alice", "2026-01-01T00:00:02Z");
    save(name, "o1", "alice", "2026-01-01T00:00:01Z");
    save(name, "o4", "alice", "2026-01-01T00:00:02Z");
    save(name, "o2", "alice", "2026-01-01T00:00:02Z");
    save(name, "o5", "bob", "2026-01-01T00:00:09Z");

    var page = by(name, "alice", PageRequest.of(0, 10, NEWEST_FIRST));

    assertEquals(List.of("o2", "o3", "o4", "o1"), ids(page));
    assertEquals(4L, page.totalElements());
  }

  /** The id breaks ties of an unfiltered sorted read too, whatever order the rows were saved in. */
  @Test
  public void aSortedReadBreaksTiesById() {
    var name = freshName();
    save(name, "c", "same", null);
    save(name, "a", "same", null);
    save(name, "b", "same", null);
    save(name, "0", "first", null);

    assertEquals(
        List.of("0", "a", "b", "c"),
        ids(all(name, PageRequest.of(0, 10, Sort.by("customer")))),
        "ascending");
    assertEquals(
        List.of("a", "b", "c", "0"),
        ids(all(name, PageRequest.of(0, 10, Sort.by("customer", SortDirection.DESC)))),
        "the id tiebreak stays ascending under a descending sort");
  }

  /** Text sorts by code point: an upper-case letter before every lower-case one. */
  @Test
  public void aFieldSortsByCodePointNotByLinguisticCollation() {
    var name = freshName();
    save(name, "1", "alice", null);
    save(name, "2", "Bob", null);
    save(name, "3", "bob", null);
    save(name, "4", "Alice", null);

    assertEquals(
        List.of("4", "2", "1", "3"), ids(all(name, PageRequest.of(0, 10, Sort.by("customer")))));
  }

  /** An unsorted read orders by id, in code-point order. */
  @Test
  public void anUnsortedReadOrdersByIdInCodePointOrder() {
    var name = freshName();
    save(name, "order-a1", "x", null);
    save(name, "order-B1", "x", null);
    save(name, "order-b0", "x", null);

    assertEquals(
        List.of("order-B1", "order-a1", "order-b0"), ids(all(name, PageRequest.of(0, 10))));
    assertEquals(
        List.of("order-B1", "order-a1", "order-b0"), ids(by(name, "x", PageRequest.of(0, 10))));
  }

  /** A missing field sorts after every value ascending and before every value descending. */
  @Test
  public void aMissingFieldSortsLastAscendingAndFirstDescending() {
    var name = freshName();
    save(name, "n1", "alice", null);
    save(name, "v1", "alice", "2026-01-01T00:00:01Z");
    save(name, "v2", "alice", "2026-01-01T00:00:02Z");

    assertEquals(
        List.of("v1", "v2", "n1"),
        ids(by(name, "alice", PageRequest.of(0, 10, Sort.by("createdAt")))));
    assertEquals(
        List.of("n1", "v2", "v1"), ids(by(name, "alice", PageRequest.of(0, 10, NEWEST_FIRST))));
  }

  /** A number field filters by its text and sorts as text. */
  @Test
  public void aNumberFieldFiltersAndSortsAsText() {
    var name = freshName();
    repository().save(name, "r9", new ContractRow("r9", "x", null, 9));
    repository().save(name, "r10", new ContractRow("r10", "x", null, 10));
    repository().save(name, "r5", new ContractRow("r5", "x", null, 5));

    var five = FieldEquals.of("rank", "5");
    assertEquals(
        List.of("r5"),
        ids(repository().findAllBy(name, ContractRow.class, five, PageRequest.of(0, 10))));
    var padded = FieldEquals.of("rank", "05");
    assertTrue(
        repository().findAllBy(name, ContractRow.class, padded, PageRequest.of(0, 10)).isEmpty());
    assertEquals(
        List.of("r10", "r5", "r9"), ids(all(name, PageRequest.of(0, 10, Sort.by("rank")))));
  }

  /** The repository columns sort by their own values: {@code version} as a number. */
  @Test
  public void theVersionAndUpdatedAtColumnsSortByTheirValues() {
    var name = freshName();
    save(name, "b", "x", null);
    save(name, "a", "x", null);
    save(name, "c", "x", null);
    save(name, "c", "x", "again");
    save(name, "c", "x", "and again");
    save(name, "a", "x", "again");

    assertEquals(
        List.of("c", "a", "b"),
        ids(all(name, PageRequest.of(0, 10, Sort.by("version", SortDirection.DESC)))));
    assertEquals(
        List.of("b", "c", "a"),
        ids(all(name, PageRequest.of(0, 10, Sort.by("updated_at")))),
        "the row written last sorts last");
  }

  /** A page past an {@code int} offset is an empty page with the total, not an error. */
  @Test
  public void aPagePastAnIntOffsetIsEmptyAndCarriesTheTotal() {
    var name = freshName();
    save(name, "r1", "alice", null);
    save(name, "r2", "alice", null);
    var far = PageRequest.of(Integer.MAX_VALUE, 1000);

    var unfiltered = all(name, far);
    var filtered = by(name, "alice", far);

    for (var page : List.of(unfiltered, filtered)) {
      assertTrue(page.isEmpty());
      assertEquals(2L, page.totalElements());
      assertFalse(page.hasNext());
    }
  }

  /** A field outside the rule, a nested path, or a repository column in a filter is refused. */
  @Test
  public void aFieldOutsideTheRuleIsRefused() {
    var name = freshName();
    save(name, "r1", "alice", null);
    for (String field : List.of("customer.name", "customer->>'x'", "a-b", "x'y", "a b")) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              repository()
                  .findAllBy(
                      name, ContractRow.class, FieldEquals.of(field, "v"), PageRequest.of(0, 1)),
          "filter field " + field);
      assertThrows(
          IllegalArgumentException.class,
          () -> all(name, PageRequest.of(0, 1, Sort.by(field))),
          "sort field " + field);
      assertThrows(
          IllegalArgumentException.class,
          () -> declareIndex(ProjectionIndex.forSort(name, Sort.by(field))),
          "index key " + field);
    }
    for (String column : List.of("id", "version", "updated_at")) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              repository()
                  .findAllBy(
                      name, ContractRow.class, FieldEquals.of(column, "1"), PageRequest.of(0, 1)),
          "filter on the column " + column);
    }
  }

  /** Every argument of the filtered read is required. */
  @Test
  public void aFilteredReadRefusesNullArguments() {
    var name = freshName();
    var filter = FieldEquals.of("customer", "alice");
    var request = PageRequest.of(0, 1);
    ProjectionRepository repository = repository();
    assertThrows(
        IllegalArgumentException.class,
        () -> repository.findAllBy(null, ContractRow.class, filter, request));
    assertThrows(
        IllegalArgumentException.class, () -> repository.findAllBy(name, null, filter, request));
    assertThrows(
        IllegalArgumentException.class,
        () -> repository.findAllBy(name, ContractRow.class, null, request));
    assertThrows(
        IllegalArgumentException.class,
        () -> repository.findAllBy(name, ContractRow.class, filter, null));
  }

  /** Declaring the same indexes twice, before and after rows exist, is accepted. */
  @Test
  public void declaringAnIndexTwiceIsAccepted() {
    var name = freshName();
    var byCustomer = ProjectionIndex.forFilter(name, "customer", Sort.unsorted());
    var byCustomerNewest = ProjectionIndex.forFilter(name, "customer", NEWEST_FIRST);
    var newest = ProjectionIndex.forSort(name, NEWEST_FIRST);

    assertDoesNotThrow(() -> declareIndex(byCustomer));
    save(name, "r1", "alice", "2026-01-01T00:00:01Z");
    save(name, "r2", "alice", "2026-01-01T00:00:02Z");
    assertDoesNotThrow(
        () -> {
          declareIndex(byCustomer);
          declareIndex(byCustomerNewest);
          declareIndex(byCustomerNewest);
          declareIndex(newest);
        });

    assertEquals(List.of("r2", "r1"), ids(by(name, "alice", PageRequest.of(0, 10, NEWEST_FIRST))));
    assertEquals(List.of("r2", "r1"), ids(all(name, PageRequest.of(0, 10, NEWEST_FIRST))));
  }

  /** Inside a transaction-scoped view, the filtered read sees the transaction's own writes. */
  @Test
  public void aFilteredReadInsideATransactionSeesItsOwnWrites() {
    Assumptions.assumeTrue(
        repository() instanceof AtomicBatchProcessor, "the repository has no transactions");
    var processor = (AtomicBatchProcessor) repository();
    var name = freshName();
    save(name, "r1", "alice", "2026-01-01T00:00:01Z");
    save(name, "r2", "alice", "2026-01-01T00:00:02Z");
    var seen = new AtomicReference<Page<ContractRow>>();

    processor.executeReplay(
        name,
        List.of(),
        tx -> {
          tx.save(name, "r3", new ContractRow("r3", "alice", "2026-01-01T00:00:03Z", null));
          tx.delete(name, "r1");
          tx.save(name, "r2", new ContractRow("r2", "bob", "2026-01-01T00:00:02Z", null));
          seen.set(
              tx.findAllBy(
                  name,
                  ContractRow.class,
                  FieldEquals.of("customer", "alice"),
                  PageRequest.of(0, 10, NEWEST_FIRST)));
        });

    assertEquals(List.of("r3"), ids(seen.get()));
    assertEquals(1L, seen.get().totalElements());
  }
}
