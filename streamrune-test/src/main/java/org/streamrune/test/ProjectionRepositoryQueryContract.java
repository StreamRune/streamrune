package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.ProjectionName;

/**
 * What the paged reads and the index declaration of a {@link ProjectionRepository} promise,
 * whatever stores the rows: {@code findAll(name, type, pageRequest)} and {@code findAllBy(name,
 * type, filter, pageRequest)} return the rows in the order of the request's sort, compared as JSON
 * text in code-point order, with the id breaking every tie and a missing field after every value
 * ascending; the filter matches the field's text exactly, a decimal number's text being the one
 * PostgreSQL prints; the total counts the rows the read selects; a page beyond the last, also past
 * an {@code int} offset, is empty and carries the total; a field outside {@code [A-Za-z0-9_]+}, a
 * repository column in a filter and an {@code Encrypted} record component are refused; declaring an
 * index twice is accepted, and a transaction-scoped repository refuses a declaration. The plain
 * reads and writes (save, find, delete, versioned save) are covered by each repository's own tests
 * and by {@link AtomicBatchProcessorContract}. Run by {@code
 * InMemoryProjectionRepositoryQueryContractTest} (streamrune-test) and {@code
 * JdbcProjectionRepositoryQueryContractTest} (streamrune-postgres), so a test on the in-memory
 * repository sees what production returns.
 */
public abstract class ProjectionRepositoryQueryContract {

  private static final AtomicInteger NAMES = new AtomicInteger();

  private static final Sort NEWEST_FIRST = Sort.by("createdAt", SortDirection.DESC);

  /** For a subclass that supplies the repository under test through the abstract hooks. */
  protected ProjectionRepositoryQueryContract() {}

  /**
   * The repository under test.
   *
   * @return the repository under test
   */
  protected abstract ProjectionRepository repository();

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

  /**
   * A read model of number, duration and boolean fields, whose text the stores must agree on.
   *
   * @param id the row id
   * @param price a decimal with a scale
   * @param measure a double
   * @param big a float
   * @param delay a duration (written as decimal seconds)
   * @param active a boolean
   */
  public record NumberRow(
      String id, BigDecimal price, Double measure, Float big, Duration delay, Boolean active) {
    /**
     * Jackson creator.
     *
     * @param id the row id
     * @param price a decimal with a scale
     * @param measure a double
     * @param big a float
     * @param delay a duration
     * @param active a boolean
     */
    @JsonCreator
    public NumberRow(
        @JsonProperty("id") String id,
        @JsonProperty("price") BigDecimal price,
        @JsonProperty("measure") Double measure,
        @JsonProperty("big") Float big,
        @JsonProperty("delay") Duration delay,
        @JsonProperty("active") Boolean active) {
      this.id = id;
      this.price = price;
      this.measure = measure;
      this.big = big;
      this.delay = delay;
      this.active = active;
    }
  }

  /**
   * A read model with an {@code Encrypted} component, which no read may filter or sort on.
   *
   * @param id the row id
   * @param email the encrypted component
   * @param name a plain component
   */
  public record SecretRow(String id, @Encrypted(subjectId = "id") String email, String name) {
    /**
     * Jackson creator.
     *
     * @param id the row id
     * @param email the encrypted component
     * @param name a plain component
     */
    @JsonCreator
    public SecretRow(
        @JsonProperty("id") String id,
        @JsonProperty("email") String email,
        @JsonProperty("name") String name) {
      this.id = id;
      this.email = email;
      this.name = name;
    }
  }

  /**
   * A record with an {@code Encrypted} component, embedded in {@link OuterRow}.
   *
   * @param customerId a plain component
   * @param email the encrypted component
   */
  public record Contact(String customerId, @Encrypted(subjectId = "customerId") String email) {}

  /**
   * A read model that embeds {@link Contact} twice with {@code JsonUnwrapped}: its {@code email}
   * and {@code billing_email} fields are stored at the top level, as ciphertext.
   *
   * @param id the row id
   * @param contact embedded without a prefix
   * @param billing embedded with the prefix {@code billing_}
   * @param name a plain component
   */
  public record OuterRow(
      String id,
      @JsonUnwrapped Contact contact,
      @JsonUnwrapped(prefix = "billing_") Contact billing,
      String name) {}

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

  /**
   * Text sorts by code point: an upper-case letter before every lower-case one, and a character
   * beyond U+FFFF after U+FF5E (UTF-16 order would put it first).
   */
  @Test
  public void aFieldSortsByCodePointNotByLinguisticCollation() {
    var name = freshName();
    save(name, "1", "alice", null);
    save(name, "2", "Bob", null);
    save(name, "3", "bob", null);
    save(name, "4", "Alice", null);
    save(name, "5", "é", null);
    save(name, "6", "😀", null);
    save(name, "7", "～", null);

    assertEquals(
        List.of("4", "2", "1", "3", "5", "7", "6"),
        ids(all(name, PageRequest.of(0, 10, Sort.by("customer")))));
  }

  /** An unsorted read orders by id, in code-point order, beyond U+FFFF too. */
  @Test
  public void anUnsortedReadOrdersByIdInCodePointOrder() {
    var name = freshName();
    for (String id : List.of("order-a1", "order-B1", "order-b0", "order-😀", "order-～")) {
      save(name, id, "x", null);
    }
    var expected = List.of("order-B1", "order-a1", "order-b0", "order-～", "order-😀");

    assertEquals(expected, ids(all(name, PageRequest.of(0, 10))));
    assertEquals(expected, ids(by(name, "x", PageRequest.of(0, 10))));
  }

  /**
   * A missing field and a JSON-{@code null} one sort after every value ascending and before every
   * value descending, and match no filter.
   */
  @Test
  @SuppressWarnings("rawtypes")
  public void aMissingFieldSortsLastAscendingAndFirstDescending() {
    var name = freshName();
    repository().save(name, "m1", row("m1", "alice", null, false)); // no createdAt key at all
    repository().save(name, "n1", row("n1", "alice", null, true)); // "createdAt": null
    repository().save(name, "v1", row("v1", "alice", "2026-01-01T00:00:01Z", true));
    repository().save(name, "v2", row("v2", "alice", "2026-01-01T00:00:02Z", true));
    var customer = FieldEquals.of("customer", "alice");

    Page<Map> ascending =
        repository()
            .findAllBy(name, Map.class, customer, PageRequest.of(0, 10, Sort.by("createdAt")));
    Page<Map> descending =
        repository().findAllBy(name, Map.class, customer, PageRequest.of(0, 10, NEWEST_FIRST));

    assertEquals(List.of("v1", "v2", "m1", "n1"), mapIds(ascending));
    assertEquals(List.of("m1", "n1", "v2", "v1"), mapIds(descending));
    assertTrue(
        repository()
            .findAllBy(name, Map.class, FieldEquals.of("createdAt", "null"), PageRequest.of(0, 10))
            .isEmpty(),
        "a missing or null field matches no value, not even the text null");
  }

  private static Map<String, Object> row(
      String id, String customer, String createdAt, boolean writeCreatedAt) {
    var row = new LinkedHashMap<String, Object>();
    row.put("id", id);
    row.put("customer", customer);
    if (writeCreatedAt) {
      row.put("createdAt", createdAt);
    }
    return row;
  }

  @SuppressWarnings("rawtypes")
  private static List<Object> mapIds(Page<Map> page) {
    return page.content().stream().map(row -> row.get("id")).toList();
  }

  private void saveMap(ProjectionName name, String id, String field, Object value, boolean write) {
    var row = new LinkedHashMap<String, Object>();
    row.put("id", id);
    if (write) {
      row.put(field, value);
    }
    repository().save(name, id, row);
  }

  @SuppressWarnings("rawtypes")
  private List<Object> mapFilter(ProjectionName name, String field, String value) {
    return mapIds(
        repository()
            .findAllBy(name, Map.class, FieldEquals.of(field, value), PageRequest.of(0, 10)));
  }

  @SuppressWarnings("rawtypes")
  private List<Object> mapSort(ProjectionName name, String field, SortDirection direction) {
    return mapIds(
        repository().findAll(name, Map.class, PageRequest.of(0, 10, Sort.by(field, direction))));
  }

  /** A JSON string and a JSON number with the same text both match the text filter. */
  @Test
  public void numericStringFieldMatchesTextFilter() {
    var name = freshName();
    saveMap(name, "s", "rank", "5", true);
    saveMap(name, "n", "rank", 5, true);
    saveMap(name, "o", "rank", 6, true);

    assertEquals(List.of("n", "s"), mapFilter(name, "rank", "5"));
  }

  /** A decimal keeps the spelling it was written with: 19.90 is not 19.9. */
  @Test
  public void trailingZeroDecimalKeepsItsSpelling() {
    var name = freshName();
    saveMap(name, "p", "price", new BigDecimal("19.90"), true);

    assertEquals(List.of("p"), mapFilter(name, "price", "19.90"));
    assertEquals(List.of(), mapFilter(name, "price", "19.9"));
  }

  /** A boolean compares as the text true or false. */
  @Test
  public void booleansCompareAsTrueFalseText() {
    var name = freshName();
    saveMap(name, "t", "active", true, true);
    saveMap(name, "f", "active", false, true);

    assertEquals(List.of("t"), mapFilter(name, "active", "true"));
    assertEquals(List.of("f"), mapFilter(name, "active", "false"));
    assertEquals(List.of(), mapFilter(name, "active", "TRUE"));
  }

  /** A missing or null field matches no value, and sorts after every value ascending. */
  @Test
  public void missingAndNullFieldsNeverMatchAValue() {
    var name = freshName();
    saveMap(name, "a-missing", "tag", null, false);
    saveMap(name, "b-null", "tag", null, true);
    saveMap(name, "c-value", "tag", "x", true);
    saveMap(name, "d-empty", "tag", "", true);

    assertEquals(List.of(), mapFilter(name, "tag", "null"));
    assertEquals(List.of("d-empty"), mapFilter(name, "tag", ""));
    assertEquals(
        List.of("d-empty", "c-value", "a-missing", "b-null"),
        mapSort(name, "tag", SortDirection.ASC));
  }

  /** Numbers and strings in one field sort as their text: 10, then 9, then a. */
  @Test
  public void mixedTypesSortAsText() {
    var name = freshName();
    saveMap(name, "s", "v", "a", true);
    saveMap(name, "n9", "v", 9, true);
    saveMap(name, "n10", "v", 10, true);

    assertEquals(List.of("n10", "n9", "s"), mapSort(name, "v", SortDirection.ASC));
    assertEquals(List.of("s", "n9", "n10"), mapSort(name, "v", SortDirection.DESC));
  }

  /** Ids and values outside ASCII compare byte-wise, so by code point, not linguistically. */
  @Test
  public void nonAsciiIdsAndValuesSortBytewise() {
    var name = freshName();
    for (String id : List.of("é", "z", "😀", "Z", "～")) {
      saveMap(name, id, "v", id, true);
    }
    var expected = List.<Object>of("Z", "z", "é", "～", "😀");

    assertEquals(expected, mapSort(name, "v", SortDirection.ASC));
    assertEquals(expected, mapIds(repository().findAll(name, Map.class, PageRequest.of(0, 10))));
    assertEquals(List.of("é"), mapFilter(name, "v", "é"));
  }

  /** A whole number filters by its text and sorts as text. */
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

  /**
   * A decimal, a double, a float and a duration compare as PostgreSQL prints the {@code numeric}
   * the JSON number is stored as: no exponent, the scale as written; a boolean as its word.
   */
  @Test
  public void decimalsDoublesDurationsAndBooleansCompareAsPostgresPrintsThem() {
    var name = freshName();
    repository()
        .save(
            name,
            "n1",
            new NumberRow(
                "n1", new BigDecimal("19.90"), 1.2345678E7, 1.0E10f, Duration.ofSeconds(90), true));
    repository()
        .save(
            name,
            "n2",
            new NumberRow("n2", new BigDecimal("100"), 1.0E-4, 2.5f, Duration.ofSeconds(5), false));
    repository()
        .save(
            name,
            "n3",
            new NumberRow("n3", new BigDecimal("5.00"), -0.0, 0.25f, Duration.ofMillis(500), true));

    assertEquals(List.of("n1"), numberIds(name, "price", "19.90"));
    assertEquals(List.of(), numberIds(name, "price", "19.9"));
    assertEquals(List.of("n2"), numberIds(name, "price", "100"));
    assertEquals(List.of("n3"), numberIds(name, "price", "5.00"));
    assertEquals(List.of("n1"), numberIds(name, "measure", "12345678"));
    assertEquals(List.of(), numberIds(name, "measure", "1.2345678E7"));
    assertEquals(List.of("n2"), numberIds(name, "measure", "0.00010"));
    assertEquals(List.of("n3"), numberIds(name, "measure", "0.0"));
    assertEquals(List.of("n1"), numberIds(name, "big", "10000000000"));
    assertEquals(List.of("n1"), numberIds(name, "delay", "90.000000000"));
    assertEquals(List.of("n3"), numberIds(name, "delay", "0.500000000"));
    assertEquals(List.of("n1", "n3"), numberIds(name, "active", "true"));
    assertEquals(List.of("n2", "n1", "n3"), numberSort(name, "price"));
    assertEquals(List.of("n3", "n2", "n1"), numberSort(name, "measure"));
    assertEquals(List.of("n3", "n1", "n2"), numberSort(name, "big"));
  }

  private List<String> numberIds(ProjectionName name, String field, String value) {
    return repository()
        .findAllBy(name, NumberRow.class, FieldEquals.of(field, value), PageRequest.of(0, 10))
        .content()
        .stream()
        .map(NumberRow::id)
        .toList();
  }

  private List<String> numberSort(ProjectionName name, String field) {
    return repository()
        .findAll(name, NumberRow.class, PageRequest.of(0, 10, Sort.by(field)))
        .content()
        .stream()
        .map(NumberRow::id)
        .toList();
  }

  /**
   * The repository columns sort by their own values: {@code version} as a number (10 after 2), and
   * {@code updated_at} by the last save.
   */
  @Test
  public void theVersionAndUpdatedAtColumnsSortByTheirValues() {
    var name = freshName();
    save(name, "b", "x", null);
    save(name, "a", "x", null);
    save(name, "c", "x", null);
    for (int i = 0; i < 10; i++) {
      save(name, "c", "x", "again " + i);
    }
    save(name, "a", "x", "again");
    save(name, "a", "x", "and again");

    assertEquals(
        List.of("b", "a", "c"),
        ids(all(name, PageRequest.of(0, 10, Sort.by("version")))),
        "versions 0, 2, 10 in number order");
    assertEquals(
        List.of("b", "c", "a"),
        ids(all(name, PageRequest.of(0, 10, Sort.by("updated_at")))),
        "the row saved last sorts last");
  }

  /** Inside a transaction its rows share one {@code updated_at}, so the id orders them. */
  @Test
  public void theRowsOfOneTransactionTieOnUpdatedAt() {
    Assumptions.assumeTrue(
        repository() instanceof AtomicBatchProcessor, "the repository has no transactions");
    var processor = (AtomicBatchProcessor) repository();
    var name = freshName();
    var inside = new AtomicReference<Page<ContractRow>>();

    processor.executeReplay(
        name,
        List.of(),
        tx -> {
          for (String id : List.of("z", "y", "a")) {
            tx.save(name, id, new ContractRow(id, "x", null, null));
          }
          inside.set(
              tx.findAll(name, ContractRow.class, PageRequest.of(0, 10, Sort.by("updated_at"))));
        });

    assertEquals(List.of("a", "y", "z"), ids(inside.get()));
    assertEquals(
        List.of("a", "y", "z"), ids(all(name, PageRequest.of(0, 10, Sort.by("updated_at")))));
  }

  /**
   * A filter or sort on an {@code Encrypted} component is refused: its stored text is ciphertext.
   */
  @Test
  public void anEncryptedComponentCannotBeFilteredOrSorted() {
    var name = freshName();
    var byEmail = FieldEquals.of("email", "a@example.com");
    var request = PageRequest.of(0, 10);

    var filter =
        assertThrows(
            IllegalArgumentException.class,
            () -> repository().findAllBy(name, SecretRow.class, byEmail, request));
    assertTrue(filter.getMessage().contains("email"), filter.getMessage());
    assertThrows(
        IllegalArgumentException.class,
        () -> repository().findAll(name, SecretRow.class, PageRequest.of(0, 10, Sort.by("email"))));
    var byName = FieldEquals.of("name", "x");
    var sortedByEmail = PageRequest.of(0, 10, Sort.by("email", SortDirection.DESC));
    assertThrows(
        IllegalArgumentException.class,
        () -> repository().findAllBy(name, SecretRow.class, byName, sortedByEmail));
    assertDoesNotThrow(
        () -> repository().findAllBy(name, SecretRow.class, byName, request),
        "a plain component of the same type is fine");
  }

  /**
   * An {@code Encrypted} component of a record embedded with {@code JsonUnwrapped} is refused under
   * its top-level JSON name, prefix included.
   */
  @Test
  public void anEncryptedComponentOfAnUnwrappedRecordCannotBeFilteredOrSorted() {
    var name = freshName();
    var request = PageRequest.of(0, 10);
    for (String field : List.of("email", "billing_email")) {
      var filter = FieldEquals.of(field, "a@example.com");
      assertThrows(
          IllegalArgumentException.class,
          () -> repository().findAllBy(name, OuterRow.class, filter, request),
          "filter " + field);
      var sorted = PageRequest.of(0, 10, Sort.by(field));
      assertThrows(
          IllegalArgumentException.class,
          () -> repository().findAll(name, OuterRow.class, sorted),
          "sort " + field);
    }
    for (String field : List.of("customerId", "billing_customerId", "name")) {
      var filter = FieldEquals.of(field, "x");
      assertDoesNotThrow(
          () -> repository().findAllBy(name, OuterRow.class, filter, request), "filter " + field);
    }
  }

  /** A string holding U+0000 is refused at save, as PostgreSQL's jsonb refuses it. */
  @Test
  public void aStringHoldingUPlus0000IsRefusedAtSave() {
    var name = freshName();
    var row = new ContractRow("z1", "a\0b", null, null);

    assertThrows(EventStoreException.class, () -> repository().save(name, "z1", row));
    assertTrue(
        repository().findById(name, "z1", ContractRow.class).isEmpty(), "nothing was stored");
  }

  /** {@code declareIndex(null)} and a declaration inside a transaction are refused. */
  @Test
  public void aNullIndexAndAnIndexDeclaredInsideATransactionAreRefused() {
    assertThrows(IllegalArgumentException.class, () -> repository().declareIndex(null));
    Assumptions.assumeTrue(
        repository() instanceof AtomicBatchProcessor, "the repository has no transactions");
    var name = freshName();
    var index = ProjectionIndex.forSort(name, NEWEST_FIRST);
    var processor = (AtomicBatchProcessor) repository();
    assertThrows(
        IllegalStateException.class,
        () -> processor.executeReplay(name, List.of(), tx -> tx.declareIndex(index)));
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
          () -> repository().declareIndex(ProjectionIndex.forSort(name, Sort.by(field))),
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

    assertDoesNotThrow(() -> repository().declareIndex(byCustomer));
    save(name, "r1", "alice", "2026-01-01T00:00:01Z");
    save(name, "r2", "alice", "2026-01-01T00:00:02Z");
    assertDoesNotThrow(
        () -> {
          repository().declareIndex(byCustomer);
          repository().declareIndex(byCustomerNewest);
          repository().declareIndex(byCustomerNewest);
          repository().declareIndex(newest);
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
