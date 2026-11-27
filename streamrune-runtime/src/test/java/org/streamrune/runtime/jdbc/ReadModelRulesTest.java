package org.streamrune.runtime.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.Versioned;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.types.ProjectionName;

class ReadModelRulesTest {

  private static final ProjectionName P = ProjectionName.of("p");

  private static final String FIELD_RULE =
      "A sort, filter or index field must be a top-level read-model field of ASCII letters,"
          + " digits and underscores (no nested path): ";

  // ── Read-model types ────────────────────────────────────────────────────────────────────────

  record Item(String name, int qty) {}

  record Person(String name, @Encrypted(subjectId = "name") String ssn) {}

  record Account(@Encrypted(subjectId = "name") String fieldName) {}

  record Address(String city, @Encrypted(subjectId = "name") String street) {}

  record Customer(String name, @JsonUnwrapped(prefix = "home_", suffix = "_x") Address home) {}

  /** The accessor is declared, so the {@code @JsonUnwrapped} is on the field only. */
  record DeclaredAccessor(@JsonUnwrapped Address address) {
    @Override
    public Address address() {
      return address;
    }
  }

  record Disabled(@JsonUnwrapped(enabled = false) Address address) {}

  record Node(@Encrypted(subjectId = "name") String secret, @JsonUnwrapped Node next) {}

  /** An encrypted component that has a repository column's name. */
  record ColumnNamed(String name, @Encrypted(subjectId = "name") String version) {}

  private static ObjectMapper mapper() {
    return new ObjectMapper();
  }

  private static String encryptedRefusal(String field, String type) {
    return "Field '"
        + field
        + "' of "
        + type
        + " is @Encrypted: its stored text is ciphertext, so it cannot be filtered or sorted"
        + " (keep a separate, non-sensitive field for the query)";
  }

  private static void assertRefused(ThrowingCallable call, String message) {
    assertThat(catchThrowable(call))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(message)
        .hasNoCause();
  }

  // ── Encrypted components ────────────────────────────────────────────────────────────────────

  @Test
  void refuseEncryptedKeys_filterAndSortMessages() {
    var rules = new ReadModelRules(mapper());

    assertRefused(
        () -> rules.refuseEncryptedKeys(Person.class, FieldEquals.of("ssn", "x"), Sort.unsorted()),
        encryptedRefusal("ssn", "Person"));
    assertRefused(
        () -> rules.refuseEncryptedKeys(Person.class, null, Sort.by("ssn", SortDirection.DESC)),
        encryptedRefusal("ssn", "Person"));
    // A later sort key is checked too.
    assertRefused(
        () ->
            rules.refuseEncryptedKeys(
                Person.class,
                FieldEquals.of("name", "x"),
                new Sort(
                    List.of(
                        new Sort.Order("name", SortDirection.ASC),
                        new Sort.Order("ssn", SortDirection.ASC)))),
        encryptedRefusal("ssn", "Person"));

    // A plain field passes, and a type without encrypted components passes anything.
    assertThatCode(
            () ->
                rules.refuseEncryptedKeys(
                    Person.class, FieldEquals.of("name", "x"), Sort.by("name")))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                rules.refuseEncryptedKeys(Item.class, FieldEquals.of("name", "x"), Sort.by("qty")))
        .doesNotThrowAnyException();
    // A type that is not a record is not checked.
    assertThatCode(
            () -> rules.refuseEncryptedKeys(Map.class, FieldEquals.of("ssn", "x"), Sort.by("ssn")))
        .doesNotThrowAnyException();
    // A sort on a repository column is never a read-model field, whatever a component is called.
    assertThatCode(() -> rules.refuseEncryptedKeys(ColumnNamed.class, null, Sort.by("version")))
        .doesNotThrowAnyException();
  }

  @Test
  void refuseEncryptedKeys_followsJsonUnwrapped() {
    var rules = new ReadModelRules(mapper());

    // The unwrapped component's JSON name carries the annotation's prefix and suffix.
    assertRefused(
        () ->
            rules.refuseEncryptedKeys(
                Customer.class, FieldEquals.of("home_street_x", "x"), Sort.unsorted()),
        encryptedRefusal("home_street_x", "Customer"));
    assertThatCode(
            () ->
                rules.refuseEncryptedKeys(
                    Customer.class, FieldEquals.of("street", "x"), Sort.by("home_city_x")))
        .doesNotThrowAnyException();

    // An annotation found on the field (the accessor is declared and carries none).
    assertRefused(
        () -> rules.refuseEncryptedKeys(DeclaredAccessor.class, null, Sort.by("street")),
        encryptedRefusal("street", "DeclaredAccessor"));

    // A disabled @JsonUnwrapped is not followed.
    assertThatCode(() -> rules.refuseEncryptedKeys(Disabled.class, null, Sort.by("street")))
        .doesNotThrowAnyException();

    // A record unwrapped inside itself ends the walk; its own component is still refused.
    assertRefused(
        () -> rules.refuseEncryptedKeys(Node.class, FieldEquals.of("secret", "x"), Sort.unsorted()),
        encryptedRefusal("secret", "Node"));
  }

  @Test
  void encryptedFieldsCacheIsPerInstance() {
    ObjectMapper snake = mapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    // The default-naming rules asked first.
    var plain = new ReadModelRules(mapper());
    var snaked = new ReadModelRules(snake);
    assertRefused(
        () ->
            plain.refuseEncryptedKeys(
                Account.class, FieldEquals.of("fieldName", "x"), Sort.unsorted()),
        encryptedRefusal("fieldName", "Account"));
    assertThatCode(
            () ->
                plain.refuseEncryptedKeys(
                    Account.class, FieldEquals.of("field_name", "x"), Sort.unsorted()))
        .doesNotThrowAnyException();
    assertRefused(
        () ->
            snaked.refuseEncryptedKeys(
                Account.class, FieldEquals.of("field_name", "x"), Sort.unsorted()),
        encryptedRefusal("field_name", "Account"));
    // The Java component name is refused under every naming.
    assertRefused(
        () ->
            snaked.refuseEncryptedKeys(
                Account.class, FieldEquals.of("fieldName", "x"), Sort.unsorted()),
        encryptedRefusal("fieldName", "Account"));

    // The snake-case rules asked first.
    var snakedFirst =
        new ReadModelRules(mapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE));
    var plainSecond = new ReadModelRules(mapper());
    assertRefused(
        () -> snakedFirst.refuseEncryptedKeys(Account.class, null, Sort.by("field_name")),
        encryptedRefusal("field_name", "Account"));
    assertThatCode(
            () -> plainSecond.refuseEncryptedKeys(Account.class, null, Sort.by("field_name")))
        .doesNotThrowAnyException();
    assertRefused(
        () -> plainSecond.refuseEncryptedKeys(Account.class, null, Sort.by("fieldName")),
        encryptedRefusal("fieldName", "Account"));
  }

  // ── Arguments and names ─────────────────────────────────────────────────────────────────────

  @Test
  void validationMessages() {
    PageRequest page = PageRequest.of(0, 10);
    FieldEquals filter = FieldEquals.of("name", "x");

    assertRefused(
        () -> ReadModelRules.validateProjectionName(null), "projectionName cannot be null");
    assertRefused(() -> ReadModelRules.validateInputs(null, "a"), "projectionName cannot be null");
    assertRefused(() -> ReadModelRules.validateInputs(P, null), "id cannot be null or blank");
    assertRefused(() -> ReadModelRules.validateInputs(P, " "), "id cannot be null or blank");
    assertRefused(
        () -> ReadModelRules.validateFilteredRead(null, Item.class, filter, page),
        "projectionName cannot be null");
    assertRefused(
        () -> ReadModelRules.validateFilteredRead(P, null, filter, page), "type cannot be null");
    assertRefused(
        () -> ReadModelRules.validateFilteredRead(P, Item.class, null, page),
        "filter cannot be null");
    assertRefused(
        () -> ReadModelRules.validateFilteredRead(P, Item.class, filter, null),
        "pageRequest cannot be null");
    // In parameter order: the first missing argument is the one named.
    assertRefused(
        () -> ReadModelRules.validateFilteredRead(null, null, null, null),
        "projectionName cannot be null");
    assertRefused(() -> ReadModelRules.validateField("a.b"), FIELD_RULE + "a.b");
    assertRefused(() -> ReadModelRules.validateField(""), FIELD_RULE);
    // The field is sanitized: the line break is stripped.
    assertRefused(() -> ReadModelRules.validateField("a\nb"), FIELD_RULE + "ab");

    assertThatCode(
            () -> {
              ReadModelRules.validateProjectionName(P);
              ReadModelRules.validateInputs(P, "a");
              ReadModelRules.validateFilteredRead(P, Item.class, filter, page);
              ReadModelRules.validateField("Some_Field_9");
            })
        .doesNotThrowAnyException();
  }

  @Test
  void tableNameRule_message() {
    assertThat(ReadModelRules.tableName(ProjectionName.of("order_summary")))
        .isEqualTo("order_summary_view");
    assertThat(ReadModelRules.tableName(ProjectionName.of("_9"))).isEqualTo("_9_view");
    assertThat(ReadModelRules.tableName(ProjectionName.of("a".repeat(58))))
        .isEqualTo("a".repeat(58) + "_view");
    // The rule has no backend-reserved prefix of its own.
    assertThat(ReadModelRules.tableName(ProjectionName.of("sqlite_x"))).isEqualTo("sqlite_x_view");

    assertRefused(
        () -> ReadModelRules.tableName(ProjectionName.of("order-summary")),
        tableRule("order-summary"));
    assertRefused(
        () -> ReadModelRules.tableName(ProjectionName.of("OrderSummary")),
        tableRule("OrderSummary"));
    assertRefused(() -> ReadModelRules.tableName(ProjectionName.of("9p")), tableRule("9p"));
    assertRefused(
        () -> ReadModelRules.tableName(ProjectionName.of("a".repeat(59))),
        tableRule("a".repeat(59)));
    // The name is sanitized in the message.
    assertRefused(() -> ReadModelRules.tableName(ProjectionName.of("a\nb")), tableRule("ab"));
  }

  private static String tableRule(String shown) {
    return "Projection name '"
        + shown
        + "' cannot name a read-model table: it must match [a-z_][a-z0-9_]{0,57} — lower-case"
        + " ASCII letters, digits and underscores, starting with a letter or an underscore, at"
        + " most 58 characters (the table is the name plus '_view'). The name is used exactly as"
        + " given, never rewritten, so that no two projection names share a table: rename the"
        + " projection, e.g. 'order_summary' instead of 'order-summary' or 'OrderSummary'.";
  }

  @Test
  void keysWithIdTiebreak_appendsIdOnce() {
    var nameDesc = new Sort.Order("name", SortDirection.DESC);
    var idAsc = new Sort.Order("id", SortDirection.ASC);
    var idDesc = new Sort.Order("id", SortDirection.DESC);
    var qty = new Sort.Order("qty", SortDirection.ASC);

    assertThat(ReadModelRules.keysWithIdTiebreak(List.of())).containsExactly(idAsc);
    assertThat(ReadModelRules.keysWithIdTiebreak(List.of(nameDesc)))
        .containsExactly(nameDesc, idAsc);
    // A key naming the id, in any position or direction, replaces the tiebreak.
    assertThat(ReadModelRules.keysWithIdTiebreak(List.of(idDesc))).containsExactly(idDesc);
    assertThat(ReadModelRules.keysWithIdTiebreak(List.of(nameDesc, idDesc, qty)))
        .containsExactly(nameDesc, idDesc, qty);

    // A new list: the caller's is not changed.
    List<Sort.Order> given = List.of(nameDesc);
    List<Sort.Order> keys = ReadModelRules.keysWithIdTiebreak(given);
    keys.add(qty);
    assertThat(given).containsExactly(nameDesc);
  }

  // ── Index names and statements ──────────────────────────────────────────────────────────────
  //
  // The expected names are those the PostgreSQL and SQLite repositories produce today for the
  // same index and their own rendered columns, taken literally from each repository's
  // characterization tests.

  /** PostgreSQL's rendering of a JSON field key. */
  private static String pgText(String field) {
    return "(data->>'" + field + "') COLLATE \"C\"";
  }

  private static final String PG_ID = "id COLLATE \"C\"";

  /** SQLite's rendering of a JSON field key. */
  private static String sqliteText(String field) {
    String path = "'$.\"" + field + "\"'";
    return "CASE json_type(data, "
        + path
        + ") WHEN 'text' THEN json_extract(data, "
        + path
        + ") WHEN 'null' THEN NULL ELSE data -> "
        + path
        + " END";
  }

  private static String name(ProjectionIndex index, String renderedColumns) {
    return ReadModelRules.indexName(
        index, ReadModelRules.tableName(index.projection()), renderedColumns);
  }

  @Test
  void indexName_isStableAndAtMost63() {
    ProjectionIndex byName = ProjectionIndex.forFilter(P, "name", Sort.unsorted());
    String pgColumns = pgText("name") + ", " + PG_ID;
    assertThat(name(byName, pgColumns)).isEqualTo("p_name_id_ix_0ac6c1bbb0");
    assertThat(name(byName, pgColumns)).isEqualTo(name(byName, pgColumns));

    // A descending key adds "_desc" to the readable part.
    assertThat(
            name(
                ProjectionIndex.forSort(P, Sort.by("qty", SortDirection.DESC)),
                pgText("qty") + " DESC, " + PG_ID))
        .isEqualTo("p_qty_desc_id_ix_95c543cea2");
    assertThat(
            name(
                new ProjectionIndex(
                    P,
                    List.of(
                        new Sort.Order("version", SortDirection.ASC),
                        new Sort.Order("id", SortDirection.DESC))),
                "version, " + PG_ID + " DESC"))
        .isEqualTo("p_version_id_desc_ix_16183b6853");
    assertThat(
            name(
                ProjectionIndex.forSort(P, Sort.by("name", SortDirection.DESC)),
                sqliteText("name") + " DESC, id"))
        .isEqualTo("p_name_desc_id_ix_e80a752cb8");
    assertThat(name(ProjectionIndex.forSort(P, Sort.by("id")), "id"))
        .isEqualTo("p_id_ix_c1a5e715ca");

    // 49 readable characters are kept whole (63 in all); 50 are cut to 49.
    String a41 = "a".repeat(41);
    String a42 = "a".repeat(42);
    String kept =
        name(
            ProjectionIndex.forFilter(ProjectionName.of(a41), "name", Sort.unsorted()),
            pgText("name") + ", " + PG_ID);
    String cut =
        name(
            ProjectionIndex.forFilter(ProjectionName.of(a42), "name", Sort.unsorted()),
            pgText("name") + ", " + PG_ID);
    assertThat(kept).isEqualTo(a41 + "_name_id_ix_0fbeffaaa7").hasSize(63);
    assertThat(cut).isEqualTo(a42 + "_name_i_ix_74cb357177").hasSize(63);

    String f44 = "f".repeat(44);
    String f45 = "f".repeat(45);
    assertThat(name(ProjectionIndex.forSort(P, Sort.by(f44)), sqliteText(f44) + ", id"))
        .isEqualTo("p_" + f44 + "_id_ix_87daa8b970")
        .hasSize(63);
    assertThat(name(ProjectionIndex.forSort(P, Sort.by(f45)), sqliteText(f45) + ", id"))
        .isEqualTo("p_" + f45 + "_i_ix_12bf81d3a7")
        .hasSize(63);

    // The readable part is lower-cased and cut mid-key.
    String c40 = "c".repeat(40);
    assertThat(
            name(
                ProjectionIndex.forSort(
                    ProjectionName.of(c40), Sort.by("SomeField", SortDirection.DESC)),
                pgText("SomeField") + " DESC, " + PG_ID))
        .isEqualTo(c40 + "_somefiel_ix_6f1cae8833");
    assertThat(name(ProjectionIndex.forSort(P, Sort.by("Qty")), sqliteText("Qty") + ", id"))
        .isEqualTo("p_qty_id_ix_c0232dbc4c");

    // The longest projection name.
    ProjectionName longest = ProjectionName.of("a".repeat(58));
    assertThat(
            name(
                ProjectionIndex.forFilter(longest, "name", Sort.unsorted()),
                sqliteText("name") + ", id"))
        .isEqualTo("a".repeat(49) + "_ix_b037fb7b88")
        .hasSize(ReadModelRules.INDEX_NAME_LENGTH);
  }

  @Test
  void indexName_dependsOnTheRenderedColumns() {
    ProjectionIndex byName = ProjectionIndex.forFilter(P, "name", Sort.unsorted());

    // One index, each backend's columns: the readable part is the same, the hash is not.
    assertThat(name(byName, pgText("name") + ", " + PG_ID)).isEqualTo("p_name_id_ix_0ac6c1bbb0");
    assertThat(name(byName, sqliteText("name") + ", id")).isEqualTo("p_name_id_ix_5a9db6eb3f");
    // The table is part of the hashed text.
    assertThat(ReadModelRules.indexName(byName, "other_view", sqliteText("name") + ", id"))
        .startsWith("p_name_id_ix_")
        .isNotEqualTo("p_name_id_ix_5a9db6eb3f");
  }

  @Test
  void createIndexSql_template() {
    assertThat(ReadModelRules.createIndexSql("n", "t", "a, b DESC"))
        .isEqualTo("CREATE INDEX IF NOT EXISTS n ON t (a, b DESC)");
    // Today's PostgreSQL and SQLite statements for one index.
    assertThat(
            ReadModelRules.createIndexSql(
                "p_name_id_ix_0ac6c1bbb0", "p_view", pgText("name") + ", " + PG_ID))
        .isEqualTo(
            "CREATE INDEX IF NOT EXISTS p_name_id_ix_0ac6c1bbb0 ON p_view ((data->>'name')"
                + " COLLATE \"C\", id COLLATE \"C\")");
    assertThat(
            ReadModelRules.createIndexSql(
                "p_name_id_ix_5a9db6eb3f", "p_view", sqliteText("name") + ", id"))
        .isEqualTo(
            "CREATE INDEX IF NOT EXISTS p_name_id_ix_5a9db6eb3f ON p_view (CASE json_type(data,"
                + " '$.\"name\"') WHEN 'text' THEN json_extract(data, '$.\"name\"') WHEN 'null'"
                + " THEN NULL ELSE data -> '$.\"name\"' END, id)");
  }

  // ── Failures ────────────────────────────────────────────────────────────────────────────────

  @Test
  void optimisticLockMessage_isUnsanitized() {
    OptimisticLockException e =
        ReadModelRules.optimisticLockConflict(ProjectionName.of("p\tq"), "a\nb", 3L);

    assertThat(e)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Optimistic lock conflict on projection 'p\tq/a\nb': expected version 3")
        .hasNoCause();
  }

  @Test
  void fromJson_failuresNameTheProjection() {
    var rules = new ReadModelRules(mapper());
    String good = "{\"name\":\"x\",\"qty\":1}";
    String bad = "not json";

    // Read back.
    assertThat(rules.fromJson(good, Item.class, "Failed: ", P, "a")).isEqualTo(new Item("x", 1));
    assertThat(rules.fromJson(List.of(good, good), Item.class, "Failed: ", P))
        .containsExactly(new Item("x", 1), new Item("x", 1));
    assertThat(rules.fromJson(new Page<>(List.of(good), 7, 2, 3), Item.class, P))
        .isEqualTo(new Page<>(List.of(new Item("x", 1)), 7, 2, 3));
    assertThat(rules.fromJson(Optional.of(new Versioned<>(good, 4L)), Item.class, P, "a"))
        .contains(new Versioned<>(new Item("x", 1), 4L));
    assertThat(rules.fromJson(Optional.<Versioned<String>>empty(), Item.class, P, "a")).isEmpty();

    // Failures: the given prefix, then the sanitized projection (and id).
    assertFailure(
        () -> rules.fromJson(bad, Item.class, "Failed to find projection: ", P, "a\nb"),
        "Failed to find projection: p/ab");
    assertFailure(
        () -> rules.fromJson(List.of(good, bad), Item.class, "Failed to find all projections: ", P),
        "Failed to find all projections: p");
    assertFailure(
        () -> rules.fromJson(new Page<>(List.of(bad), 1, 0, 10), Item.class, P),
        "Failed to find paginated projections: p");
    assertFailure(
        () -> rules.fromJson(Optional.of(new Versioned<>(bad, 1L)), Item.class, P, "a"),
        "Failed to find projection with lock: p/a");

    assertThat(ReadModelRules.shown(ProjectionName.of("p\nq"))).isEqualTo("pq");
    assertThat(ReadModelRules.shown(P, "a\u0007b")).isEqualTo("p/ab");
  }

  private static void assertFailure(ThrowingCallable call, String message) {
    assertThat(catchThrowable(call))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(message)
        .cause()
        .isInstanceOf(JsonProcessingException.class);
  }
}
