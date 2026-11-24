package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.RawValue;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.LockMode;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.PageRequest;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.test.InMemoryCryptoEngine;

/**
 * {@link SqliteProjectionRepository}'s own behaviour beyond the shared contracts: construction, the
 * table it creates, the JSON it stores, the plain reads and writes and their failures, the
 * transaction-scoped repository, after-commit actions, how a batch reports a failure, and the
 * {@link BaseProjection} composition.
 */
class SqliteProjectionRepositoryTest {

  record TestView(String id, String data) {
    @JsonCreator
    TestView(@JsonProperty("id") String id, @JsonProperty("data") String data) {
      this.id = id;
      this.data = data;
    }
  }

  /** A read model the mapper cannot write. */
  static final class Unwritable {
    public String getBoom() {
      throw new IllegalStateException("cannot be written");
    }
  }

  @TempDir Path dir;

  private Path file;
  private SqliteDatabase database;
  private SqliteReadModelCheckpoints checkpoints;
  private SqliteProjectionRepository repo;
  private DataSource outside;

  @BeforeEach
  void open() {
    file = dir.resolve("repository.db");
    database = SqliteDatabase.open(TestDatabases.dataSource(file));
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    checkpoints = new SqliteReadModelCheckpoints(database);
    repo = new SqliteProjectionRepository(database, mapper(), checkpoints);
    outside = TestDatabases.dataSource(file);
  }

  @AfterEach
  void close() {
    database.close();
  }

  private static ObjectMapper mapper() {
    return SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP);
  }

  private String rawData(String table, String id) throws SQLException {
    try (Connection conn = outside.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT data FROM " + table + " WHERE id = ?")) {
      ps.setString(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  private boolean tableExists(String table) throws SQLException {
    try (Connection conn = outside.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?")) {
      ps.setString(1, table);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private void execute(String sql) throws SQLException {
    try (Connection conn = outside.getConnection();
        Statement st = conn.createStatement()) {
      st.execute(sql);
    }
  }

  private void batch(ProjectionName name, long offset, AtomicBatchUpdater updater) {
    repo.executeAtomically(
        name, List.of(), GlobalOffset.of(offset), 0L, updater::update, checkpoints);
  }

  @FunctionalInterface
  interface AtomicBatchUpdater {
    void update(ProjectionRepository tx);
  }

  // ---- construction ----

  @Test
  void refusesNullArgumentsAndCheckpointsOfAnotherFile() {
    var mapper = mapper();
    assertThatThrownBy(() -> new SqliteProjectionRepository(null, mapper, checkpoints))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("database is required");
    assertThatThrownBy(() -> new SqliteProjectionRepository(database, null, checkpoints))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("objectMapper is required");
    assertThatThrownBy(() -> new SqliteProjectionRepository(database, mapper, null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("checkpoints is required");

    try (SqliteDatabase other =
        SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("o.db")))) {
      var foreign = new SqliteReadModelCheckpoints(other);
      assertThatThrownBy(() -> new SqliteProjectionRepository(database, mapper, foreign))
          .isExactlyInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              "The checkpoints live in "
                  + other.file()
                  + ", not in the repository's database "
                  + database.file()
                  + ": a batch advances the checkpoint in its own transaction on the repository's"
                  + " database, so the checkpoints must be SqliteReadModelCheckpoints of that same"
                  + " database");
    }
  }

  @Test
  void checkpointsOfAnotherHandleOnTheSameFileAreAccepted() {
    try (SqliteDatabase second = SqliteDatabase.open(TestDatabases.dataSource(file))) {
      assertThatCode(
              () ->
                  new SqliteProjectionRepository(
                      database, mapper(), new SqliteReadModelCheckpoints(second)))
          .doesNotThrowAnyException();
    }
  }

  @Test
  void theMapperFactoryAddsCryptoOnlyWithAnEngine() throws Exception {
    assertThatThrownBy(() -> SqliteProjectionRepository.createObjectMapper(null, null))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("metrics is required");
    var blind = SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP);
    var aware =
        SqliteProjectionRepository.createObjectMapper(
            new InMemoryCryptoEngine(), StreamRuneMetrics.NOOP);
    assertThat(blind.getRegisteredModuleIds()).noneMatch(id -> id.toString().contains("Crypto"));
    assertThat(aware.getRegisteredModuleIds()).anyMatch(id -> id.toString().contains("Crypto"));
    assertThat(blind.writeValueAsString(Instant.parse("2026-01-01T00:00:00Z")))
        .isEqualTo("\"2026-01-01T00:00:00Z\"");
  }

  // ---- the table ----

  @Test
  void theViewTableIsStrictWithTheDocumentedColumnsAndCreatedLazily() throws SQLException {
    var name = ProjectionName.of("lazy");
    assertThat(tableExists("lazy_view")).isFalse();

    repo.findAll(name, TestView.class);

    assertThat(tableExists("lazy_view")).isTrue();
    try (Connection conn = outside.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT sql FROM sqlite_schema WHERE name = 'lazy_view'")) {
      rs.next();
      assertThat(rs.getString(1))
          .isEqualTo(
              "CREATE TABLE lazy_view (id TEXT NOT NULL PRIMARY KEY CHECK (length(id) <= 255),"
                  + " data TEXT NOT NULL CHECK (json_valid(data)), version INTEGER NOT NULL"
                  + " DEFAULT 0, updated_at INTEGER NOT NULL) STRICT");
    }
    // A checkpoint commit alone creates no table.
    repo.executeAtomically(
        ProjectionName.of("no_table"), List.of(), GlobalOffset.of(1), 0L, tx -> {}, checkpoints);
    assertThat(tableExists("no_table_view")).isFalse();
    repo.prepareReadModel(ProjectionName.of("prepared"));
    assertThat(tableExists("prepared_view")).isTrue();
  }

  @Test
  void projectionNamesOutsideTheRuleAreRefusedBeforeAnyIo() throws SQLException {
    for (String bad : List.of("Orders", "order-summary", "a".repeat(59), "9lives")) {
      var name = ProjectionName.of(bad);
      assertThatThrownBy(() -> repo.checkProjectionName(name))
          .isExactlyInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("cannot name a read-model table");
      assertThatThrownBy(() -> repo.save(name, "x", new TestView("x", "v")))
          .isExactlyInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(
              () ->
                  repo.executeAtomically(
                      name, List.of(), GlobalOffset.of(1), 0L, tx -> {}, checkpoints))
          .isExactlyInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> repo.executeReplay(name, List.of(), tx -> {}))
          .isExactlyInstanceOf(IllegalArgumentException.class);
    }
    assertThatCode(() -> repo.checkProjectionName(ProjectionName.of("_a" + "b".repeat(56))))
        .doesNotThrowAnyException();
    assertThatThrownBy(() -> repo.checkProjectionName(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("projectionName cannot be null");
    try (Connection conn = outside.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs =
            st.executeQuery("SELECT count(*) FROM sqlite_schema WHERE name LIKE '%_view'")) {
      rs.next();
      assertThat(rs.getInt(1)).isZero();
    }
  }

  /**
   * SQLite reserves every name that starts with sqlite_: refused at registration, not first use.
   */
  @Test
  void aProjectionNameSqliteReservesIsRefusedAtRegistration() {
    var reserved = ProjectionName.of("sqlite_stats");

    assertThatThrownBy(() -> repo.checkProjectionName(reserved))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Projection name 'sqlite_stats' cannot name a read-model table on SQLite: SQLite"
                + " reserves every name that starts with 'sqlite_' for its own tables, so"
                + " 'sqlite_stats_view' cannot be created. Rename the projection.");
    assertThatThrownBy(() -> repo.findAll(reserved, TestView.class))
        .isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatCode(() -> repo.checkProjectionName(ProjectionName.of("sqlitestats")))
        .doesNotThrowAnyException();
  }

  @Test
  void anIdLongerThan255CharactersIsRefusedAsOnPostgres() {
    var name = ProjectionName.of("long_ids");
    String ok = "x".repeat(255);
    String tooLong = "é".repeat(256);

    repo.save(name, ok, new TestView(ok, "v"));
    assertThatThrownBy(() -> repo.save(name, tooLong, new TestView(tooLong, "v")))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageStartingWith("Failed to save projection: long_ids/");
    assertThat(repo.findAll(name, TestView.class)).hasSize(1);
  }

  // ---- the stored JSON ----

  @Test
  void numbersAreStoredInThePlainSpellingJsonbPrints() throws SQLException {
    var name = ProjectionName.of("numbers");
    var row = new LinkedHashMap<String, Object>();
    row.put("double", 1.2345678E7);
    row.put("small", 1.0E-4);
    row.put("negativeZero", -0.0);
    row.put("float", 1.0E10f);
    row.put("decimal", new BigDecimal("19.90"));
    row.put("exponent", new RawValue("1e5"));
    row.put("negativeInteger", new RawValue("-0"));
    row.put("big", new RawValue("123456789012345678901234567890"));
    row.put("text", "é\n😀");

    repo.save(name, "n", row);

    assertThat(rawData("numbers_view", "n"))
        .isEqualTo(
            "{\"double\":12345678,\"small\":0.00010,\"negativeZero\":0.0,\"float\":10000000000,"
                + "\"decimal\":19.90,\"exponent\":100000,\"negativeInteger\":0,"
                + "\"big\":123456789012345678901234567890,\"text\":\"é\\n😀\"}");
    @SuppressWarnings("unchecked")
    Map<String, Object> read = repo.findById(name, "n", Map.class).orElseThrow();
    assertThat(read.get("double")).isEqualTo(12345678);
    assertThat(read.get("text")).isEqualTo("é\n😀");
  }

  /**
   * The re-read of the written JSON has none of Jackson's default limits PostgreSQL lacks: a key of
   * 60 000 characters, a document nested as deep as SQLite's JSON parser allows (1000), and numbers
   * up to numeric's own bounds (131072 digits before the point, 16383 after) are stored, printed as
   * PostgreSQL prints them; a number beyond those bounds is refused, as PostgreSQL refuses it
   * ("value overflows numeric format").
   */
  @Test
  void documentsWithinJsonbsLimitsAreStoredAndNumbersBeyondNumericAreRefused() throws Exception {
    var name = ProjectionName.of("limits");
    String longKey = "k".repeat(60_000);
    repo.save(name, "key", Map.of(longKey, "v"));
    assertThat(rawData("limits_view", "key")).isEqualTo("{\"" + longKey + "\":\"v\"}");

    // SQLite's JSON parser nests at most 1000 deep (the document's own object included), a limit
    // of SQLite's that PostgreSQL does not share; the re-read refuses a deeper document before any
    // statement, at any depth (no StackOverflowError in the checks that walk the tree).
    String deep = "[".repeat(999) + "]".repeat(999);
    repo.save(name, "deep", Map.of("v", new RawValue(deep)));
    assertThat(rawData("limits_view", "deep")).isEqualTo("{\"v\":" + deep + "}");
    for (int depth : new int[] {1_000, 9_999}) {
      String tooDeep = "[".repeat(depth) + "]".repeat(depth);
      assertThatThrownBy(() -> repo.save(name, "too_deep", Map.of("v", new RawValue(tooDeep))))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage(
              "Failed to save projection: limits/too_deep: the document nests deeper than 1000"
                  + " levels, the most SQLite's JSON functions accept");
    }
    assertThat(rawData("limits_view", "too_deep")).isNull();

    repo.save(name, "big", Map.of("v", new RawValue("1E+10000")));
    assertThat(rawData("limits_view", "big")).isEqualTo("{\"v\":1" + "0".repeat(10_000) + "}");
    repo.save(name, "small", Map.of("v", new RawValue("1E-10000")));
    assertThat(rawData("limits_view", "small")).isEqualTo("{\"v\":0." + "0".repeat(9_999) + "1}");
    repo.save(name, "edge", Map.of("v", new RawValue("1E+131071"), "w", new RawValue("1E-16383")));
    repo.save(name, "zero", Map.of("v", new RawValue("0E+200000")));
    assertThat(rawData("limits_view", "zero")).isEqualTo("{\"v\":0}");
    repo.save(name, "zero_scale", Map.of("v", new RawValue("-0E-3")));
    assertThat(rawData("limits_view", "zero_scale")).isEqualTo("{\"v\":0.000}");
    repo.save(name, "integer", Map.of("v", new RawValue("9".repeat(131_072))));

    for (String overflowing :
        List.of("1E+131072", "1E-16384", "1.5E-16383", "0E-16384", "9".repeat(131_073))) {
      assertThatThrownBy(() -> repo.save(name, "o", Map.of("v", new RawValue(overflowing))))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage(
              "Failed to save projection: limits/o: a number has more digits than PostgreSQL's"
                  + " numeric stores (131072 before the decimal point, 16383 after)");
    }
    assertThat(rawData("limits_view", "o")).isNull();
  }

  /**
   * PostgreSQL's {@code numeric} refuses a number whose written exponent is beyond ±1073741823,
   * whatever its digits ("value overflows numeric format", probed on PostgreSQL 17 and 18): a zero
   * with such an exponent is refused, not stored as {@code 0}, and an exponent beyond Java's int
   * range is refused the same way, never leaking a {@link NumberFormatException}. The edge itself
   * is stored, as PostgreSQL stores it, also when the zero has fraction digits.
   */
  @Test
  void aNumberWhoseExponentNumericRefusesIsRefused() throws Exception {
    var name = ProjectionName.of("exponents");
    for (String edge :
        List.of("0E+1073741823", "-0E+1073741823", "0e+01073741823", "0.0E+1073741823")) {
      repo.save(name, "edge", Map.of("v", new RawValue(edge)));
      assertThat(rawData("exponents_view", "edge")).as(edge).isEqualTo("{\"v\":0}");
    }

    for (String refused :
        List.of(
            "0E+1073741824",
            "0.000E+1073741824",
            "0E-1073741824",
            "0E+2147483648",
            "1E+2147483648",
            "1.5e-2147483649",
            "0E+99999999999999999999")) {
      assertThatThrownBy(() -> repo.save(name, "o", Map.of("v", new RawValue(refused))))
          .as(refused)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage(
              "Failed to save projection: exponents/o: a number's exponent is beyond ±1073741823,"
                  + " which PostgreSQL's numeric refuses (value overflows numeric format)");
    }
    assertThat(rawData("exponents_view", "o")).isNull();
  }

  /**
   * Text a {@link RawValue} writes that is not JSON is refused before anything is written, as
   * PostgreSQL's {@code jsonb} input refuses it, never as the table's {@code json_valid} check
   * failing.
   */
  @Test
  void aReadModelThatSerializesToInvalidJsonIsRefused() throws Exception {
    var name = ProjectionName.of("invalid");

    for (String notJson : List.of("{", "nope", "1.", "NaN")) {
      assertThatThrownBy(() -> repo.save(name, "i", Map.of("v", new RawValue(notJson))))
          .as(notJson)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage(
              "Failed to save projection: invalid/i: the read model serializes to text that is"
                  + " not JSON, which PostgreSQL's jsonb refuses")
          .hasCauseInstanceOf(com.fasterxml.jackson.core.JsonProcessingException.class);
      assertThatThrownBy(() -> repo.save(name, "i", Map.of("v", new RawValue(notJson)), 0))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessageStartingWith("Failed to save projection with version check: invalid/i: ");
    }
    assertThat(rawData("invalid_view", "i")).isNull();
  }

  @Test
  void theLastOfTwoEqualKeysIsKept_asJsonbKeepsIt() {
    var name = ProjectionName.of("duplicates");
    var row = new LinkedHashMap<String, Object>();
    row.put("id", "d");
    row.put("v", new RawValue("1,\"v\":2"));

    repo.save(name, "d", row);

    var page = repo.findAllBy(name, Map.class, FieldEquals.of("v", "2"), PageRequest.of(0, 10));
    assertThat(page.content()).hasSize(1);
    assertThat(
            repo.findAllBy(name, Map.class, FieldEquals.of("v", "1"), PageRequest.of(0, 10))
                .content())
        .isEmpty();
  }

  @Test
  void aKeyHoldingUPlus0000IsRefusedAndAnUnwritableReadModelFails() {
    var name = ProjectionName.of("refused");
    var nulKey = Map.of("a\0b", "v");
    var nested = Map.of("list", List.of("ok", Map.of("x", "a\0")));

    assertThatThrownBy(() -> repo.save(name, "k", nulKey))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(
            "Failed to save projection: refused/k: a string holds U+0000, which PostgreSQL jsonb"
                + " cannot store");
    assertThatThrownBy(() -> repo.save(name, "n", nested))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThatThrownBy(() -> repo.save(name, "u", new Unwritable()))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to save projection: refused/u");
    assertThatThrownBy(() -> repo.save(name, "u", new Unwritable(), 0))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to save projection with version check: refused/u");
    assertThat(repo.findAll(name, Map.class)).isEmpty();
  }

  @Test
  void aStoredDocumentTheTypeCannotReadFailsEveryRead() throws SQLException {
    var name = ProjectionName.of("unreadable");
    repo.save(name, "a", Map.of("id", "a", "data", Map.of("nested", true)));

    assertThatThrownBy(() -> repo.findById(name, "a", TestView.class))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to find projection: unreadable/a");
    assertThatThrownBy(() -> repo.findById(name, "a", TestView.class, LockMode.OPTIMISTIC))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to find projection with lock: unreadable/a");
    assertThatThrownBy(() -> repo.findAll(name, TestView.class))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to find all projections: unreadable");
    assertThatThrownBy(() -> repo.findAll(name, TestView.class, PageRequest.of(0, 1)))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to find paginated projections: unreadable");
  }

  // ---- plain reads and writes ----

  @Test
  void savesFindsVersionsAndDeletes() {
    var name = ProjectionName.of("plain");

    repo.save(name, "a", new TestView("a", "one"));
    repo.save(name, "a", new TestView("a", "two"));
    repo.save(name, "b", new TestView("b", "bee"));

    assertThat(repo.findById(name, "a", TestView.class)).contains(new TestView("a", "two"));
    assertThat(repo.findById(name, "zz", TestView.class)).isEmpty();
    var versioned = repo.findById(name, "a", TestView.class, LockMode.PESSIMISTIC).orElseThrow();
    assertThat(versioned.version()).isEqualTo(1);
    assertThat(repo.findById(name, "zz", TestView.class, LockMode.OPTIMISTIC)).isEmpty();

    repo.save(name, "a", new TestView("a", "three"), 1);
    assertThat(
            repo.findById(name, "a", TestView.class, LockMode.OPTIMISTIC).orElseThrow().version())
        .isEqualTo(2);
    assertThatThrownBy(() -> repo.save(name, "a", new TestView("a", "stale"), 1))
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Optimistic lock conflict on projection 'plain/a': expected version 1");
    assertThatThrownBy(() -> repo.save(name, "missing", new TestView("missing", "x"), 0))
        .isExactlyInstanceOf(OptimisticLockException.class);

    assertThat(repo.findAll(name, TestView.class)).hasSize(2);
    repo.delete(name, "a");
    repo.delete(name, "a");
    assertThat(repo.findAll(name, TestView.class)).containsExactly(new TestView("b", "bee"));
  }

  @Test
  void updatedAtIsTheClocksInstantInMicros() throws SQLException {
    var clock = Clock.fixed(Instant.parse("2026-03-04T05:06:07.123456Z"), ZoneOffset.UTC);
    var stamped =
        new SqliteProjectionRepository(
            database, mapper(), new SqliteReadModelCheckpoints(database, clock));
    var name = ProjectionName.of("stamped");

    stamped.save(name, "a", new TestView("a", "v"));

    try (Connection conn = outside.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT updated_at FROM stamped_view")) {
      rs.next();
      assertThat(SqliteTimestamps.fromMicros(rs.getLong(1))).isEqualTo(clock.instant());
    }
  }

  @Test
  void everyMethodRefusesNullAndBlankArguments() {
    var name = ProjectionName.of("args");
    var view = new TestView("x", "v");
    var page = PageRequest.of(0, 1);
    List<Runnable> calls =
        List.of(
            () -> repo.save(null, "x", view),
            () -> repo.save(name, null, view),
            () -> repo.save(name, " ", view),
            () -> repo.save(name, "x", null),
            () -> repo.save(name, "x", null, 0),
            () -> repo.save(name, "x", view, -1),
            () -> repo.findById(name, "x", null),
            () -> repo.findById(name, "x", null, LockMode.OPTIMISTIC),
            () -> repo.findById(name, "x", TestView.class, null),
            () -> repo.findAll(null, TestView.class),
            () -> repo.findAll(name, null),
            () -> repo.findAll(null, TestView.class, page),
            () -> repo.findAll(name, null, page),
            () -> repo.findAll(name, TestView.class, null),
            () -> repo.delete(name, ""),
            () -> repo.declareIndex(null),
            () -> repo.prepareReadModel(null),
            () -> repo.stampFencingEpoch(null, 1),
            () -> repo.resetOffset(null),
            () -> repo.executeAtomically(null, List.of(), GlobalOffset.of(1), 0, tx -> {}, null),
            () -> repo.executeReplay(null, List.of(), tx -> {}));
    for (Runnable call : calls) {
      assertThatThrownBy(call::run).isExactlyInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void stampResetAndTheCheckpointsShareOneRow() {
    var name = ProjectionName.of("stamp");
    batch(name, 7, tx -> {});

    repo.stampFencingEpoch(name, 3);
    repo.stampFencingEpoch(name, 3);
    assertThatThrownBy(() -> repo.stampFencingEpoch(name, 2))
        .isExactlyInstanceOf(ProjectionEpochRegressionException.class);
    assertThatThrownBy(
            () ->
                repo.executeAtomically(
                    name, List.of(), GlobalOffset.of(8), 2L, tx -> {}, checkpoints))
        .isExactlyInstanceOf(ProjectionCommitFencedException.class);

    repo.resetOffset(name);
    assertThat(checkpoints.getLastOffset(name)).isEqualTo(GlobalOffset.initial());
    repo.executeAtomically(name, List.of(), GlobalOffset.of(1), 3L, tx -> {}, checkpoints);
    assertThat(checkpoints.getLastOffset(name)).isEqualTo(GlobalOffset.of(1));
  }

  @Test
  void theWriteTargetIsTheFileAndTheMapper() {
    var mapper = mapper();
    var one = new SqliteProjectionRepository(database, mapper, checkpoints);
    try (SqliteDatabase second = SqliteDatabase.open(TestDatabases.dataSource(file));
        SqliteDatabase other = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("o.db")))) {
      var sameFileSameMapper =
          new SqliteProjectionRepository(second, mapper, new SqliteReadModelCheckpoints(second));
      var sameFileOtherMapper =
          new SqliteProjectionRepository(second, mapper(), new SqliteReadModelCheckpoints(second));
      var otherFile =
          new SqliteProjectionRepository(other, mapper, new SqliteReadModelCheckpoints(other));

      assertThat(one.writesTo(sameFileSameMapper)).isTrue();
      assertThat(one.writeTargetIdentity())
          .isEqualTo(sameFileSameMapper.writeTargetIdentity())
          .hasSameHashCodeAs(sameFileSameMapper.writeTargetIdentity());
      assertThat(one.writesTo(sameFileOtherMapper)).isFalse();
      assertThat(one.writesTo(otherFile)).isFalse();
    }
    assertThat(one.writeTargetIdentity()).isNotEqualTo("not a target");
    assertThat(one.writeTargetIdentity().toString())
        .startsWith("SqliteProjectionRepository(file=" + database.file() + ", objectMapper=");
    assertThat(one.transactional()).isTrue();
    assertThat(one.fencesCheckpoint()).isTrue();
    assertThat(one.serializesReplay()).isTrue();
  }

  // ---- the transaction-scoped repository ----

  @Test
  void theTransactionScopedRepositoryHandlesEveryOperationAndValidatesInputs() {
    var name = ProjectionName.of("scoped_ops");
    var handedIdentity = new AtomicReference<Object>();

    batch(
        name,
        11,
        tx -> {
          tx.save(name, "a", new TestView("a", "one"));
          tx.save(name, "b", new TestView("b", "two"));
          tx.save(name, "b", new TestView("b", "two-updated"), 0);
          assertThatThrownBy(() -> tx.save(name, "b", new TestView("b", "stale"), 0))
              .isExactlyInstanceOf(OptimisticLockException.class);

          assertThat(tx.findById(name, "a", TestView.class)).contains(new TestView("a", "one"));
          var locked = tx.findById(name, "b", TestView.class, LockMode.PESSIMISTIC).orElseThrow();
          assertThat(locked.version()).isEqualTo(1);
          assertThat(locked.data().data()).isEqualTo("two-updated");
          assertThat(tx.findAll(name, TestView.class)).hasSize(2);
          var page = tx.findAll(name, TestView.class, PageRequest.of(0, 1));
          assertThat(page.content()).hasSize(1);
          assertThat(page.totalElements()).isEqualTo(2);
          tx.delete(name, "a");
          assertThat(tx.findById(name, "a", TestView.class)).isEmpty();
          handedIdentity.set(tx.writeTargetIdentity());

          var view = new TestView("x", "v");
          var request = PageRequest.of(0, 1);
          var filter = FieldEquals.of("data", "v");
          List<Runnable> calls =
              List.of(
                  () -> tx.save(name, "x", null),
                  () -> tx.save(name, "x", null, 0),
                  () -> tx.save(name, "x", view, -1),
                  () -> tx.findById(name, "x", null),
                  () -> tx.findById(name, "x", null, LockMode.OPTIMISTIC),
                  () -> tx.findById(name, "x", TestView.class, null),
                  () -> tx.findAll(name, null),
                  () -> tx.findAll(name, null, request),
                  () -> tx.findAll(name, TestView.class, null),
                  () -> tx.findAllBy(name, null, filter, request),
                  () -> tx.delete(name, " "),
                  () -> tx.afterCommit(null));
          for (Runnable call : calls) {
            assertThatThrownBy(call::run).isExactlyInstanceOf(IllegalArgumentException.class);
          }
          assertThatThrownBy(
                  () ->
                      tx.declareIndex(
                          ProjectionIndex.forFilter(
                              name, "data", org.streamrune.core.Sort.unsorted())))
              .isExactlyInstanceOf(IllegalStateException.class);
        });

    assertThat(handedIdentity.get()).isEqualTo(repo.writeTargetIdentity());
    assertThat(repo.findAll(name, TestView.class))
        .containsExactly(new TestView("b", "two-updated"));
    assertThat(checkpoints.getLastOffset(name)).isEqualTo(GlobalOffset.of(11));
  }

  @Test
  void aFailingStatementInsideTheBatchIsAnEventStoreExceptionAndRollsBack() throws SQLException {
    var name = ProjectionName.of("scoped_fail");
    repo.prepareReadModel(name);
    var tooLong = "x".repeat(256);

    assertThatThrownBy(
            () ->
                batch(
                    name,
                    1,
                    tx -> {
                      tx.save(name, "a", new TestView("a", "kept?"));
                      tx.save(name, tooLong, new TestView(tooLong, "v"));
                    }))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageStartingWith("Failed to save projection: scoped_fail/");
    assertThat(rawData("scoped_fail_view", "a")).isNull();
    assertThat(checkpoints.getLastOffset(name)).isEqualTo(GlobalOffset.initial());
  }

  /** A table the repository has seen and someone else dropped fails each scoped statement. */
  @Test
  void theScopedViewReportsEachStatementsFailure_andTheFirstAbortsTheBatch() throws SQLException {
    var name = ProjectionName.of("scoped_dropped");
    repo.prepareReadModel(name);
    execute("DROP TABLE scoped_dropped_view");
    var view = new TestView("x", "v");
    Map<String, AtomicBatchUpdater> calls = new LinkedHashMap<>();
    calls.put("Failed to save projection: scoped_dropped/x", tx -> tx.save(name, "x", view));
    calls.put(
        "Failed to save projection with version check: scoped_dropped/x",
        tx -> tx.save(name, "x", view, 0));
    calls.put(
        "Failed to find projection: scoped_dropped/x",
        tx -> tx.findById(name, "x", TestView.class));
    calls.put(
        "Failed to find projection with lock: scoped_dropped/x",
        tx -> tx.findById(name, "x", TestView.class, LockMode.OPTIMISTIC));
    calls.put(
        "Failed to find all projections: scoped_dropped", tx -> tx.findAll(name, TestView.class));
    calls.put(
        "Failed to find paginated projections: scoped_dropped",
        tx -> tx.findAll(name, TestView.class, PageRequest.of(0, 1)));
    calls.put("Failed to delete projection: scoped_dropped/x", tx -> tx.delete(name, "x"));

    // Each statement, the first of its batch, reports its own failure and fails the batch.
    for (Map.Entry<String, AtomicBatchUpdater> call : calls.entrySet()) {
      assertThatThrownBy(() -> batch(name, 1, call.getValue()))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage(call.getKey())
          .hasCauseInstanceOf(SQLException.class);
    }

    // An updater that swallows the failures: the first statement aborted the transaction, so the
    // others run no statement and the batch rolls back, as PostgreSQL's 25P02 and its COMMIT that
    // rolls back do.
    var failures = new ArrayList<EventStoreException>();
    assertThatThrownBy(
            () ->
                batch(
                    name,
                    1,
                    tx -> {
                      for (AtomicBatchUpdater call : calls.values()) {
                        try {
                          call.update(tx);
                        } catch (EventStoreException e) {
                          failures.add(e);
                        }
                      }
                    }))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(
            "The projection transaction was aborted by an earlier failure, so it runs no further"
                + " statement and rolls back: Failed to save projection: scoped_dropped/x")
        .hasCause(failures.getFirst());
    assertThat(failures).hasSize(7);
    assertThat(failures.getFirst()).hasMessage("Failed to save projection: scoped_dropped/x");
    for (EventStoreException later : failures.subList(1, 7)) {
      assertThat(later)
          .hasMessageStartingWith("The projection transaction was aborted by an earlier failure")
          .hasCause(failures.getFirst());
    }
    assertThat(checkpoints.getLastOffset(name)).isEqualTo(GlobalOffset.initial());
  }

  /** A table the scoped view must create and cannot fails the statement that needs it. */
  @Test
  void aTableTheScopedViewCannotCreateFailsTheStatement() throws SQLException {
    var name = ProjectionName.of("blocked");
    execute("CREATE TABLE occupier (x INTEGER) STRICT");
    execute("CREATE INDEX blocked_view ON occupier (x)");

    assertThatThrownBy(() -> batch(name, 1, tx -> tx.save(name, "a", new TestView("a", "v"))))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to create projection table: blocked_view");
    assertThatThrownBy(() -> batch(name, 1, tx -> tx.findAll(name, TestView.class)))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to create projection table: blocked_view");
    assertThatThrownBy(() -> repo.findAll(name, TestView.class))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to create projection table: blocked_view");
    assertThat(checkpoints.getLastOffset(name)).isEqualTo(GlobalOffset.initial());
  }

  /**
   * A COMMIT that SQLite refuses (here a deferred foreign key the transaction violates) rolls the
   * whole batch back, the table it created included: the repository remembers that table only after
   * a commit, so the next batch creates it again. The failure is the body failure, and the
   * after-commit actions run, because a failed COMMIT is an unknown outcome to the repository.
   */
  @Test
  void aBatchWhoseCommitFailsRollsBackTheTableItCreated() throws SQLException {
    var name = ProjectionName.of("commit_refused");
    execute("CREATE TABLE parent (id TEXT NOT NULL PRIMARY KEY) STRICT");
    execute(
        "CREATE TABLE child (parent_id TEXT NOT NULL REFERENCES parent (id)"
            + " DEFERRABLE INITIALLY DEFERRED) STRICT");
    execute(
        "CREATE TRIGGER dangling AFTER INSERT ON readmodel_checkpoint"
            + " BEGIN INSERT INTO child VALUES ('missing'); END");
    var afterCommit = new AtomicBoolean();

    assertThatThrownBy(
            () ->
                batch(
                    name,
                    1,
                    tx -> {
                      tx.save(name, "a", new TestView("a", "v"));
                      tx.afterCommit(() -> afterCommit.set(true));
                    }))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Transaction failed during projection update");
    assertThat(tableExists("commit_refused_view")).isFalse();
    assertThat(afterCommit).isTrue();

    execute("DROP TRIGGER dangling");
    batch(name, 1, tx -> tx.save(name, "a", new TestView("a", "v")));
    assertThat(repo.findById(name, "a", TestView.class)).contains(new TestView("a", "v"));
  }

  @Test
  void aTableCreatedInsideARolledBackBatchIsCreatedAgainByTheNextOne() throws SQLException {
    var name = ProjectionName.of("created_in_batch");

    assertThatThrownBy(
            () ->
                batch(
                    name,
                    1,
                    tx -> {
                      tx.save(name, "a", new TestView("a", "v"));
                      throw new IllegalStateException("roll back");
                    }))
        .isExactlyInstanceOf(IllegalStateException.class);
    assertThat(tableExists("created_in_batch_view")).isFalse();

    batch(name, 1, tx -> tx.save(name, "a", new TestView("a", "v")));
    assertThat(tableExists("created_in_batch_view")).isTrue();
    assertThat(repo.findById(name, "a", TestView.class)).isPresent();
  }

  @Test
  void anUpdaterErrorRollsBackAndPropagatesUnchanged() {
    var name = ProjectionName.of("updater_error");
    var error = new AssertionError("from user mapping code");

    assertThatThrownBy(
            () ->
                batch(
                    name,
                    5,
                    tx -> {
                      tx.save(name, "a", new TestView("a", "v"));
                      throw error;
                    }))
        .isSameAs(error);
    assertThat(repo.findAll(name, TestView.class)).isEmpty();
    assertThat(checkpoints.getLastOffset(name)).isEqualTo(GlobalOffset.initial());
  }

  // ---- after-commit actions ----

  @Test
  void afterCommitActionsRunInOrderAfterTheCommit_aFailingOneIsLogged() {
    var name = ProjectionName.of("after_commit");
    var ran = new ArrayList<String>();
    var kept = new AtomicReference<ProjectionRepository>();

    batch(
        name,
        1,
        tx -> {
          kept.set(tx);
          tx.save(name, "a", new TestView("a", "v"));
          tx.afterCommit(() -> ran.add("first"));
          tx.afterCommit(
              () -> {
                throw new IllegalStateException("eviction failed");
              });
          tx.afterCommit(() -> ran.add("third"));
          assertThat(ran).isEmpty();
        });

    assertThat(ran).containsExactly("first", "third");
    kept.get().afterCommit(() -> ran.add("late"));
    assertThat(ran).containsExactly("first", "third", "late");
  }

  @Test
  void anErrorFromAnAfterCommitAction_afterACleanCommit_propagatesAndTheBatchStaysCommitted() {
    var name = ProjectionName.of("after_commit_error");
    var error = new AssertionError("from an action");

    assertThatThrownBy(
            () ->
                batch(
                    name,
                    1,
                    tx -> {
                      tx.save(name, "a", new TestView("a", "v"));
                      tx.afterCommit(
                          () -> {
                            throw error;
                          });
                    }))
        .isSameAs(error);
    assertThat(repo.findById(name, "a", TestView.class)).isPresent();
    assertThat(checkpoints.getLastOffset(name)).isEqualTo(GlobalOffset.of(1));
  }

  @Test
  void anActionOfARolledBackBatchIsDiscarded_evenWhenRegisteredLater() {
    var name = ProjectionName.of("after_rollback");
    var kept = new AtomicReference<ProjectionRepository>();
    var ran = new AtomicBoolean();

    assertThatThrownBy(
            () ->
                batch(
                    name,
                    1,
                    tx -> {
                      kept.set(tx);
                      tx.afterCommit(() -> ran.set(true));
                      throw new IllegalStateException("roll back");
                    }))
        .isExactlyInstanceOf(IllegalStateException.class);
    kept.get().afterCommit(() -> ran.set(true));
    assertThat(ran).isFalse();
  }

  // ---- how a batch reports a failure ----

  @Test
  void aFailedStatementOfTheFenceIsTheBodyFailure() throws SQLException {
    try (SqliteDatabase bare =
        SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("unmigrated.db")))) {
      var unmigrated =
          new SqliteProjectionRepository(bare, mapper(), new SqliteReadModelCheckpoints(bare));
      var name = ProjectionName.of("unmigrated");

      assertThatThrownBy(
              () ->
                  unmigrated.executeAtomically(
                      name, List.of(), GlobalOffset.of(1), 0L, tx -> {}, null))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Transaction failed during projection update")
          .hasCauseInstanceOf(SQLException.class);
      assertThatThrownBy(() -> unmigrated.executeReplay(name, List.of(), tx -> {}))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Transaction failed during projection dead-letter replay");
    }
  }

  @Test
  void aConnectionThatCannotBeBorrowedIsTheConnectionFailure() {
    var failing = new AtomicBoolean();
    DataSource delegate = TestDatabases.dataSource(dir.resolve("borrow.db"));
    DataSource source =
        (DataSource)
            Proxy.newProxyInstance(
                DataSource.class.getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("getConnection") && failing.get()) {
                    throw new SQLException("the pool is closed");
                  }
                  try {
                    return method.invoke(delegate, args);
                  } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                  }
                });
    try (SqliteDatabase handle = SqliteDatabase.open(source)) {
      handle.migrate(SqliteSchemaSeries.READ_MODEL);
      var borrowing =
          new SqliteProjectionRepository(handle, mapper(), new SqliteReadModelCheckpoints(handle));
      var name = ProjectionName.of("borrow");
      borrowing.prepareReadModel(name);
      failing.set(true);

      assertThatThrownBy(
              () ->
                  borrowing.executeAtomically(
                      name, List.of(), GlobalOffset.of(1), 0L, tx -> {}, null))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to process projection batch atomically");
      assertThatThrownBy(() -> borrowing.executeReplay(name, List.of(), tx -> {}))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to replay a projection dead-letter range");
      assertThatThrownBy(() -> borrowing.findById(name, "a", TestView.class))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to find projection: borrow/a");
      assertThatThrownBy(() -> borrowing.findById(name, "a", TestView.class, LockMode.OPTIMISTIC))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to find projection with lock: borrow/a");
      assertThatThrownBy(() -> borrowing.findAll(name, TestView.class))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to find all projections: borrow");
      assertThatThrownBy(() -> borrowing.findAll(name, TestView.class, PageRequest.of(0, 1)))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to find paginated projections: borrow");
      assertThatThrownBy(() -> borrowing.save(name, "a", new TestView("a", "v")))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to save projection: borrow/a");
      assertThatThrownBy(() -> borrowing.save(name, "a", new TestView("a", "v"), 0))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to save projection with version check: borrow/a");
      assertThatThrownBy(() -> borrowing.delete(name, "a"))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to delete projection: borrow/a");
      // The probe for the table comes first, on a connection of its own.
      assertThatThrownBy(() -> borrowing.prepareReadModel(ProjectionName.of("fresh")))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to look up projection table: fresh_view");
      assertThatThrownBy(
              () ->
                  borrowing.declareIndex(
                      ProjectionIndex.forFilter(name, "data", org.streamrune.core.Sort.unsorted())))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessageStartingWith("Failed to create projection index: borrow_data_id_ix_");
    }
  }

  @Test
  void aWriteLockHeldElsewhereIsReportedAsALockFailureNamingTheTransaction() throws Exception {
    try (SqliteDatabase quick =
        SqliteDatabase.open(TestDatabases.dataSource(file, "busy_timeout=50"))) {
      var impatient =
          new SqliteProjectionRepository(quick, mapper(), new SqliteReadModelCheckpoints(quick));
      var name = ProjectionName.of("locked_out");
      impatient.prepareReadModel(name);
      try (Connection holder = outside.getConnection();
          Statement st = holder.createStatement()) {
        st.execute("BEGIN IMMEDIATE");
        try {
          assertThatThrownBy(
                  () ->
                      impatient.executeAtomically(
                          name, List.of(), GlobalOffset.of(1), 0L, tx -> {}, null))
              .isExactlyInstanceOf(EventStoreException.class)
              .hasMessageContaining("projection batch of projection locked_out stored nothing");
          assertThatThrownBy(() -> impatient.save(name, "a", new TestView("a", "v")))
              .isExactlyInstanceOf(EventStoreException.class)
              .hasMessageContaining("save of projection locked_out/a stored nothing");
          // A read does not wait for the write lock.
          assertThat(impatient.findAll(name, TestView.class)).isEmpty();
        } finally {
          st.execute("ROLLBACK");
        }
      }
    }
  }

  @Test
  void aConnectionInManualCommitModeIsRefusedBeforeAnythingRuns() {
    var switchable = new SwitchableDataSource(TestDatabases.dataSource(dir.resolve("manual.db")));
    try (SqliteDatabase handle = SqliteDatabase.open(switchable)) {
      handle.migrate(SqliteSchemaSeries.READ_MODEL);
      var manual =
          new SqliteProjectionRepository(handle, mapper(), new SqliteReadModelCheckpoints(handle));
      var name = ProjectionName.of("manual");
      manual.prepareReadModel(name);
      switchable.manualCommit();
      var ran = new AtomicBoolean();

      assertThatThrownBy(
              () ->
                  manual.executeAtomically(
                      name, List.of(), GlobalOffset.of(1), 0L, tx -> ran.set(true), null))
          .isExactlyInstanceOf(IllegalStateException.class)
          .hasMessage(SqliteTransactions.MANUAL_COMMIT_CONNECTION);
      assertThat(ran).isFalse();
    }
  }

  // ---- BaseProjection composes with the transaction ----

  record Counter(String id, int applyCount) {
    @JsonCreator
    Counter(@JsonProperty("id") String id, @JsonProperty("applyCount") int applyCount) {
      this.id = id;
      this.applyCount = applyCount;
    }
  }

  /** A non-idempotent projection writing only through BaseProjection's inherited helpers. */
  static final class CountingProjection extends BaseProjection {
    CountingProjection(ProjectionRepository repository) {
      super(repository, "base_counting");
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      int current = findById("counter", Counter.class).map(Counter::applyCount).orElse(0);
      save("counter", new Counter("counter", current + 1));
    }
  }

  @Test
  void aBaseProjectionsReDeliveredBatchIsNotDoubleApplied() {
    var name = ProjectionName.of("base_counting");
    var projection = new CountingProjection(repo);

    repo.executeAtomically(
        name, List.of(), GlobalOffset.of(300), 0L, tx -> projection.process(List.of(), tx), null);
    repo.executeAtomically(
        name, List.of(), GlobalOffset.of(310), 0L, tx -> projection.process(List.of(), tx), null);
    assertThatThrownBy(
            () ->
                repo.executeAtomically(
                    name,
                    List.of(),
                    GlobalOffset.of(200),
                    0L,
                    tx -> projection.process(List.of(), tx),
                    null))
        .isInstanceOf(OptimisticLockException.class);
    assertThatThrownBy(
            () ->
                repo.executeAtomically(
                    name,
                    List.of(),
                    GlobalOffset.of(320),
                    0L,
                    tx -> {
                      projection.process(List.of(), tx);
                      throw new IllegalStateException("boom after the base-projection write");
                    },
                    null))
        .isExactlyInstanceOf(IllegalStateException.class);

    assertThat(repo.findById(name, "counter", Counter.class).orElseThrow().applyCount())
        .isEqualTo(2);
    assertThat(checkpoints.getLastOffset(name)).isEqualTo(GlobalOffset.of(310));
  }
}
