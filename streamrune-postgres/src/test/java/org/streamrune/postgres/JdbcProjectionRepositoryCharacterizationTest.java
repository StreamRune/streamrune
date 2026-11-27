package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PSQLException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.LockMode;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.Versioned;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Characterization of the PostgreSQL projection repository: the statements, connections and
 * transactions of the table creation, the index declaration, the reads and the writes, the exact
 * exception type, message, cause and suppressed exceptions of every refusal and failure, when
 * created tables are remembered, the savepoint a table creation inside a batch runs under, the
 * after-commit actions and their log lines, and the {@code @Encrypted} key refusal with its
 * per-instance cache. The values are what the repository does on a real database today, written
 * down as they come out; these tests stay unchanged while the projection rules and the after-commit
 * actions move into shared helpers.
 *
 * <p>Every repository borrows from a {@link RecordingDataSource}. Over a pool that hands out {@code
 * autoCommit=true} connections a read or a write is one statement on one connection, and a table is
 * created by its own {@code CREATE TABLE IF NOT EXISTS} on a connection of its own (no catalog
 * probe). Over a pool that hands out {@code autoCommit=false} connections each write commits, and a
 * refusal decided inside it (the optimistic lock) rolls it back and is thrown unwrapped. A batch or
 * a replay is one transaction that locks the checkpoint row first; tables it creates are remembered
 * only once its COMMIT returned.
 */
@Testcontainers
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class JdbcProjectionRepositoryCharacterizationTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_projection_repository_characterization");

  private static final ProjectionName P = ProjectionName.of("p");
  private static final ProjectionName Q = ProjectionName.of("q");
  private static final ProjectionName R = ProjectionName.of("r");

  // ── today's statements, as literals ─────────────────────────────────────────────────────────

  private static String createTable(String table) {
    return "CREATE TABLE IF NOT EXISTS "
        + table
        + " (id VARCHAR(255) COLLATE \"C\" PRIMARY KEY, data JSONB NOT NULL, version BIGINT NOT"
        + " NULL DEFAULT 0, updated_at TIMESTAMPTZ DEFAULT NOW())";
  }

  private static String upsert(String table) {
    return "INSERT INTO "
        + table
        + " (id, data, version, updated_at) VALUES (?, ?::jsonb, 0, NOW()) ON CONFLICT (id) DO"
        + " UPDATE SET data = EXCLUDED.data, version = "
        + table
        + ".version + 1, updated_at = NOW()";
  }

  private static String versionedUpdate(String table) {
    return "UPDATE "
        + table
        + " SET data = ?::jsonb, version = version + 1, updated_at = NOW() WHERE id = ? AND"
        + " version = ?";
  }

  private static String selectById(String table) {
    return "SELECT data FROM " + table + " WHERE id = ?";
  }

  private static final String INDEX_PROBE =
      "SELECT i.indisvalid, t.relname FROM pg_class c JOIN pg_namespace n ON n.oid ="
          + " c.relnamespace LEFT JOIN pg_index i ON i.indexrelid = c.oid LEFT JOIN pg_class t ON"
          + " t.oid = i.indrelid WHERE c.relname = ? AND n.nspname = current_schema()";

  private static final String SEED =
      "INSERT INTO readmodel_checkpoint (projection_name, last_offset, updated_at) VALUES (?, 0,"
          + " NOW()) ON CONFLICT (projection_name) DO NOTHING";
  private static final String LOCK =
      "SELECT last_offset, epoch FROM readmodel_checkpoint WHERE projection_name = ? FOR UPDATE";
  private static final String SAVE_LOCKED =
      "INSERT INTO readmodel_checkpoint (projection_name, last_offset, epoch, updated_at) VALUES"
          + " (?, ?, ?, NOW()) ON CONFLICT (projection_name) DO UPDATE SET last_offset ="
          + " EXCLUDED.last_offset, epoch = GREATEST(readmodel_checkpoint.epoch,"
          + " EXCLUDED.epoch), updated_at = NOW() WHERE EXCLUDED.last_offset >"
          + " readmodel_checkpoint.last_offset";

  /** {@code ProjectionIndex.forFilter(P, "name", Sort.unsorted())}. */
  private static final ProjectionIndex NAME_INDEX =
      ProjectionIndex.forFilter(P, "name", Sort.unsorted());

  private static final String NAME_INDEX_NAME = "p_name_id_ix_0ac6c1bbb0";
  private static final String NAME_INDEX_DDL =
      "CREATE INDEX IF NOT EXISTS "
          + NAME_INDEX_NAME
          + " ON p_view ((data->>'name') COLLATE \"C\", id COLLATE \"C\")";

  private static final String FIELD_RULE =
      "A sort, filter or index field must be a top-level read-model field of ASCII letters,"
          + " digits and underscores (no nested path): ";

  private static String tableRule(String shown) {
    return "Projection name '"
        + shown
        + "' cannot name a read-model table: it must match [a-z_][a-z0-9_]{0,57} — lower-case"
        + " ASCII letters, digits and underscores, starting with a letter or an underscore, at"
        + " most 58 characters (the table is the name plus '_view'). The name is used exactly as"
        + " given, never rewritten, so that no two projection names share a table: rename the"
        + " projection, e.g. 'order_summary' instead of 'order-summary' or 'OrderSummary'.";
  }

  private static String encryptedRefusal(String field, String type) {
    return "Field '"
        + field
        + "' of "
        + type
        + " is @Encrypted: its stored text is ciphertext, so it cannot be filtered or sorted"
        + " (keep a separate, non-sensitive field for the query)";
  }

  private static final String HANDED_INDEX_REFUSAL =
      "declareIndex on a transaction-scoped repository: declare indexes at wiring, on the"
          + " repository itself, not inside a projection batch";

  private static final String AFTER_COMMIT_WARN =
      "Projection '{}' after-commit action failed; the batch stays committed and the remaining"
          + " after-commit actions still run";

  private static final String LOST_RACE_DEBUG =
      "Projection table {} was created concurrently by another session; creating it again finds"
          + " it";

  // ── read models ─────────────────────────────────────────────────────────────────────────────

  record Item(String name, int qty) {}

  record Person(String id, @Encrypted(subjectId = "id") String ssn, String city) {}

  record Address(@Encrypted(subjectId = "zip") String street, String zip) {}

  record Resident(String id, @JsonUnwrapped(prefix = "home_") Address address) {}

  record Account(String id, @Encrypted(subjectId = "id") String fieldName) {}

  /** An {@code @Encrypted} component named like a repository column. */
  record Odd(String id, @Encrypted(subjectId = "id") String version) {}

  /** An unwrapped record with a prefix and a suffix. */
  record Office(String id, @JsonUnwrapped(prefix = "work_", suffix = "_enc") Address address) {}

  /** A disabled {@code @JsonUnwrapped}: the record is nested, not unwrapped. */
  record Nested(String id, @JsonUnwrapped(enabled = false) Address address) {}

  /** What a stored {@link Item} does not bind to: its {@code name} is an unknown property. */
  record Strict(int qty) {}

  /** A read model whose serialization always fails: its one property throws. */
  static final class Unserializable {
    public String getBoom() {
      throw new IllegalStateException("cannot serialize");
    }
  }

  /** A pool that hands out {@code autoCommit=false} connections, as a configured pool may. */
  static final class ManualCommitDataSource extends PGSimpleDataSource {
    @Override
    public Connection getConnection() throws SQLException {
      Connection connection = super.getConnection();
      connection.setAutoCommit(false);
      return connection;
    }
  }

  static PGSimpleDataSource dataSource;
  static ManualCommitDataSource manualDataSource;

  private RecordingDataSource recorder;
  private JdbcProjectionRepository repo;

  @BeforeAll
  static void initPools() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    manualDataSource = new ManualCommitDataSource();
    manualDataSource.setUrl(PG.getJdbcUrl());
    manualDataSource.setUser(PG.getUsername());
    manualDataSource.setPassword(PG.getPassword());
  }

  /**
   * Every test starts from an empty schema with only the read-model series applied, and a fresh
   * repository over a recorder over the autoCommit=true pool.
   */
  @BeforeEach
  void freshSchema() throws SQLException {
    execute("DROP SCHEMA public CASCADE");
    execute("CREATE SCHEMA public");
    ReadModelTestSchema.apply(dataSource);
    recorder = new RecordingDataSource(dataSource);
    repo = repository(mapper());
  }

  private static ObjectMapper mapper() {
    return JdbcProjectionRepository.createObjectMapper(null);
  }

  private JdbcProjectionRepository repository(ObjectMapper mapper) {
    return new JdbcProjectionRepository(recorder, mapper);
  }

  /** Switches the test to a recorder over the autoCommit=false pool. */
  private void useManualCommitPool() {
    recorder = new RecordingDataSource(manualDataSource);
    repo = repository(mapper());
  }

  private static void execute(String sql) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute(sql);
    }
  }

  private static long checkpoint(ProjectionName name) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT last_offset FROM readmodel_checkpoint WHERE projection_name = ?")) {
      ps.setString(1, name.value());
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : -1L;
      }
    }
  }

  @FunctionalInterface
  interface Updater {
    void update(ProjectionRepository handed);
  }

  /** A batch of projection {@link #P} advancing its checkpoint to {@code offset}. */
  private void batch(long offset, Updater updater) {
    repo.executeAtomically(P, List.of(), GlobalOffset.of(offset), 0L, updater::update, null);
  }

  private static List<String> concat(List<String>... parts) {
    var all = new ArrayList<String>();
    for (List<String> part : parts) {
      all.addAll(part);
    }
    return all;
  }

  /** One statement on connection {@code n} of the autoCommit=true pool. */
  private static List<String> single(int n, String event) {
    return List.of("open #" + n, "#" + n + " " + event, "#" + n + " close");
  }

  private static void assertNoLog(LogCapture log) {
    for (Level level : List.of(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR)) {
      assertThat(log.events(level)).as("%s events", level).isEmpty();
    }
  }

  // ── Golden transcripts ──────────────────────────────────────────────────────────────────────

  @Test
  void createIndexSql_isTodaysStatement() {
    assertThat(JdbcProjectionRepository.createIndexSql(NAME_INDEX)).isEqualTo(NAME_INDEX_DDL);
    assertThat(JdbcProjectionRepository.indexName(NAME_INDEX)).isEqualTo(NAME_INDEX_NAME);
  }

  @Test
  void indexName_descendingKeysAndRepositoryColumns() {
    // A descending key adds "_desc" to the readable part and " DESC" to its column.
    assertIndex(
        ProjectionIndex.forSort(P, Sort.by("qty", SortDirection.DESC)),
        "p_qty_desc_id_ix_95c543cea2",
        "CREATE INDEX IF NOT EXISTS p_qty_desc_id_ix_95c543cea2 ON p_view ((data->>'qty') COLLATE"
            + " \"C\" DESC, id COLLATE \"C\")");
    // A repository column is its own expression; a key naming the id replaces the tiebreak.
    assertIndex(
        new ProjectionIndex(
            P,
            List.of(
                new Sort.Order("version", SortDirection.ASC),
                new Sort.Order("id", SortDirection.DESC))),
        "p_version_id_desc_ix_16183b6853",
        "CREATE INDEX IF NOT EXISTS p_version_id_desc_ix_16183b6853 ON p_view (version, id COLLATE"
            + " \"C\" DESC)");
    assertIndex(
        ProjectionIndex.forSort(P, Sort.by("updated_at", SortDirection.DESC)),
        "p_updated_at_desc_id_ix_ac57785524",
        "CREATE INDEX IF NOT EXISTS p_updated_at_desc_id_ix_ac57785524 ON p_view (updated_at DESC,"
            + " id COLLATE \"C\")");
  }

  @Test
  void indexName_readablePartIsCutAt49Characters() {
    String a41 = "a".repeat(41);
    String a42 = "a".repeat(42);
    String c40 = "c".repeat(40);

    // 49 readable characters: kept whole (the name is exactly 63 bytes).
    assertIndex(
        ProjectionIndex.forFilter(ProjectionName.of(a41), "name", Sort.unsorted()),
        a41 + "_name_id_ix_0fbeffaaa7",
        "CREATE INDEX IF NOT EXISTS "
            + a41
            + "_name_id_ix_0fbeffaaa7 ON "
            + a41
            + "_view ((data->>'name') COLLATE \"C\", id COLLATE \"C\")");
    // 50 readable characters: the last one is cut.
    assertIndex(
        ProjectionIndex.forFilter(ProjectionName.of(a42), "name", Sort.unsorted()),
        a42 + "_name_i_ix_74cb357177",
        "CREATE INDEX IF NOT EXISTS "
            + a42
            + "_name_i_ix_74cb357177 ON "
            + a42
            + "_view ((data->>'name') COLLATE \"C\", id COLLATE \"C\")");
    // The readable part is lower-cased and cut mid-key; the column keeps the field's case.
    ProjectionIndex mixed =
        ProjectionIndex.forSort(ProjectionName.of(c40), Sort.by("SomeField", SortDirection.DESC));
    String mixedDdl =
        "CREATE INDEX IF NOT EXISTS "
            + c40
            + "_somefiel_ix_6f1cae8833 ON "
            + c40
            + "_view ((data->>'SomeField') COLLATE \"C\" DESC, id COLLATE \"C\")";
    assertIndex(mixed, c40 + "_somefiel_ix_6f1cae8833", mixedDdl);

    // PostgreSQL takes the 63-byte name as it is.
    repo.declareIndex(mixed);
    assertThat(recorder.transcript()).contains("#2 execute " + mixedDdl);
  }

  private static void assertIndex(ProjectionIndex index, String name, String ddl) {
    assertThat(JdbcProjectionRepository.indexName(index)).isEqualTo(name);
    assertThat(JdbcProjectionRepository.createIndexSql(index)).isEqualTo(ddl);
  }

  @Test
  void transcript_firstSaveThenRead() {
    repo.save(P, "a", new Item("x", 1));

    // The table is created by its own statement on a connection of its own, without a probe.
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(
            concat(
                single(1, "execute " + createTable("p_view")),
                single(2, "prepare " + upsert("p_view"))));

    // The table is remembered: the next read creates nothing.
    recorder.clear();
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(single(1, "prepare " + selectById("p_view")));
  }

  @Test
  void transcript_secondInstanceRunsTheCreationOnce() {
    repo.save(P, "a", new Item("x", 1));
    JdbcProjectionRepository second = repository(mapper());
    recorder.clear();

    // A second instance keeps its own table set: it runs the creation once, then remembers it.
    assertThat(second.findById(P, "a", Item.class)).isPresent();
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(
            concat(
                single(1, "execute " + createTable("p_view")),
                single(2, "prepare " + selectById("p_view"))));

    recorder.clear();
    assertThat(second.findById(P, "a", Item.class)).isPresent();
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(single(1, "prepare " + selectById("p_view")));
  }

  @Test
  void transcript_declareIndex() {
    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      repo.declareIndex(NAME_INDEX);

      // Table creation, then the catalog probe and the DDL on one connection.
      assertThat(recorder.transcript())
          .containsExactly(
              "open #1",
              "#1 execute " + createTable("p_view"),
              "#1 close",
              "open #2",
              "#2 prepare " + INDEX_PROBE,
              "#2 execute " + NAME_INDEX_DDL,
              "#2 close");

      // Declared again: the table is remembered, the index found, nothing executed.
      recorder.clear();
      repo.declareIndex(NAME_INDEX);
      assertThat(recorder.transcript())
          .containsExactlyElementsOf(single(1, "prepare " + INDEX_PROBE));
      assertThat(log.events(Level.INFO)).hasSize(1);
    }
  }

  @Test
  void indexCreation_infoLogOnRepositoryCategory() {
    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      repo.declareIndex(NAME_INDEX);

      List<ILoggingEvent> info = log.events(Level.INFO);
      assertThat(info).hasSize(1);
      assertThat(info.getFirst().getLoggerName())
          .isEqualTo("org.streamrune.postgres.JdbcProjectionRepository");
      assertThat(info.getFirst().getMessage()).isEqualTo("Created projection index: {}");
      assertThat(info.getFirst().getArgumentArray()).containsExactly(NAME_INDEX_DDL);
      assertThat(info.getFirst().getFormattedMessage())
          .isEqualTo("Created projection index: " + NAME_INDEX_DDL);

      // A second declaration, by this or by another instance, finds the index and logs nothing.
      repo.declareIndex(NAME_INDEX);
      repository(mapper()).declareIndex(NAME_INDEX);
      assertThat(log.events(Level.INFO)).hasSize(1);
      assertThat(log.events(Level.TRACE)).isEmpty();
      assertThat(log.events(Level.DEBUG)).isEmpty();
      assertThat(log.events(Level.WARN)).isEmpty();
      assertThat(log.events(Level.ERROR)).isEmpty();
    }
  }

  @Test
  void transcript_versionedSaveAndDelete() {
    repo.save(P, "a", new Item("x", 1));
    recorder.clear();

    repo.save(P, "a", new Item("y", 2), 0L);
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(single(1, "prepare " + versionedUpdate("p_view")));

    recorder.clear();
    repo.delete(P, "a");
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(single(1, "prepare DELETE FROM p_view WHERE id = ?"));
  }

  @Test
  void transcript_findAllStreamsInAFetchSizeTransaction() {
    repo.save(P, "a", new Item("x", 1));
    recorder.clear();

    assertThat(repo.findAll(P, Item.class)).containsExactly(new Item("x", 1));
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 prepare SELECT data FROM p_view",
            "#1 fetchSize=500",
            "#1 commit",
            "#1 autoCommit=true",
            "#1 close");
  }

  @Test
  void transcript_lockedReads() {
    repo.save(P, "a", new Item("x", 1));
    recorder.clear();

    assertThat(repo.findById(P, "a", Item.class, LockMode.PESSIMISTIC))
        .contains(new Versioned<>(new Item("x", 1), 0L));
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(
            single(1, "prepare SELECT data, version FROM p_view WHERE id = ? FOR UPDATE"));

    recorder.clear();
    assertThat(repo.findById(P, "a", Item.class, LockMode.OPTIMISTIC))
        .contains(new Versioned<>(new Item("x", 1), 0L));
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(
            single(1, "prepare SELECT data, version FROM p_view WHERE id = ?"));
  }

  @Test
  void transcript_pagedRead() {
    repo.save(P, "a", new Item("x", 1));
    recorder.clear();

    var page =
        repo.findAllBy(
            P,
            Item.class,
            FieldEquals.of("name", "x"),
            PageRequest.of(0, 10, Sort.by("qty", SortDirection.DESC)));

    assertThat(page.content()).containsExactly(new Item("x", 1));
    assertThat(page.totalElements()).isEqualTo(1L);
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(
            single(
                1,
                "prepare SELECT total.n, page.data FROM (SELECT count(*) AS n FROM p_view WHERE"
                    + " (data->>'name') COLLATE \"C\" = ?) AS total LEFT JOIN LATERAL (SELECT"
                    + " data, (data->>'qty') COLLATE \"C\" AS k0, id COLLATE \"C\" AS k1 FROM"
                    + " p_view WHERE total.n > ? AND (data->>'name') COLLATE \"C\" = ? ORDER BY"
                    + " (data->>'qty') COLLATE \"C\" DESC, id COLLATE \"C\" LIMIT ? OFFSET ?) AS"
                    + " page ON true ORDER BY page.k0 DESC, page.k1"));
  }

  @Test
  void transcript_manualCommitPool() {
    useManualCommitPool();

    repo.save(P, "a", new Item("x", 1));
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 execute " + createTable("p_view"),
            "#1 commit",
            "#1 close",
            "open #2",
            "#2 prepare " + upsert("p_view"),
            "#2 commit",
            "#2 close");

    recorder.clear();
    repo.save(P, "a", new Item("y", 2), 0L);
    repo.delete(P, "a");
    repo.declareIndex(NAME_INDEX);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare " + versionedUpdate("p_view"),
            "#1 commit",
            "#1 close",
            "open #2",
            "#2 prepare DELETE FROM p_view WHERE id = ?",
            "#2 commit",
            "#2 close",
            "open #3",
            "#3 prepare " + INDEX_PROBE,
            "#3 execute " + NAME_INDEX_DDL,
            "#3 commit",
            "#3 close");

    // A read runs without a commit of its own.
    recorder.clear();
    assertThat(repo.findById(P, "a", Item.class)).isEmpty();
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(single(1, "prepare " + selectById("p_view")));
  }

  @Test
  void transcript_batch() throws SQLException {
    repo.prepareReadModel(P);
    recorder.clear();

    batch(
        3,
        handed -> {
          handed.save(P, "a", new Item("x", 1));
          assertThat(handed.findById(P, "a", Item.class, LockMode.PESSIMISTIC)).isPresent();
          // A table nothing prepared is created inside the batch, once.
          handed.save(Q, "a", new Item("x", 1));
          handed.save(Q, "b", new Item("x", 1));
        });

    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 prepare " + SEED,
            "#1 prepare " + LOCK,
            "#1 prepare " + upsert("p_view"),
            "#1 prepare SELECT data, version FROM p_view WHERE id = ? FOR UPDATE",
            "#1 execute " + createTable("q_view"),
            "#1 prepare " + upsert("q_view"),
            "#1 prepare " + upsert("q_view"),
            "#1 prepare " + SAVE_LOCKED,
            "#1 commit",
            "#1 autoCommit=true",
            "#1 close");
    assertThat(checkpoint(P)).isEqualTo(3L);
  }

  @Test
  void transcript_replay() throws SQLException {
    repo.prepareReadModel(P);
    recorder.clear();
    var seen = new ArrayList<List<String>>();

    repo.executeReplay(
        P,
        List.of(),
        handed -> {
          handed.save(P, "a", new Item("x", 1));
          handed.afterCommit(() -> seen.add(recorder.transcript()));
        });

    // The lock a batch takes, the updater's writes, no checkpoint write; after-commit actions run
    // after the COMMIT, the autoCommit restore and the close.
    List<String> expected =
        List.of(
            "open #1",
            "#1 autoCommit=false",
            "#1 prepare " + SEED,
            "#1 prepare " + LOCK,
            "#1 prepare " + upsert("p_view"),
            "#1 commit",
            "#1 autoCommit=true",
            "#1 close");
    assertThat(recorder.transcript()).containsExactlyElementsOf(expected);
    assertThat(seen).containsExactly(expected);
    assertThat(checkpoint(P)).isZero();
  }

  @Test
  void batchBodyStatementFailure_isWrappedWithTheTransactionLabel() {
    var ran = new ArrayList<String>();
    recorder.beforePrepare(
        SEED,
        sameConnection -> {
          throw new SQLException("injected seed failure", "XX000");
        });

    Throwable thrown = catchThrowable(() -> batch(1, handed -> ran.add("updater")));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Transaction failed during projection update");
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected seed failure");
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(ran).isEmpty();
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1", "#1 autoCommit=false", "#1 rollback", "#1 autoCommit=true", "#1 close");

    recorder.clear();
    recorder.beforePrepare(
        SEED,
        sameConnection -> {
          throw new SQLException("injected seed failure", "XX000");
        });
    Throwable replay = catchThrowable(() -> repo.executeReplay(P, List.of(), handed -> {}));
    assertThat(replay)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Transaction failed during projection dead-letter replay");
    assertThat(replay.getCause()).hasMessage("injected seed failure");
  }

  // ── When a created table is remembered ──────────────────────────────────────────────────────

  @Test
  void tablesCreatedInARolledBackBatch_areNotRecorded() {
    RuntimeException boom = new IllegalStateException("boom");

    Throwable thrown =
        catchThrowable(
            () ->
                batch(
                    1,
                    handed -> {
                      handed.save(Q, "a", new Item("x", 1));
                      throw boom;
                    }));

    assertThat(thrown).isSameAs(boom);
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 prepare " + SEED,
            "#1 prepare " + LOCK,
            "#1 execute " + createTable("q_view"),
            "#1 prepare " + upsert("q_view"),
            "#1 rollback",
            "#1 autoCommit=true",
            "#1 close");

    // The rolled-back table was never remembered: a later save outside a batch runs the table
    // creation again.
    recorder.clear();
    repo.save(Q, "a", new Item("x", 1));
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(
            concat(
                single(1, "execute " + createTable("q_view")),
                single(2, "prepare " + upsert("q_view"))));
  }

  @Test
  void tablesCreatedInACommittedBatch_areRecordedAfterTheCommit() {
    batch(1, handed -> handed.save(R, "a", new Item("x", 1)));
    recorder.clear();

    assertThat(repo.findById(R, "a", Item.class)).contains(new Item("x", 1));
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(single(1, "prepare " + selectById("r_view")));
  }

  @Test
  void tablesCreatedInABatchWhoseCloseFailsAfterTheCommit_areRecorded() {
    recorder.failNextCloseAfterCommit();

    Throwable thrown =
        catchThrowable(() -> batch(1, handed -> handed.save(Q, "a", new Item("x", 1))));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to process projection batch atomically");
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure");

    // The tables are recorded right after the COMMIT returned, before the close failed: the next
    // read creates nothing.
    recorder.clear();
    assertThat(repo.findById(Q, "a", Item.class)).contains(new Item("x", 1));
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(single(1, "prepare " + selectById("q_view")));
  }

  @Test
  void prepareReadModelInsideABatch_createsOnAConnectionOfItsOwn() {
    batch(
        1,
        handed -> {
          repo.prepareReadModel(Q);
          handed.save(Q, "a", new Item("x", 1));
        });

    // No joined path: the repository itself creates the table on a second, committed connection,
    // and the batch finds it remembered.
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 prepare " + SEED,
            "#1 prepare " + LOCK,
            "open #2",
            "#2 execute " + createTable("q_view"),
            "#2 close",
            "#1 prepare " + upsert("q_view"),
            "#1 prepare " + SAVE_LOCKED,
            "#1 commit",
            "#1 autoCommit=true",
            "#1 close");
  }

  // ── Table creation under a lost race ────────────────────────────────────────────────────────

  @Test
  void tableCreationInsideABatch_rollsBackToItsSavepointOnEachLostRace() throws SQLException {
    // A type of the table's name makes CREATE TABLE IF NOT EXISTS fail as a lost creation race
    // (42710, duplicate object) on every attempt.
    execute("CREATE DOMAIN q_view AS integer");

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      Throwable thrown =
          catchThrowable(() -> batch(1, handed -> handed.save(Q, "a", new Item("x", 1))));

      assertThat(thrown)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to create projection table: q_view");
      assertLostRaceChain(thrown.getCause());
      assertThat(recorder.transcript())
          .containsExactly(
              "open #1",
              "#1 autoCommit=false",
              "#1 prepare " + SEED,
              "#1 prepare " + LOCK,
              "#1 execute " + createTable("q_view"),
              "#1 rollback to savepoint",
              "#1 execute " + createTable("q_view"),
              "#1 rollback to savepoint",
              "#1 execute " + createTable("q_view"),
              "#1 rollback",
              "#1 autoCommit=true",
              "#1 close");
      assertLostRaceDebugLines(log);
    }
  }

  @Test
  void tableCreationOutsideABatch_retriesOnAConnectionPerAttempt() throws SQLException {
    execute("CREATE DOMAIN q_view AS integer");

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      Throwable thrown = catchThrowable(() -> repo.save(Q, "a", new Item("x", 1)));

      assertThat(thrown)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to create projection table: q_view");
      assertLostRaceChain(thrown.getCause());
      assertThat(recorder.transcript())
          .containsExactlyElementsOf(
              concat(
                  single(1, "execute " + createTable("q_view")),
                  single(2, "execute " + createTable("q_view")),
                  single(3, "execute " + createTable("q_view"))));
      assertLostRaceDebugLines(log);
    }
  }

  /** The third attempt's failure, carrying the second's, carrying the first's, all 42710. */
  private static void assertLostRaceChain(Throwable third) {
    assertThat(third).isExactlyInstanceOf(PSQLException.class);
    assertThat(((SQLException) third).getSQLState()).isEqualTo("42710");
    assertThat(third.getSuppressed()).hasSize(1);
    Throwable second = third.getSuppressed()[0];
    assertThat(second).isExactlyInstanceOf(PSQLException.class);
    assertThat(((SQLException) second).getSQLState()).isEqualTo("42710");
    assertThat(second.getSuppressed()).hasSize(1);
    Throwable first = second.getSuppressed()[0];
    assertThat(first).isExactlyInstanceOf(PSQLException.class);
    assertThat(((SQLException) first).getSQLState()).isEqualTo("42710");
    assertThat(first.getSuppressed()).isEmpty();
  }

  private static void assertLostRaceDebugLines(LogCapture log) {
    List<ILoggingEvent> debug = log.events(Level.DEBUG);
    assertThat(debug).hasSize(2);
    for (ILoggingEvent event : debug) {
      assertThat(event.getLoggerName())
          .isEqualTo("org.streamrune.postgres.JdbcProjectionRepository");
      assertThat(event.getMessage()).isEqualTo(LOST_RACE_DEBUG);
      assertThat(event.getArgumentArray()).containsExactly("q_view");
    }
    assertThat(log.events(Level.TRACE)).isEmpty();
    assertThat(log.events(Level.INFO)).isEmpty();
    assertThat(log.events(Level.WARN)).isEmpty();
    assertThat(log.events(Level.ERROR)).isEmpty();
  }

  // ── Index declaration inside a batch and unusable index names ───────────────────────────────

  @Test
  void declareIndexInsideABatch_runsOnTheRepositoryAndIsRefusedOnTheHandedOne() {
    repo.prepareReadModel(P);
    recorder.clear();
    var failures = new ArrayList<Throwable>();

    batch(
        1,
        handed -> {
          // The repository itself has no batch path: a valid index is created on its own
          // connection; an invalid one is refused by validation.
          repo.declareIndex(NAME_INDEX);
          failures.add(
              catchThrowable(
                  () ->
                      repo.declareIndex(
                          ProjectionIndex.forFilter(P, "bad-field", Sort.unsorted()))));
          failures.add(catchThrowable(() -> repo.declareIndex(null)));
          // The handed repository validates nothing: every index is refused as inside a batch.
          failures.add(catchThrowable(() -> handed.declareIndex(NAME_INDEX)));
          failures.add(
              catchThrowable(
                  () ->
                      handed.declareIndex(
                          ProjectionIndex.forFilter(P, "bad-field", Sort.unsorted()))));
          handed.save(P, "a", new Item("x", 1));
        });

    assertThat(failures.get(0))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(FIELD_RULE + "bad-field")
        .hasNoCause();
    assertThat(failures.get(1))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("index cannot be null");
    assertThat(failures.get(2))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(HANDED_INDEX_REFUSAL);
    assertThat(failures.get(3))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(HANDED_INDEX_REFUSAL);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 prepare " + SEED,
            "#1 prepare " + LOCK,
            "open #2",
            "#2 prepare " + INDEX_PROBE,
            "#2 execute " + NAME_INDEX_DDL,
            "#2 close",
            "#1 prepare " + upsert("p_view"),
            "#1 prepare " + SAVE_LOCKED,
            "#1 commit",
            "#1 autoCommit=true",
            "#1 close");
  }

  @Test
  void foreignRelationUnderTheIndexName_isEventStoreException() throws SQLException {
    repo.prepareReadModel(P);
    execute("CREATE VIEW " + NAME_INDEX_NAME + " AS SELECT 1");
    recorder.clear();

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      Throwable thrown = catchThrowable(() -> repo.declareIndex(NAME_INDEX));

      assertThat(thrown)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage(
              "A relation named "
                  + NAME_INDEX_NAME
                  + " exists and is not an index of p_view: the projection index of that name"
                  + " cannot be created; rename or drop it")
          .hasNoCause();
      assertThat(thrown.getSuppressed()).isEmpty();
      assertThat(recorder.transcript())
          .containsExactlyElementsOf(single(1, "prepare " + INDEX_PROBE));
      assertThat(recorder.openConnections()).isZero();
      assertNoLog(log);
    }

    // Over an autoCommit=false pool the probe's transaction rolls back.
    useManualCommitPool();
    repo.prepareReadModel(P);
    recorder.clear();
    Throwable manual = catchThrowable(() -> repo.declareIndex(NAME_INDEX));
    assertThat(manual).isExactlyInstanceOf(EventStoreException.class).hasNoCause();
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + INDEX_PROBE, "#1 rollback", "#1 close");
  }

  @Test
  void indexOfAnotherTableUnderTheIndexName_isEventStoreException() throws SQLException {
    repo.prepareReadModel(P);
    repo.prepareReadModel(Q);
    execute("CREATE INDEX " + NAME_INDEX_NAME + " ON q_view (id)");

    Throwable thrown = catchThrowable(() -> repo.declareIndex(NAME_INDEX));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(
            "A relation named "
                + NAME_INDEX_NAME
                + " exists and is not an index of p_view: the projection index of that name"
                + " cannot be created; rename or drop it")
        .hasNoCause();
  }

  @Test
  void invalidIndexUnderTheIndexName_isEventStoreException() throws SQLException {
    repo.save(P, "a", new Item("x", 1));
    repo.save(P, "b", new Item("x", 2));
    // A failed CONCURRENTLY build leaves an INVALID index of that name on the table.
    assertThat(
            catchThrowable(
                () ->
                    execute(
                        "CREATE UNIQUE INDEX CONCURRENTLY "
                            + NAME_INDEX_NAME
                            + " ON p_view ((data->>'name'))")))
        .isInstanceOf(SQLException.class);
    recorder.clear();

    Throwable thrown = catchThrowable(() -> repo.declareIndex(NAME_INDEX));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(
            "Projection index "
                + NAME_INDEX_NAME
                + " exists but is INVALID (a failed or still running CREATE INDEX CONCURRENTLY):"
                + " wait for a running build to finish, or run DROP INDEX "
                + NAME_INDEX_NAME
                + " and start again")
        .hasNoCause();
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(single(1, "prepare " + INDEX_PROBE));
  }

  // ── The optimistic lock ─────────────────────────────────────────────────────────────────────

  @Test
  void optimisticLock_isNotWrapped() {
    repo.save(P, "a", new Item("x", 1));
    recorder.clear();

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      Throwable thrown = catchThrowable(() -> repo.save(P, "a", new Item("y", 2), 3L));

      assertThat(thrown)
          .isExactlyInstanceOf(OptimisticLockException.class)
          .hasMessage("Optimistic lock conflict on projection 'p/a': expected version 3")
          .hasNoCause();
      assertThat(thrown.getSuppressed()).isEmpty();
      assertThat(recorder.transcript())
          .containsExactlyElementsOf(single(1, "prepare " + versionedUpdate("p_view")));
      assertThat(recorder.openConnections()).isZero();
      assertNoLog(log);
    }
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
  }

  @Test
  void optimisticLock_isThrownInsideTheManualCommitAndRollsItBack() {
    useManualCommitPool();
    repo.save(P, "a", new Item("x", 1));
    recorder.clear();

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      Throwable thrown = catchThrowable(() -> repo.save(P, "a", new Item("y", 2), 3L));

      assertThat(thrown)
          .isExactlyInstanceOf(OptimisticLockException.class)
          .hasMessage("Optimistic lock conflict on projection 'p/a': expected version 3")
          .hasNoCause();
      assertThat(thrown.getSuppressed()).isEmpty();
      assertThat(recorder.transcript())
          .containsExactly(
              "open #1", "#1 prepare " + versionedUpdate("p_view"), "#1 rollback", "#1 close");
      assertNoLog(log);
    }
  }

  @Test
  void optimisticLockMessage() {
    repo.save(P, "a\nb", new Item("x", 1));

    Throwable thrown = catchThrowable(() -> repo.save(P, "a\nb", new Item("y", 2), 3L));

    // Unsanitized, as today: the id's line break stays in the message.
    assertThat(thrown)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Optimistic lock conflict on projection 'p/a\nb': expected version 3");
  }

  @Test
  void optimisticLockInsideABatch_isNotWrappedAndDoesNotAbort() throws SQLException {
    repo.save(P, "a", new Item("x", 1));
    var failures = new ArrayList<Throwable>();

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      batch(
          1,
          handed -> {
            failures.add(catchThrowable(() -> handed.save(P, "a", new Item("y", 2), 5L)));
            failures.add(catchThrowable(() -> repo.save(P, "a", new Item("y", 2), 6L)));
            handed.save(P, "b", new Item("z", 3));
          });
      assertNoLog(log);
    }

    assertThat(failures.get(0))
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Optimistic lock conflict on projection 'p/a': expected version 5")
        .hasNoCause();
    assertThat(failures.get(1))
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Optimistic lock conflict on projection 'p/a': expected version 6")
        .hasNoCause();
    assertThat(repo.findById(P, "b", Item.class)).contains(new Item("z", 3));
    assertThat(repo.findById(P, "a", Item.class, LockMode.OPTIMISTIC))
        .contains(new Versioned<>(new Item("x", 1), 0L));
    assertThat(checkpoint(P)).isEqualTo(1L);
  }

  // ── After-commit actions ────────────────────────────────────────────────────────────────────

  @Test
  void afterCommitActions_runAfterTheCommitAndTheClose() {
    var seen = new ArrayList<List<String>>();
    var openAtRun = new ArrayList<Integer>();
    var repositoryActionSaw = new ArrayList<List<String>>();

    batch(
        1,
        handed -> {
          handed.save(P, "a", new Item("x", 1));
          handed.afterCommit(
              () -> {
                seen.add(recorder.transcript());
                openAtRun.add(recorder.openConnections());
              });
          // The repository itself has no batch path: its action runs at once, before the commit.
          repo.afterCommit(() -> repositoryActionSaw.add(recorder.transcript()));
        });

    assertThat(seen).hasSize(1);
    assertThat(seen.getFirst()).endsWith("#1 commit", "#1 autoCommit=true", "#1 close");
    assertThat(openAtRun).containsExactly(0);
    assertThat(repositoryActionSaw).hasSize(1);
    assertThat(repositoryActionSaw.getFirst()).doesNotContain("#1 commit");
  }

  @Test
  void afterCommitActions_areDiscardedOnRollback() {
    var ran = new ArrayList<String>();
    RuntimeException boom = new IllegalStateException("boom");
    var kept = new ArrayList<ProjectionRepository>();

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      Throwable thrown =
          catchThrowable(
              () ->
                  batch(
                      1,
                      handed -> {
                        kept.add(handed);
                        handed.afterCommit(() -> ran.add("handed"));
                        throw boom;
                      }));

      assertThat(thrown).isSameAs(boom);
      assertThat(thrown.getSuppressed()).isEmpty();
      // One registered after the rollback, through the kept repository, is discarded too.
      kept.getFirst().afterCommit(() -> ran.add("late"));
      assertThat(ran).isEmpty();
      assertNoLog(log);
    }
  }

  @Test
  void afterCommitRegisteredAfterTheActionsRan_runsAtOnce() {
    var ran = new ArrayList<String>();
    var kept = new ArrayList<ProjectionRepository>();

    batch(1, kept::add);
    kept.getFirst().afterCommit(() -> ran.add("late"));

    assertThat(ran).containsExactly("late");
  }

  @Test
  void afterCommitOutsideABatch_runsAtOnce() {
    var ran = new ArrayList<String>();

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      repo.afterCommit(() -> ran.add("now"));

      assertThat(ran).containsExactly("now");
      assertThat(recorder.transcript()).isEmpty();
      assertThat(catchThrowable(() -> repo.afterCommit(null)))
          .isExactlyInstanceOf(NullPointerException.class);
      assertNoLog(log);
    }
  }

  @Test
  void afterCommitNullAction_insideABatch_isIllegalArgument() {
    var failures = new ArrayList<Throwable>();

    batch(1, handed -> failures.add(catchThrowable(() -> handed.afterCommit(null))));

    assertThat(failures.getFirst())
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("action cannot be null");
  }

  @Test
  void afterCommitFailure_warnsOnRepositoryCategory() throws SQLException {
    var ran = new ArrayList<String>();
    RuntimeException failure = new IllegalStateException("evict failed");

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      batch(
          1,
          handed -> {
            handed.save(P, "a", new Item("x", 1));
            handed.afterCommit(() -> ran.add("first"));
            handed.afterCommit(
                () -> {
                  throw failure;
                });
            handed.afterCommit(() -> ran.add("third"));
          });

      assertThat(ran).containsExactly("first", "third");
      List<ILoggingEvent> warn = log.events(Level.WARN);
      assertThat(warn).hasSize(1);
      assertThat(warn.getFirst().getLoggerName())
          .isEqualTo("org.streamrune.postgres.JdbcProjectionRepository");
      assertThat(warn.getFirst().getMessage()).isEqualTo(AFTER_COMMIT_WARN);
      assertThat(warn.getFirst().getFormattedMessage())
          .startsWith("Projection 'p' after-commit action failed;")
          .isEqualTo(
              "Projection 'p' after-commit action failed; the batch stays committed and the"
                  + " remaining after-commit actions still run");
      assertThat(warn.getFirst().getThrowableProxy().getClassName())
          .isEqualTo("java.lang.IllegalStateException");
      assertThat(warn.getFirst().getThrowableProxy().getMessage()).isEqualTo("evict failed");
      assertThat(log.events(Level.TRACE)).isEmpty();
      assertThat(log.events(Level.DEBUG)).isEmpty();
      assertThat(log.events(Level.INFO)).isEmpty();
      assertThat(log.events(Level.ERROR)).isEmpty();
    }
    // The batch stays committed.
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
    assertThat(checkpoint(P)).isEqualTo(1L);
  }

  @Test
  void afterCommitError_propagatesAfterACleanCommit() throws SQLException {
    var ran = new ArrayList<String>();
    Error error = new Error("action error");

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      Throwable thrown =
          catchThrowable(
              () ->
                  batch(
                      1,
                      handed -> {
                        handed.save(P, "a", new Item("x", 1));
                        handed.afterCommit(() -> ran.add("first"));
                        handed.afterCommit(
                            () -> {
                              throw error;
                            });
                        handed.afterCommit(() -> ran.add("third"));
                      }));

      assertThat(thrown).isSameAs(error);
      assertThat(thrown.getSuppressed()).isEmpty();
      assertNoLog(log);
    }
    // The Error ends the run: the later action does not run; the batch stays committed.
    assertThat(ran).containsExactly("first");
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
    assertThat(checkpoint(P)).isEqualTo(1L);
  }

  @Test
  void afterCommitError_isSuppressedOnTheInFlightFailure() {
    var ran = new ArrayList<String>();
    Error error = new Error("action error");
    recorder.failNextCloseAfterCommit();

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      Throwable thrown =
          catchThrowable(
              () ->
                  batch(
                      1,
                      handed -> {
                        handed.save(P, "a", new Item("x", 1));
                        handed.afterCommit(
                            () -> {
                              throw new IllegalStateException("evict failed");
                            });
                        handed.afterCommit(
                            () -> {
                              throw error;
                            });
                        handed.afterCommit(() -> ran.add("third"));
                      }));

      // The batch's failure after the COMMIT was issued (here: the close) stays the one thrown.
      assertThat(thrown)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to process projection batch atomically");
      assertThat(thrown.getCause())
          .isExactlyInstanceOf(SQLException.class)
          .hasMessage("injected close failure");
      assertThat(thrown.getSuppressed()).containsExactly(error);
      assertThat(ran).isEmpty();
      assertThat(log.events(Level.WARN)).hasSize(1);
      assertThat(log.events(Level.WARN).getFirst().getFormattedMessage())
          .startsWith("Projection 'p' after-commit action failed;");
      assertThat(log.events(Level.ERROR)).isEmpty();
    }
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
  }

  // ── (De)serialization failures ──────────────────────────────────────────────────────────────

  @Test
  void serializationFailure_afterTheTableCreationAndBeforeTheWrite() {
    Throwable thrown = catchThrowable(() -> repo.save(P, "a\nb", new Unserializable()));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to save projection: p/ab");
    assertThat(thrown.getCause()).isExactlyInstanceOf(JsonMappingException.class);
    // The table is created first; nothing is written.
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(single(1, "execute " + createTable("p_view")));

    recorder.clear();
    Throwable versioned = catchThrowable(() -> repo.save(P, "a", new Unserializable(), 0L));
    assertThat(versioned)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to save projection with version check: p/a");
    assertThat(versioned.getCause()).isExactlyInstanceOf(JsonMappingException.class);
    assertThat(recorder.transcript()).isEmpty();
  }

  @Test
  void deserializationFailure_messages() {
    repo.save(P, "a\nb", new Item("x", 1));
    recorder.clear();

    assertStorageFailure(
        () -> repo.findById(P, "a\nb", Strict.class), "Failed to find projection: p/ab");
    assertStorageFailure(
        () -> repo.findById(P, "a\nb", Strict.class, LockMode.PESSIMISTIC),
        "Failed to find projection with lock: p/ab");
    assertStorageFailure(() -> repo.findAll(P, Strict.class), "Failed to find all projections: p");
    assertStorageFailure(
        () -> repo.findAll(P, Strict.class, PageRequest.of(0, 10)),
        "Failed to find paginated projections: p");
    assertStorageFailure(
        () -> repo.findAllBy(P, Strict.class, FieldEquals.of("name", "x"), PageRequest.of(0, 10)),
        "Failed to find paginated projections: p");
    // Every row is turned into an object after its connection closed.
    assertThat(recorder.openConnections()).isZero();
  }

  @Test
  void serializationFailureInsideABatch_doesNotAbort() throws SQLException {
    var failures = new ArrayList<Throwable>();

    batch(
        1,
        handed -> {
          failures.add(catchThrowable(() -> handed.save(P, "a", new Unserializable())));
          failures.add(catchThrowable(() -> handed.save(P, "a", new Unserializable(), 0L)));
          handed.save(P, "a", new Item("x", 1));
        });

    assertThat(failures)
        .extracting(Throwable::getMessage)
        .containsExactly(
            "Failed to save projection: p/a", "Failed to save projection with version check: p/a");
    assertThat(failures)
        .allSatisfy(f -> assertThat(f).isExactlyInstanceOf(EventStoreException.class));
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
    assertThat(checkpoint(P)).isEqualTo(1L);
  }

  private static void assertStorageFailure(ThrowingCallable call, String message) {
    Throwable thrown = catchThrowable(call);
    assertThat(thrown).isExactlyInstanceOf(EventStoreException.class).hasMessage(message);
    assertThat(thrown.getCause()).isExactlyInstanceOf(UnrecognizedPropertyException.class);
  }

  // ── The @Encrypted key refusal ──────────────────────────────────────────────────────────────

  @Test
  void encryptedKeyRefusal_message() {
    PageRequest unsorted = PageRequest.of(0, 10);

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      assertRefused(
          () -> repo.findAllBy(P, Person.class, FieldEquals.of("ssn", "x"), unsorted),
          encryptedRefusal("ssn", "Person"));
      assertRefused(
          () ->
              repo.findAll(
                  P, Person.class, PageRequest.of(0, 10, Sort.by("ssn", SortDirection.DESC))),
          encryptedRefusal("ssn", "Person"));
      assertRefused(
          () ->
              repo.findAllBy(
                  P,
                  Person.class,
                  FieldEquals.of("city", "x"),
                  PageRequest.of(0, 10, Sort.by("ssn"))),
          encryptedRefusal("ssn", "Person"));
      // Under @JsonUnwrapped the key carries the annotation's prefix; the type named is the outer
      // one.
      assertRefused(
          () -> repo.findAllBy(P, Resident.class, FieldEquals.of("home_street", "x"), unsorted),
          encryptedRefusal("home_street", "Resident"));
      assertRefused(
          () -> repo.findAll(P, Resident.class, PageRequest.of(0, 10, Sort.by("home_street"))),
          encryptedRefusal("home_street", "Resident"));
      // The encryption refusal comes before the table-name rule.
      assertRefused(
          () ->
              repo.findAllBy(
                  ProjectionName.of("Bad"), Person.class, FieldEquals.of("ssn", "x"), unsorted),
          encryptedRefusal("ssn", "Person"));
      // Every refusal is decided before any I/O, and logs nothing.
      assertThat(recorder.transcript()).isEmpty();
      assertNoLog(log);
    }
  }

  @Test
  void encryptedKeyRefusal_unwrappedSuffixAndDisabledUnwrapping() {
    PageRequest unsorted = PageRequest.of(0, 10);

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      // The key carries the annotation's prefix and suffix.
      assertRefused(
          () -> repo.findAllBy(P, Office.class, FieldEquals.of("work_street_enc", "x"), unsorted),
          encryptedRefusal("work_street_enc", "Office"));
      assertRefused(
          () -> repo.findAll(P, Office.class, PageRequest.of(0, 10, Sort.by("work_street_enc"))),
          encryptedRefusal("work_street_enc", "Office"));
      assertThat(recorder.transcript()).isEmpty();
      assertNoLog(log);
    }
    // Without the suffix, or by the inner name, nothing is refused.
    assertThat(
            repo.findAllBy(P, Office.class, FieldEquals.of("work_street", "x"), unsorted).content())
        .isEmpty();
    assertThat(repo.findAllBy(P, Office.class, FieldEquals.of("street", "x"), unsorted).content())
        .isEmpty();
    // A disabled @JsonUnwrapped contributes no name: neither the inner one nor the component's.
    assertThat(repo.findAllBy(P, Nested.class, FieldEquals.of("street", "x"), unsorted).content())
        .isEmpty();
    assertThat(repo.findAll(P, Nested.class, PageRequest.of(0, 10, Sort.by("address"))).content())
        .isEmpty();
  }

  @Test
  void encryptedKeyRefusal_quietBranches() {
    PageRequest unsorted = PageRequest.of(0, 10);

    // The unwrapped component's own name, a plain field, a repository column named like an
    // encrypted component, and a type that is not a record: none is refused.
    assertThat(repo.findAllBy(P, Resident.class, FieldEquals.of("street", "x"), unsorted).content())
        .isEmpty();
    assertThat(repo.findAllBy(P, Person.class, FieldEquals.of("city", "x"), unsorted).content())
        .isEmpty();
    assertThat(repo.findAll(P, Odd.class, PageRequest.of(0, 10, Sort.by("version"))).content())
        .isEmpty();
    assertThat(repo.findAllBy(P, Map.class, FieldEquals.of("ssn", "x"), unsorted).content())
        .isEmpty();
    // An index carries no read-model type: an index on an encrypted field's name is created.
    recorder.clear();
    repo.declareIndex(ProjectionIndex.forFilter(P, "ssn", Sort.unsorted()));
    assertThat(recorder.transcript())
        .contains(
            "#1 execute CREATE INDEX IF NOT EXISTS p_ssn_id_ix_cde4b3dfee ON p_view"
                + " ((data->>'ssn') COLLATE \"C\", id COLLATE \"C\")");
  }

  @Test
  void encryptedKeyRefusalInsideABatch_doesNotAbort() throws SQLException {
    var failures = new ArrayList<Throwable>();

    batch(
        1,
        handed -> {
          failures.add(
              catchThrowable(
                  () ->
                      handed.findAllBy(
                          P, Person.class, FieldEquals.of("ssn", "x"), PageRequest.of(0, 10))));
          failures.add(
              catchThrowable(
                  () -> handed.findAll(P, Person.class, PageRequest.of(0, 10, Sort.by("ssn")))));
          handed.save(P, "a", new Item("x", 1));
        });

    assertThat(failures)
        .allSatisfy(
            f ->
                assertThat(f)
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage(encryptedRefusal("ssn", "Person")));
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
    assertThat(checkpoint(P)).isEqualTo(1L);
  }

  @Test
  void encryptedFieldsCacheIsPerInstance() {
    PageRequest unsorted = PageRequest.of(0, 10);
    ObjectMapper snake = mapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    // The default-naming repository asked first.
    JdbcProjectionRepository plain = repository(mapper());
    JdbcProjectionRepository snaked = repository(snake);
    assertRefused(
        () -> plain.findAllBy(P, Account.class, FieldEquals.of("fieldName", "x"), unsorted),
        encryptedRefusal("fieldName", "Account"));
    assertThat(
            plain
                .findAllBy(P, Account.class, FieldEquals.of("field_name", "x"), unsorted)
                .content())
        .isEmpty();
    assertRefused(
        () -> snaked.findAllBy(P, Account.class, FieldEquals.of("field_name", "x"), unsorted),
        encryptedRefusal("field_name", "Account"));
    // The Java component name is refused under every naming.
    assertRefused(
        () -> snaked.findAllBy(P, Account.class, FieldEquals.of("fieldName", "x"), unsorted),
        encryptedRefusal("fieldName", "Account"));

    // The snake-case repository asked first.
    JdbcProjectionRepository snakedFirst =
        repository(mapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE));
    JdbcProjectionRepository plainSecond = repository(mapper());
    assertRefused(
        () -> snakedFirst.findAll(P, Account.class, PageRequest.of(0, 10, Sort.by("field_name"))),
        encryptedRefusal("field_name", "Account"));
    assertThat(
            plainSecond
                .findAll(P, Account.class, PageRequest.of(0, 10, Sort.by("field_name")))
                .content())
        .isEmpty();
    assertRefused(
        () -> plainSecond.findAll(P, Account.class, PageRequest.of(0, 10, Sort.by("fieldName"))),
        encryptedRefusal("fieldName", "Account"));
  }

  private static void assertRefused(ThrowingCallable call, String message) {
    assertThat(catchThrowable(call))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(message)
        .hasNoCause();
  }

  // ── Validation ──────────────────────────────────────────────────────────────────────────────

  @Test
  void nullNameOnExecuteAtomically_isNullPointer() {
    var ran = new ArrayList<String>();

    Throwable thrown =
        catchThrowable(
            () ->
                repo.executeAtomically(
                    null, List.of(), GlobalOffset.of(1), 0L, handed -> ran.add("updater"), null));

    // The JVM's helpful-NPE text, naming the parameter the table-name rule dereferences: a refactor
    // must keep that dereference on a parameter named projectionName.
    assertThat(thrown)
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage(
            "Cannot invoke \"org.streamrune.core.types.ProjectionName.value()\" because"
                + " \"projectionName\" is null");
    assertThat(ran).isEmpty();
    assertThat(recorder.transcript()).isEmpty();

    assertThat(
            catchThrowable(
                () ->
                    repo.executeAtomically(
                        ProjectionName.of("Bad"),
                        List.of(),
                        GlobalOffset.of(1),
                        0L,
                        handed -> {},
                        null)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(tableRule("Bad"));
    // The replay checks the name for null first.
    assertThat(catchThrowable(() -> repo.executeReplay(null, List.of(), handed -> {})))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("projectionName cannot be null");
    assertThat(
            catchThrowable(
                () -> repo.executeReplay(ProjectionName.of("Bad"), List.of(), handed -> {})))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(tableRule("Bad"));
    assertThat(recorder.transcript()).isEmpty();
  }

  @Test
  void validationMessages() {
    Item item = new Item("x", 1);
    PageRequest page = PageRequest.of(0, 10);
    FieldEquals filter = FieldEquals.of("name", "x");
    ProjectionName upper = ProjectionName.of("Orders");

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      // validateInputs and the argument checks, in their order.
      assertIllegal(() -> repo.save(null, null, null), "projectionName cannot be null");
      assertIllegal(() -> repo.save(P, null, item), "id cannot be null or blank");
      assertIllegal(() -> repo.save(P, " ", item), "id cannot be null or blank");
      assertIllegal(() -> repo.save(P, "a", null), "readModel cannot be null");
      assertIllegal(() -> repo.save(upper, "a", null), "readModel cannot be null");
      assertIllegal(() -> repo.findById(null, "a", Item.class), "projectionName cannot be null");
      assertIllegal(() -> repo.findById(P, "", Item.class), "id cannot be null or blank");
      assertIllegal(() -> repo.findById(P, "a", null), "type cannot be null");
      assertIllegal(() -> repo.findAll(null, Item.class), "projectionName cannot be null");
      assertIllegal(() -> repo.findAll(P, null), "type cannot be null");
      assertIllegal(() -> repo.delete(null, "a"), "projectionName cannot be null");
      assertIllegal(() -> repo.delete(P, null), "id cannot be null or blank");
      assertIllegal(() -> repo.findAll(null, Item.class, page), "projectionName cannot be null");
      assertIllegal(() -> repo.findAll(P, null, page), "type cannot be null");
      assertIllegal(() -> repo.findAll(P, Item.class, null), "pageRequest cannot be null");
      assertIllegal(
          () -> repo.findById(null, "a", Item.class, LockMode.OPTIMISTIC),
          "projectionName cannot be null");
      assertIllegal(
          () -> repo.findById(P, " ", Item.class, LockMode.OPTIMISTIC),
          "id cannot be null or blank");
      assertIllegal(() -> repo.findById(P, "a", null, LockMode.OPTIMISTIC), "type cannot be null");
      assertIllegal(() -> repo.findById(P, "a", Item.class, null), "lockMode cannot be null");
      assertIllegal(() -> repo.save(null, "a", item, 0L), "projectionName cannot be null");
      assertIllegal(() -> repo.save(P, null, item, 0L), "id cannot be null or blank");
      assertIllegal(() -> repo.save(P, "a", null, 0L), "readModel cannot be null");
      assertIllegal(() -> repo.save(P, "a", item, -1L), "expectedVersion must be >= 0");

      // validateFilteredRead, in its order.
      assertIllegal(() -> repo.findAllBy(null, null, null, null), "projectionName cannot be null");
      assertIllegal(() -> repo.findAllBy(P, null, null, null), "type cannot be null");
      assertIllegal(() -> repo.findAllBy(P, Item.class, null, null), "filter cannot be null");
      assertIllegal(
          () -> repo.findAllBy(P, Item.class, filter, null), "pageRequest cannot be null");

      // validateField, sanitized.
      assertIllegal(
          () -> repo.findAllBy(P, Item.class, FieldEquals.of("bad-field", "x"), page),
          FIELD_RULE + "bad-field");
      assertIllegal(
          () -> repo.findAll(P, Item.class, PageRequest.of(0, 10, Sort.by("a.b"))),
          FIELD_RULE + "a.b");
      assertIllegal(
          () -> repo.findAll(P, Item.class, PageRequest.of(0, 10, Sort.by("bad\nfield"))),
          FIELD_RULE + "badfield");
      assertIllegal(
          () -> repo.declareIndex(ProjectionIndex.forSort(P, Sort.by("na me"))),
          FIELD_RULE + "na me");
      assertIllegal(
          () -> JdbcProjectionRepository.createIndexSql(ProjectionIndex.forSort(P, Sort.by("a-b"))),
          FIELD_RULE + "a-b");

      // The table-name rule, sanitized; it comes before the field rule.
      assertIllegal(() -> repo.save(upper, "a", item), tableRule("Orders"));
      assertIllegal(() -> repo.findById(upper, "a", Item.class), tableRule("Orders"));
      assertIllegal(() -> repo.findAll(upper, Item.class), tableRule("Orders"));
      assertIllegal(() -> repo.delete(upper, "a"), tableRule("Orders"));
      assertIllegal(
          () -> repo.findAll(upper, Item.class, PageRequest.of(0, 10, Sort.by("bad-field"))),
          tableRule("Orders"));
      assertIllegal(
          () -> repo.findAllBy(upper, Item.class, FieldEquals.of("bad-field", "x"), page),
          tableRule("Orders"));
      assertIllegal(
          () -> repo.save(ProjectionName.of("bad\nname"), "a", item), tableRule("badname"));
      assertIllegal(
          () -> repo.save(ProjectionName.of("a".repeat(59)), "a", item), tableRule("a".repeat(59)));
      assertIllegal(
          () -> repo.declareIndex(ProjectionIndex.forSort(upper, Sort.by("bad-field"))),
          tableRule("Orders"));
      assertIllegal(() -> repo.checkProjectionName(null), "projectionName cannot be null");
      assertIllegal(() -> repo.checkProjectionName(upper), tableRule("Orders"));
      assertIllegal(() -> repo.prepareReadModel(null), "projectionName cannot be null");
      assertIllegal(() -> repo.prepareReadModel(upper), tableRule("Orders"));

      // The index and the remaining name checks.
      assertIllegal(() -> repo.declareIndex(null), "index cannot be null");
      assertIllegal(() -> JdbcProjectionRepository.createIndexSql(null), "index cannot be null");
      assertIllegal(() -> repo.stampFencingEpoch(null, 1L), "projectionName cannot be null");
      assertIllegal(() -> repo.resetOffset(null), "projectionName cannot be null");

      // Every refusal is decided before any I/O, and logs nothing.
      assertThat(recorder.transcript()).isEmpty();
      assertNoLog(log);
    }
  }

  @Test
  void constructorAndMapperValidation() {
    ObjectMapper mapper = mapper();

    assertIllegal(() -> new JdbcProjectionRepository(null, mapper), "dataSource is required");
    assertIllegal(() -> new JdbcProjectionRepository(recorder, null), "objectMapper is required");
    assertIllegal(() -> new JdbcProjectionRepository(null), "dataSource is required");
    assertThat(catchThrowable(() -> JdbcProjectionRepository.createObjectMapper(null, null)))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("metrics is required");
  }

  @Test
  void validationInsideABatch_doesNotAbort() throws SQLException {
    var failures = new ArrayList<Throwable>();

    try (LogCapture log = LogCapture.attach(JdbcProjectionRepository.class)) {
      batch(
          1,
          handed -> {
            failures.add(catchThrowable(() -> handed.save(P, " ", new Item("x", 1))));
            failures.add(catchThrowable(() -> repo.save(P, "a", null)));
            failures.add(catchThrowable(() -> handed.save(ProjectionName.of("Bad"), "a", "x")));
            failures.add(
                catchThrowable(
                    () ->
                        handed.findAllBy(
                            P,
                            Item.class,
                            FieldEquals.of("bad-field", "x"),
                            PageRequest.of(0, 10))));
            failures.add(catchThrowable(() -> handed.save(P, "a", new Item("x", 1), -1L)));
            handed.save(P, "a", new Item("x", 1));
          });
      assertNoLog(log);
    }

    assertThat(failures)
        .allSatisfy(f -> assertThat(f).isExactlyInstanceOf(IllegalArgumentException.class));
    assertThat(failures)
        .extracting(Throwable::getMessage)
        .containsExactly(
            "id cannot be null or blank",
            "readModel cannot be null",
            tableRule("Bad"),
            FIELD_RULE + "bad-field",
            "expectedVersion must be >= 0");
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
    assertThat(checkpoint(P)).isEqualTo(1L);
  }

  private static void assertIllegal(ThrowingCallable call, String message) {
    assertThat(catchThrowable(call))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(message)
        .hasNoCause();
  }
}
