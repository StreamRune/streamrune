package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.LockMode;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.Versioned;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * Characterization of the SQLite projection repository: the statements and connections of the table
 * check, the index declaration and the writes, the exact exception type, message and cause of every
 * refusal, where each failure surfaces relative to the write transaction, where the joined (batch)
 * path is taken relative to validation, when created tables are remembered, the after-commit
 * actions and their log lines, and the {@code @Encrypted} key refusal with its per-instance cache.
 * The values are what the repository does on a real database file today, written down as they come
 * out; these tests stay unchanged while the projection rules and the after-commit actions move into
 * shared helpers.
 *
 * <p>Every repository borrows from a {@link RecordingDataSource}. A read runs in autocommit on one
 * connection; a write runs in one connection's {@code BEGIN IMMEDIATE} transaction committed with a
 * {@code COMMIT} statement. A refusal decided by a write (the optimistic lock, a foreign schema
 * object under an index name) is thrown after that transaction committed, so it keeps its own type
 * and is never wrapped.
 */
class SqliteProjectionRepositoryCharacterizationTest {

  private static final ProjectionName P = ProjectionName.of("p");
  private static final ProjectionName Q = ProjectionName.of("q");
  private static final ProjectionName R = ProjectionName.of("r");

  // ── today's statements, as literals ─────────────────────────────────────────────────────────

  private static final String TABLE_EXISTS =
      "SELECT 1 FROM sqlite_schema WHERE type IN ('table', 'view') AND name = ? COLLATE NOCASE";
  private static final String SCHEMA_OBJECT =
      "SELECT type, tbl_name FROM sqlite_schema WHERE name = ? COLLATE NOCASE";

  private static String createTable(String table) {
    return "CREATE TABLE IF NOT EXISTS "
        + table
        + " (id TEXT NOT NULL PRIMARY KEY CHECK (length(id) <= 255), data TEXT NOT NULL CHECK"
        + " (json_valid(data)), version INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL)"
        + " STRICT";
  }

  private static String upsert(String table) {
    return "INSERT INTO "
        + table
        + " (id, data, version, updated_at) VALUES (?, ?, 0, ?) ON CONFLICT (id) DO UPDATE SET"
        + " data = excluded.data, version = "
        + table
        + ".version + 1, updated_at = excluded.updated_at";
  }

  private static String versionedUpdate(String table) {
    return "UPDATE "
        + table
        + " SET data = ?, version = version + 1, updated_at = ? WHERE id = ? AND version = ?";
  }

  private static String selectById(String table) {
    return "SELECT data FROM " + table + " WHERE id = ?";
  }

  private static final String SEED =
      "INSERT INTO readmodel_checkpoint (projection_name, last_offset, updated_at) VALUES (?, 0, ?)"
          + " ON CONFLICT (projection_name) DO NOTHING";
  private static final String READ_FOR_BATCH =
      "SELECT last_offset, epoch FROM readmodel_checkpoint WHERE projection_name = ?";
  private static final String SAVE_LOCKED =
      "INSERT INTO readmodel_checkpoint (projection_name, last_offset, epoch, updated_at)"
          + " VALUES (?, ?, ?, ?) ON CONFLICT (projection_name) DO UPDATE SET"
          + " last_offset = excluded.last_offset,"
          + " epoch = max(readmodel_checkpoint.epoch, excluded.epoch),"
          + " updated_at = excluded.updated_at"
          + " WHERE excluded.last_offset > readmodel_checkpoint.last_offset";

  /** {@code ProjectionIndex.forFilter(P, "name", Sort.unsorted())}. */
  private static final ProjectionIndex NAME_INDEX =
      ProjectionIndex.forFilter(P, "name", Sort.unsorted());

  private static final String NAME_INDEX_NAME = "p_name_id_ix_5a9db6eb3f";
  private static final String NAME_INDEX_DDL =
      "CREATE INDEX IF NOT EXISTS "
          + NAME_INDEX_NAME
          + " ON p_view (CASE json_type(data, '$.\"name\"') WHEN 'text' THEN json_extract(data,"
          + " '$.\"name\"') WHEN 'null' THEN NULL ELSE data -> '$.\"name\"' END, id)";

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

  private static String reservedRule(String shown) {
    return "Projection name '"
        + shown
        + "' cannot name a read-model table on SQLite: SQLite reserves every name that starts"
        + " with 'sqlite_' for its own tables, so '"
        + shown
        + "_view' cannot be created. Rename the projection.";
  }

  private static String encryptedRefusal(String field, String type) {
    return "Field '"
        + field
        + "' of "
        + type
        + " is @Encrypted: its stored text is ciphertext, so it cannot be filtered or sorted"
        + " (keep a separate, non-sensitive field for the query)";
  }

  private static final String JOINED_INDEX_REFUSAL =
      "declareIndex on a transaction-scoped repository: declare indexes at wiring, on the"
          + " repository itself, not inside a projection batch";

  private static final String AFTER_COMMIT_WARN =
      "Projection '{}' after-commit action failed; the batch stays committed and the remaining"
          + " after-commit actions still run";

  // ── read models ─────────────────────────────────────────────────────────────────────────────

  record Item(String name, int qty) {}

  record Person(String id, @Encrypted(subjectId = "id") String ssn, String city) {}

  record Address(@Encrypted(subjectId = "zip") String street, String zip) {}

  record Resident(String id, @JsonUnwrapped(prefix = "home_") Address address) {}

  record Account(String id, @Encrypted(subjectId = "id") String fieldName) {}

  /** An {@code @Encrypted} component named like a repository column. */
  record Odd(String id, @Encrypted(subjectId = "id") String version) {}

  /** What a stored {@link Item} does not bind to: its {@code name} is an unknown property. */
  record Strict(int qty) {}

  /** A read model whose serialization always fails: its one property throws. */
  static final class Unserializable {
    public String getBoom() {
      throw new IllegalStateException("cannot serialize");
    }
  }

  @TempDir Path dir;

  private final List<AutoCloseable> resources = new ArrayList<>();

  private Path file;
  private RecordingDataSource recorder;
  private SqliteDatabase database;
  private SqliteReadModelCheckpoints checkpoints;
  private SqliteProjectionRepository repo;

  @BeforeEach
  void openDatabase() {
    file = dir.resolve("projections.db");
    recorder = new RecordingDataSource(TestDatabases.dataSource(file));
    database = SqliteDatabase.open(recorder);
    resources.add(database);
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    checkpoints = new SqliteReadModelCheckpoints(database);
    repo = repository(mapper());
    recorder.clear();
  }

  @AfterEach
  void closeDatabases() throws Exception {
    for (AutoCloseable resource : resources.reversed()) {
      resource.close();
    }
  }

  private static ObjectMapper mapper() {
    return SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP);
  }

  private SqliteProjectionRepository repository(ObjectMapper mapper) {
    return new SqliteProjectionRepository(database, mapper, checkpoints);
  }

  @FunctionalInterface
  interface Updater {
    void update(ProjectionRepository handed);
  }

  /** A batch of projection {@link #P} advancing its checkpoint to {@code offset}. */
  private void batch(long offset, Updater updater) {
    repo.executeAtomically(P, List.of(), GlobalOffset.of(offset), 0L, updater::update, checkpoints);
  }

  private static List<String> write(String... statements) {
    var events = new ArrayList<String>();
    events.add("open #1");
    events.add("#1 execute BEGIN IMMEDIATE");
    for (String statement : statements) {
      events.add(statement);
    }
    events.add("#1 execute COMMIT");
    events.add("#1 close");
    return events;
  }

  private static List<String> renumbered(List<String> events, int n) {
    return events.stream().map(e -> e.replace("#1", "#" + n)).toList();
  }

  private static List<String> concat(List<String>... parts) {
    var all = new ArrayList<String>();
    for (List<String> part : parts) {
      all.addAll(part);
    }
    return all;
  }

  /** The table probe, then the creation of {@code table} on a second connection. */
  private static List<String> probeAndCreate(String table) {
    return concat(
        List.of("open #1", "#1 prepare " + TABLE_EXISTS, "#1 close"),
        renumbered(write("#1 execute " + createTable(table)), 2));
  }

  // ── Golden transcripts ──────────────────────────────────────────────────────────────────────

  @Test
  void createIndexSql_isTodaysStatement() {
    assertThat(SqliteProjectionRepository.createIndexSql(NAME_INDEX)).isEqualTo(NAME_INDEX_DDL);
    assertThat(SqliteProjectionRepository.indexName(NAME_INDEX)).isEqualTo(NAME_INDEX_NAME);
  }

  @Test
  void transcript_firstSaveThenRead() {
    repo.save(P, "a", new Item("x", 1));

    assertThat(recorder.transcript())
        .containsExactlyElementsOf(
            concat(
                probeAndCreate("p_view"), renumbered(write("#1 prepare " + upsert("p_view")), 3)));

    // The table is remembered: the next read checks nothing.
    recorder.clear();
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + selectById("p_view"), "#1 close");
  }

  @Test
  void transcript_existingTableIsFoundWithoutAWrite() {
    repo.save(P, "a", new Item("x", 1));
    SqliteProjectionRepository second = repository(mapper());
    recorder.clear();

    // A second instance keeps its own table set: it probes once, finds the table, creates nothing.
    assertThat(second.findById(P, "a", Item.class)).isPresent();
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare " + TABLE_EXISTS,
            "#1 close",
            "open #2",
            "#2 prepare " + selectById("p_view"),
            "#2 close");

    recorder.clear();
    assertThat(second.findById(P, "a", Item.class)).isPresent();
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + selectById("p_view"), "#1 close");
  }

  @Test
  void transcript_declareIndex() {
    try (LogCapture log = LogCapture.attach(SqliteProjectionRepository.class)) {
      repo.declareIndex(NAME_INDEX);

      assertThat(recorder.transcript())
          .containsExactlyElementsOf(
              concat(
                  probeAndCreate("p_view"),
                  renumbered(
                      write("#1 prepare " + SCHEMA_OBJECT, "#1 execute " + NAME_INDEX_DDL), 3)));

      // Declared again: the table is remembered, the index found, nothing executed.
      recorder.clear();
      repo.declareIndex(NAME_INDEX);
      assertThat(recorder.transcript())
          .containsExactlyElementsOf(write("#1 prepare " + SCHEMA_OBJECT));
      assertThat(log.events(Level.INFO)).hasSize(1);
    }
  }

  @Test
  void indexCreation_infoLogOnRepositoryCategory() {
    try (LogCapture log = LogCapture.attach(SqliteProjectionRepository.class)) {
      repo.declareIndex(NAME_INDEX);

      List<ILoggingEvent> info = log.events(Level.INFO);
      assertThat(info).hasSize(1);
      assertThat(info.getFirst().getLoggerName())
          .isEqualTo("org.streamrune.sqlite.SqliteProjectionRepository");
      assertThat(info.getFirst().getMessage()).isEqualTo("Created projection index: {}");
      assertThat(info.getFirst().getArgumentArray()).containsExactly(NAME_INDEX_DDL);
      assertThat(info.getFirst().getFormattedMessage())
          .isEqualTo("Created projection index: " + NAME_INDEX_DDL);

      // A second declaration, by this or by another instance, finds the index and logs nothing.
      repo.declareIndex(NAME_INDEX);
      repository(mapper()).declareIndex(NAME_INDEX);
      assertThat(log.events(Level.INFO)).hasSize(1);
      assertThat(log.events(Level.WARN)).isEmpty();
      assertThat(log.events(Level.DEBUG)).isEmpty();
    }
  }

  @Test
  void transcript_versionedSaveAndDelete() {
    repo.save(P, "a", new Item("x", 1));
    recorder.clear();

    repo.save(P, "a", new Item("y", 2), 0L);
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(write("#1 prepare " + versionedUpdate("p_view")));

    recorder.clear();
    repo.delete(P, "a");
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(write("#1 prepare DELETE FROM p_view WHERE id = ?"));
  }

  @Test
  void transcript_batch() {
    repo.prepareReadModel(P);
    recorder.clear();

    batch(
        3,
        handed -> {
          handed.save(P, "a", new Item("x", 1));
          // A table nothing prepared is created inside the batch, once.
          handed.save(Q, "a", new Item("x", 1));
          handed.save(Q, "b", new Item("x", 1));
        });

    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 execute BEGIN IMMEDIATE",
            "#1 execute BEGIN",
            "#1 prepare " + SEED,
            "#1 prepare " + READ_FOR_BATCH,
            "#1 prepare " + upsert("p_view"),
            "#1 execute " + createTable("q_view"),
            "#1 prepare " + upsert("q_view"),
            "#1 prepare " + upsert("q_view"),
            "#1 execute BEGIN",
            "#1 prepare " + SAVE_LOCKED,
            "#1 execute COMMIT",
            "#1 close");
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
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 execute BEGIN IMMEDIATE",
            "#1 execute BEGIN",
            "#1 prepare " + SEED,
            "#1 prepare " + READ_FOR_BATCH,
            "#1 execute " + createTable("q_view"),
            "#1 prepare " + upsert("q_view"),
            "#1 execute ROLLBACK",
            "#1 close");

    // The rolled-back table was never remembered: the next read outside a batch probes the schema
    // again, finds nothing and creates the table.
    recorder.clear();
    assertThat(repo.findById(Q, "a", Item.class)).isEmpty();
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(
            concat(
                probeAndCreate("q_view"),
                List.of("open #3", "#3 prepare " + selectById("q_view"), "#3 close")));
  }

  @Test
  void tablesCreatedInACommittedBatch_areRecordedAfterTheCommit() {
    batch(1, handed -> handed.save(R, "a", new Item("x", 1)));
    recorder.clear();

    assertThat(repo.findById(R, "a", Item.class)).contains(new Item("x", 1));
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + selectById("r_view"), "#1 close");
  }

  @Test
  void tablesCreatedInABatchWhoseCloseFailsAfterTheCommit_areNotRecorded() {
    recorder.failNextCloseAfterCommit();

    Throwable thrown =
        catchThrowable(() -> batch(1, handed -> handed.save(Q, "a", new Item("x", 1))));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to process projection batch atomically");
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure");

    // The batch committed, but the table was not remembered: the next read probes again and finds
    // it (no creation).
    recorder.clear();
    assertThat(repo.findById(Q, "a", Item.class)).contains(new Item("x", 1));
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare " + TABLE_EXISTS,
            "#1 close",
            "open #2",
            "#2 prepare " + selectById("q_view"),
            "#2 close");
  }

  @Test
  void prepareReadModelInsideABatch_createsOnTheBatchConnection() {
    batch(1, handed -> repo.prepareReadModel(Q));

    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 execute BEGIN IMMEDIATE",
            "#1 execute BEGIN",
            "#1 prepare " + SEED,
            "#1 prepare " + READ_FOR_BATCH,
            "#1 execute " + createTable("q_view"),
            "#1 execute BEGIN",
            "#1 prepare " + SAVE_LOCKED,
            "#1 execute COMMIT",
            "#1 close");

    recorder.clear();
    repo.prepareReadModel(Q);
    assertThat(recorder.transcript()).isEmpty();
  }

  // ── Where the joined path is taken ──────────────────────────────────────────────────────────

  @Test
  void invalidNonNullIndexInsideABatch_isIllegalArgument() {
    var failures = new ArrayList<Throwable>();

    batch(
        1,
        handed -> {
          // On the repository itself, an invalid index is refused by validation, which runs before
          // the joined path: an IllegalArgumentException, and the batch is not aborted.
          failures.add(
              catchThrowable(
                  () ->
                      repo.declareIndex(
                          ProjectionIndex.forFilter(P, "bad-field", Sort.unsorted()))));
          failures.add(
              catchThrowable(
                  () ->
                      repo.declareIndex(
                          ProjectionIndex.forSort(ProjectionName.of("Bad"), Sort.by("name")))));
          failures.add(catchThrowable(() -> repo.declareIndex(null)));
          // A valid index reaches the joined path and is refused there.
          failures.add(catchThrowable(() -> repo.declareIndex(NAME_INDEX)));
          // The handed repository validates nothing: every index is refused as inside a batch.
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
        .hasMessage(tableRule("Bad"));
    assertThat(failures.get(2))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("index cannot be null");
    assertThat(failures.get(3))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(JOINED_INDEX_REFUSAL);
    assertThat(failures.get(4))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(JOINED_INDEX_REFUSAL);

    // Not aborted: the batch committed its write and advanced the checkpoint.
    assertThat(recorder.transcript()).endsWith("#1 execute COMMIT", "#1 close");
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(1));
  }

  @Test
  void prepareReadModelInsideABatch_validatesBeforeTheJoinedPath() {
    var failures = new ArrayList<Throwable>();

    batch(
        1,
        handed -> {
          failures.add(catchThrowable(() -> repo.prepareReadModel(null)));
          failures.add(catchThrowable(() -> repo.prepareReadModel(ProjectionName.of("Bad"))));
          handed.save(P, "a", new Item("x", 1));
        });

    assertThat(failures.get(0))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("projectionName cannot be null");
    assertThat(failures.get(1))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(tableRule("Bad"));
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
  }

  // ── Failures decided by a write surface after it ────────────────────────────────────────────

  @Test
  void foreignIndexName_isEventStoreExceptionOutsideTheWrite() throws SQLException {
    repo.prepareReadModel(P);
    try (Connection conn = TestDatabases.dataSource(file).getConnection();
        Statement st = conn.createStatement()) {
      st.execute("CREATE VIEW " + NAME_INDEX_NAME + " AS SELECT 1");
    }
    recorder.clear();

    try (LogCapture log = LogCapture.attach(SqliteProjectionRepository.class)) {
      Throwable thrown = catchThrowable(() -> repo.declareIndex(NAME_INDEX));

      assertThat(thrown)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage(
              "A schema object named "
                  + NAME_INDEX_NAME
                  + " exists and is not an index of p_view: the projection index of that name"
                  + " cannot be created; rename or drop it")
          .hasNoCause();
      assertThat(thrown.getSuppressed()).isEmpty();
      // The check ran in a transaction that committed; the refusal came after it.
      assertThat(recorder.transcript())
          .containsExactlyElementsOf(write("#1 prepare " + SCHEMA_OBJECT));
      assertThat(recorder.openConnections()).isZero();
      assertThat(log.events(Level.INFO)).isEmpty();
    }
  }

  @Test
  void indexNameHeldByAnIndexOfAnotherTable_isForeignToo() throws SQLException {
    repo.prepareReadModel(P);
    repo.prepareReadModel(Q);
    try (Connection conn = TestDatabases.dataSource(file).getConnection();
        Statement st = conn.createStatement()) {
      st.execute("CREATE INDEX " + NAME_INDEX_NAME.toUpperCase() + " ON q_view (id)");
    }

    Throwable thrown = catchThrowable(() -> repo.declareIndex(NAME_INDEX));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageStartingWith("A schema object named " + NAME_INDEX_NAME + " exists")
        .hasNoCause();
  }

  @Test
  void optimisticLock_isNotWrapped() {
    repo.save(P, "a", new Item("x", 1));
    recorder.clear();

    Throwable thrown = catchThrowable(() -> repo.save(P, "a", new Item("y", 2), 3L));

    assertThat(thrown)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Optimistic lock conflict on projection 'p/a': expected version 3")
        .hasNoCause();
    // The update ran in a transaction that committed; the refusal came after it.
    assertThat(recorder.transcript())
        .containsExactlyElementsOf(write("#1 prepare " + versionedUpdate("p_view")));
    assertThat(recorder.openConnections()).isZero();
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
  }

  @Test
  void optimisticLockMessage_isUnsanitized() {
    repo.save(P, "a\nb", new Item("x", 1));

    Throwable thrown = catchThrowable(() -> repo.save(P, "a\nb", new Item("y", 2), 7L));

    assertThat(thrown)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Optimistic lock conflict on projection 'p/a\nb': expected version 7");
  }

  @Test
  void optimisticLockInsideABatch_isNotWrappedAndDoesNotAbort() {
    repo.save(P, "a", new Item("x", 1));
    var failures = new ArrayList<Throwable>();

    batch(
        1,
        handed -> {
          failures.add(catchThrowable(() -> handed.save(P, "a", new Item("y", 2), 5L)));
          failures.add(catchThrowable(() -> repo.save(P, "a", new Item("y", 2), 6L)));
          handed.save(P, "b", new Item("z", 3));
        });

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
  }

  // ── After-commit actions ────────────────────────────────────────────────────────────────────

  @Test
  void afterCommitActions_runAfterTheCommitAndTheClose() {
    var seen = new ArrayList<List<String>>();
    var openAtRun = new ArrayList<Integer>();

    batch(
        1,
        handed -> {
          handed.save(P, "a", new Item("x", 1));
          handed.afterCommit(
              () -> {
                seen.add(recorder.transcript());
                openAtRun.add(recorder.openConnections());
              });
          // The repository itself, joined, queues with the batch too.
          repo.afterCommit(() -> openAtRun.add(recorder.openConnections()));
        });

    assertThat(seen).hasSize(1);
    assertThat(seen.getFirst()).endsWith("#1 execute COMMIT", "#1 close");
    assertThat(openAtRun).containsExactly(0, 0);
  }

  @Test
  void afterCommitActions_areDiscardedOnRollback() {
    var ran = new ArrayList<String>();
    RuntimeException boom = new IllegalStateException("boom");

    Throwable thrown =
        catchThrowable(
            () ->
                batch(
                    1,
                    handed -> {
                      handed.afterCommit(() -> ran.add("handed"));
                      repo.afterCommit(() -> ran.add("joined"));
                      throw boom;
                    }));

    assertThat(thrown).isSameAs(boom);
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(ran).isEmpty();
  }

  @Test
  void afterCommitOutsideABatch_runsAtOnce() {
    var ran = new ArrayList<String>();

    repo.afterCommit(() -> ran.add("now"));

    assertThat(ran).containsExactly("now");
    assertThat(recorder.transcript()).isEmpty();
    assertThat(catchThrowable(() -> repo.afterCommit(null)))
        .isExactlyInstanceOf(NullPointerException.class);
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
  void afterCommitFailure_warnsOnRepositoryCategory() {
    var ran = new CopyOnWriteArrayList<String>();
    RuntimeException failure = new IllegalStateException("evict failed");

    try (LogCapture log = LogCapture.attach(SqliteProjectionRepository.class)) {
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
          .isEqualTo("org.streamrune.sqlite.SqliteProjectionRepository");
      assertThat(warn.getFirst().getMessage()).isEqualTo(AFTER_COMMIT_WARN);
      assertThat(warn.getFirst().getFormattedMessage())
          .isEqualTo(
              "Projection 'p' after-commit action failed; the batch stays committed and the"
                  + " remaining after-commit actions still run");
      assertThat(warn.getFirst().getThrowableProxy().getClassName())
          .isEqualTo(IllegalStateException.class.getName());
      assertThat(warn.getFirst().getThrowableProxy().getMessage()).isEqualTo("evict failed");
      assertThat(log.events(Level.ERROR)).isEmpty();
      assertThat(log.events(Level.INFO)).isEmpty();
    }
    // The batch stays committed.
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(1));
  }

  @Test
  void afterCommitError_propagatesAfterACleanCommit() {
    var ran = new ArrayList<String>();
    Error error = new Error("action error");

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
    // The Error ends the run: the later action does not run; the batch stays committed.
    assertThat(ran).containsExactly("first");
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
  }

  @Test
  void afterCommitError_isSuppressedOnTheInFlightFailure() {
    var ran = new ArrayList<String>();
    Error error = new Error("action error");
    recorder.failNextCloseAfterCommit();

    try (LogCapture log = LogCapture.attach(SqliteProjectionRepository.class)) {
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

      assertThat(thrown)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to process projection batch atomically");
      assertThat(thrown.getCause())
          .isExactlyInstanceOf(SQLException.class)
          .hasMessage("injected close failure");
      assertThat(thrown.getSuppressed()).containsExactly(error);
      assertThat(ran).isEmpty();
      assertThat(log.events(Level.WARN)).hasSize(1);
    }
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
  }

  // ── (De)serialization failures ──────────────────────────────────────────────────────────────

  @Test
  void serializationFailure_afterTheTableCheckAndBeforeTheWrite() {
    Throwable thrown = catchThrowable(() -> repo.save(P, "a\nb", new Unserializable()));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to save projection: p/ab");
    assertThat(thrown.getCause()).isExactlyInstanceOf(JsonMappingException.class);
    // The table is checked (and created) first; nothing is written.
    assertThat(recorder.transcript()).containsExactlyElementsOf(probeAndCreate("p_view"));

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
  }

  @Test
  void serializationFailureInsideABatch_doesNotAbort() {
    var failures = new ArrayList<Throwable>();

    batch(
        1,
        handed -> {
          failures.add(catchThrowable(() -> handed.save(P, "a", new Unserializable())));
          failures.add(catchThrowable(() -> repo.save(P, "a", new Unserializable(), 0L)));
          handed.save(P, "a", new Item("x", 1));
        });

    assertThat(failures)
        .extracting(Throwable::getMessage)
        .containsExactly(
            "Failed to save projection: p/a", "Failed to save projection with version check: p/a");
    assertThat(failures)
        .allSatisfy(f -> assertThat(f).isExactlyInstanceOf(EventStoreException.class));
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
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
    // The encryption refusal comes before the table-name and field rules.
    assertRefused(
        () ->
            repo.findAllBy(
                ProjectionName.of("Bad"), Person.class, FieldEquals.of("ssn", "x"), unsorted),
        encryptedRefusal("ssn", "Person"));
    // Every refusal is decided before any I/O.
    assertThat(recorder.transcript()).isEmpty();
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
    repo.declareIndex(ProjectionIndex.forFilter(P, "ssn", Sort.unsorted()));
  }

  @Test
  void encryptedKeyRefusalInsideABatch_doesNotAbort() {
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
                  () -> repo.findAll(P, Person.class, PageRequest.of(0, 10, Sort.by("ssn")))));
          handed.save(P, "a", new Item("x", 1));
        });

    assertThat(failures)
        .allSatisfy(
            f ->
                assertThat(f)
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage(encryptedRefusal("ssn", "Person")));
    assertThat(repo.findById(P, "a", Item.class)).contains(new Item("x", 1));
  }

  @Test
  void encryptedFieldsCacheIsPerInstance() {
    PageRequest unsorted = PageRequest.of(0, 10);
    ObjectMapper snake = mapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    // The default-naming repository asked first.
    SqliteProjectionRepository plain = repository(mapper());
    SqliteProjectionRepository snaked = repository(snake);
    assertRefused(
        () -> plain.findAllBy(P, Account.class, FieldEquals.of("fieldName", "x"), unsorted),
        encryptedRefusal("fieldName", "Account"));
    assertThat(plain.findAllBy(P, Account.class, FieldEquals.of("field_name", "x"), unsorted))
        .isNotNull();
    assertRefused(
        () -> snaked.findAllBy(P, Account.class, FieldEquals.of("field_name", "x"), unsorted),
        encryptedRefusal("field_name", "Account"));
    // The Java component name is refused under every naming.
    assertRefused(
        () -> snaked.findAllBy(P, Account.class, FieldEquals.of("fieldName", "x"), unsorted),
        encryptedRefusal("fieldName", "Account"));

    // The snake-case repository asked first.
    SqliteProjectionRepository snakedFirst =
        repository(mapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE));
    SqliteProjectionRepository plainSecond = repository(mapper());
    assertRefused(
        () -> snakedFirst.findAll(P, Account.class, PageRequest.of(0, 10, Sort.by("field_name"))),
        encryptedRefusal("field_name", "Account"));
    assertThat(plainSecond.findAll(P, Account.class, PageRequest.of(0, 10, Sort.by("field_name"))))
        .isNotNull();
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
  void nullNameOnExecuteAtomically_isIllegalArgument() {
    Throwable thrown =
        catchThrowable(
            () ->
                repo.executeAtomically(
                    null, List.of(), GlobalOffset.of(1), 0L, handed -> {}, checkpoints));

    assertThat(thrown)
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("projectionName cannot be null")
        .hasNoCause();
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
                        checkpoints)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(tableRule("Bad"));
    assertThat(catchThrowable(() -> repo.executeReplay(null, List.of(), handed -> {})))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("projectionName cannot be null");
    assertThat(
            catchThrowable(
                () -> repo.executeReplay(ProjectionName.of("sqlite_x"), List.of(), handed -> {})))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(reservedRule("sqlite_x"));
    assertThat(recorder.transcript()).isEmpty();
  }

  @Test
  void validationMessages() {
    Item item = new Item("x", 1);
    PageRequest page = PageRequest.of(0, 10);
    FieldEquals filter = FieldEquals.of("name", "x");
    ProjectionName upper = ProjectionName.of("Orders");

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
        () -> repo.findById(P, " ", Item.class, LockMode.OPTIMISTIC), "id cannot be null or blank");
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
    assertIllegal(() -> repo.findAllBy(P, Item.class, filter, null), "pageRequest cannot be null");

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
        () -> SqliteProjectionRepository.createIndexSql(ProjectionIndex.forSort(P, Sort.by("a-b"))),
        FIELD_RULE + "a-b");

    // The table-name rule, sanitized; it comes before the field rule.
    assertIllegal(() -> repo.save(upper, "a", item), tableRule("Orders"));
    assertIllegal(() -> repo.findById(upper, "a", Item.class), tableRule("Orders"));
    assertIllegal(
        () -> repo.findAll(upper, Item.class, PageRequest.of(0, 10, Sort.by("bad-field"))),
        tableRule("Orders"));
    assertIllegal(
        () -> repo.findAllBy(upper, Item.class, FieldEquals.of("bad-field", "x"), page),
        tableRule("Orders"));
    assertIllegal(() -> repo.save(ProjectionName.of("bad\nname"), "a", item), tableRule("badname"));
    assertIllegal(
        () -> repo.save(ProjectionName.of("a".repeat(59)), "a", item), tableRule("a".repeat(59)));
    assertIllegal(
        () -> repo.save(ProjectionName.of("sqlite_x"), "a", item), reservedRule("sqlite_x"));
    assertIllegal(
        () -> repo.declareIndex(ProjectionIndex.forSort(upper, Sort.by("bad-field"))),
        tableRule("Orders"));
    assertIllegal(() -> repo.checkProjectionName(null), "projectionName cannot be null");
    assertIllegal(() -> repo.checkProjectionName(upper), tableRule("Orders"));
    assertIllegal(
        () -> repo.checkProjectionName(ProjectionName.of("sqlite_x")), reservedRule("sqlite_x"));
    assertIllegal(() -> repo.prepareReadModel(null), "projectionName cannot be null");
    assertIllegal(() -> repo.prepareReadModel(upper), tableRule("Orders"));

    // The index and the remaining name checks.
    assertIllegal(() -> repo.declareIndex(null), "index cannot be null");
    assertIllegal(() -> SqliteProjectionRepository.createIndexSql(null), "index cannot be null");
    assertIllegal(() -> repo.stampFencingEpoch(null, 1L), "projectionName cannot be null");
    assertIllegal(() -> repo.resetOffset(null), "projectionName cannot be null");

    // Every refusal is decided before any I/O.
    assertThat(recorder.transcript()).isEmpty();
  }

  @Test
  void constructorAndMapperValidation() {
    ObjectMapper mapper = mapper();
    SqliteDatabase other = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("other.db")));
    resources.add(other);
    other.migrate(SqliteSchemaSeries.READ_MODEL);
    var otherCheckpoints = new SqliteReadModelCheckpoints(other);

    assertIllegal(
        () -> new SqliteProjectionRepository(null, mapper, checkpoints), "database is required");
    assertIllegal(
        () -> new SqliteProjectionRepository(database, null, checkpoints),
        "objectMapper is required");
    assertIllegal(
        () -> new SqliteProjectionRepository(database, mapper, null), "checkpoints is required");
    assertIllegal(
        () -> new SqliteProjectionRepository(database, mapper, otherCheckpoints),
        "The checkpoints live in "
            + other.file()
            + ", not in the repository's database "
            + database.file()
            + ": a batch advances the checkpoint in its own transaction on the repository's"
            + " database, so the checkpoints must be SqliteReadModelCheckpoints of that same"
            + " database");
    assertThat(catchThrowable(() -> SqliteProjectionRepository.createObjectMapper(null, null)))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("metrics is required");
  }

  @Test
  void validationInsideABatch_doesNotAbort() {
    var failures = new ArrayList<Throwable>();

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
                          P, Item.class, FieldEquals.of("bad-field", "x"), PageRequest.of(0, 10))));
          failures.add(catchThrowable(() -> repo.save(P, "a", new Item("x", 1), -1L)));
          handed.save(P, "a", new Item("x", 1));
        });

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
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(1));
  }

  private static void assertIllegal(ThrowingCallable call, String message) {
    assertThat(catchThrowable(call))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(message)
        .hasNoCause();
  }
}
