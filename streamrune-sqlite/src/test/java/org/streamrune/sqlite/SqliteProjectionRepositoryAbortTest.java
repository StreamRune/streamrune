package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.runtime.ProjectionDeadLetterReplayer;
import org.streamrune.test.InMemoryProjectionDeadLetterStore;

/**
 * A failed statement aborts the batch or replay transaction, as on PostgreSQL (the first failed
 * statement aborts it, and COMMIT then rolls back): an updater that catches the failure and goes on
 * can neither run another statement nor commit. Two failures, each swallowed by the updater, for a
 * batch and for a replay run by the real dead-letter replayer:
 *
 * <ul>
 *   <li>a CHECK failure (an id longer than 255 characters), after which SQLite keeps the
 *       transaction open;
 *   <li>SQLITE_FULL ({@code PRAGMA max_page_count}), after which SQLite has already rolled the
 *       transaction back, so a further statement would commit on its own.
 * </ul>
 *
 * Each throws, leaves no row of the transaction and leaves the checkpoint (and, for a replay, the
 * dead-letter entry) as it was.
 */
class SqliteProjectionRepositoryAbortTest {

  private static final ProjectionName P = ProjectionName.of("aborting");
  private static final String TOO_LONG = "x".repeat(256);

  record Row(String id, String value) {
    @JsonCreator
    Row(@JsonProperty("id") String id, @JsonProperty("value") String value) {
      this.id = id;
      this.value = value;
    }
  }

  @TempDir Path dir;

  private Path file;
  private SqliteDatabase database;
  private SqliteProjectionRepository repo;
  private SqliteReadModelCheckpoints checkpoints;
  private final List<SqliteDatabase> extraHandles = new ArrayList<>();

  @BeforeEach
  void open() {
    file = dir.resolve("abort.db");
    database = SqliteDatabase.open(TestDatabases.dataSource(file));
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    checkpoints = new SqliteReadModelCheckpoints(database);
    repo = repository(database);
    // The table and the checkpoint row exist, so the transactions below grow nothing but the rows.
    repo.executeAtomically(
        P,
        SqliteProjectionRepositoryCrashTest.Batches.of(1, 1),
        GlobalOffset.of(1),
        0L,
        tx -> tx.save(P, "seed", new Row("seed", "v")),
        null);
  }

  @AfterEach
  void close() {
    extraHandles.forEach(SqliteDatabase::close);
    database.close();
  }

  private static SqliteProjectionRepository repository(SqliteDatabase database) {
    return new SqliteProjectionRepository(
        database,
        SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP),
        new SqliteReadModelCheckpoints(database));
  }

  /** A repository whose connections cannot grow the file past its current size. */
  private SqliteProjectionRepository full() throws SQLException {
    long pages;
    try (Connection conn = TestDatabases.dataSource(file).getConnection();
        Statement st = conn.createStatement()) {
      st.execute("PRAGMA wal_checkpoint(TRUNCATE)");
      try (ResultSet rs = st.executeQuery("PRAGMA page_count")) {
        rs.next();
        pages = rs.getLong(1);
      }
    }
    DataSource capped =
        TestDatabases.withInitSql(
            "jdbc:sqlite:file:" + file + "?" + TestDatabases.REQUIRED_SETTINGS,
            "PRAGMA max_page_count = " + pages);
    SqliteDatabase handle = SqliteDatabase.open(capped);
    extraHandles.add(handle);
    return repository(handle);
  }

  private List<String> rows() throws SQLException {
    var ids = new ArrayList<String>();
    try (Connection conn = TestDatabases.dataSource(file).getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT id FROM aborting_view ORDER BY id")) {
      while (rs.next()) {
        ids.add(rs.getString(1));
      }
    }
    return ids;
  }

  /**
   * What the updater does in every case: a write, the failing write, a later write, each failure
   * caught and recorded, as an updater that "logs and goes on" would.
   */
  private static Consumer<ProjectionRepository> swallowing(
      Consumer<ProjectionRepository> failing, List<RuntimeException> caught) {
    return tx -> {
      tx.save(P, "a", new Row("a", "before the failure"));
      try {
        failing.accept(tx);
      } catch (EventStoreException e) {
        caught.add(e);
      }
      try {
        tx.save(P, "c", new Row("c", "after the failure"));
      } catch (EventStoreException e) {
        caught.add(e);
      }
    };
  }

  private static final Consumer<ProjectionRepository> CHECK_FAILURE =
      tx -> tx.save(P, TOO_LONG, new Row(TOO_LONG, "v"));

  private static final Consumer<ProjectionRepository> FULL =
      tx -> tx.save(P, "b", new Row("b", "x".repeat(400_000)));

  private void assertAbortedBatch(SqliteProjectionRepository on, Consumer<ProjectionRepository> f)
      throws SQLException {
    var caught = new ArrayList<RuntimeException>();

    assertThatThrownBy(
            () ->
                on.executeAtomically(
                    P,
                    SqliteProjectionRepositoryCrashTest.Batches.of(2, 2),
                    GlobalOffset.of(2),
                    0L,
                    tx -> swallowing(f, caught).accept(tx),
                    null))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageStartingWith(
            "The projection transaction was aborted by an earlier failure, so it runs no further"
                + " statement and rolls back: Failed to save projection: aborting/")
        .hasCause(caught.getFirst());

    assertAbortedTransaction(caught);
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(1));
  }

  private void assertAbortedTransaction(List<RuntimeException> caught) throws SQLException {
    assertThat(caught).hasSize(2);
    assertThat(caught.get(1))
        .as("the write after the failure did not run")
        .hasMessageStartingWith("The projection transaction was aborted by an earlier failure")
        .hasCause(caught.getFirst());
    assertThat(rows()).as("no row of the transaction remains").containsExactly("seed");
  }

  @Test
  void aSwallowedCheckFailureAbortsTheBatch() throws SQLException {
    assertAbortedBatch(repo, CHECK_FAILURE);
  }

  @Test
  void aSwallowedSqliteFullAbortsTheBatch_andNoLaterWriteAutocommits() throws SQLException {
    assertAbortedBatch(full(), FULL);
  }

  private void assertAbortedReplay(SqliteProjectionRepository on, Consumer<ProjectionRepository> f)
      throws SQLException {
    var deadLetters = new InMemoryProjectionDeadLetterStore();
    var entry =
        new ProjectionDeadLetterEntry(
            P,
            GlobalOffset.of(2),
            GlobalOffset.of(2),
            1,
            "java.lang.RuntimeException",
            "boom",
            1,
            Instant.parse("2026-01-01T00:00:00Z"));
    deadLetters.save(entry);
    var caught = new ArrayList<RuntimeException>();
    var projection =
        new Projection() {
          @Override
          public void process(List<EventEnvelope> batch) {
            throw new UnsupportedOperationException("replayed through the repository only");
          }

          @Override
          public void process(List<EventEnvelope> batch, ProjectionRepository repository) {
            swallowing(f, caught).accept(repository);
          }

          @Override
          public boolean writesThroughRepository() {
            return true;
          }
        };
    var replayer = new ProjectionDeadLetterReplayer(eventStoreHolding(2), deadLetters, on);

    var result = replayer.replay(P, projection, ProjectionDeliveryMode.TRANSACTIONAL_LOCAL, 10);

    assertThat(result).isEqualTo(new ProjectionDeadLetterReplayer.ReplayResult(0, 1, 0));
    assertThat(deadLetters.all())
        .as("the dead-letter entry is kept as it was")
        .containsExactly(entry);
    assertAbortedTransaction(caught);
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(1));
  }

  @Test
  void aSwallowedCheckFailureAbortsTheReplay() throws SQLException {
    assertAbortedReplay(repo, CHECK_FAILURE);
  }

  @Test
  void aSwallowedSqliteFullAbortsTheReplay_andNoLaterWriteAutocommits() throws SQLException {
    assertAbortedReplay(full(), FULL);
  }

  /** An event store that re-reads the global stream of the events at offsets 1..{@code last}. */
  private static EventStore eventStoreHolding(long last) {
    List<EventEnvelope> events = SqliteProjectionRepositoryCrashTest.Batches.of(1, last);
    return (EventStore)
        Proxy.newProxyInstance(
            EventStore.class.getClassLoader(),
            new Class<?>[] {EventStore.class},
            (proxy, method, args) -> {
              if (method.getName().equals("readGlobalStream")) {
                long after = ((GlobalOffset) args[0]).value();
                int max = (int) args[1];
                return events.stream()
                    .filter(e -> e.globalOffset().value() > after)
                    .limit(max)
                    .toList();
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  /**
   * A read model PostgreSQL's jsonb refuses aborts the transaction too, as its INSERT does there.
   */
  @Test
  void aRefusedReadModelAbortsTheBatch() throws SQLException {
    assertAbortedBatch(repo, tx -> tx.save(P, "b", Map.of("id", "b", "v", "a\0b")));
  }

  /**
   * A versioned save that finds no row of the expected version does not abort, as on PostgreSQL
   * (zero rows updated is no failed statement): the batch commits the updater's other writes.
   */
  @Test
  void anOptimisticLockConflictDoesNotAbortTheBatch() throws SQLException {
    var caught = new ArrayList<RuntimeException>();

    repo.executeAtomically(
        P,
        SqliteProjectionRepositoryCrashTest.Batches.of(2, 2),
        GlobalOffset.of(2),
        0L,
        tx -> {
          try {
            tx.save(P, "seed", new Row("seed", "stale"), 7);
          } catch (OptimisticLockException e) {
            caught.add(e);
          }
          tx.save(P, "c", new Row("c", "after the conflict"));
        },
        null);

    assertThat(caught).hasSize(1);
    assertThat(rows()).containsExactly("c", "seed");
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(2));
  }

  /** A read model the mapper cannot write is the application's error, not a failed statement. */
  @Test
  void aReadModelTheMapperCannotWriteDoesNotAbortTheBatch() throws SQLException {
    repo.executeAtomically(
        P,
        SqliteProjectionRepositoryCrashTest.Batches.of(2, 2),
        GlobalOffset.of(2),
        0L,
        tx -> {
          try {
            tx.save(P, "u", new SqliteProjectionRepositoryTest.Unwritable());
          } catch (EventStoreException expected) {
            // the mapper's failure, before any statement
          }
          tx.save(P, "c", new Row("c", "after"));
        },
        null);

    assertThat(rows()).containsExactly("c", "seed");
  }
}
