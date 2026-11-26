package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * A projection batch and a dead-letter replay hold SQLite's write lock while the application's
 * updater runs, and publish that transaction on their thread through {@link
 * SqliteWriteTransaction}, for their file only and only while the updater runs. A second connection
 * of the same thread cannot write meanwhile: it waits {@code busy_timeout} for a lock its own
 * thread holds. A component that runs a statement on the published connection runs it inside the
 * batch: it commits and rolls back with it, and a failure it reports aborts the batch.
 */
class SqliteWriteTransactionTest {

  private static final ProjectionName P = ProjectionName.of("joined");
  private static final int BUSY_MILLIS = 300;

  @TempDir Path dir;

  private SqliteDatabase database;
  private SqliteReadModelCheckpoints checkpoints;
  private SqliteProjectionRepository repo;
  private final List<SqliteDatabase> others = new ArrayList<>();

  @BeforeEach
  void open() throws SQLException {
    database =
        SqliteDatabase.open(
            TestDatabases.dataSource(dir.resolve("joined.db"), "busy_timeout=" + BUSY_MILLIS));
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    checkpoints = new SqliteReadModelCheckpoints(database);
    repo =
        new SqliteProjectionRepository(
            database,
            SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP),
            checkpoints);
    try (Connection conn = database.dataSource().getConnection();
        Statement statement = conn.createStatement()) {
      statement.execute("CREATE TABLE side (id TEXT PRIMARY KEY) STRICT");
    }
  }

  @AfterEach
  void close() {
    others.forEach(SqliteDatabase::close);
    database.close();
  }

  /**
   * The hazard the published transaction answers: inside a batch, a write on a second connection of
   * the same file waits busy_timeout for the lock its own thread holds, and fails.
   */
  @Test
  void aSecondConnectionOfTheBatchsThreadCannotWriteUntilBusyTimeout() {
    var refusal = new AtomicReference<SQLException>();
    var waited = new java.util.concurrent.atomic.AtomicLong();

    repo.executeAtomically(
        P,
        List.of(),
        GlobalOffset.of(1),
        0L,
        tx -> {
          long start = System.nanoTime();
          try (Connection second = database.dataSource().getConnection();
              Statement statement = second.createStatement()) {
            statement.execute("BEGIN IMMEDIATE");
          } catch (SQLException e) {
            refusal.set(e);
          }
          waited.set((System.nanoTime() - start) / 1_000_000);
        },
        checkpoints);

    assertThat((Throwable) refusal.get()).isInstanceOf(SQLiteException.class);
    assertThat(((SQLiteException) refusal.get()).getResultCode().code & 0xff)
        .isEqualTo(SQLiteErrorCode.SQLITE_BUSY.code);
    assertThat(waited.get()).isGreaterThanOrEqualTo(BUSY_MILLIS - 50L);
  }

  @Test
  void aBatchPublishesItsTransactionForItsFileWhileTheUpdaterRuns() throws SQLException {
    Path otherFile = dir.resolve("other.db");
    others.add(SqliteDatabase.open(TestDatabases.dataSource(otherFile)));
    var held = new AtomicReference<SqliteWriteTransaction>();
    var other = new AtomicReference<SqliteWriteTransaction>();
    assertThat(SqliteWriteTransaction.heldOn(database.file())).isNull();

    repo.executeAtomically(
        P,
        List.of(),
        GlobalOffset.of(1),
        0L,
        tx -> {
          held.set(SqliteWriteTransaction.heldOn(database.file()));
          other.set(SqliteWriteTransaction.heldOn(others.getFirst().file()));
          insertSide(held.get().connection(), "committed-with-the-batch");
        },
        checkpoints);

    assertThat(held.get()).isNotNull();
    assertThat(held.get().file()).isEqualTo(database.file());
    assertThat(other.get()).as("another file's transaction is not this one").isNull();
    assertThat(SqliteWriteTransaction.heldOn(database.file())).as("only while it runs").isNull();
    assertThat(sideRows()).containsExactly("committed-with-the-batch");
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(1));
  }

  @Test
  void aStatementOnThePublishedConnectionRollsBackWithTheBatch() throws SQLException {
    assertThatThrownBy(
            () ->
                repo.executeAtomically(
                    P,
                    List.of(),
                    GlobalOffset.of(1),
                    0L,
                    tx -> {
                      insertSide(
                          SqliteWriteTransaction.heldOn(database.file()).connection(), "gone");
                      throw new IllegalStateException("updater fails");
                    },
                    checkpoints))
        .isExactlyInstanceOf(IllegalStateException.class);

    assertThat(sideRows()).isEmpty();
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.initial());
  }

  /**
   * A failure reported through {@link SqliteWriteTransaction#abort} aborts the batch as a failed
   * statement of the handed repository does: the updater that swallows it can neither write again
   * nor commit.
   */
  @Test
  void aReportedFailureAbortsTheBatchEvenWhenTheUpdaterSwallowsIt() throws SQLException {
    var laterWrite = new AtomicReference<RuntimeException>();

    assertThatThrownBy(
            () ->
                repo.executeAtomically(
                    P,
                    List.of(),
                    GlobalOffset.of(1),
                    0L,
                    tx -> {
                      SqliteWriteTransaction held = SqliteWriteTransaction.heldOn(database.file());
                      insertSide(held.connection(), "before-the-failure");
                      try (Statement statement = held.connection().createStatement()) {
                        statement.execute("INSERT INTO missing_table VALUES (1)");
                      } catch (SQLException e) {
                        held.abort(e);
                      }
                      try {
                        tx.save(P, "after", java.util.Map.of("id", "after"));
                      } catch (RuntimeException e) {
                        laterWrite.set(e);
                      }
                    },
                    checkpoints))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("aborted by an earlier failure")
        .hasRootCauseInstanceOf(SQLiteException.class);

    assertThat(laterWrite.get()).isInstanceOf(EventStoreException.class);
    assertThat(sideRows()).isEmpty();
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.initial());
  }

  @Test
  void aReplayPublishesItsTransactionToo() throws SQLException {
    var held = new AtomicReference<SqliteWriteTransaction>();

    repo.executeReplay(
        P,
        List.of(),
        tx -> {
          held.set(SqliteWriteTransaction.heldOn(database.file()));
          insertSide(held.get().connection(), "replayed");
        });

    assertThat(held.get()).isNotNull();
    assertThat(sideRows()).containsExactly("replayed");
    assertThat(SqliteWriteTransaction.heldOn(database.file())).isNull();
  }

  @Test
  void aWriteOutsideABatchPublishesNothing() {
    var held = new AtomicReference<SqliteWriteTransaction>(null);
    repo.save(P, "outer", java.util.Map.of("id", "outer"));
    held.set(SqliteWriteTransaction.heldOn(database.file()));

    assertThat(held.get()).isNull();
  }

  /**
   * After SQLite rolled the transaction back by itself, {@link
   * SqliteWriteTransaction#requireOpen()} refuses, so a joining component runs nothing that would
   * commit on its own.
   */
  @Test
  void requireOpenRefusesOnceTheTransactionIsGone() {
    var whileOpen = new AtomicReference<Throwable>();
    var afterRollback = new AtomicReference<Throwable>();

    assertThatThrownBy(
            () ->
                repo.executeAtomically(
                    P,
                    List.of(),
                    GlobalOffset.of(1),
                    0L,
                    tx -> {
                      SqliteWriteTransaction held = SqliteWriteTransaction.heldOn(database.file());
                      try {
                        held.requireOpen();
                      } catch (Throwable t) {
                        whileOpen.set(t);
                      }
                      try (Statement statement = held.connection().createStatement()) {
                        statement.execute("ROLLBACK");
                        held.requireOpen();
                      } catch (Throwable t) {
                        afterRollback.set(t);
                      }
                    },
                    checkpoints))
        .isExactlyInstanceOf(IllegalStateException.class);

    assertThat(whileOpen.get()).isNull();
    assertThat(afterRollback.get()).isExactlyInstanceOf(IllegalStateException.class);
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.initial());
  }

  @Test
  void nullArgumentsAreRefused() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> SqliteWriteTransaction.heldOn(null))
        .withMessage("file must not be null");
    var held = new AtomicReference<SqliteWriteTransaction>();
    repo.executeAtomically(
        P,
        List.of(),
        GlobalOffset.of(1),
        0L,
        tx -> held.set(SqliteWriteTransaction.heldOn(database.file())),
        checkpoints);
    assertThatIllegalArgumentException()
        .isThrownBy(() -> held.get().abort(null))
        .withMessage("failure must not be null");
    assertThat(held.get().toString()).contains(database.file().toString());
  }

  /**
   * A reference kept past the updater is ended: it refuses to hand out the connection, which has
   * gone back to the pool, and to abort or probe a transaction that is over. It ends when the
   * updater throws too.
   */
  @Test
  void aTransactionKeptPastItsUpdaterIsEnded() {
    var kept = new AtomicReference<SqliteWriteTransaction>();
    repo.executeAtomically(
        P,
        List.of(),
        GlobalOffset.of(1),
        0L,
        tx -> kept.set(SqliteWriteTransaction.heldOn(database.file())),
        checkpoints);
    var keptFromAFailure = new AtomicReference<SqliteWriteTransaction>();
    assertThatThrownBy(
            () ->
                repo.executeReplay(
                    P,
                    List.of(),
                    tx -> {
                      keptFromAFailure.set(SqliteWriteTransaction.heldOn(database.file()));
                      throw new IllegalStateException("updater fails");
                    }))
        .hasMessage("updater fails");

    for (SqliteWriteTransaction ended : List.of(kept.get(), keptFromAFailure.get())) {
      assertThatThrownBy(ended::connection)
          .isExactlyInstanceOf(IllegalStateException.class)
          .hasMessageContaining("has ended");
      assertThatThrownBy(() -> ended.abort(new SQLException("late")))
          .isExactlyInstanceOf(IllegalStateException.class);
      assertThatThrownBy(ended::requireOpen).isExactlyInstanceOf(IllegalStateException.class);
    }
  }

  /**
   * The binding answers on the thread that published it only. A thread that carries the binding by
   * inheritance (a fork of a structured task scope opened in the updater) gets {@code null}, so it
   * never runs statements on the batch's connection beside the updater; here the inherited binding
   * is reproduced by binding the published transaction on another thread.
   */
  @Test
  void anotherThreadCarryingTheBindingDoesNotSeeTheTransaction() throws Exception {
    var held = new AtomicReference<SqliteWriteTransaction>();
    var seenByAnotherThread = new AtomicReference<Object>("not run");
    @SuppressWarnings("unchecked")
    ScopedValue<SqliteWriteTransaction> binding =
        (ScopedValue<SqliteWriteTransaction>) bindingOfTheClass();

    repo.executeAtomically(
        P,
        List.of(),
        GlobalOffset.of(1),
        0L,
        tx -> {
          held.set(SqliteWriteTransaction.heldOn(database.file()));
          Thread other =
              Thread.ofPlatform()
                  .start(
                      () ->
                          ScopedValue.where(binding, held.get())
                              .run(
                                  () ->
                                      seenByAnotherThread.set(
                                          SqliteWriteTransaction.heldOn(database.file()))));
          try {
            other.join();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        },
        checkpoints);

    assertThat(held.get()).isNotNull();
    assertThat(seenByAnotherThread.get()).isNull();
  }

  private static Object bindingOfTheClass() throws ReflectiveOperationException {
    var field = SqliteWriteTransaction.class.getDeclaredField("CURRENT");
    field.setAccessible(true);
    return field.get(null);
  }

  private static void insertSide(Connection conn, String id) {
    try (var ps = conn.prepareStatement("INSERT INTO side (id) VALUES (?)")) {
      ps.setString(1, id);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private List<String> sideRows() throws SQLException {
    List<String> rows = new ArrayList<>();
    try (Connection conn = database.dataSource().getConnection();
        Statement statement = conn.createStatement();
        var rs = statement.executeQuery("SELECT id FROM side ORDER BY id")) {
      while (rs.next()) {
        rows.add(rs.getString(1));
      }
    }
    return rows;
  }
}
