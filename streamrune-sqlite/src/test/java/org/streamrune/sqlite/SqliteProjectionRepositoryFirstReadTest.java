package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.LockMode;
import org.streamrune.core.PageRequest;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * The first read of a projection's table by a new {@link SqliteProjectionRepository}, as after a
 * process restart: a table that exists is found with a plain read of the schema, so the read never
 * waits for SQLite's write lock; only a table that is missing is created, under the lock.
 *
 * <p>Each test "restarts": it writes through one database handle and repository, closes them, and
 * reads through a new handle and a new repository on the same file, which has checked no table yet.
 * The connections give up on the write lock after {@value #BUSY_MILLIS} ms.
 */
class SqliteProjectionRepositoryFirstReadTest {

  static final int BUSY_MILLIS = 300;

  static final ProjectionName CUSTOMERS = ProjectionName.of("customers");
  static final ProjectionName ORDERS = ProjectionName.of("orders");

  record View(String id, String data) {
    @JsonCreator
    View(@JsonProperty("id") String id, @JsonProperty("data") String data) {
      this.id = id;
      this.data = data;
    }
  }

  @TempDir Path dir;

  private Path file;
  private SqliteDatabase database;
  private SqliteReadModelCheckpoints checkpoints;
  private SqliteProjectionRepository repo;
  private final ExecutorService otherThread = Executors.newSingleThreadExecutor();

  @BeforeEach
  void writeThenRestart() {
    file = dir.resolve("first-read.db");
    open();
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    repo.save(CUSTOMERS, "c-1", new View("c-1", "Ada"));
    repo.save(ORDERS, "o-0", new View("o-0", "seed"));
    database.close();
    open();
  }

  /** A new handle and a new repository on the file: no table checked yet. */
  private void open() {
    database = SqliteDatabase.open(TestDatabases.dataSource(file, "busy_timeout=" + BUSY_MILLIS));
    checkpoints = new SqliteReadModelCheckpoints(database);
    repo = newRepository();
  }

  /** A new repository on the open handle: it has checked no table yet. */
  private SqliteProjectionRepository newRepository() {
    return new SqliteProjectionRepository(
        database,
        SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP),
        checkpoints);
  }

  @AfterEach
  void close() {
    otherThread.shutdownNow();
    database.close();
  }

  private boolean tableExists(String table) throws SQLException {
    try (Connection conn = TestDatabases.dataSource(file).getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?")) {
      ps.setString(1, table);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * Every kind of read, each as the first touch of the table by a new repository, answers while
   * another connection holds the write lock: none of them waits for it.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void afterARestartEveryReadOfAnExistingTableAnswersWhileAnotherConnectionHoldsTheWriteLock()
      throws SQLException {
    try (Connection holder = TestDatabases.dataSource(file).getConnection();
        Statement st = holder.createStatement()) {
      st.execute("BEGIN IMMEDIATE");
      try {
        long start = System.nanoTime();
        assertThat(repo.findById(CUSTOMERS, "c-1", View.class)).contains(new View("c-1", "Ada"));
        assertThat(newRepository().findAll(CUSTOMERS, View.class))
            .containsExactly(new View("c-1", "Ada"));
        assertThat(newRepository().findAll(CUSTOMERS, View.class, PageRequest.of(0, 10)).content())
            .containsExactly(new View("c-1", "Ada"));
        assertThat(
                newRepository()
                    .findAllBy(
                        CUSTOMERS, View.class, FieldEquals.of("data", "Ada"), PageRequest.of(0, 10))
                    .totalElements())
            .isEqualTo(1);
        assertThat(newRepository().findById(CUSTOMERS, "c-1", View.class, LockMode.OPTIMISTIC))
            .map(v -> v.data())
            .contains(new View("c-1", "Ada"));
        // A table found once is remembered: the repeated read probes nothing.
        assertThat(repo.findById(CUSTOMERS, "c-1", View.class)).isPresent();
        assertThat((System.nanoTime() - start) / 1_000_000)
            .as("no read waited for the write lock (busy_timeout is %d ms)", BUSY_MILLIS)
            .isLessThan(BUSY_MILLIS);
      } finally {
        st.execute("ROLLBACK");
      }
    }
  }

  /**
   * The worst case of the lazy creation: after a restart, a batch's updater on another thread's
   * connection looks another projection's read model up through the repository itself, as the first
   * touch of that table in the process. The lookup runs on a thread the updater started, so it has
   * no transaction to join; it reads the table the first process created without waiting for the
   * write lock the batch holds.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void afterARestartALookupFromAThreadOfTheUpdaterReadsWhileTheBatchHoldsTheWriteLock() {
    repo.executeAtomically(
        ORDERS,
        List.of(),
        GlobalOffset.of(1),
        0L,
        tx -> {
          Future<View> customer =
              otherThread.submit(() -> repo.findById(CUSTOMERS, "c-1", View.class).orElseThrow());
          try {
            tx.save(ORDERS, "o-1", new View("o-1", customer.get(30, TimeUnit.SECONDS).data()));
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        },
        checkpoints);

    assertThat(repo.findById(ORDERS, "o-1", View.class)).contains(new View("o-1", "Ada"));
    assertThat(checkpoints.getLastOffset(ORDERS)).isEqualTo(GlobalOffset.of(1));
  }

  /** A read of a table no one created creates it, as before: it is empty, and it exists. */
  @Test
  void aReadOfAMissingTableCreatesIt() throws SQLException {
    var unseen = ProjectionName.of("unseen");
    assertThat(tableExists("unseen_view")).isFalse();

    assertThat(repo.findAll(unseen, View.class)).isEmpty();

    assertThat(tableExists("unseen_view")).isTrue();
  }

  /**
   * Only the creation takes the write lock: the first read of a table that does not exist waits for
   * a writer and fails after busy_timeout, naming the creation, while the same read of a table that
   * exists answers.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aReadOfAMissingTableWaitsForTheWriteLockItsCreationNeeds() throws SQLException {
    var unseen = ProjectionName.of("unseen");
    try (Connection holder = TestDatabases.dataSource(file).getConnection();
        Statement st = holder.createStatement()) {
      st.execute("BEGIN IMMEDIATE");
      try {
        assertThatThrownBy(() -> repo.findAll(unseen, View.class))
            .isExactlyInstanceOf(EventStoreException.class)
            .hasMessageContaining("creation of projection table unseen_view stored nothing");
        assertThat(repo.findAll(ORDERS, View.class)).containsExactly(new View("o-0", "seed"));
      } finally {
        st.execute("ROLLBACK");
      }
    }
    assertThat(repo.findAll(unseen, View.class)).isEmpty();
  }

  /**
   * A table another connection created after this repository last looked is found by the probe: a
   * write to it creates nothing and stores the row.
   */
  @Test
  void aWriteToATableThatExistsCreatesNothingAndStoresTheRow() {
    repo.save(CUSTOMERS, "c-2", new View("c-2", "Grace"));

    assertThat(repo.findAll(CUSTOMERS, View.class))
        .containsExactlyInAnyOrder(new View("c-1", "Ada"), new View("c-2", "Grace"));
  }

  /** A schema that cannot be read fails the probe with the operation's storage exception. */
  @Test
  void aProbeThatCannotReadTheSchemaIsAnEventStoreException() {
    var failing = new AtomicBoolean();
    DataSource delegate = TestDatabases.dataSource(dir.resolve("probe.db"));
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
                  } catch (InvocationTargetException e) {
                    throw e.getCause();
                  }
                });
    try (SqliteDatabase handle = SqliteDatabase.open(source)) {
      handle.migrate(SqliteSchemaSeries.READ_MODEL);
      var broken =
          new SqliteProjectionRepository(
              handle,
              SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP),
              new SqliteReadModelCheckpoints(handle));
      failing.set(true);

      assertThatThrownBy(() -> broken.findAll(CUSTOMERS, View.class))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to look up projection table: customers_view")
          .hasCauseInstanceOf(SQLException.class);
    }
  }
}
