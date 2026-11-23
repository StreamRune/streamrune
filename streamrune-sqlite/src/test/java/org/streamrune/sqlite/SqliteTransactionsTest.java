package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.EventStoreContract.ContractEvent;

/**
 * {@link SqliteTransactions} against the real driver on a HikariCP pool of one connection, without
 * any proxy between the store and the driver (but the one that injects a failed ROLLBACK): a failed
 * write never leaves the pool's connection in manual commit, holding the write lock, or waiting for
 * the lock a second time.
 */
class SqliteTransactionsTest {

  private static final StreamId A = StreamId.of(AggregateType.of("order"), AggregateId.of("A"));

  @TempDir Path dir;

  private final List<AutoCloseable> resources = new ArrayList<>();

  @AfterEach
  void close() throws Exception {
    for (AutoCloseable resource : resources.reversed()) {
      resource.close();
    }
  }

  private HikariDataSource pool(Path file, String settings) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(
        "jdbc:sqlite:file:" + file + "?" + TestDatabases.REQUIRED_SETTINGS + settings);
    config.setMaximumPoolSize(1);
    config.setPoolName("sqlite-transactions-test");
    HikariDataSource pool = new HikariDataSource(config);
    resources.add(pool);
    return pool;
  }

  private SqliteDatabase open(HikariDataSource pool) {
    SqliteDatabase database = SqliteDatabase.open(pool);
    resources.add(database);
    return database;
  }

  /**
   * Disk full inside an append (SQLite undoes the failed statement; the ROLLBACK undoes the rest):
   * the pool's one connection stays in autocommit, holds no lock, and stores the next append once
   * there is room again.
   */
  @Test
  void anAppendThatFillsTheDiskLeavesThePoolsConnectionUsable() throws Exception {
    Path file = dir.resolve("full.db");
    HikariDataSource pool = pool(file, "");
    SqliteDatabase database = open(pool);
    EventStore store =
        new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry())
            .create();
    store.append(A, List.of(event("first")), Version.initial());
    SQLiteConnection physical;
    try (Connection conn = pool.getConnection();
        Statement st = conn.createStatement()) {
      physical = conn.unwrap(SQLiteConnection.class);
      long pages = single(st, "PRAGMA page_count");
      st.execute("PRAGMA max_page_count = " + (pages + 1));
    }

    assertThatThrownBy(() -> store.append(A, List.of(event("x".repeat(200_000))), new Version(1)))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasRootCauseInstanceOf(SQLiteException.class)
        .satisfies(
            e ->
                assertThat(((SQLiteException) rootCause(e)).getResultCode())
                    .isEqualTo(SQLiteErrorCode.SQLITE_FULL));

    try (Connection conn = pool.getConnection();
        Statement st = conn.createStatement()) {
      assertThat(conn.unwrap(SQLiteConnection.class))
          .as("SQLite rolled back by itself; the same connection is kept, not closed")
          .isSameAs(physical);
      assertThat(physical.isClosed()).isFalse();
      assertThat(conn.getAutoCommit()).isTrue();
      assertThat(physical.getAutoCommit()).isTrue();
      st.execute("PRAGMA max_page_count = 1073741823");
    }
    assertWriteLockFree(file);
    assertThat(store.append(A, List.of(event("second")), new Version(1)).globalOffsets())
        .containsExactly(GlobalOffset.of(2));
    assertThat(pool.getHikariPoolMXBean().getTotalConnections()).isEqualTo(1);
  }

  /**
   * Disk full where SQLite rolls the whole transaction back by itself: the ROLLBACK then finds no
   * transaction, which counts as rolled back. The connection is kept, not closed, stays in
   * autocommit and writes once there is room.
   */
  @Test
  void aTransactionSqliteRolledBackByItselfCountsAsRolledBack() throws Exception {
    Path file = dir.resolve("auto-rollback.db");
    HikariDataSource pool = pool(file, "");
    open(pool);
    execute(pool, "CREATE TABLE scratch (x TEXT) STRICT");
    execute(pool, "CREATE TABLE tally (n INTEGER) STRICT");
    execute(pool, "INSERT INTO tally VALUES (0)");

    try (Connection conn = pool.getConnection();
        Statement st = conn.createStatement()) {
      SQLiteConnection physical = conn.unwrap(SQLiteConnection.class);
      st.execute("PRAGMA max_page_count = " + (single(st, "PRAGMA page_count") + 1));
      SqliteTransactions.begin(conn);
      execute(conn, "UPDATE tally SET n = n + 1");
      assertThatThrownBy(
              () -> execute(conn, "INSERT INTO scratch VALUES ('" + "x".repeat(200_000) + "')"))
          .isInstanceOfSatisfying(
              SQLiteException.class,
              e -> assertThat(e.getResultCode()).isEqualTo(SQLiteErrorCode.SQLITE_FULL));
      assertThatThrownBy(() -> execute(conn, "ROLLBACK"))
          .as("SQLite already ended the transaction")
          .hasMessageContaining("no transaction is active");

      SqliteTransactions.begin(conn);
      execute(conn, "UPDATE tally SET n = n + 1");
      assertThatThrownBy(
              () -> execute(conn, "INSERT INTO scratch VALUES ('" + "x".repeat(200_000) + "')"))
          .isInstanceOf(SQLiteException.class);
      assertThat(SqliteTransactions.rollback(conn, "test")).isTrue();

      assertThat(physical.isClosed()).as("the connection is kept").isFalse();
      assertThat(conn.unwrap(SQLiteConnection.class)).isSameAs(physical);
      assertThat(conn.getAutoCommit()).isTrue();
      assertWriteLockFree(file);
      assertThat(single(st, "SELECT n FROM tally")).isZero();
      st.execute("PRAGMA max_page_count = 1073741823");
      SqliteTransactions.inTransaction(
          conn, "test", c -> execute(c, "INSERT INTO scratch VALUES ('fits')"));
      assertThat(single(st, "SELECT count(*) FROM scratch")).isEqualTo(1);
    }
  }

  /**
   * A rollback while another writer waits for the lock and then holds it longer than busy_timeout:
   * the ROLLBACK does not take the lock again, so it returns at once; the pool's connection stays
   * in autocommit, waits one busy_timeout for its next write, and writes once the lock is free.
   */
  @Test
  @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aRollbackUnderContentionDoesNotWaitForTheLockAgain() throws Exception {
    Path file = dir.resolve("contended.db");
    HikariDataSource pool = pool(file, "&busy_timeout=300");
    open(pool);
    execute(pool, "CREATE TABLE scratch (x INTEGER) STRICT");
    CountDownLatch acquired = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CompletableFuture<Void> other;
    try (Connection conn = pool.getConnection()) {
      SqliteTransactions.begin(conn);
      execute(conn, "INSERT INTO scratch VALUES (1)");
      other =
          CompletableFuture.runAsync(
              () -> {
                try (Connection holder = TestDatabases.dataSource(file).getConnection()) {
                  execute(holder, "BEGIN IMMEDIATE");
                  acquired.countDown();
                  release.await(20, TimeUnit.SECONDS);
                  execute(holder, "ROLLBACK");
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              });
      Thread.sleep(200); // the other writer is now waiting for the lock

      long started = System.nanoTime();
      assertThat(SqliteTransactions.rollback(conn, "test")).isTrue();
      assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)).isLessThan(250);
      assertThat(conn.getAutoCommit()).isTrue();
    }
    assertThat(acquired.await(10, TimeUnit.SECONDS)).isTrue();

    try (Connection conn = pool.getConnection()) {
      assertThat(count(conn, "SELECT count(*) FROM scratch")).isZero();
      long started = System.nanoTime();
      assertThatThrownBy(
              () ->
                  SqliteTransactions.inTransaction(
                      conn, "test", c -> execute(c, "INSERT INTO scratch VALUES (2)")))
          .isInstanceOfSatisfying(
              SQLException.class, e -> assertThat(SqliteEventStore.isBusy(e)).isTrue());
      assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)).isLessThan(1_500);
      assertThat(conn.getAutoCommit()).isTrue();
      release.countDown();
      other.get(10, TimeUnit.SECONDS);
      SqliteTransactions.inTransaction(
          conn, "test", c -> execute(c, "INSERT INTO scratch VALUES (3)"));
      assertThat(count(conn, "SELECT count(*) FROM scratch")).isEqualTo(1);
    }
    assertWriteLockFree(file);
  }

  /**
   * A COMMIT that fails with the transaction still open (a deferred foreign key): the helper rolls
   * it back, no row is left, no lock is held, and the next transaction on that connection works.
   */
  @Test
  void aCommitRefusedByADeferredForeignKeyLeavesNoRowAndNoLock() throws Exception {
    Path file = dir.resolve("deferred.db");
    HikariDataSource pool = pool(file, "");
    open(pool);
    execute(pool, "CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY) STRICT");
    execute(
        pool,
        "CREATE TABLE child (id INTEGER NOT NULL PRIMARY KEY, parent_id INTEGER REFERENCES"
            + " parent (id) DEFERRABLE INITIALLY DEFERRED) STRICT");

    try (Connection conn = pool.getConnection()) {
      assertThatThrownBy(
              () ->
                  SqliteTransactions.inTransaction(
                      conn, "test", c -> execute(c, "INSERT INTO child VALUES (1, 99)")))
          .isInstanceOfSatisfying(
              SQLiteException.class,
              e ->
                  assertThat(e.getResultCode())
                      .isEqualTo(SQLiteErrorCode.SQLITE_CONSTRAINT_FOREIGNKEY));
      assertThat(conn.getAutoCommit()).isTrue();
      assertWriteLockFree(file);
      assertThat(count(conn, "SELECT count(*) FROM child")).isZero();

      SqliteTransactions.inTransaction(
          conn,
          "test",
          c -> {
            execute(c, "INSERT INTO parent VALUES (99)");
            return execute(c, "INSERT INTO child VALUES (1, 99)");
          });
    }
    try (Connection other = TestDatabases.dataSource(file).getConnection()) {
      assertThat(count(other, "SELECT count(*) FROM child")).isEqualTo(1);
    }
    assertWriteLockFree(file);
  }

  /**
   * A begin on a connection whose application already began a transaction fails, and neither
   * commits nor rolls back that transaction: it is not the helper's.
   */
  @Test
  void aBeginInsideAnotherTransactionLeavesThatTransactionAlone() throws Exception {
    Path file = dir.resolve("foreign.db");
    HikariDataSource pool = pool(file, "");
    open(pool);
    execute(pool, "CREATE TABLE scratch (x INTEGER) STRICT");

    try (Connection conn = pool.getConnection()) {
      execute(conn, "BEGIN");
      execute(conn, "INSERT INTO scratch VALUES (1)");

      assertThatThrownBy(() -> SqliteTransactions.begin(conn))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("within a transaction");

      assertThat(count(conn, "SELECT count(*) FROM scratch"))
          .as("the application's transaction still sees its row")
          .isEqualTo(1);
      try (Connection other = TestDatabases.dataSource(file).getConnection()) {
        assertThat(count(other, "SELECT count(*) FROM scratch"))
            .as("nothing was committed")
            .isZero();
      }
      execute(conn, "ROLLBACK");
    }
    try (Connection other = TestDatabases.dataSource(file).getConnection()) {
      assertThat(count(other, "SELECT count(*) FROM scratch")).isZero();
    }
  }

  /**
   * A ROLLBACK statement that fails (injected) makes the helper close the physical connection:
   * SQLite rolls the transaction back and releases the lock. HikariCP evicts the closed connection
   * at the first borrow that comes after its alive-bypass window (500 ms) and validates it; until
   * then it would hand the closed connection out again.
   */
  @Test
  void aFailedRollbackClosesThePhysicalConnection() throws Exception {
    Path file = dir.resolve("closed.db");
    HikariDataSource pool = pool(file, "");
    open(pool);
    execute(pool, "CREATE TABLE scratch (x INTEGER) STRICT");

    Connection conn = pool.getConnection();
    SqliteTransactions.begin(conn);
    execute(conn, "INSERT INTO scratch VALUES (1)");
    SQLiteConnection physical = conn.unwrap(SQLiteConnection.class);

    assertThat(SqliteTransactions.rollback(failingRollback(conn), "test")).isFalse();

    assertThat(physical.isClosed()).isTrue();
    assertWriteLockFree(file);
    try {
      conn.close();
    } catch (SQLException expected) {
      // returning a wrapper whose physical connection is closed may report it
    }
    try (Connection other = TestDatabases.dataSource(file).getConnection()) {
      assertThat(count(other, "SELECT count(*) FROM scratch")).isZero();
    }
    // Past HikariCP's alive-bypass window (500 ms) the pool validates and replaces the connection.
    Thread.sleep(700);
    try (Connection again = pool.getConnection()) {
      SqliteTransactions.inTransaction(
          again, "test", c -> execute(c, "INSERT INTO scratch VALUES (2)"));
      assertThat(count(again, "SELECT count(*) FROM scratch")).isEqualTo(1);
    }
  }

  /**
   * The statements the helper issues, recorded: a rollback is never followed by a statement of the
   * helper's (a re-BEGIN after the ROLLBACK would take the write lock again and could hold it).
   */
  @Test
  void aRollbackIsNeverFollowedByAStatementOfTheHelper() throws Exception {
    Path file = dir.resolve("recorded.db");
    HikariDataSource pool = pool(file, "");
    open(pool);
    execute(pool, "CREATE TABLE scratch (x INTEGER NOT NULL) STRICT");

    try (Connection real = pool.getConnection()) {
      List<String> statements = new ArrayList<>();
      Connection conn = recording(real, statements);

      assertThatThrownBy(
              () ->
                  SqliteTransactions.inTransaction(
                      conn,
                      "test",
                      c -> {
                        execute(c, "INSERT INTO scratch VALUES (1)");
                        throw new SQLException("work failed");
                      }))
          .hasMessage("work failed");

      assertThat(statements)
          .containsExactly("BEGIN IMMEDIATE", "INSERT INTO scratch VALUES (1)", "ROLLBACK");
      assertThat(count(real, "SELECT count(*) FROM scratch")).isZero();
      assertWriteLockFree(file);

      statements.clear();
      assertThatThrownBy(
              () ->
                  SqliteTransactions.inTransaction(
                      conn, "test", c -> execute(c, "INSERT INTO scratch VALUES (NULL)")))
          .isInstanceOf(SQLiteException.class);
      assertThat(statements)
          .containsExactly("BEGIN IMMEDIATE", "INSERT INTO scratch VALUES (NULL)", "ROLLBACK");
      assertWriteLockFree(file);

      statements.clear();
      assertThat(SqliteTransactions.rollback(conn, "test"))
          .as("nothing open: SQLite reports no transaction, which counts as rolled back")
          .isTrue();
      assertThat(statements).containsExactly("ROLLBACK");
    }
  }

  /**
   * A failed BEGIN (the application's own transaction is open on the connection) is outside the
   * helper's try: no ROLLBACK follows it, so the application's transaction is neither rolled back
   * nor otherwise touched, and the work does not run.
   */
  @Test
  void aFailedBeginIsNotFollowedByARollbackOrACommit() throws Exception {
    Path file = dir.resolve("failed-begin.db");
    HikariDataSource pool = pool(file, "");
    open(pool);
    execute(pool, "CREATE TABLE scratch (x INTEGER) STRICT");

    try (Connection real = pool.getConnection()) {
      List<String> statements = new ArrayList<>();
      Connection conn = recording(real, statements);
      execute(real, "BEGIN");
      execute(real, "INSERT INTO scratch VALUES (1)");
      boolean[] ran = {false};

      assertThatThrownBy(
              () ->
                  SqliteTransactions.inTransaction(
                      conn,
                      "test",
                      c -> {
                        ran[0] = true;
                        return null;
                      }))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("within a transaction");

      assertThat(statements).containsExactly("BEGIN IMMEDIATE");
      assertThat(ran[0]).isFalse();
      assertThat(count(real, "SELECT count(*) FROM scratch"))
          .as("the application's transaction still holds its row")
          .isEqualTo(1);
      execute(real, "ROLLBACK");
    }
  }

  /**
   * Only SQLite's own SQLITE_ERROR "no transaction is active" counts as rolled back: the same words
   * in any other exception are a failed rollback, and the connection is closed.
   */
  @Test
  void onlyASqliteErrorSayingNoTransactionIsActiveCountsAsRolledBack() throws Exception {
    Path file = dir.resolve("no-tx.db");
    HikariDataSource pool = pool(file, "");
    open(pool);

    Connection first = pool.getConnection();
    SQLiteConnection physicalFirst = first.unwrap(SQLiteConnection.class);
    assertThat(
            SqliteTransactions.rollback(
                failingRollback(
                    first, new SQLException("cannot rollback - no transaction is active")),
                "test"))
        .as("a plain SQLException is not SQLite's answer")
        .isFalse();
    assertThat(physicalFirst.isClosed()).isTrue();
    try {
      first.close();
    } catch (SQLException expected) {
      // the physical connection is closed
    }

    Thread.sleep(700); // past the pool's alive-bypass window: it replaces the closed connection
    Connection second = pool.getConnection();
    SQLiteConnection physicalSecond = second.unwrap(SQLiteConnection.class);
    assertThat(
            SqliteTransactions.rollback(
                failingRollback(
                    second,
                    new SQLiteException(
                        "[SQLITE_IOERR] cannot rollback - no transaction is active",
                        SQLiteErrorCode.SQLITE_IOERR)),
                "test"))
        .as("another result code is not SQLITE_ERROR")
        .isFalse();
    assertThat(physicalSecond.isClosed()).isTrue();
    try {
      second.close();
    } catch (SQLException expected) {
      // the physical connection is closed
    }
  }

  /**
   * {@code conn} whose statements' SQL is appended to {@code statements} as they run: every {@code
   * execute*} and {@code addBatch} call of a plain statement (its SQL argument), and every {@code
   * execute*} and {@code addBatch} call of a prepared statement (the SQL it was prepared with), so
   * that no statement the helper could issue goes unrecorded.
   */
  private static Connection recording(Connection conn, List<String> statements) {
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              Object result = invokeUnwrapped(conn, method, args);
              if (method.getName().equals("createStatement") && result instanceof Statement st) {
                return recordingStatement(st, Statement.class, null, statements);
              }
              if (method.getName().equals("prepareStatement")
                  && result instanceof PreparedStatement ps) {
                return recordingStatement(
                    ps, PreparedStatement.class, (String) args[0], statements);
              }
              return result;
            });
  }

  /**
   * {@code target} recording each {@code execute*} and {@code addBatch} call: the SQL of a prepared
   * statement is {@code preparedSql}, that of a plain statement the call's SQL argument.
   */
  private static Object recordingStatement(
      Statement target, Class<?> type, String preparedSql, List<String> statements) {
    return Proxy.newProxyInstance(
        type.getClassLoader(),
        new Class<?>[] {type},
        (proxy, method, args) -> {
          String name = method.getName();
          if ((name.startsWith("execute") && !name.equals("executeBatch"))
              || name.equals("addBatch")) {
            String sql =
                args != null && args.length > 0 && args[0] instanceof String text
                    ? text
                    : preparedSql;
            if (sql != null) {
              statements.add(sql);
            }
          }
          return invokeUnwrapped(target, method, args);
        });
  }

  private static Object invokeUnwrapped(Object target, Method method, Object[] args)
      throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  /**
   * The open-transaction probe outside any transaction: it refuses, leaves no transaction open on
   * the connection and holds no lock.
   */
  @Test
  void theOpenTransactionProbeRefusesAConnectionWithoutATransactionAndLeavesNoneOpen()
      throws Exception {
    Path file = dir.resolve("probe-none.db");
    SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(file));
    resources.add(database);
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      assertThatThrownBy(() -> SqliteTransactions.requireOpenTransaction(conn))
          .isExactlyInstanceOf(IllegalStateException.class)
          .hasMessage(SqliteTransactions.NO_OPEN_TRANSACTION);

      // No transaction is left open: a BEGIN of the caller's starts one.
      st.execute("BEGIN IMMEDIATE");
      st.execute("ROLLBACK");
      assertThat(conn.getAutoCommit()).isTrue();
    }
  }

  /**
   * Inside a write transaction with a pending write, the probe passes and changes nothing: the
   * write stays pending (invisible elsewhere), the lock stays held, and the COMMIT is durable. A
   * deferred BEGIN counts as open too.
   */
  @Test
  void theOpenTransactionProbePassesInsideATransactionAndKeepsItsPendingWrite() throws Exception {
    Path file = dir.resolve("probe-open.db");
    SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(file));
    resources.add(database);
    try (Connection setup = database.dataSource().getConnection();
        Statement st = setup.createStatement()) {
      st.execute("CREATE TABLE probe_rows (x INTEGER NOT NULL) STRICT");
    }
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute("BEGIN");
      SqliteTransactions.requireOpenTransaction(conn);
      st.execute("ROLLBACK");

      SqliteTransactions.begin(conn);
      st.execute("INSERT INTO probe_rows (x) VALUES (1)");
      SqliteTransactions.requireOpenTransaction(conn);

      assertThat(countRows(file)).as("the write is still pending").isZero();
      try (Connection other = TestDatabases.dataSource(file, "busy_timeout=1").getConnection();
          Statement otherSt = other.createStatement()) {
        assertThatThrownBy(() -> otherSt.execute("BEGIN IMMEDIATE"))
            .as("the write lock is still held")
            .isInstanceOfSatisfying(
                SQLiteException.class,
                e -> assertThat(e.getResultCode()).isEqualTo(SQLiteErrorCode.SQLITE_BUSY));
      }
      SqliteTransactions.commit(conn);
    }
    assertThat(countRows(file)).as("the COMMIT is durable").isEqualTo(1);
  }

  private static long countRows(Path file) throws SQLException {
    try (Connection conn = TestDatabases.dataSource(file).getConnection();
        Statement st = conn.createStatement();
        var rs = st.executeQuery("SELECT count(*) FROM probe_rows")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  @Test
  void aConnectionInManualCommitModeIsRefusedWithBothCauses() throws Exception {
    Path file = dir.resolve("manual.db");
    try (Connection conn = TestDatabases.dataSource(file).getConnection()) {
      conn.setAutoCommit(false);
      assertThatThrownBy(() -> SqliteTransactions.begin(conn))
          .isExactlyInstanceOf(IllegalStateException.class)
          .hasMessageContaining("autoCommit=false")
          .hasMessageContaining("application transaction");
      conn.rollback();
      conn.setAutoCommit(true);
    }
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────────────────────

  /** {@code conn}, whose next ROLLBACK statement fails as an I/O error would. */
  private static Connection failingRollback(Connection conn) {
    return failingRollback(conn, new SQLException("[SQLITE_IOERR] disk I/O error (injected)"));
  }

  /** {@code conn}, whose next statement creation fails with {@code failure}. */
  private static Connection failingRollback(Connection conn, SQLException failure) {
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              if (method.getName().equals("createStatement")) {
                throw failure;
              }
              try {
                return method.invoke(conn, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }

  private static EventEnvelope event(String value) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        A,
        Version.initial(),
        EventStoreContract.EVENT_TYPE,
        new ContractEvent(value),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            CorrelationId.of(UUID.randomUUID().toString()),
            null,
            null,
            Instant.parse("2026-11-23T10:00:00Z")));
  }

  private static Throwable rootCause(Throwable t) {
    Throwable root = t;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return root;
  }

  private static long single(Statement st, String sql) throws SQLException {
    try (var rs = st.executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private static long count(Connection conn, String sql) throws SQLException {
    try (Statement st = conn.createStatement()) {
      return single(st, sql);
    }
  }

  private static Void execute(Connection conn, String sql) throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute(sql);
    }
    return null;
  }

  private static void execute(HikariDataSource pool, String sql) throws SQLException {
    try (Connection conn = pool.getConnection()) {
      execute(conn, sql);
    }
  }

  /** Another connection gets the write lock at once: nothing holds it. */
  private static void assertWriteLockFree(Path file) throws SQLException {
    try (Connection probe = TestDatabases.dataSource(file, "busy_timeout=1").getConnection()) {
      execute(probe, "BEGIN IMMEDIATE");
      execute(probe, "ROLLBACK");
    }
  }
}
