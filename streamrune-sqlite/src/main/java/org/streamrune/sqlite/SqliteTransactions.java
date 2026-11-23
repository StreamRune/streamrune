package org.streamrune.sqlite;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConnection;

/**
 * How the SQLite stores run a write transaction on a borrowed connection: with SQL statements, on a
 * connection the driver keeps in autocommit.
 *
 * <p><b>Why statements.</b> sqlite-jdbc (3.54) ties a transaction to the connection's autocommit
 * flag in a way no store can make safe. {@code setAutoCommit(false)} issues {@code BEGIN IMMEDIATE}
 * at once; {@link Connection#commit()} and {@link Connection#rollback()} each issue the next {@code
 * BEGIN IMMEDIATE} right after their {@code COMMIT} or {@code ROLLBACK}, so a connection in manual
 * commit holds the database's write lock between operations, and a {@code BEGIN} that times out
 * after a durable {@code COMMIT} reports the commit as failed; {@link Connection#abort} does
 * nothing. A connection whose flag stays wrong after a failure goes back to the pool and refuses or
 * holds every later write. The flag is therefore never touched: {@link #begin} runs {@code BEGIN
 * IMMEDIATE}, {@link #commit} runs {@code COMMIT}, {@link #rollback} runs {@code ROLLBACK}, each as
 * a statement, and neither the driver's flag nor the pool's bookkeeping ever changes. (After each
 * statement in autocommit the driver runs its own autocommit check, which leaves an open
 * transaction alone: the write lock stays held from {@code BEGIN IMMEDIATE} to {@code COMMIT}.)
 *
 * <p><b>Begin.</b> {@code BEGIN IMMEDIATE} takes SQLite's write lock up front, waiting at most the
 * connection's {@code busy_timeout}, so writers queue for the lock instead of failing later on a
 * read-to-write upgrade. A begin that fails opened nothing, and nothing is undone: in particular, a
 * begin refused because the connection already has a transaction open (one the application began)
 * neither commits nor rolls back that transaction.
 *
 * <p><b>Rollback.</b> {@link #rollback} runs {@code ROLLBACK}, which does not take the write lock
 * again. When SQLite has already rolled the transaction back by itself (it does so on {@code
 * SQLITE_FULL}, for one), the statement reports that no transaction is active, and the transaction
 * is taken as rolled back. When the statement fails for any other reason, the physical connection
 * is closed: SQLite rolls back the open transaction and releases the lock as it does for a
 * connection that closes, or a process that dies, inside one, and the pool replaces the connection
 * when the caller returns its wrapper.
 *
 * <p><b>Autocommit connections only.</b> A connection that arrives in manual-commit mode is refused
 * with {@link IllegalStateException}: such a connection holds the write lock while it is not in
 * use, and a transaction on it is not this helper's to commit. {@link SqliteDatabase#open} refuses
 * a data source that hands one out; the check here also catches a connection that changed later.
 */
final class SqliteTransactions {

  private static final Logger log = LoggerFactory.getLogger(SqliteTransactions.class);

  /** The message of the refusal of a connection in manual-commit mode. */
  static final String MANUAL_COMMIT_CONNECTION =
      "The SQLite connection is in manual-commit mode (autoCommit=false). Either the connection pool"
          + " hands out connections with autoCommit=false, or the connection is enlisted in an"
          + " application transaction (Spring, JTA). With transaction_mode=IMMEDIATE the driver"
          + " opens a write transaction as soon as a connection enters manual commit, so such a"
          + " connection holds the database's write lock while it is not in use and blocks every"
          + " other writer, and a transaction on it is not the store's to commit. Configure the"
          + " pool to hand out connections in autocommit mode (for HikariCP: autoCommit=true, its"
          + " default), and call the store outside an application transaction.";

  /** What SQLite says when ROLLBACK finds no transaction: it rolled back by itself. */
  private static final String NO_TRANSACTION_ACTIVE = "no transaction is active";

  private SqliteTransactions() {}

  /** Work run inside one write transaction by {@link #inTransaction}. */
  @FunctionalInterface
  interface Work<T> {
    T run(Connection conn) throws SQLException;
  }

  /**
   * Runs {@code work} in one write transaction on {@code conn} and commits it; any exit without the
   * commit rolls the transaction back through {@link #rollback}.
   *
   * @param transaction what the work is, for the log record of a failed rollback (a constant, never
   *     an id)
   * @throws IllegalStateException if {@code conn} is in manual-commit mode
   */
  static <T> T inTransaction(Connection conn, String transaction, Work<T> work)
      throws SQLException {
    begin(conn);
    boolean committed = false;
    try {
      T result = work.run(conn);
      commit(conn);
      committed = true;
      return result;
    } finally {
      if (!committed) {
        rollback(conn, transaction);
      }
    }
  }

  /**
   * Begins a write transaction with {@code BEGIN IMMEDIATE}. A failure, for example the write lock
   * not granted within {@code busy_timeout}, opened nothing and propagates.
   *
   * @throws IllegalStateException if {@code conn} is in manual-commit mode
   */
  static void begin(Connection conn) throws SQLException {
    requireAutoCommit(conn);
    execute(conn, "BEGIN IMMEDIATE");
  }

  /**
   * Refuses a connection in manual-commit mode, before a write: also the check of a write that is
   * one statement in autocommit, which on such a connection would join a transaction that is not
   * the store's.
   *
   * @throws IllegalStateException if {@code conn} is in manual-commit mode
   */
  static void requireAutoCommit(Connection conn) throws SQLException {
    if (!conn.getAutoCommit()) {
      throw new IllegalStateException(MANUAL_COMMIT_CONNECTION);
    }
  }

  /**
   * Commits the transaction {@link #begin} opened. A failure propagates; the transaction may still
   * be open, so the caller rolls it back.
   */
  static void commit(Connection conn) throws SQLException {
    execute(conn, "COMMIT");
  }

  /**
   * Rolls back the transaction {@link #begin} opened, and returns {@code true}; returns {@code
   * false} when the rollback failed and the physical connection was closed instead. A {@code
   * SQLException} or {@code RuntimeException} is logged, never thrown, so it cannot replace the
   * exception in flight; an {@link Error} propagates, after the close.
   *
   * @param transaction what the transaction was, for the log record (a constant, never an id)
   */
  static boolean rollback(Connection conn, String transaction) {
    boolean rolledBack = false;
    try {
      execute(conn, "ROLLBACK");
      rolledBack = true;
    } catch (SQLException rollbackFailure) {
      String message = rollbackFailure.getMessage();
      if (message != null && message.contains(NO_TRANSACTION_ACTIVE)) {
        // SQLite rolled the transaction back by itself (SQLITE_FULL, SQLITE_IOERR, ...).
        rolledBack = true;
      } else {
        logRollbackFailure(transaction, rollbackFailure);
      }
    } catch (RuntimeException rollbackFailure) {
      logRollbackFailure(transaction, rollbackFailure);
    } finally {
      if (!rolledBack) {
        closePhysicalQuietly(conn);
      }
    }
    return rolledBack;
  }

  private static void logRollbackFailure(String transaction, Exception rollbackFailure) {
    log.warn(
        "Rollback of an uncommitted {} transaction failed; closing the SQLite connection so that"
            + " SQLite rolls it back and releases the write lock: {}",
        transaction,
        rollbackFailure.toString());
  }

  /**
   * Closes the physical SQLite connection under {@code conn}: SQLite rolls back its open
   * transaction and releases the write lock, and the pool replaces the connection when the caller
   * returns its wrapper. Never throws a {@code SQLException} or {@code RuntimeException}.
   */
  static void closePhysicalQuietly(Connection conn) {
    try {
      conn.unwrap(SQLiteConnection.class).close();
    } catch (SQLException | RuntimeException closeFailure) {
      log.warn(
          "Failed to close a SQLite connection whose rollback failed: {}", closeFailure.toString());
    }
  }

  private static void execute(Connection conn, String sql) throws SQLException {
    try (Statement statement = conn.createStatement()) {
      statement.execute(sql);
    }
  }
}
