package org.streamrune.sqlite;

import java.sql.Connection;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * How the SQLite stores begin, commit and end a write transaction on a borrowed connection.
 *
 * <p><b>Begin.</b> {@link #begin} switches the connection to manual commit. With the required
 * {@code transaction_mode=IMMEDIATE} the driver issues {@code BEGIN IMMEDIATE} right then, so the
 * transaction holds SQLite's write lock from its first statement on and writers queue for the lock
 * up front, waiting at most {@code busy_timeout}, instead of failing later on a read-to-write
 * upgrade. A begin that times out throws, and the connection is put back into autocommit.
 *
 * <p><b>Commit.</b> {@link #commit} switches the connection back to autocommit, which makes the
 * driver issue {@code COMMIT} and nothing after it. {@link Connection#commit()} is never used: the
 * driver follows its {@code COMMIT} with the next {@code BEGIN IMMEDIATE} at once, so a connection
 * would take the write lock again right after each commit, and a timeout on that second statement
 * would throw after the commit was durable, reporting a stored write as failed.
 *
 * <p><b>Rollback.</b> {@link #rollbackOrAbort} rolls back and returns the connection to autocommit.
 * When either step fails the transaction may still be open, and nothing after it may commit it: the
 * physical connection is aborted instead, which makes SQLite roll the transaction back as it does
 * for a connection that closes, or a process that dies, inside one.
 *
 * <p><b>Autocommit connections only.</b> A connection that arrives in manual-commit mode is refused
 * with {@link IllegalStateException}. The driver opens the {@code IMMEDIATE} transaction as soon as
 * a connection enters manual commit, so such a connection holds the database's write lock while it
 * sits idle in the pool, and every other connection's write waits for it. Configure the pool to
 * hand out connections in autocommit, its default.
 */
final class SqliteTransactions {

  private static final Logger log = LoggerFactory.getLogger(SqliteTransactions.class);

  /** The message of the refusal of a connection handed out in manual-commit mode. */
  static final String MANUAL_COMMIT_CONNECTION =
      "The connection pool handed out a SQLite connection in manual-commit mode (autoCommit=false)."
          + " With transaction_mode=IMMEDIATE the driver opens a write transaction as soon as a"
          + " connection enters manual commit, so such a connection holds the database's write"
          + " lock while it sits idle in the pool and blocks every other writer. Configure the pool"
          + " to hand out connections in autocommit mode (for HikariCP: autoCommit=true, its"
          + " default).";

  private SqliteTransactions() {}

  /** Work run inside one write transaction by {@link #inTransaction}. */
  @FunctionalInterface
  interface Work<T> {
    T run(Connection conn) throws SQLException;
  }

  /**
   * Runs {@code work} in one write transaction on {@code conn} and commits it; any exit without the
   * commit rolls the transaction back through {@link #rollbackOrAbort}.
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
        rollbackOrAbort(conn, transaction);
      }
    }
  }

  /**
   * Begins a write transaction: manual commit, which issues {@code BEGIN IMMEDIATE}. When the begin
   * fails, for example because the write lock was not granted within {@code busy_timeout}, the
   * connection is put back into autocommit before the failure propagates.
   *
   * @throws IllegalStateException if {@code conn} is in manual-commit mode
   */
  static void begin(Connection conn) throws SQLException {
    if (!conn.getAutoCommit()) {
      throw new IllegalStateException(MANUAL_COMMIT_CONNECTION);
    }
    try {
      conn.setAutoCommit(false);
    } catch (SQLException | RuntimeException e) {
      // The driver records manual commit before it runs BEGIN, so a BEGIN that failed leaves the
      // connection believing a transaction is open. Switching back issues a COMMIT that finds none
      // and fails, but the connection is in autocommit again, as the pool expects it.
      restoreAutoCommitQuietly(conn);
      throw e;
    }
  }

  /**
   * Commits the transaction {@link #begin} opened: autocommit again, which issues {@code COMMIT}.
   */
  static void commit(Connection conn) throws SQLException {
    conn.setAutoCommit(true);
  }

  /**
   * Rolls back {@code conn}'s open transaction, returns the connection to autocommit and returns
   * {@code true}. When that fails it aborts the physical connection ({@link #abortPhysicalQuietly})
   * and returns {@code false}. A {@code SQLException} or {@code RuntimeException} is logged, never
   * thrown, so it cannot replace the exception in flight; an {@link Error} propagates, after the
   * abort.
   *
   * @param transaction what the transaction was, for the log record (a constant, never an id)
   */
  static boolean rollbackOrAbort(Connection conn, String transaction) {
    boolean rolledBack = false;
    try {
      conn.rollback();
      conn.setAutoCommit(true);
      rolledBack = true;
    } catch (SQLException | RuntimeException rollbackFailure) {
      log.warn(
          "Rollback of an uncommitted {} transaction failed; aborting the connection so that"
              + " SQLite rolls it back and nothing can commit it: {}",
          transaction,
          rollbackFailure.toString());
    } finally {
      if (!rolledBack) {
        abortPhysicalQuietly(conn);
      }
    }
    return rolledBack;
  }

  /**
   * Aborts the physical connection under {@code conn}: SQLite closes it and rolls back its open
   * transaction, and the pool evicts it when the caller closes its wrapper. Never throws a {@code
   * SQLException} or {@code RuntimeException}.
   */
  static void abortPhysicalQuietly(Connection conn) {
    try {
      conn.unwrap(Connection.class).abort(Runnable::run);
    } catch (SQLException | RuntimeException abortFailure) {
      log.warn(
          "Failed to abort a SQLite connection whose rollback failed: {}", abortFailure.toString());
    }
  }

  /**
   * Returns {@code conn} to autocommit, swallowing a failure: used where a failure must not replace
   * the exception in flight.
   */
  static void restoreAutoCommitQuietly(Connection conn) {
    try {
      conn.setAutoCommit(true);
    } catch (SQLException | RuntimeException restoreFailure) {
      log.debug(
          "Switching a SQLite connection back to autocommit reported: {}",
          restoreFailure.toString());
    }
  }
}
