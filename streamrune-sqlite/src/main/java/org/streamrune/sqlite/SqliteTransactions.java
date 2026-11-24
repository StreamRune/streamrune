package org.streamrune.sqlite;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.Internal;

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
 * connection that closes, or a process that dies, inside one. A connection pool does not learn of
 * the close when the caller returns its wrapper. HikariCP, for one, validates a connection on
 * borrow only when it was last used longer ago than its alive-bypass window (500 ms), and evicts it
 * then; a connection borrowed more often than that is handed out again until HikariCP's keepalive
 * check validates it while it idles in the pool, or {@code maxLifetime} retires it (retirement
 * closes it without validating). Until then every call on that connection fails cleanly with an
 * exception, and the connection holds no lock.
 *
 * <p><b>Autocommit connections only.</b> A connection that arrives in manual-commit mode is refused
 * with {@link IllegalStateException}: such a connection holds the write lock while it is not in
 * use, and a transaction on it is not this helper's to commit. {@link SqliteDatabase#open} refuses
 * a data source that hands one out; the check here also catches a connection that changed later.
 *
 * <p>Public for {@code streamrune-sqlite-crypto}, whose key store runs its writes the same way: one
 * implementation of the begin, commit and rollback rules for every SQLite store.
 *
 * @hidden
 */
@Internal
public final class SqliteTransactions {

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

  /** The message of the refusal of a step that needs the caller's open transaction. */
  static final String NO_OPEN_TRANSACTION =
      "This SQLite step must run inside the caller's open write transaction (BEGIN IMMEDIATE ..."
          + " COMMIT, as SqliteTransactions.inTransaction runs it, together with the writes it"
          + " guards), but the connection has no transaction open. Outside one, each statement"
          + " commits on its own and a check reads a row another writer can change before the"
          + " guarded write lands; nothing was written.";

  /** What SQLite says when BEGIN finds a transaction open. */
  private static final String TRANSACTION_WITHIN_TRANSACTION =
      "cannot start a transaction within a transaction";

  /** What SQLite says when ROLLBACK finds no transaction: it rolled back by itself. */
  private static final String NO_TRANSACTION_ACTIVE = "no transaction is active";

  private SqliteTransactions() {}

  /**
   * Work run inside one write transaction by {@link #inTransaction}.
   *
   * @param <T> the work's result
   */
  @FunctionalInterface
  public interface Work<T> {
    /**
     * Runs the work on the transaction's connection.
     *
     * @param conn the connection of the open transaction
     * @return the work's result
     * @throws SQLException if a statement fails
     */
    T run(Connection conn) throws SQLException;
  }

  /**
   * The one way a SQLite store runs a write: borrows a connection from {@code dataSource}, runs
   * {@code work} in one write transaction on it ({@link #inTransaction}) and returns it. The work
   * reports its outcome (a row count, a stale view) as its result, never as an exception.
   *
   * <p>Failures, the same for every store that calls it:
   *
   * <ul>
   *   <li>a connection handed out in manual-commit mode: {@link IllegalStateException} ({@link
   *       #MANUAL_COMMIT_CONNECTION}), unwrapped and before anything is written. It is a
   *       configuration error, not a storage failure (the pool or an application transaction must
   *       change, a retry cannot succeed), and every SQLite store refuses it the same way, the
   *       event store, the snapshot purger and the offset store included;
   *   <li>SQLite's write lock not granted within {@code busy_timeout}, or a same-connection lock
   *       ({@code SQLITE_LOCKED}): an {@link EventStoreException} whose message names {@code what}
   *       and the cause ({@link SqliteEventStore#lockFailure});
   *   <li>any other {@link SQLException} or {@link RuntimeException}, from borrowing the
   *       connection, the work or closing it: an {@link EventStoreException} with {@code
   *       failureMessage}.
   * </ul>
   *
   * Every failure before the commit rolls the transaction back (see {@link #inTransaction}).
   *
   * @param dataSource the pool to borrow from
   * @param transaction what the work is, for the log record of a failed rollback (a constant, never
   *     an id)
   * @param what what the write is, for the message of a lock failure (may name an id, sanitized)
   * @param failureMessage the message of any other failure (may name an id, sanitized)
   * @param work the work to run in the transaction
   * @param <T> the work's result
   * @return the work's result
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode
   * @throws EventStoreException if the write fails
   */
  static <T> T write(
      DataSource dataSource, String transaction, String what, String failureMessage, Work<T> work) {
    try (Connection conn = dataSource.getConnection()) {
      if (conn.getAutoCommit()) {
        return inTransaction(conn, transaction, work);
      }
    } catch (SQLException e) {
      EventStoreException lockFailure = SqliteEventStore.lockFailure(e, what);
      throw lockFailure != null ? lockFailure : new EventStoreException(failureMessage, e);
    } catch (RuntimeException e) {
      throw new EventStoreException(failureMessage, e);
    }
    throw new IllegalStateException(MANUAL_COMMIT_CONNECTION);
  }

  /**
   * Runs {@code work} in one write transaction on {@code conn} and commits it; any exit without the
   * commit rolls the transaction back through {@link #rollback}.
   *
   * @param conn the connection, in autocommit mode
   * @param transaction what the work is, for the log record of a failed rollback (a constant, never
   *     an id)
   * @param work the work to run in the transaction
   * @param <T> the work's result
   * @return the work's result
   * @throws IllegalStateException if {@code conn} is in manual-commit mode
   * @throws SQLException if the begin, a statement of the work or the commit fails (rolled back)
   */
  public static <T> T inTransaction(Connection conn, String transaction, Work<T> work)
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
   * Refuses a connection that has no transaction open, before a step whose checks are only sound
   * inside the caller's write transaction. sqlite-jdbc exposes no transaction state, so this probes
   * the way the driver itself does after each autocommit statement: a deferred {@code BEGIN}, which
   * takes no lock, fails with SQLITE_ERROR ("cannot start a transaction within a transaction") when
   * a transaction is open, and nothing changes. When it succeeds, no transaction was open: the
   * probe's own empty transaction is rolled back and the step refused.
   *
   * <p>The probe cannot see two steps the caller split across two transactions; keeping a check and
   * the write it guards in one transaction stays the caller's contract.
   *
   * @throws IllegalStateException if {@code conn} has no transaction open ({@link
   *     #NO_OPEN_TRANSACTION}); nothing is left open
   * @throws SQLException if the probe fails for another reason
   */
  static void requireOpenTransaction(Connection conn) throws SQLException {
    try (Statement statement = conn.createStatement()) {
      try {
        statement.execute("BEGIN");
      } catch (SQLiteException e) {
        if (isTransactionWithinTransaction(e)) {
          return;
        }
        throw e;
      }
      statement.execute("ROLLBACK");
    }
    throw new IllegalStateException(NO_OPEN_TRANSACTION);
  }

  /** Whether {@code e} is SQLite's SQLITE_ERROR refusing a BEGIN inside an open transaction. */
  private static boolean isTransactionWithinTransaction(SQLiteException e) {
    String message = e.getMessage();
    return e.getResultCode() == SQLiteErrorCode.SQLITE_ERROR
        && message != null
        && message.contains(TRANSACTION_WITHIN_TRANSACTION);
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
      if (isNoTransactionActive(rollbackFailure)) {
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

  /** Whether {@code e} is SQLite's SQLITE_ERROR "cannot rollback - no transaction is active". */
  private static boolean isNoTransactionActive(SQLException e) {
    String message = e.getMessage();
    return e instanceof SQLiteException sqlite
        && sqlite.getResultCode() == SQLiteErrorCode.SQLITE_ERROR
        && message != null
        && message.contains(NO_TRANSACTION_ACTIVE);
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
   * transaction and releases the write lock. The pool does not learn of it when the caller returns
   * its wrapper: HikariCP evicts it when a borrow finds it unused for longer than the alive-bypass
   * window (500 ms) and validates it, when its keepalive check validates it while it idles, or when
   * {@code maxLifetime} retires it; until then every call on it fails cleanly and it holds no lock
   * (see the class Javadoc). Never throws a {@code SQLException} or {@code RuntimeException}.
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
