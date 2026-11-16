package org.streamrune.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The exit rule the Postgres stores share for a write transaction that did not commit.
 *
 * <p>An uncommitted write transaction is rolled back before anything restores autoCommit, because
 * pgjdbc's {@code setAutoCommit(true)} COMMITS an open transaction. When the rollback itself fails,
 * with a {@code SQLException}, a {@code RuntimeException} or an {@link Error}, the transaction may
 * still be open, and nothing after it may commit it: not an autoCommit restore by the store, and
 * not the pool's reset when the store closes the connection. HikariCP rolls a dirty connection back
 * on return, but Agroal, the Quarkus pool, resets a changed autoCommit with {@code
 * setAutoCommit(true)} and no rollback first, and pgjdbc commits the open transaction there
 * (confirmed against a live Agroal 3.0 pool). So a failed rollback aborts the physical connection
 * instead, and the server discards the transaction.
 *
 * <p>Callers restore autoCommit only after a successful commit or a successful rollback, that is
 * when {@link #rollbackOrAbort} returned {@code true}.
 *
 * <p>{@link #commitIfManual} is the entry rule for every store write that is not an explicit
 * multi-statement transaction: a pool may hand out connections with {@code autoCommit=false} (for
 * example {@code spring.datasource.hikari.auto-commit=false}, the usual Hibernate tuning), and on
 * such a connection the driver opens a transaction implicitly on the first statement. A write that
 * assumed {@code autoCommit=true} then never committed; HikariCP rolled it back when the connection
 * returned to the pool, and the store's caller saw a success.
 */
final class PostgresTransactions {

  private static final Logger log = LoggerFactory.getLogger(PostgresTransactions.class);

  private PostgresTransactions() {}

  /** Work run on a borrowed connection by {@link #commitIfManual}. */
  @FunctionalInterface
  interface ConnectionWork<T> {
    T run(Connection conn) throws SQLException;
  }

  /**
   * Runs {@code work} on {@code conn} and makes its writes durable whatever the connection's
   * autoCommit mode. With {@code autoCommit=true} every statement commits itself and nothing is
   * added. With {@code autoCommit=false} the statements run in the transaction the driver opens
   * implicitly, and this method commits it once {@code work} returns; every exit without a commit
   * (a {@code SQLException}, a {@code RuntimeException} or an {@link Error}) rolls the transaction
   * back through {@link #rollbackOrAbort}, so neither the pool's reset nor its rollback-on-return
   * decides what the caller observed. The autoCommit setting itself is never changed: the pool
   * configured it, and the pool keeps it.
   *
   * <p>A failed commit propagates as a {@code SQLException} after the rollback attempt; nothing of
   * {@code work} is durable then, exactly as under {@code autoCommit=true} when the statement
   * itself failed, and the store's idempotent statements converge on a rerun.
   *
   * @param conn the borrowed connection, in whatever autoCommit mode the pool hands out
   * @param transaction what the work is, for the log record of a failed rollback (a constant, never
   *     an id)
   * @param work the statements to run; their result is returned
   */
  static <T> T commitIfManual(Connection conn, String transaction, ConnectionWork<T> work)
      throws SQLException {
    if (conn.getAutoCommit()) {
      return work.run(conn);
    }
    boolean committed = false;
    try {
      T result = work.run(conn);
      conn.commit();
      committed = true;
      return result;
    } finally {
      if (!committed) {
        rollbackOrAbort(conn, transaction);
      }
    }
  }

  /**
   * Rolls back {@code conn}'s open transaction and returns {@code true}. When the rollback fails it
   * aborts the physical connection ({@link #abortPhysicalQuietly}) and returns {@code false}; the
   * caller must then leave autoCommit alone. A {@code SQLException} or {@code RuntimeException}
   * from the rollback is logged, never thrown, so it cannot replace the exception in flight; an
   * {@link Error} from the rollback propagates, after the abort.
   *
   * @param conn the connection whose transaction did not commit
   * @param transaction what the transaction was, for the log record (a constant, never an id)
   */
  static boolean rollbackOrAbort(Connection conn, String transaction) {
    boolean rolledBack = false;
    try {
      conn.rollback();
      rolledBack = true;
    } catch (SQLException | RuntimeException rollbackFailure) {
      log.warn(
          "Rollback of an uncommitted {} transaction failed; aborting the connection so that"
              + " neither an autoCommit restore nor the pool's reset can commit it: {}",
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
   * Aborts the physical connection under {@code conn}, so the server discards its open transaction
   * and the pool's reset on the caller's {@code close()} fails on a closed connection: HikariCP
   * then evicts it, and Agroal destroys it under the PostgreSQL exception sorter Quarkus configures
   * (SQLState 08). The physical connection, not the pool's wrapper: Agroal's wrapper {@code
   * abort()} only marks the wrapper closed and never returns the connection to the pool, which
   * leaks it with the transaction open.
   *
   * <p>This relies on {@code unwrap(Connection.class)} returning the physical connection: the
   * HikariCP and Agroal wrappers do, and raw pgjdbc returns itself. The JDBC {@code Wrapper}
   * contract also lets a wrapper that implements {@code Connection} return itself; a proxy layered
   * above the pool that does so and forwards {@code abort()} to the pool's wrapper would leak the
   * connection idle in transaction until a server timeout. Nothing commits on that path, so it
   * costs liveness (the transaction's row locks), not correctness. Never throws a {@code
   * SQLException} or {@code RuntimeException}.
   */
  static void abortPhysicalQuietly(Connection conn) {
    try {
      conn.unwrap(Connection.class).abort(Runnable::run);
    } catch (SQLException | RuntimeException abortFailure) {
      log.warn(
          "Failed to abort a connection whose rollback failed; its pool may commit the open"
              + " transaction: {}",
          abortFailure.toString());
    }
  }
}
