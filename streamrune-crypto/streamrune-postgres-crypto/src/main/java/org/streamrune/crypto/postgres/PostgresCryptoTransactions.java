package org.streamrune.crypto.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The transaction exit rules the crypto stores share; the same discipline the event store's {@code
 * PostgresTransactions} applies, kept here because this module does not depend on it.
 *
 * <p>An uncommitted write transaction is rolled back before anything restores autoCommit, because
 * pgjdbc's {@code setAutoCommit(true)} COMMITS an open transaction. When the rollback itself fails,
 * the transaction may still be open, and nothing after it may commit it: not an autoCommit restore
 * by the store, and not the pool's reset when the store closes the connection. HikariCP rolls a
 * dirty connection back on return, but Agroal, the Quarkus pool, resets a changed autoCommit with
 * {@code setAutoCommit(true)} and no rollback first, and pgjdbc commits the open transaction there.
 * So a failed rollback aborts the physical connection instead, and the server discards the
 * transaction.
 *
 * <p>{@link #commitIfManual} is the entry rule for every write that is not an explicit
 * multi-statement transaction: a pool may hand out connections with {@code autoCommit=false} (for
 * example {@code spring.datasource.hikari.auto-commit=false}), and on such a connection the driver
 * opens a transaction implicitly on the first statement. A tombstone INSERT or a {@code pg_notify}
 * that assumed {@code autoCommit=true} then never committed; HikariCP rolled it back when the
 * connection returned to the pool, and the caller saw a success.
 */
final class PostgresCryptoTransactions {

  private static final Logger log = LoggerFactory.getLogger(PostgresCryptoTransactions.class);

  private PostgresCryptoTransactions() {}

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
   * back through {@link #rollbackOrAbort}. The autoCommit setting itself is never changed: the pool
   * configured it, and the pool keeps it.
   *
   * @param conn the borrowed connection, in whatever autoCommit mode the pool hands out
   * @param work the statements to run; their result is returned
   */
  static <T> T commitIfManual(Connection conn, ConnectionWork<T> work) throws SQLException {
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
        rollbackOrAbort(conn);
      }
    }
  }

  /**
   * Rolls back and returns {@code true}. When the rollback fails it aborts the physical connection
   * ({@link #abortPhysicalQuietly}) and returns {@code false}; the caller must then leave
   * autoCommit alone. A {@code SQLException} or {@code RuntimeException} from the rollback is
   * logged, never thrown, so it cannot replace the exception in flight; an {@link Error} from the
   * rollback propagates, after the abort.
   */
  static boolean rollbackOrAbort(Connection conn) {
    boolean rolledBack = false;
    try {
      conn.rollback();
      rolledBack = true;
    } catch (SQLException | RuntimeException rollbackFailure) {
      log.warn(
          "Rollback of an uncommitted crypto transaction failed; aborting the connection so"
              + " that returning it to the pool cannot commit the transaction: {}",
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
   * leaks it with the transaction open. HikariCP's and Agroal's {@code unwrap} return the physical
   * connection; raw pgjdbc returns itself. The JDBC {@code Wrapper} contract also lets a wrapper
   * that implements {@code Connection} return itself: a proxy layered above the pool that does so
   * and forwards {@code abort()} to the pool's wrapper would leak the connection idle in
   * transaction, holding its row locks until a server timeout. Nothing commits on that path. Never
   * throws a {@code SQLException} or {@code RuntimeException}.
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

  /** Restores autoCommit after a rollback, without replacing the exception in flight. */
  static void restoreAutoCommitQuietly(Connection conn, boolean autoCommit) {
    try {
      conn.setAutoCommit(autoCommit);
    } catch (SQLException | RuntimeException restoreFailure) {
      log.debug(
          "Failed to restore autoCommit after a rollback (the pool resets it on return): {}",
          restoreFailure.toString());
    }
  }
}
