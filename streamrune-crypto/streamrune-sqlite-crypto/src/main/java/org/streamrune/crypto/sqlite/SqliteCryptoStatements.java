package org.streamrune.crypto.sqlite;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.sqlite.SqliteTransactions;
import org.streamrune.sqlite.SqliteWriteTransaction;

/**
 * Where the SQLite crypto stores run their statements: inside the write transaction a projection
 * batch holds on this thread for the same file, or on a connection of their own.
 *
 * <p>A projection batch or replay holds the file's write lock while the application's updater runs
 * and serializes read models with their {@code @Encrypted} fields. A write on another connection of
 * the file would wait {@code busy_timeout} for the lock its own thread holds, and on a pool of one
 * connection even a read would wait for the batch's connection. So when {@link
 * SqliteWriteTransaction#heldOn(Path)} finds the batch's transaction on this thread for the store's
 * file, reads ({@link #read}) and a key's mint ({@link #write}) run on its connection, inside it,
 * with no begin or commit of their own: a minted key commits and rolls back with the batch,
 * together with every value encrypted under it. Every {@link SQLException} there is reported to the
 * batch, which aborts, as a failed statement of its own does; a write first checks that the
 * transaction is still open, so it never commits on its own. Otherwise reads run on a borrowed
 * connection in autocommit (in WAL mode they never wait for a writer) and writes in one {@code
 * BEGIN IMMEDIATE} ... {@code COMMIT} on a borrowed connection ({@link SqliteTransactions}).
 *
 * <p><b>An erasure never joins.</b> {@code deleteKey}, {@code reinstate} and the tombstone writes
 * of the forgotten-subject store run through {@link #writeAlone}, which refuses at once inside a
 * batch on the file: joined, an erasure would not be durable when it returns. A rolled-back batch
 * would bring the key back; {@code CachedCryptoEngine} would publish the forget before the {@code
 * COMMIT}, so another thread could cache the plaintext again; and the Vault engine would destroy
 * its transit key while the tombstone it relies on is still uncommitted. Outside a batch it commits
 * in a transaction of its own before it returns, as on PostgreSQL.
 */
final class SqliteCryptoStatements {

  /** Statements run on one connection. */
  @FunctionalInterface
  interface Work<T> {
    T run(Connection conn) throws SQLException;
  }

  private final DataSource dataSource;
  private final Path file;
  private final String transaction;

  /**
   * @param transaction what the writes are, for the log record of a failed rollback (a constant)
   */
  SqliteCryptoStatements(DataSource dataSource, Path file, String transaction) {
    this.dataSource = dataSource;
    this.file = file;
    this.transaction = transaction;
  }

  /** Runs reads: in the batch's transaction on this thread, or on a borrowed connection. */
  <T> T read(Work<T> reads) throws SQLException {
    SqliteWriteTransaction joined = SqliteWriteTransaction.heldOn(file);
    if (joined != null) {
      return joined(joined, reads, false);
    }
    try (Connection conn = dataSource.getConnection()) {
      return reads.run(conn);
    }
  }

  /** The refusal of an erasure inside a projection batch on the same file. */
  static final String ERASURE_INSIDE_A_BATCH =
      "An erasure (deleteKey, reinstate, or a forgotten-subject tombstone write) cannot run inside a"
          + " projection batch on the same SQLite database file: it would commit or roll back with"
          + " the batch, so it would not be durable when it returns, and a cache or a key backend"
          + " acting on its return could act on an erasure that never commits. Nothing was written."
          + " Run the erasure after the batch, from a command handler or a saga step.";

  /**
   * Runs an erasure's writes in a write transaction of their own, committed before this returns.
   *
   * @throws IllegalStateException inside a projection batch on the same file, before anything runs
   */
  <T> T writeAlone(Work<T> writes) throws SQLException {
    refuseInsideABatch();
    try (Connection conn = dataSource.getConnection()) {
      return SqliteTransactions.inTransaction(conn, transaction, writes::run);
    }
  }

  /**
   * Refuses an erasure inside a projection batch on the same file, before anything runs.
   *
   * @throws IllegalStateException with {@link #ERASURE_INSIDE_A_BATCH}
   */
  void refuseInsideABatch() {
    if (SqliteWriteTransaction.heldOn(file) != null) {
      throw new IllegalStateException(ERASURE_INSIDE_A_BATCH);
    }
  }

  /**
   * Runs a key's mint as one transaction: the batch's on this thread, or one of its own. Only the
   * mint joins a batch (see the class Javadoc).
   */
  <T> T write(Work<T> writes) throws SQLException {
    SqliteWriteTransaction joined = SqliteWriteTransaction.heldOn(file);
    if (joined != null) {
      return joined(joined, writes, true);
    }
    try (Connection conn = dataSource.getConnection()) {
      return SqliteTransactions.inTransaction(conn, transaction, writes::run);
    }
  }

  private static <T> T joined(SqliteWriteTransaction joined, Work<T> work, boolean writes)
      throws SQLException {
    try {
      if (writes) {
        joined.requireOpen();
      }
      return work.run(joined.connection());
    } catch (SQLException e) {
      joined.abort(e);
      throw e;
    }
  }

  /**
   * Whether {@code e} is SQLite refusing the write lock within {@code busy_timeout}: the cause
   * chain holds {@code SQLITE_BUSY} or one of its extended codes.
   */
  static boolean isBusy(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof SQLiteException sqlite) {
        // An extended code keeps its primary code in its low byte.
        return (sqlite.getResultCode().code & 0xff) == SQLiteErrorCode.SQLITE_BUSY.code;
      }
    }
    return false;
  }

  /**
   * The part of a failure message that explains a write lock not granted: why it can happen to a
   * key-store write, and what to do. Empty for any other failure.
   */
  static String lockAdvice(Throwable e) {
    if (!isBusy(e)) {
      return "";
    }
    return " — SQLite's write lock was not granted within busy_timeout. Another connection held"
        + " it that long, or this thread holds it itself: inside an application transaction on"
        + " the same database file (a connection in manual-commit mode), the crypto store cannot"
        + " write. Write @Encrypted values of a subject without a key outside such a"
        + " transaction, or raise busy_timeout";
  }
}
