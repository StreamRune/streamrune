package org.streamrune.sqlite;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Consumer;
import org.streamrune.core.Internal;
import org.streamrune.core.ScopedValueReads;

/**
 * The SQLite write transaction a projection batch or a dead-letter replay holds on the current
 * thread while the application's updater runs, published for the one component that must write to
 * the same file meanwhile: the SQLite crypto key store, which mints a subject's key when an
 * {@code @Encrypted} read-model field is first written.
 *
 * <p><b>Why.</b> SQLite has one write lock per database file. The batch holds it from its {@code
 * BEGIN IMMEDIATE} to its {@code COMMIT}, and the updater runs in between. A write on any other
 * connection of the same file, from the same thread, waits {@code busy_timeout} for a lock its own
 * thread holds and then fails. So a component that must write while the updater runs writes on the
 * batch's connection, inside the batch's transaction, instead of on a connection of its own.
 *
 * <p><b>How it is explicit.</b> {@link SqliteProjectionRepository} binds the transaction, as a
 * {@link ScopedValue}, around the updater of {@code executeAtomically} and {@code executeReplay}
 * and nowhere else: every other SQLite store serializes before it begins its transaction, so no
 * application code runs while it holds the lock. The binding names the canonical database file, and
 * {@link #heldOn(Path)} returns it only for that file and only on the thread that runs the updater,
 * while it runs. A component on another file never sees it.
 *
 * <p><b>The joining component's duties.</b> It runs its statements on {@link #connection()} and
 * never begins, commits or rolls back: what it writes commits and rolls back with the batch. It
 * reports every {@link SQLException} of a statement it ran there through {@link
 * #abort(SQLException)}, which aborts the batch as a failed statement of the batch's own repository
 * does: on PostgreSQL the first failed statement aborts the transaction, and after {@code
 * SQLITE_FULL} SQLite has already rolled it back, so nothing may run on the connection afterwards.
 *
 * @hidden
 */
@Internal
public final class SqliteWriteTransaction {

  private static final ScopedValue<SqliteWriteTransaction> CURRENT = ScopedValue.newInstance();

  private final Path file;
  private final Connection connection;
  private final Consumer<SQLException> abort;

  private SqliteWriteTransaction(Path file, Connection connection, Consumer<SQLException> abort) {
    this.file = file;
    this.connection = connection;
    this.abort = abort;
  }

  /** Work run while the transaction is published. */
  @FunctionalInterface
  interface Work {
    void run() throws SQLException;
  }

  /**
   * Runs {@code work} with the write transaction on {@code connection} published for {@code file}.
   *
   * @param file the canonical database file of the transaction
   * @param connection the connection of the open write transaction
   * @param abort marks the holder's transaction aborted by a joined statement's failure
   * @param work what runs while the transaction is published: the application's updater
   */
  static void publishWhile(
      Path file, Connection connection, Consumer<SQLException> abort, Work work)
      throws SQLException {
    ScopedValue.where(CURRENT, new SqliteWriteTransaction(file, connection, abort))
        .call(
            () -> {
              work.run();
              return null;
            });
  }

  /**
   * The write transaction the current thread holds on {@code file} while a projection batch or
   * replay runs its updater, or {@code null} when it holds none there.
   *
   * @param file the canonical database file ({@link SqliteDatabase#file()})
   * @return the transaction, or {@code null}
   * @throws IllegalArgumentException if {@code file} is {@code null}
   */
  public static SqliteWriteTransaction heldOn(Path file) {
    if (file == null) {
      throw new IllegalArgumentException("file must not be null");
    }
    SqliteWriteTransaction current = ScopedValueReads.valueOrNull(CURRENT);
    return current != null && current.file.equals(file) ? current : null;
  }

  /**
   * The canonical database file of the transaction.
   *
   * @return the file
   */
  public Path file() {
    return file;
  }

  /**
   * The connection of the open write transaction. Statements run on it are part of the transaction;
   * never begin, commit, roll back or close it.
   *
   * @return the connection
   */
  public Connection connection() {
    return connection;
  }

  /**
   * Aborts the holder's transaction because a statement run on {@link #connection()} failed: the
   * batch or replay runs no further statement and rolls back, with {@code failure} as the cause of
   * what it throws.
   *
   * @param failure the failure of the statement
   * @throws IllegalArgumentException if {@code failure} is {@code null}
   */
  public void abort(SQLException failure) {
    if (failure == null) {
      throw new IllegalArgumentException("failure must not be null");
    }
    abort.accept(failure);
  }

  /**
   * Refuses to go on when the connection has no transaction open any more (SQLite rolled it back by
   * itself, for one): a statement run then would commit on its own, outside the batch.
   *
   * @throws IllegalStateException if no transaction is open on {@link #connection()}
   * @throws SQLException if the check itself fails
   */
  public void requireOpen() throws SQLException {
    SqliteTransactions.requireOpenTransaction(connection);
  }

  @Override
  public String toString() {
    return "SqliteWriteTransaction[" + file + "]";
  }
}
