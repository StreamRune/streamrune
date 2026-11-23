package org.streamrune.sqlite;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.sqlite.SQLiteConnection;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.LogSanitizer;

/**
 * A SQLite database file opened for StreamRune: the {@link DataSource} of the application's pool,
 * checked, and guarded against a second application instance.
 *
 * <p>Every SQLite store takes a {@code SqliteDatabase}, never a bare {@link DataSource}, so no
 * store exists on a file that is not guarded. {@link #open(DataSource)} borrows one connection and
 * fails at startup, before anything is written, when:
 *
 * <ul>
 *   <li>the data source does not produce SQLite connections;
 *   <li>the database is in-memory or temporary ({@code jdbc:sqlite::memory:}, {@code mode=memory},
 *       an empty file name): each pooled connection would get a private database;
 *   <li>the SQLite library is older than {@code 3.45.0};
 *   <li>a connection setting differs from what StreamRune requires: {@code journal_mode=WAL},
 *       {@code synchronous=FULL}, {@code foreign_keys=ON}, a {@code busy_timeout} above 0 (5000 ms
 *       is the documented value) and {@code transaction_mode=IMMEDIATE}. The application sets them
 *       in the pool's connection properties, for example as JDBC URL parameters;
 *   <li>another process holds the instance guard of the file ({@link SqliteGuardException}).
 * </ul>
 *
 * <p><b>The instance guard.</b> A SQLite database serves exactly one application instance: a second
 * instance on the same file would apply projections twice and deliver outbox entries twice. The
 * guard is an exclusive operating-system lock on {@code <database-file>.lock}, next to the database
 * file. Every handle on one file in this JVM shares the lock and counts as one reference to it; the
 * lock is released when the last handle closes, and the operating system releases it when the
 * process ends, so a crashed instance never blocks a restart. On Linux and macOS the lock is a
 * {@code flock(2)}, so other code that opens and closes the lock file does not release it. The lock
 * file is never deleted; deleting it while an instance runs would let a second instance lock a new
 * file.
 *
 * <p>{@link #close()} releases this handle's reference to the guard. It does not close the data
 * source, which belongs to the application.
 */
public final class SqliteDatabase implements AutoCloseable {

  private final DataSource dataSource;
  private final InstanceGuardRegistry.Lease guard;

  private SqliteDatabase(DataSource dataSource, InstanceGuardRegistry.Lease guard) {
    this.dataSource = dataSource;
    this.guard = guard;
  }

  /**
   * Opens the SQLite database behind {@code dataSource}: checks the connection, then takes a
   * reference to the instance guard of its file.
   *
   * <p>All checks read one borrowed connection and write nothing. The guard is taken last, after
   * the connection is returned, so a refused database leaves no reference behind.
   *
   * @param dataSource the application's pool of SQLite connections
   * @return the open handle; close it when the application stops
   * @throws IllegalArgumentException if {@code dataSource} is {@code null}
   * @throws IllegalStateException if the connections are not SQLite, the database is in-memory or
   *     temporary, the SQLite library is older than {@code 3.45.0}, or a required connection
   *     setting is wrong; the message names the setting and its expected value
   * @throws SqliteGuardException if another process holds the guard, or the lock file cannot be
   *     created, opened or locked
   * @throws EventStoreException if no connection can be borrowed or read
   */
  public static SqliteDatabase open(DataSource dataSource) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource must not be null");
    }
    Path file;
    try (Connection connection = dataSource.getConnection()) {
      SQLiteConnection sqlite = SqliteSetup.sqliteConnection(connection);
      file = SqliteSetup.databaseFile(sqlite);
      SqliteVersion.require(SqliteSetup.libraryVersion(sqlite));
      SqliteSetup.verify(sqlite);
    } catch (SQLException e) {
      throw new EventStoreException(
          "Cannot open the SQLite database: " + LogSanitizer.sanitizeFreeText(e.getMessage()), e);
    }
    return new SqliteDatabase(dataSource, InstanceGuardRegistry.acquire(file));
  }

  /**
   * The application's data source this handle was opened on.
   *
   * @return the data source
   */
  public DataSource dataSource() {
    return dataSource;
  }

  /**
   * The canonical path of the database file: absolute, with symbolic links resolved.
   *
   * @return the database file
   */
  public Path file() {
    return guard.databaseFile();
  }

  /**
   * Releases this handle's reference to the instance guard; the last handle on a file releases its
   * lock. Calling it again does nothing. The data source stays open.
   *
   * @throws SqliteGuardException if the lock file's descriptor cannot be closed
   */
  @Override
  public void close() {
    guard.release();
  }

  @Override
  public String toString() {
    return "SqliteDatabase[" + file() + "]";
  }
}
