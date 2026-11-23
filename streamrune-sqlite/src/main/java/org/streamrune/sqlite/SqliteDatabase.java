package org.streamrune.sqlite;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 *   <li>the connection has other databases attached ({@code ATTACH DATABASE}): their files would be
 *       outside the instance guard;
 *   <li>the SQLite library is older than {@code 3.45.0};
 *   <li>a connection setting differs from what StreamRune requires: {@code journal_mode=WAL},
 *       {@code synchronous=FULL} (or the stricter {@code EXTRA}), {@code foreign_keys=ON}, a {@code
 *       busy_timeout} above 0 (5000 ms is the documented value) and {@code
 *       transaction_mode=IMMEDIATE}. The application sets them in the pool's connection properties,
 *       for example as JDBC URL parameters;
 *   <li>the database file has more than one hard link, another process holds the instance guard of
 *       the file, or the guard cannot be taken ({@link SqliteGuardException}).
 * </ul>
 *
 * <p><b>The instance guard.</b> A SQLite database serves exactly one application instance: a second
 * instance on the same file would apply projections twice and deliver outbox entries twice. The
 * guard is an exclusive operating-system lock on {@code <database-file>.lock}, next to the database
 * file. Every handle on one file in this JVM shares the lock and counts as one reference to it; the
 * lock is released when the last handle closes, and the operating system releases it when the
 * process ends, so a crashed instance never blocks a restart. On Linux and macOS the lock is a
 * {@code flock(2)}, so other code that opens and closes the lock file does not release it.
 *
 * <p>The guard identifies a database by the canonical path it is opened under, so it cannot see
 * through three setups, which therefore must not be used:
 *
 * <ul>
 *   <li>deleting the lock file while an instance runs: a second instance would lock a new file of
 *       that name. This JVM refuses to open the database again when it notices;
 *   <li>hard links: a second name of the database file would get a lock file of its own. A file
 *       with more than one link is refused on POSIX systems;
 *   <li>mounting the database <em>file</em> on its own into two containers (a single-file bind
 *       mount): each container would create its own lock file next to its mount point. Mount the
 *       directory that holds the database instead. This cannot be detected.
 * </ul>
 *
 * <p><b>Schema.</b> {@link #migrate(SqliteSchemaSeries)} applies a schema series with Flyway and
 * {@link #validate(SqliteSchemaSeries)} checks it; both run on this handle's data source, under the
 * instance guard.
 *
 * <p>{@link #close()} releases this handle's reference to the guard. It does not close the data
 * source, which belongs to the application.
 */
public final class SqliteDatabase implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(SqliteDatabase.class);

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
   *     temporary or has other databases attached, the SQLite library is older than {@code 3.45.0},
   *     or a required connection setting is wrong; the message names the setting and its expected
   *     value
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
      SqliteSetup.refuseAttachedDatabases(sqlite);
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
   * Applies {@code series} to this database: every script of the series that its history table does
   * not record yet. Running it again applies nothing. The series is recorded in its own history
   * table, so several series and the application's own tables share one file.
   *
   * <p>Each script runs in one transaction with the history row that records it, so a script that
   * fails or is interrupted leaves nothing behind and the next run applies it again from its start.
   *
   * @param series the series to apply
   * @throws IllegalArgumentException if {@code series} is {@code null}
   * @throws IllegalStateException if this handle is closed, or a script of the series is not on the
   *     class path (nothing is written then)
   * @throws EventStoreException if the migration fails; the message names the history table
   */
  public void migrate(SqliteSchemaSeries series) {
    requireOpen(series);
    SqliteSchemaMigration.migrate(series, dataSource);
  }

  /**
   * Checks that this database holds {@code series}: its tables, the columns the runtime reads and
   * writes, the global-offset counter row of the event store, and the version its history table
   * records. Warnings (a missing history table, a schema ahead of this build) are logged.
   *
   * @param series the series to check
   * @throws IllegalArgumentException if {@code series} is {@code null}
   * @throws IllegalStateException if this handle is closed, or the schema is missing a table, a
   *     column or the counter row, or is behind this build; the message lists every such issue
   */
  public void validate(SqliteSchemaSeries series) {
    requireOpen(series);
    List<SqliteSchemaValidator.Issue> issues = SqliteSchemaValidator.validate(dataSource, series);
    boolean failed = false;
    for (SqliteSchemaValidator.Issue issue : issues) {
      if (issue.severity() == SqliteSchemaValidator.Severity.WARNING) {
        log.warn("Schema validation warning [{}]: {}", issue.table(), issue.message());
      } else {
        failed = true;
      }
    }
    if (failed) {
      throw new IllegalStateException(SqliteSchemaValidator.failureMessage(issues));
    }
  }

  private void requireOpen(SqliteSchemaSeries series) {
    if (series == null) {
      throw new IllegalArgumentException("series must not be null");
    }
    if (guard.isReleased()) {
      throw new IllegalStateException(
          this + " is closed; open a new handle to migrate or validate its schema");
    }
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
