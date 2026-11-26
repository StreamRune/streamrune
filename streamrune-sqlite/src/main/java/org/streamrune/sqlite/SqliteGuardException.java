package org.streamrune.sqlite;

/**
 * Thrown by {@link SqliteDatabase#open} when the instance guard of a SQLite database file cannot be
 * taken: another process holds it, or its lock file cannot be created or opened.
 *
 * <p>A SQLite database serves exactly one application instance. The message names the database file
 * and its lock file ({@code <database-file>.lock}). Nothing was written to the database.
 */
public final class SqliteGuardException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  SqliteGuardException(String message) {
    super(message);
  }

  SqliteGuardException(String message, Throwable cause) {
    super(message, cause);
  }
}
