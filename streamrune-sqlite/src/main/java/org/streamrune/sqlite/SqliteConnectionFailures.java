package org.streamrune.sqlite;

import java.util.Set;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.core.ConnectionFailureClassifier;

/**
 * What SQLite reports, beyond the generic JDBC rules, when the database file is unusable.
 *
 * <p>SQLite has no server to lose: the counterpart of an unreachable database is a file that cannot
 * be opened, read or written. The driver reports it as an {@link SQLiteException} with a result
 * code and no SQLState, so the classifier reads the code. The failure of one write is not among
 * them: the write lock not granted within {@code busy_timeout} ({@code SQLITE_BUSY}), a lock on the
 * same connection ({@code SQLITE_LOCKED}) and a constraint say something about that write, and the
 * next one can succeed.
 */
public final class SqliteConnectionFailures {
  private SqliteConnectionFailures() {}

  /** The primary result codes of an unusable file; an extended code counts as its primary code. */
  private static final Set<Integer> UNUSABLE_FILE =
      Set.of(
          SQLiteErrorCode.SQLITE_READONLY.code,
          SQLiteErrorCode.SQLITE_IOERR.code,
          SQLiteErrorCode.SQLITE_CORRUPT.code,
          SQLiteErrorCode.SQLITE_FULL.code,
          SQLiteErrorCode.SQLITE_CANTOPEN.code,
          SQLiteErrorCode.SQLITE_NOTADB.code);

  /**
   * The generic rules, or an {@link SQLiteException} in the cause chain whose result code is {@code
   * SQLITE_CANTOPEN} (the file cannot be opened), {@code SQLITE_NOTADB} or {@code SQLITE_CORRUPT}
   * (it is not a database, or is damaged), {@code SQLITE_IOERR} (a read or a write of it failed),
   * {@code SQLITE_FULL} (the disk is full) or {@code SQLITE_READONLY} (it cannot be written), in
   * their primary or extended forms.
   */
  public static final ConnectionFailureClassifier CLASSIFIER =
      ConnectionFailureClassifier.GENERIC.or(SqliteConnectionFailures::unusableFile);

  private static boolean unusableFile(Throwable failure) {
    Throwable c = failure;
    for (int depth = 0;
        c != null && depth < ConnectionFailureClassifier.MAX_CAUSE_DEPTH;
        depth++, c = c.getCause()) {
      if (c instanceof SQLiteException sqlite
          && sqlite.getResultCode() != null
          && UNUSABLE_FILE.contains(sqlite.getResultCode().code & 0xff)) {
        return true;
      }
    }
    return false;
  }
}
