package org.streamrune.sqlite;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.Internal;
import org.streamrune.runtime.jdbc.Causes;

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
 * read-to-write upgrade. A begin that fails is rolled back, because the driver can throw after the
 * begin took effect; when it took none, the rollback finds nothing open and counts as done. A begin
 * refused because the connection already has a transaction open (one the application began) neither
 * commits nor rolls back that transaction: it is not the helper's.
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
 * <p><b>The in-process write mutex.</b> SQLite's write lock is waited for inside the driver's
 * native call, where its busy handler sleeps: a virtual thread waiting there never unmounts, it
 * keeps its carrier for up to {@code busy_timeout}. With as many waiting writers as carriers (one
 * or two on a small machine), a lock holder that parked inside its transaction, a projection batch
 * whose updater makes a blocking call, could not be resumed until every waiter had timed out. So
 * each database file has one mutex in this process ({@link #awaitWriteLock}), taken before the
 * {@code BEGIN IMMEDIATE} of every write transaction of the stores ({@link #open}) and before a
 * write that is one statement in autocommit ({@link #writeStatement}), and released after its
 * {@code COMMIT} or {@code ROLLBACK}. Writers of this process therefore queue for the mutex in
 * Java, first come first served, where a virtual thread parks and frees its carrier, and the one
 * that holds it finds SQLite's lock free. The wait is bounded by the waiting connection's own
 * {@code busy_timeout}, is not interruptible (as the native wait is not; the thread's interrupt
 * status is kept), and ends in the failure SQLite itself reports for a lock not granted, {@code
 * SQLITE_BUSY}, so every store reports it as before. The mutex is re-entrant: the thread that holds
 * it (a batch's thread, inside its updater) passes straight through to SQLite, which decides. A
 * write that joins a batch's transaction through {@link SqliteWriteTransaction} runs on the batch's
 * connection and takes nothing. The mutex is found by the connection's database file, so every pool
 * and every URL spelling of one file shares it; a connection whose file cannot be told (an
 * in-memory database, a connection that is not the driver's) takes none. It is memory only: it
 * changes no statement and no crash point.
 *
 * <p><b>What the mutex does not cover.</b> A writer outside the stores holds SQLite's write lock
 * without the mutex: an application transaction on the same file, and the schema migration
 * (Flyway), which runs at startup. A store write then gets the mutex at once and waits for SQLite's
 * lock inside the driver, at most {@code busy_timeout}, as before; the store writers behind it wait
 * for the mutex in Java. A writer can so wait {@code busy_timeout} for the mutex and, in that case
 * only, {@code busy_timeout} again for SQLite's lock.
 *
 * <p><b>Lock order.</b> A store write takes, in this order: a connection of the pool; this mutex
 * (bounded by {@code busy_timeout}); SQLite's write lock ({@code BEGIN IMMEDIATE}, bounded by
 * {@code busy_timeout}); then, inside the transaction, what its statements need (the checkpoint and
 * saga fences read rows under the write lock and take no lock of their own). The mutex is taken
 * immediately before SQLite's write lock and released immediately after it, on the same thread, so
 * it sits at exactly the place of that lock in every order: a wait cycle through the mutex is a
 * cycle that existed through SQLite's lock, with the same bound.
 *
 * <ul>
 *   <li>The pool: a batch holds its connection and the mutex while its updater borrows a second
 *       connection (bounded by the pool's own timeout); a waiter holds a connection while it waits
 *       for the mutex (bounded by {@code busy_timeout}). Both waits end, as they did when the
 *       waiter waited inside the driver.
 *   <li>The crypto cache's subject stripe ({@code CachedCryptoEngine}): a batch decrypting on a
 *       cache miss waits at most 250 ms for the stripe an erasure holds, then decrypts without it;
 *       the erasure waits for the mutex at most {@code busy_timeout}. Neither waits without bound.
 *   <li>The instance-guard registry lock: taken to open and close a {@link SqliteDatabase} only,
 *       and nothing under it runs a statement, so it never waits for the mutex.
 *   <li>Two files: a batch on one file that writes to a store of another takes the second file's
 *       mutex inside the first. Two such batches in opposite directions wait for each other, and
 *       one fails after {@code busy_timeout}, as with SQLite's locks alone.
 *   <li>The same thread, the same file: a write on a second connection from inside a batch's
 *       updater passes the mutex (re-entrant) and waits for the lock its own thread holds; it fails
 *       after {@code busy_timeout}, as before, and the failure says whose lock it was ({@link
 *       WriteLockHeldByOwnBatch}).
 * </ul>
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
   *   <li>the file's write mutex or SQLite's write lock not granted within {@code busy_timeout}, or
   *       a same-connection lock ({@code SQLITE_LOCKED}): an {@link EventStoreException} whose
   *       message names {@code what} and the cause ({@link SqliteEventStore#lockFailure});
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
   * commit rolls the transaction back through {@link #rollback}. The transaction holds the file's
   * write mutex from before its begin until after its commit or rollback ({@link #open}).
   *
   * @param conn the connection, in autocommit mode
   * @param transaction what the work is, for the log record of a failed rollback (a constant, never
   *     an id)
   * @param work the work to run in the transaction
   * @param <T> the work's result
   * @return the work's result
   * @throws IllegalStateException if {@code conn} is in manual-commit mode
   * @throws SQLException if the write mutex or SQLite's write lock is not granted within {@code
   *     busy_timeout}, or the begin, a statement of the work or the commit fails (rolled back)
   */
  public static <T> T inTransaction(Connection conn, String transaction, Work<T> work)
      throws SQLException {
    try (OpenTransaction open = open(conn, transaction)) {
      T result = work.run(conn);
      open.commit();
      return result;
    }
  }

  /**
   * Opens a write transaction on {@code conn}: waits for the file's write mutex ({@link
   * #awaitWriteLock}), then begins ({@link #begin}). The caller runs its statements, calls {@link
   * OpenTransaction#commit()} and closes the returned transaction, in a try-with-resources: closing
   * it without a commit rolls back ({@link #rollback}), and closing it releases the mutex in every
   * case. A failed wait opened nothing and holds nothing; a failed begin is rolled back unless it
   * proves the open transaction is not this call's ({@link #undoBegin}), the mutex is released, and
   * the failure propagates unchanged.
   *
   * @param conn the connection, in autocommit mode
   * @param transaction what the transaction is, for the log record of a failed rollback (a
   *     constant, never an id)
   * @return the open transaction
   * @throws IllegalStateException if {@code conn} is in manual-commit mode
   * @throws SQLException if the mutex or SQLite's write lock is not granted within {@code
   *     busy_timeout} ({@code SQLITE_BUSY}), or the begin fails otherwise
   */
  static OpenTransaction open(Connection conn, String transaction) throws SQLException {
    WriteLock writeLock = awaitWriteLock(conn);
    OpenTransaction open;
    try {
      // Built before the begin: nothing is allocated between a begin that succeeded and the
      // return, so nothing thrown there can leave a transaction open with no one to end it, and a
      // failure here releases the mutex like a failed begin.
      open = new OpenTransaction(conn, transaction, writeLock);
      begin(conn);
    } catch (SQLException beginFailure) {
      undoBegin(conn, transaction, beginFailure, writeLock);
      throw heldByOwnBatch(beginFailure, writeLock.file());
    } catch (Throwable beginFailure) {
      undoBegin(conn, transaction, beginFailure, writeLock);
      throw beginFailure;
    }
    return open;
  }

  /**
   * Ends what a failed {@link #open} may have begun, then releases the write mutex. The driver can
   * throw after {@code BEGIN IMMEDIATE} took effect (from its own code after the native step, or
   * from closing the statement), so the transaction is rolled back first, as {@link
   * OpenTransaction#close()} rolls back: a pool returns an autocommit connection as it is, and one
   * left with the transaction open would hold SQLite's write lock, refuse its next begin, and keep
   * the writes of its next one-statement write from committing. A rollback that finds nothing open
   * counts as done ({@link #rollback}). Two failures prove that nothing of this call is open and
   * skip the rollback, which would end a transaction that is not this call's: the refusal of a
   * connection in manual-commit mode, and SQLite's refusal of a begin inside an open transaction.
   * An Error of the rollback rides on {@code failure} as suppressed; the mutex is released in every
   * case.
   */
  private static void undoBegin(
      Connection conn, String transaction, Throwable failure, WriteLock writeLock) {
    try {
      if (!beganNothing(failure)) {
        rollback(conn, transaction);
      }
    } catch (Throwable rollbackFailure) {
      if (rollbackFailure != failure) {
        failure.addSuppressed(rollbackFailure);
      }
    } finally {
      writeLock.close();
    }
  }

  /** Whether {@code failure} of a begin proves that the begin opened nothing of its own. */
  private static boolean beganNothing(Throwable failure) {
    return (failure instanceof IllegalStateException
            && MANUAL_COMMIT_CONNECTION.equals(failure.getMessage()))
        || (failure instanceof SQLiteException sqlite && isTransactionWithinTransaction(sqlite));
  }

  /**
   * Runs a write that is one statement in autocommit, with no {@code BEGIN}: waits for the file's
   * write mutex ({@link #awaitWriteLock}), runs {@code statement}, which SQLite executes as one
   * transaction of its own, and releases the mutex.
   *
   * @param conn the connection, in autocommit mode
   * @param statement the one write statement
   * @param <T> the statement's result
   * @return the statement's result
   * @throws IllegalStateException if {@code conn} is in manual-commit mode
   * @throws SQLException if the mutex or SQLite's write lock is not granted within {@code
   *     busy_timeout} ({@code SQLITE_BUSY}), or the statement fails otherwise
   */
  static <T> T writeStatement(Connection conn, Work<T> statement) throws SQLException {
    try (WriteLock writeLock = awaitWriteLock(conn)) {
      try {
        return statement.run(conn);
      } catch (SQLException statementFailure) {
        throw heldByOwnBatch(statementFailure, writeLock.file());
      }
    }
  }

  /**
   * SQLite's refusal of the write lock ({@code SQLITE_BUSY}) whose holder is known: the projection
   * batch or replay the refused thread itself runs on the same database file. The write was made
   * from inside the batch's updater on a connection of its own, so it waited for a transaction that
   * cannot end before the updater returns. {@link SqliteEventStore#lockFailure} says so in its
   * message instead of guessing at an application transaction.
   */
  static final class WriteLockHeldByOwnBatch extends SQLiteException {

    private static final long serialVersionUID = 1L;

    private WriteLockHeldByOwnBatch(SQLException refusal) {
      super(refusal.getMessage(), SQLiteErrorCode.SQLITE_BUSY);
      initCause(refusal);
    }
  }

  /**
   * {@code failure} itself, unless it is SQLite refusing the write lock of {@code file} while this
   * thread's own projection batch holds it ({@link SqliteWriteTransaction#heldOn}): then the same
   * refusal as a {@link WriteLockHeldByOwnBatch}.
   */
  private static SQLException heldByOwnBatch(SQLException failure, Path file) {
    return file != null
            && SqliteEventStore.isBusy(failure)
            && SqliteWriteTransaction.heldOn(file) != null
        ? new WriteLockHeldByOwnBatch(failure)
        : failure;
  }

  /**
   * Whether {@code e}, or a cause of it, is a {@link WriteLockHeldByOwnBatch}.
   *
   * @param e the failure of a write
   * @return {@code true} when this thread's own batch held the write lock the write waited for
   */
  static boolean isHeldByOwnBatch(Throwable e) {
    return Causes.find(e, WriteLockHeldByOwnBatch.class) != null;
  }

  /**
   * A write transaction {@link #open} began: holds the file's write mutex and SQLite's write lock
   * until it is closed. Used by one thread, the one that opened it.
   */
  static final class OpenTransaction implements AutoCloseable {

    private final Connection conn;
    private final String transaction;
    private final WriteLock writeLock;
    private boolean committed;

    private OpenTransaction(Connection conn, String transaction, WriteLock writeLock) {
      this.conn = conn;
      this.transaction = transaction;
      this.writeLock = writeLock;
    }

    /**
     * Commits the transaction. A failure propagates; the transaction may still be open, and {@link
     * #close()} rolls it back.
     *
     * @throws SQLException if the {@code COMMIT} fails
     */
    void commit() throws SQLException {
      SqliteTransactions.commit(conn);
      committed = true;
    }

    /**
     * Rolls the transaction back unless it committed, then releases the write mutex. Never throws a
     * {@code SQLException} or {@code RuntimeException} (see {@link SqliteTransactions#rollback}).
     */
    @Override
    public void close() {
      try {
        if (!committed) {
          rollback(conn, transaction);
        }
      } finally {
        writeLock.close();
      }
    }
  }

  // ---- the in-process write mutex ----

  /** The message of the mutex's timeout, which carries SQLite's code for a lock not granted. */
  private static final String WRITE_LOCK_TIMEOUT =
      "[SQLITE_BUSY] The database file is locked (another write transaction of this process held"
          + " the write lock for longer than this connection's busy_timeout of %d ms)";

  /** The write mutex of one database file: fair, so writers are served first come first served. */
  private static final class FileMutex {
    private final Path file;
    private final ReentrantLock lock = new ReentrantLock(true);

    private FileMutex(Path file) {
      this.file = file;
    }
  }

  /**
   * The mutexes by canonical database file, and by the connection URL that led to the file, so the
   * file is asked of the database once per URL. One entry per database file this process writes
   * through the stores, and per URL spelling of it; never removed.
   */
  private static final ConcurrentHashMap<Path, FileMutex> MUTEX_BY_FILE = new ConcurrentHashMap<>();

  private static final ConcurrentHashMap<String, FileMutex> MUTEX_BY_URL =
      new ConcurrentHashMap<>();

  /** A hold of a file's write mutex, or of none; {@link #close()} releases it, once. */
  static final class WriteLock implements AutoCloseable {

    private static final WriteLock NONE = new WriteLock(null);

    private final FileMutex mutex;
    private boolean released;

    private WriteLock(FileMutex mutex) {
      this.mutex = mutex;
    }

    /**
     * The canonical database file whose mutex this holds, or {@code null} when the connection's
     * file could not be told and nothing is held.
     */
    Path file() {
      return mutex == null ? null : mutex.file;
    }

    /** Releases the mutex, on the thread that took it; a second call does nothing. */
    @Override
    public void close() {
      if (mutex != null && !released) {
        released = true;
        mutex.lock.unlock();
      }
    }
  }

  /**
   * Waits for the write mutex of {@code conn}'s database file, at most the connection's {@code
   * busy_timeout}: the step before every {@code BEGIN IMMEDIATE} of a store, and before a write
   * that is one statement in autocommit. The caller releases it by closing the returned hold after
   * its {@code COMMIT} or {@code ROLLBACK}, in a try-with-resources; the stores reach it through
   * {@link #open} and {@link #writeStatement}. See the class Javadoc for what the mutex is for and
   * where it stands among the other locks.
   *
   * <p>The thread that already holds the mutex gets it again at once. The wait is not
   * interruptible: an interrupt leaves the thread's interrupt status set and the wait goes on, as
   * the wait for SQLite's own lock does. A connection whose database file cannot be told takes no
   * mutex, and the returned hold releases nothing.
   *
   * @param conn the connection the write will run on, in autocommit mode
   * @return the hold; close it to release the mutex
   * @throws IllegalStateException if {@code conn} is in manual-commit mode, before any wait
   * @throws SQLException with {@code SQLITE_BUSY} if the mutex is not granted within the
   *     connection's {@code busy_timeout}; or if the connection cannot be read
   */
  static WriteLock awaitWriteLock(Connection conn) throws SQLException {
    requireAutoCommit(conn);
    FileMutex mutex = mutexOf(conn);
    if (mutex == null) {
      return WriteLock.NONE;
    }
    // Built before the mutex is asked for: nothing is allocated between the grant and the return,
    // so nothing thrown there can leave the mutex held with no hold to release it. A hold whose
    // wait failed is dropped unclosed: it holds nothing.
    WriteLock hold = new WriteLock(mutex);
    if (!tryLockUninterruptibly(mutex.lock, 0)) {
      long busyTimeoutMillis = busyTimeoutMillis(conn);
      if (!tryLockUninterruptibly(mutex.lock, busyTimeoutMillis)) {
        throw new SQLiteException(
            String.format(WRITE_LOCK_TIMEOUT, busyTimeoutMillis), SQLiteErrorCode.SQLITE_BUSY);
      }
    }
    return hold;
  }

  /**
   * The mutex of {@code conn}'s database file, or {@code null} when the file cannot be told: the
   * connection is not the driver's, the database is in-memory or temporary, or its path cannot be
   * resolved. Such a write is serialized by SQLite's own lock and {@code busy_timeout} alone.
   */
  private static FileMutex mutexOf(Connection conn) {
    try {
      if (!conn.isWrapperFor(SQLiteConnection.class)) {
        return null;
      }
      SQLiteConnection sqlite = conn.unwrap(SQLiteConnection.class);
      String url = sqlite.getUrl();
      FileMutex known = url == null ? null : MUTEX_BY_URL.get(url);
      if (known != null) {
        return known;
      }
      // The canonical path SqliteDatabase.file() reports: SQLite's own name of the main database,
      // with symbolic links resolved.
      Path file = SqliteSetup.databaseFile(sqlite).toRealPath();
      FileMutex mutex = MUTEX_BY_FILE.computeIfAbsent(file, FileMutex::new);
      if (url != null) {
        MUTEX_BY_URL.putIfAbsent(url, mutex);
      }
      return mutex;
    } catch (SQLException | IOException | RuntimeException unknownFile) {
      return null;
    }
  }

  /** The connection's {@code busy_timeout} in milliseconds, as SQLite reports it. */
  private static long busyTimeoutMillis(Connection conn) throws SQLException {
    try (Statement statement = conn.createStatement();
        ResultSet result = statement.executeQuery("PRAGMA busy_timeout")) {
      result.next();
      return Math.max(0, result.getLong(1));
    }
  }

  /**
   * {@link ReentrantLock#tryLock(long, TimeUnit)} that an interrupt does not end: the wait goes on
   * for what is left of {@code millis}, and the thread's interrupt status is set when it returns. A
   * thread whose status is already set waits too.
   */
  private static boolean tryLockUninterruptibly(ReentrantLock lock, long millis) {
    boolean interrupted = Thread.interrupted();
    try {
      long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
      while (true) {
        try {
          return lock.tryLock(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Begins a write transaction with {@code BEGIN IMMEDIATE}. A failure, for example the write lock
   * not granted within {@code busy_timeout}, opened nothing and propagates. The statement alone: it
   * takes no write mutex, so the stores begin through {@link #open} or {@link #inTransaction}.
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
