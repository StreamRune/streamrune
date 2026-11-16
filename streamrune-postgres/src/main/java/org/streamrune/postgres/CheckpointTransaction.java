package org.streamrune.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStoreException;

/**
 * The two transactions that hold a projection's checkpoint row — a batch and a dead-letter replay —
 * how each reports a failure, and the one transaction shell both run in ({@link #run}). {@link
 * JdbcProjectionRepository} and {@code PostgresSubscriptionCheckpointProcessor} share it, so a
 * checkpoint transaction commits, rolls back and gives its connection back the same way in both.
 *
 * <p>The shell does not lock: the body's first statement locks the checkpoint row through {@link
 * PostgresCheckpointFence}, with the batch's fences or with none for a replay.
 */
enum CheckpointTransaction {

  /** A batch: the lock with its fences, the updater, and the checkpoint advance. */
  BATCH(
      "projection batch",
      "Transaction failed during projection update",
      "Failed to process projection batch atomically"),

  /** A dead-letter replay: the lock and the updater; the checkpoint row is not written. */
  REPLAY(
      "projection dead-letter replay",
      "Transaction failed during projection dead-letter replay",
      "Failed to replay a projection dead-letter range");

  private static final Logger log = LoggerFactory.getLogger(CheckpointTransaction.class);

  /** What the transaction is, for the log record of a failed rollback (a constant, never an id). */
  private final String label;

  /** The message of a failure inside the transaction: a body statement or the commit. */
  private final String bodyFailure;

  /** The message of a failure of the connection: getting it, autocommit off, or closing it. */
  private final String connectionFailure;

  CheckpointTransaction(String label, String bodyFailure, String connectionFailure) {
    this.label = label;
    this.bodyFailure = bodyFailure;
    this.connectionFailure = connectionFailure;
  }

  /** The statements one checkpoint transaction runs. */
  @FunctionalInterface
  interface Body {

    /**
     * Runs the transaction's statements on {@code conn}, the first of which locks the checkpoint
     * row. Returning means the COMMIT is issued next.
     *
     * @param conn the transaction's connection, autocommit off
     * @return what to run once the COMMIT succeeded, before the connection goes back to the pool;
     *     {@code null} for nothing
     * @throws SQLException if a statement fails
     */
    Runnable run(Connection conn) throws SQLException;
  }

  /**
   * Runs {@code body} in one transaction on a pooled connection of {@code dataSource} and commits
   * it.
   *
   * <p>Every exit that did not commit — an exception from {@code body} (a fence rejection, an
   * updater's own exception or {@link Error}), a failed statement, a failed {@code commit()} —
   * rolls the transaction back once, in a {@code finally}, before anything restores autocommit:
   * pgjdbc's {@code setAutoCommit(true)} COMMITS an open transaction, so a restore first would make
   * a partial transaction durable. A rollback that fails aborts the physical connection instead and
   * leaves autocommit alone, so neither the restore nor the pool's reset on return (Agroal resets
   * autocommit with no rollback first) can commit it ({@link
   * PostgresTransactions#rollbackOrAbort}); an {@link Error} from the rollback propagates after the
   * abort. Autocommit is restored after a commit or a successful rollback; a failure there is
   * logged and swallowed.
   *
   * <p>Failures are reported as follows; anything else {@code body} throws propagates unchanged:
   *
   * <ul>
   *   <li>a {@link SQLException} from {@code body} or from {@code commit()}: an {@link
   *       EventStoreException} with this transaction's body-failure message;
   *   <li>a {@link SQLException} getting the connection, switching autocommit off or closing the
   *       connection: an {@link EventStoreException} with its connection-failure message. Closing
   *       comes after the commit: that failure does not undo a committed transaction.
   * </ul>
   *
   * @param dataSource the pool to borrow the transaction's connection from
   * @param body the transaction's statements
   */
  void run(DataSource dataSource, Body body) {
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      boolean committed = false;
      try {
        Runnable onCommitted = body.run(conn);
        conn.commit();
        committed = true;
        if (onCommitted != null) {
          onCommitted.run();
        }
      } catch (SQLException e) {
        // Wrap the transaction-body SQLException (rollback happens once, in the finally).
        throw new EventStoreException(bodyFailure, e);
      } finally {
        // The SINGLE rollback point for every non-committed exit — a RuntimeException (e.g. an
        // updater IllegalStateException or the checkpoint fence's ProjectionCommitFencedException),
        // the wrapped SQLException above, OR any OTHER Throwable (an Error such as
        // StackOverflowError / OutOfMemoryError / AssertionError from user mapping code in the
        // projection updater). It must run BEFORE restoring autoCommit: pgjdbc's
        // setAutoCommit(true) COMMITS an open transaction, so without this rollback a
        // partially-applied read-model write would become durable WITHOUT the offset advance, and
        // the retried batch would re-apply the same events (the overlap guard passes because the
        // checkpoint never moved) — double-applying a non-idempotent projection. Mirrors
        // PostgresEventStore's append discipline. A FAILED rollback must not be followed by that
        // restore, and the pool must not be left to commit the transaction on return (Agroal
        // resets autoCommit with no rollback first), so the physical connection is aborted
        // instead and autoCommit is left alone (PostgresTransactions). An Error from the rollback
        // propagates after the abort.
        if (committed || PostgresTransactions.rollbackOrAbort(conn, label)) {
          // Restore auto-commit before the connection returns to the pool. Swallow a throw here:
          // on the success path the tx has already committed (a throw must not fail a durable
          // apply); on the abort path the rollback is already done.
          try {
            conn.setAutoCommit(true);
          } catch (SQLException restoreEx) {
            log.debug(
                "Failed to restore autoCommit before returning connection to pool (benign — pool"
                    + " resets on return): {}",
                restoreEx.getMessage());
          }
        }
      }
    } catch (SQLException e) {
      throw new EventStoreException(connectionFailure, e);
    }
  }
}
