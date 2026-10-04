package org.streamrune.postgres;

import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import org.postgresql.jdbc.PgStatement;

/**
 * The bound {@link PostgresEventStore} puts on its own statements, applied so that nothing is left
 * on the connection it hands back to the application's pool.
 *
 * <ul>
 *   <li><b>Inside a transaction</b> the bound is {@code SET LOCAL statement_timeout}, which
 *       PostgreSQL ends with the transaction (commit or rollback) and which covers every statement
 *       of that transaction — the store's own and the outbox, inbox and audit writes made in it.
 *   <li><b>A statement that runs on its own</b> carries the bound as its JDBC query timeout, which
 *       the driver enforces by asking the server to cancel the statement once it runs too long.
 * </ul>
 *
 * <p>Neither changes the session: the next borrower of the connection sees whatever {@code
 * statement_timeout} the application, its pool or its database role set. Both end a statement that
 * ran too long with SQLState {@code 57014} ({@code query_canceled}).
 */
final class StatementTimeout {

  /** The store's default bound: 30 seconds. */
  static final Duration DEFAULT = Duration.ofSeconds(30);

  /** The largest bound, in milliseconds: {@code statement_timeout} is a 32-bit integer. */
  static final long MAX_MILLIS = Integer.MAX_VALUE;

  /** No bound: statements run as long as the session's own {@code statement_timeout} allows. */
  static final StatementTimeout DISABLED = new StatementTimeout(0L);

  private final long millis;

  private StatementTimeout(long millis) {
    this.millis = millis;
  }

  /**
   * Validates and wraps a configured bound.
   *
   * <p>A positive value shorter than one millisecond is rejected rather than accepted: the bound is
   * a whole number of milliseconds ({@code statement_timeout} takes an integer count of them), such
   * a value truncates to {@code 0}, and {@code 0} is PostgreSQL's spelling of "no timeout at all" —
   * the opposite of what was asked. A value above {@value #MAX_MILLIS} ms is rejected too: it is
   * more than {@code statement_timeout} accepts, so every append would fail when it set the bound.
   *
   * @param timeout the bound; {@link Duration#ZERO} disables it
   * @return the bound
   * @throws IllegalArgumentException if {@code timeout} is null, negative, positive and shorter
   *     than one millisecond, or longer than {@value #MAX_MILLIS} ms
   */
  static StatementTimeout of(Duration timeout) {
    if (timeout == null || timeout.isNegative()) {
      throw new IllegalArgumentException(
          "statementTimeout must be a non-negative duration (Duration.ZERO disables it)");
    }
    if (timeout.isZero()) {
      return DISABLED;
    }
    if (timeout.compareTo(Duration.ofMillis(MAX_MILLIS)) > 0) {
      throw new IllegalArgumentException(
          "statementTimeout must be at most "
              + MAX_MILLIS
              + "ms, the largest statement_timeout PostgreSQL accepts; got: "
              + timeout);
    }
    if (timeout.toMillis() < 1L) {
      throw new IllegalArgumentException(
          "statementTimeout must be either exactly 0 (disabled) or at least 1ms — a shorter"
              + " duration truncates to statement_timeout = 0, which PostgreSQL reads as"
              + " \"no timeout at all\", the opposite of what was asked; got: "
              + timeout);
    }
    return new StatementTimeout(timeout.toMillis());
  }

  /** Whether a bound is set. */
  boolean enabled() {
    return millis > 0L;
  }

  /** The bound in whole milliseconds; {@code 0} when disabled. */
  long millis() {
    return millis;
  }

  /**
   * The statement that bounds the rest of the current transaction. Only meaningful when {@link
   * #enabled()}.
   */
  String setLocalSql() {
    return "SET LOCAL statement_timeout = " + millis;
  }

  /**
   * Gives {@code statement} the bound as its JDBC query timeout; no-op when disabled. The driver's
   * own statement takes it in milliseconds; a statement that cannot be unwrapped to it (a proxy
   * that hides the driver) gets the standard whole-second timeout, rounded up so it is never
   * shorter than the configured bound.
   *
   * @param statement the statement about to run
   * @throws SQLException if the driver refuses the timeout
   */
  void applyTo(Statement statement) throws SQLException {
    if (!enabled()) {
      return;
    }
    if (statement.isWrapperFor(PgStatement.class)) {
      statement.unwrap(PgStatement.class).setQueryTimeoutMs(millis);
    } else {
      statement.setQueryTimeout((int) Math.ceilDiv(millis, 1000L));
    }
  }
}
