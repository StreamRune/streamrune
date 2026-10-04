package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.sql.SQLException;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * {@link PostgresOutboxStore#isBenignInflightUniqueViolation} walked the cause chain via {@code
 * getCause()} with NO cycle guard at all — a self-referential/cyclic chain would spin forever.
 * Bounded to depth 50, mirroring {@code PostgresEventStore#hasCryptoCause}/{@code
 * isRetryableConflict}. Pure unit test — no PostgreSQL container needed.
 */
class PostgresOutboxStoreCauseChainTest {

  /** A {@link SQLException} whose getCause() returns a settable field, so two can form a cycle. */
  static final class CyclicSqlException extends SQLException {
    private transient Throwable next;

    void setNext(Throwable next) {
      this.next = next;
    }

    @Override
    public synchronized Throwable getCause() {
      return next;
    }
  }

  @Test
  void isBenignInflightUniqueViolation_cyclicChainWithoutMatch_terminatesAndReturnsFalse() {
    var a = new CyclicSqlException();
    var b = new CyclicSqlException();
    a.setNext(b);
    b.setNext(a); // a -> b -> a -> ... an unbounded getCause() walk would never terminate

    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () ->
            assertFalse(
                PostgresOutboxStore.isBenignInflightUniqueViolation(a),
                "no ONE_INFLIGHT_INDEX unique-violation anywhere in the cycle"));
  }
}
