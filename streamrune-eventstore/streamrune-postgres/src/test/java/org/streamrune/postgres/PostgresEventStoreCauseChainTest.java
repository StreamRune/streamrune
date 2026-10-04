package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoOperationException;

/**
 * {@link PostgresEventStore#hasCryptoCause} must walk the cause chain with a depth bound so a
 * self-referential/cyclic chain terminates instead of spinning forever. Pure unit test — no
 * PostgreSQL container needed.
 *
 * <p>Also covers {@link PostgresEventStore#isRetryableConflict}, whose {@code getCause()} walk had
 * no cycle guard at all.
 */
class PostgresEventStoreCauseChainTest {

  /** A throwable whose getCause() returns a settable field, so two of them can form a cycle. */
  static final class CyclicThrowable extends RuntimeException {
    private transient Throwable next;

    void setNext(Throwable next) {
      this.next = next;
    }

    @Override
    public synchronized Throwable getCause() {
      return next;
    }
  }

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
  void hasCryptoCause_cyclicChainWithoutCrypto_terminatesAndReturnsFalse() {
    var a = new CyclicThrowable();
    var b = new CyclicThrowable();
    a.setNext(b);
    b.setNext(a); // a -> b -> a -> ... an unbounded walk would never terminate

    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () ->
            assertFalse(
                PostgresEventStore.hasCryptoCause(a), "no CryptoOperationException in the cycle"));
  }

  @Test
  void hasCryptoCause_detectsWrappedCryptoCause() {
    var wrapped = new RuntimeException("jackson", new CryptoOperationException("decrypt failed"));
    assertTrue(PostgresEventStore.hasCryptoCause(wrapped));
  }

  @Test
  void isRetryableConflict_cyclicChainWithoutRetryableState_terminatesAndReturnsFalse() {
    var a = new CyclicSqlException();
    var b = new CyclicSqlException();
    a.setNext(b);
    b.setNext(a); // a -> b -> a -> ... an unbounded getCause() walk would never terminate

    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () ->
            assertFalse(
                PostgresEventStore.isRetryableConflict(a),
                "no retryable SQLState anywhere in the cycle"));
  }
}
