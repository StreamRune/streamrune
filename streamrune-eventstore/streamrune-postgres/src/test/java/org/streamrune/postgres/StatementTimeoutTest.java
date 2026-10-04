package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.postgresql.jdbc.PgStatement;

/** How the event store's statement bound is validated and put on a statement. */
class StatementTimeoutTest {

  @Test
  void theDefaultIsThirtySeconds() {
    assertEquals(30_000L, StatementTimeout.DEFAULT.toMillis());
    assertEquals(30_000L, StatementTimeout.of(StatementTimeout.DEFAULT).millis());
  }

  @Test
  void zeroMeansNoBound() {
    StatementTimeout none = StatementTimeout.of(Duration.ZERO);
    assertSame(StatementTimeout.DISABLED, none);
    assertFalse(none.enabled());
    assertEquals(0L, none.millis());
  }

  @Test
  void aNullNegativeOrSubMillisecondBoundIsRefused() {
    assertThrows(IllegalArgumentException.class, () -> StatementTimeout.of(null));
    assertThrows(IllegalArgumentException.class, () -> StatementTimeout.of(Duration.ofMillis(-1)));
    var subMs =
        assertThrows(
            IllegalArgumentException.class, () -> StatementTimeout.of(Duration.ofNanos(999_999)));
    assertTrue(subMs.getMessage().contains("at least 1ms"), subMs.getMessage());
    assertEquals(1L, StatementTimeout.of(Duration.ofMillis(1)).millis());
  }

  @Test
  void aBoundBeyondWhatPostgresAcceptsIsRefused() {
    // statement_timeout is an int count of milliseconds: a larger SET LOCAL would fail every
    // append at run time, so the configuration is refused up front.
    assertEquals(
        Integer.MAX_VALUE, StatementTimeout.of(Duration.ofMillis(Integer.MAX_VALUE)).millis());
    var tooLong =
        assertThrows(
            IllegalArgumentException.class,
            () -> StatementTimeout.of(Duration.ofMillis(Integer.MAX_VALUE + 1L)));
    assertTrue(tooLong.getMessage().contains("2147483647ms"), tooLong.getMessage());
    assertThrows(
        IllegalArgumentException.class,
        () -> StatementTimeout.of(Duration.ofSeconds(Long.MAX_VALUE)));
  }

  @Test
  void theTransactionFormIsSetLocal() {
    assertEquals(
        "SET LOCAL statement_timeout = 1500",
        StatementTimeout.of(Duration.ofMillis(1500)).setLocalSql());
  }

  @Test
  void theDriversStatementTakesTheBoundInMilliseconds() throws Exception {
    PreparedStatement statement = mock(PreparedStatement.class);
    PgStatement driverStatement = mock(PgStatement.class);
    when(statement.isWrapperFor(PgStatement.class)).thenReturn(true);
    when(statement.unwrap(PgStatement.class)).thenReturn(driverStatement);

    StatementTimeout.of(Duration.ofMillis(1500)).applyTo(statement);

    verify(driverStatement).setQueryTimeoutMs(1500L);
    verify(statement, never()).setQueryTimeout(org.mockito.ArgumentMatchers.anyInt());
  }

  @Test
  void aStatementThatHidesTheDriverGetsWholeSecondsRoundedUp() throws Exception {
    PreparedStatement statement = mock(PreparedStatement.class);
    when(statement.isWrapperFor(PgStatement.class)).thenReturn(false);

    StatementTimeout.of(Duration.ofMillis(1500)).applyTo(statement);
    verify(statement).setQueryTimeout(2);

    PreparedStatement shortest = mock(PreparedStatement.class);
    StatementTimeout.of(Duration.ofMillis(1)).applyTo(shortest);
    verify(shortest).setQueryTimeout(1);

    PreparedStatement longest = mock(PreparedStatement.class);
    StatementTimeout.of(Duration.ofMillis(Integer.MAX_VALUE)).applyTo(longest);
    verify(longest).setQueryTimeout(2_147_484);
  }

  @Test
  void noBoundLeavesTheStatementAlone() throws Exception {
    PreparedStatement statement = mock(PreparedStatement.class);
    StatementTimeout.DISABLED.applyTo(statement);
    verifyNoInteractions(statement);
  }
}
