package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link PostgresEventStore#isRetryableConflict(SQLException)} — the conflict
 * classifier that decides whether a caught SQLException is a retryable optimistic conflict (mapped
 * to {@code OptimisticLockException}) or a hard failure (dead-lettering {@code
 * EventStoreException}).
 *
 * <p>These lock the contract: a MULTI-event batch conflict surfaces as a {@link
 * BatchUpdateException} whose {@code 23505} PSQLException is linked via {@code getNextException()}
 * (not the top-level SQLState), and a {@code 40001} serialization_failure must also be treated as
 * retryable.
 */
class PostgresEventStoreConflictClassificationTest {

  @Test
  void topLevelUniqueViolationIsRetryable() {
    assertTrue(PostgresEventStore.isRetryableConflict(new SQLException("dup", "23505")));
  }

  @Test
  void topLevelSerializationFailureIsRetryable() {
    assertTrue(PostgresEventStore.isRetryableConflict(new SQLException("serialize", "40001")));
  }

  @Test
  void batchUpdateException_withNextExceptionUniqueViolation_isRetryable() {
    // Models pgjdbc: executeBatch() fails and links the failing sub-statement's 23505 PSQLException
    // via getNextException(); the BatchUpdateException's own SQLState may be null.
    BatchUpdateException batch =
        new BatchUpdateException("batch failed", null, new int[] {Statement.EXECUTE_FAILED});
    batch.setNextException(new SQLException("duplicate key value", "23505"));
    assertTrue(PostgresEventStore.isRetryableConflict(batch));
  }

  @Test
  void batchUpdateException_withNextExceptionSerializationFailure_isRetryable() {
    BatchUpdateException batch =
        new BatchUpdateException("batch failed", null, new int[] {Statement.EXECUTE_FAILED});
    batch.setNextException(new SQLException("could not serialize access", "40001"));
    assertTrue(PostgresEventStore.isRetryableConflict(batch));
  }

  @Test
  void nestedCauseUniqueViolationIsRetryable() {
    SQLException cause = new SQLException("dup", "23505");
    SQLException wrapper = new SQLException("wrapping");
    wrapper.initCause(cause);
    assertTrue(PostgresEventStore.isRetryableConflict(wrapper));
  }

  @Test
  void unrelatedSqlStateIsNotRetryable() {
    // 42P01 = undefined_table — a genuine failure that must dead-letter, not retry forever.
    assertFalse(PostgresEventStore.isRetryableConflict(new SQLException("no such table", "42P01")));
  }

  @Test
  void nullSqlStateIsNotRetryable() {
    assertFalse(PostgresEventStore.isRetryableConflict(new SQLException("no state")));
  }
}
