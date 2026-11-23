package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateLocker;

class SagaConnectionBudgetTest {

  private static AggregateLocker lockerHolding(int connections) {
    return new AggregateLocker() {
      @Override
      public AutoCloseable acquireLock(
          org.streamrune.core.types.StreamId streamId, java.time.Duration timeout) {
        return () -> {};
      }

      @Override
      public int applicationPoolConnectionsPerLock() {
        return connections;
      }
    };
  }

  @Test
  void aStepCountsOnePlusWhatTheLockerHolds() {
    assertEquals(1, SagaConnectionBudget.connectionsPerStep(lockerHolding(0)));
    assertEquals(2, SagaConnectionBudget.connectionsPerStep(lockerHolding(1)));
    assertEquals(3, SagaConnectionBudget.connectionsPerStep(lockerHolding(2)));
  }

  @Test
  void aNegativeAnswerIsRefusedNamingTheLocker() {
    var locker = lockerHolding(-1);

    var refused =
        assertThrows(
            IllegalStateException.class, () -> SagaConnectionBudget.connectionsPerStep(locker));
    assertTrue(refused.getMessage().contains(locker.getClass().getName()));
  }

  @Test
  void warnsAboveHalfThePool() {
    assertEquals(
        Optional.of(
            "StreamRune: saga steps can hold up to 8 connections at once (2 per step, 4 steps over"
                + " 1 saga type(s)), more than half of the connection pool's maximum of 10. Raise"
                + " the pool, give the aggregate locker a pool of its own"
                + " (PgAdvisoryLocker.withDedicatedPool), or lower streamrune.saga.parallelism."),
        SagaConnectionBudget.warning(4, 2, 1, OptionalInt.of(10)));
  }

  @Test
  void warnsAtTheFirstValueAboveHalf() {
    assertTrue(SagaConnectionBudget.warning(3, 2, 1, OptionalInt.of(11)).isPresent());
    assertTrue(SagaConnectionBudget.warning(3, 2, 1, OptionalInt.of(12)).isEmpty());
  }

  @Test
  void aLockerHoldingNoPoolConnectionIsNotAdvisedADedicatedPool() {
    assertEquals(
        Optional.of(
            "StreamRune: saga steps can hold up to 8 connections at once (1 per step, 8 steps over"
                + " 2 saga type(s)), more than half of the connection pool's maximum of 10. Raise"
                + " the pool, or lower streamrune.saga.parallelism."),
        SagaConnectionBudget.warning(8, 1, 2, OptionalInt.of(10)));
  }

  @Test
  void aPoolMaximumBelowOneIsRefusedAsAnArgument() {
    for (int bad : new int[] {0, -1}) {
      assertThrows(
          IllegalArgumentException.class,
          () -> SagaConnectionBudget.warning(4, 2, 1, OptionalInt.of(bad)));
      assertThrows(
          IllegalArgumentException.class,
          () -> SagaConnectionBudget.refusal(4, 1, OptionalInt.of(bad)));
    }
  }

  @Test
  void doesNotWarnAtExactlyHalf() {
    assertTrue(SagaConnectionBudget.warning(5, 1, 1, OptionalInt.of(10)).isEmpty());
  }

  @Test
  void doesNotWarnWhenThePoolSizeIsUnknown() {
    assertTrue(SagaConnectionBudget.warning(64, 2, 1, OptionalInt.empty()).isEmpty());
  }

  @Test
  void aStepHoldingNoConnectionIsRefusedByTheWarning() {
    assertThrows(
        IllegalStateException.class,
        () -> SagaConnectionBudget.warning(4, 0, 1, OptionalInt.of(10)));
  }

  @Test
  void refusesWhenTheLocksCanTakeThePool() {
    assertEquals(
        Optional.of(
            "StreamRune: saga steps can hold up to 4 connections of the connection pool as"
                + " aggregate locks at once (4 steps, 1 per lock), at least the pool's maximum of"
                + " 4: every connection can be taken by a lock while its step waits for a second"
                + " one. Give the aggregate locker a pool of its own"
                + " (PgAdvisoryLocker.withDedicatedPool), raise the pool, or lower"
                + " streamrune.saga.parallelism."),
        SagaConnectionBudget.refusal(4, 1, OptionalInt.of(4)));
  }

  @Test
  void theRefusalMultipliesStepsByConnectionsPerLock() {
    assertEquals(
        Optional.of(
            "StreamRune: saga steps can hold up to 12 connections of the connection pool as"
                + " aggregate locks at once (6 steps, 2 per lock), at least the pool's maximum of"
                + " 12: every connection can be taken by a lock while its step waits for a second"
                + " one. Give the aggregate locker a pool of its own"
                + " (PgAdvisoryLocker.withDedicatedPool), raise the pool, or lower"
                + " streamrune.saga.parallelism."),
        SagaConnectionBudget.refusal(6, 2, OptionalInt.of(12)));
    assertTrue(SagaConnectionBudget.refusal(5, 2, OptionalInt.of(12)).isEmpty());
    assertTrue(SagaConnectionBudget.refusal(6, 2, OptionalInt.of(13)).isEmpty());
  }

  @Test
  void theRefusalNamesEachOfItsNumbersInItsPlace() {
    assertEquals(
        Optional.of(
            "StreamRune: saga steps can hold up to 6 connections of the connection pool as"
                + " aggregate locks at once (3 steps, 2 per lock), at least the pool's maximum of"
                + " 5: every connection can be taken by a lock while its step waits for a second"
                + " one. Give the aggregate locker a pool of its own"
                + " (PgAdvisoryLocker.withDedicatedPool), raise the pool, or lower"
                + " streamrune.saga.parallelism."),
        SagaConnectionBudget.refusal(3, 2, OptionalInt.of(5)));
  }

  @Test
  void aNonPositivePoolMaximumIsRefusedEvenForALockerWithoutPoolConnections() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SagaConnectionBudget.refusal(4, 0, OptionalInt.of(0)));
  }

  @Test
  void doesNotRefuseOneBelowThePool() {
    assertTrue(SagaConnectionBudget.refusal(4, 1, OptionalInt.of(5)).isEmpty());
  }

  @Test
  void aLockerWithoutPoolConnectionsIsNeverRefused() {
    assertTrue(SagaConnectionBudget.refusal(4, 0, OptionalInt.of(1)).isEmpty());
  }

  @Test
  void doesNotRefuseWhenThePoolSizeIsUnknown() {
    assertTrue(SagaConnectionBudget.refusal(64, 1, OptionalInt.empty()).isEmpty());
  }

  @Test
  void aNegativeConnectionsPerLockIsRefused() {
    assertThrows(
        IllegalStateException.class, () -> SagaConnectionBudget.refusal(4, -1, OptionalInt.of(10)));
  }
}
