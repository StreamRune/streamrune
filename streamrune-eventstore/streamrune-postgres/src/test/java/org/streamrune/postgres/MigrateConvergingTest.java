package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.flywaydb.core.api.FlywayException;
import org.junit.jupiter.api.Test;

/**
 * When {@link PostgresEventStoreFactory#migrateConverging} runs a series a second time. The race
 * itself, with real replicas against a real database, is {@link ConcurrentSchemaInitializationIT}.
 */
class MigrateConvergingTest {

  /**
   * What Flyway 13 says when another instance created the history table meanwhile; that real Flyway
   * still says it is checked by {@link ConcurrentSchemaInitializationIT}.
   */
  private static final String REFUSAL =
      "Unable to baseline schema history table \"public\".\"flyway_schema_history_streamrune\" as"
          + " it already exists, and is empty.\nDelete the schema history table, and run baseline"
          + " again.";

  private final AtomicInteger runs = new AtomicInteger();

  @Test
  void aSuccessfulRunIsNotRepeated() {
    PostgresEventStoreFactory.migrateConverging(runs::incrementAndGet);

    assertThat(runs).hasValue(1);
  }

  @Test
  void aRunRefusedBecauseAnotherInstanceCreatedTheHistoryTableIsRunAgainOnce() {
    PostgresEventStoreFactory.migrateConverging(
        failingFrom(new ArrayDeque<>(List.of(new FlywayException(REFUSAL)))));

    assertThat(runs).hasValue(2);
  }

  @Test
  void anyOtherFailureIsRethrownWithoutASecondRun() {
    // e.g. a pooler without a second server connection: the run fails after creating the table.
    var failure = new FlywayException("Unable to obtain connection from database");

    assertThatThrownBy(
            () ->
                PostgresEventStoreFactory.migrateConverging(
                    failingFrom(new ArrayDeque<>(List.of(failure)))))
        .isSameAs(failure);
    assertThat(runs).hasValue(1);
  }

  @Test
  void aFailureWithoutAMessageIsRethrownWithoutASecondRun() {
    var failure = new FlywayException((String) null);

    assertThatThrownBy(
            () ->
                PostgresEventStoreFactory.migrateConverging(
                    failingFrom(new ArrayDeque<>(List.of(failure)))))
        .isSameAs(failure);
    assertThat(runs).hasValue(1);
  }

  @Test
  void aFailedSecondRunIsRethrownWithTheRefusalSuppressed() {
    var refusal = new FlywayException(REFUSAL);
    var second = new FlywayException("Migration V001__streamrune_baseline.sql failed");

    assertThatThrownBy(
            () ->
                PostgresEventStoreFactory.migrateConverging(
                    failingFrom(new ArrayDeque<>(List.of(refusal, second)))))
        .isSameAs(second)
        .satisfies(thrown -> assertThat(thrown.getSuppressed()).containsExactly(refusal));
    assertThat(runs).hasValue(2);
  }

  @Test
  void aSecondRefusalIsNotRunAThirdTime() {
    var refusal = new FlywayException(REFUSAL);
    var again = new FlywayException(REFUSAL);

    assertThatThrownBy(
            () ->
                PostgresEventStoreFactory.migrateConverging(
                    failingFrom(new ArrayDeque<>(List.of(refusal, again)))))
        .isSameAs(again);
    assertThat(runs).hasValue(2);
  }

  /** Each run throws the next failure; once they are used up, runs succeed. */
  private Runnable failingFrom(Deque<FlywayException> failures) {
    return () -> {
      runs.incrementAndGet();
      FlywayException next = failures.pollFirst();
      if (next != null) {
        throw next;
      }
    };
  }
}
