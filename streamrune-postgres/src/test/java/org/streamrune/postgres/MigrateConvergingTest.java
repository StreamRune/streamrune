package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.flywaydb.core.api.FlywayException;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * When {@link SeriesMigration#migrateConverging} runs a series a second time. The race itself, with
 * real replicas against a real database, is {@link ConcurrentSchemaInitializationIT}.
 */
class MigrateConvergingTest {

  /**
   * What Flyway 13 says when another instance created the history table meanwhile; that real Flyway
   * still says it is checked by {@link ConcurrentSchemaInitializationIT}.
   */
  private static final String REFUSAL =
      """
      Unable to baseline schema history table "public"."flyway_schema_history_streamrune" as\
       it already exists, and is empty.
      Delete the schema history table, and run baseline\
       again.""";

  private static final String HISTORY = PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE;

  private final AtomicInteger runs = new AtomicInteger();

  @Test
  void aSuccessfulRunIsNotRepeated() {
    SeriesMigration.migrateConverging(HISTORY, runs::incrementAndGet);

    assertThat(runs).hasValue(1);
  }

  @Test
  void aRunRefusedBecauseAnotherInstanceCreatedTheHistoryTableIsRunAgainOnce() {
    SeriesMigration.migrateConverging(
        HISTORY, failingFrom(new ArrayDeque<>(List.of(new FlywayException(REFUSAL)))));

    assertThat(runs).hasValue(2);
  }

  @Test
  void theSecondRunIsLoggedNamingTheSeriesHistoryTable() {
    // Three series (event store, crypto, read model) converge through this one method; the INFO
    // line must say which one ran again, or an operator cannot tell them apart.
    var logger = (Logger) LoggerFactory.getLogger(SeriesMigration.class);
    Level previous = logger.getLevel();
    boolean additive = logger.isAdditive();
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    logger.setAdditive(false);
    try {
      SeriesMigration.migrateConverging(
          PostgresReadModelStore.READ_MODEL_HISTORY_TABLE,
          failingFrom(new ArrayDeque<>(List.of(new FlywayException(REFUSAL)))));
    } finally {
      logger.setAdditive(additive);
      logger.setLevel(previous);
      logger.detachAppender(appender);
      appender.stop();
    }

    assertThat(appender.list)
        .singleElement()
        .extracting(ILoggingEvent::getFormattedMessage)
        .asString()
        .startsWith(
            "Another instance created the schema history table"
                + " 'flyway_schema_history_readmodel' while this one was starting");
  }

  @Test
  void anyOtherFailureIsRethrownWithoutASecondRun() {
    // e.g. a pooler without a second server connection: the run fails after creating the table.
    var failure = new FlywayException("Unable to obtain connection from database");

    assertThatThrownBy(
            () ->
                SeriesMigration.migrateConverging(
                    HISTORY, failingFrom(new ArrayDeque<>(List.of(failure)))))
        .isSameAs(failure);
    assertThat(runs).hasValue(1);
  }

  @Test
  void aFailureWithoutAMessageIsRethrownWithoutASecondRun() {
    var failure = new FlywayException((String) null);

    assertThatThrownBy(
            () ->
                SeriesMigration.migrateConverging(
                    HISTORY, failingFrom(new ArrayDeque<>(List.of(failure)))))
        .isSameAs(failure);
    assertThat(runs).hasValue(1);
  }

  @Test
  void aFailedSecondRunIsRethrownWithTheRefusalSuppressed() {
    var refusal = new FlywayException(REFUSAL);
    var second = new FlywayException("Migration V001__streamrune_baseline.sql failed");

    assertThatThrownBy(
            () ->
                SeriesMigration.migrateConverging(
                    HISTORY, failingFrom(new ArrayDeque<>(List.of(refusal, second)))))
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
                SeriesMigration.migrateConverging(
                    HISTORY, failingFrom(new ArrayDeque<>(List.of(refusal, again)))))
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
