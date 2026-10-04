package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.LoggerFactory;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The Flyway on the classpath must officially support the PostgreSQL server StreamRune is tested
 * on, so a boot against that server leaves the operator's log free of Flyway warnings.
 *
 * <p>Flyway 10.22.0 knew PostgreSQL up to 17. Against the 18 the default build runs on, every
 * {@code Flyway.migrate()} logged {@code Flyway upgrade recommended: PostgreSQL 18.x is newer than
 * this version of Flyway and support has not been tested} — twice per boot when the crypto series
 * is applied too, because each series is its own Flyway run. The line was harmless, but it taught
 * operators to ignore Flyway warnings, which is the one channel that reports a half-applied or
 * foreign schema. Upgrading Flyway (to 13.9.0, which names 18 as its newest tested server) removed
 * it; this test keeps it removed — it runs on whichever server {@link PostgresTestImage} names, so
 * the {@code postgres-17} CI job covers the other end of the range.
 *
 * <p>The capture is checked against a positive control: at INFO the same Flyway loggers must
 * produce records during the run. Without that, a Flyway that logged through a channel this test
 * does not listen to would pass vacuously.
 */
@Testcontainers
class FlywayServerSupportIT {

  /** The Flyway root logger; every {@code org.flywaydb.*} logger inherits from it. */
  private static final String FLYWAY_LOGGER = "org.flywaydb";

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("flyway_support_test");

  @Test
  void initializeSchema_onTheTestedServer_logsNoFlywayWarning() {
    var dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    var cryptoEngine = PostgresCryptoEngine.builder().dataSource(dataSource).build();

    // Both Flyway runs: the event-store series and — because the engine needs the shared crypto
    // tables — the crypto series on its own history table.
    List<ILoggingEvent> flywayEvents =
        captureFlywayEvents(
            () ->
                new PostgresEventStoreFactory(dataSource, SimpleEventTypeRegistry.builder().build())
                    .cryptoEngine(cryptoEngine)
                    .initializeSchema());

    // Positive control: Flyway's records reach this capture at all (it announces each applied
    // migration run at INFO), so an empty warning list below means "nothing to warn about", not
    // "nothing was listening".
    assertTrue(
        flywayEvents.stream()
            .anyMatch(e -> e.getFormattedMessage().contains("Successfully applied")),
        "the capture must see Flyway's INFO records; saw " + messages(flywayEvents));

    List<String> warnings =
        flywayEvents.stream()
            .filter(e -> e.getLevel().isGreaterOrEqual(Level.WARN))
            .map(e -> e.getLoggerName() + " " + e.getFormattedMessage())
            .toList();
    assertEquals(
        List.of(),
        warnings,
        "Flyway must have nothing to warn about on "
            + PostgresTestImage.NAME
            + " — a 'Flyway upgrade recommended' line means the Flyway in the build predates this"
            + " PostgreSQL major version");
  }

  private static List<ILoggingEvent> captureFlywayEvents(Runnable body) {
    var logger = (Logger) LoggerFactory.getLogger(FLYWAY_LOGGER);
    Level previous = logger.getLevel();
    boolean additive = logger.isAdditive();
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    // Keep the INFO chatter this capture switches on out of the build log (the root console
    // appender).
    logger.setAdditive(false);
    try {
      body.run();
    } finally {
      logger.setAdditive(additive);
      logger.setLevel(previous);
      logger.detachAppender(appender);
      appender.stop();
    }
    return List.copyOf(appender.list);
  }

  private static List<String> messages(List<ILoggingEvent> events) {
    return events.stream().map(ILoggingEvent::getFormattedMessage).toList();
  }
}
