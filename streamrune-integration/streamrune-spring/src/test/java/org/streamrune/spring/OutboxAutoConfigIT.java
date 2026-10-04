package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.postgres.PostgresEventStoreFactory;
import org.streamrune.postgres.PostgresOutboxStore;
import org.streamrune.runtime.OutboxPoller;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration test that verifies the auto-configuration wires {@link OutboxEventMapper} and {@link
 * PostgresOutboxStore} into the event store factory so that appending an event causes an outbox row
 * to be written in the same transaction.
 */
@Testcontainers
class OutboxAutoConfigIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_outbox_autoconfig_it");

  record OrderPlaced(String orderId) implements DomainEvent {}

  static PGSimpleDataSource rawDataSource;
  static EventTypeRegistry typeRegistry;

  @BeforeAll
  static void initSchema() {
    rawDataSource = new PGSimpleDataSource();
    rawDataSource.setUrl(PG.getJdbcUrl());
    rawDataSource.setUser(PG.getUsername());
    rawDataSource.setPassword(PG.getPassword());

    typeRegistry =
        SimpleEventTypeRegistry.builder().registerEvent("OrderPlaced", OrderPlaced.class).build();

    // Run Flyway migrations (includes outbox_events table)
    new PostgresEventStoreFactory(rawDataSource, typeRegistry).create();
  }

  @Test
  void appendingEventWritesOutboxRowWhenMapperAndStoreWiredViaAutoConfig() {
    var streamId =
        StreamId.of(AggregateType.of("order"), AggregateId.of("order-" + UUID.randomUUID()));
    var outboxStore = new PostgresOutboxStore(rawDataSource);

    // An OutboxEventMapper that maps every event to exactly one OutboxEntry
    OutboxEventMapper mapper =
        envelope ->
            List.of(
                OutboxEntry.pending(
                    OutboxEntryId.of(UUID.randomUUID().toString()),
                    "{\"orderId\":\"" + ((OrderPlaced) envelope.event()).orderId() + "\"}",
                    "OrderPlaced",
                    envelope.streamId()));

    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(DataSource.class, () -> rawDataSource)
        .withBean(EventTypeRegistry.class, () -> typeRegistry)
        .withBean(PostgresOutboxStore.class, () -> outboxStore)
        .withBean(OutboxEventMapper.class, () -> mapper)
        .withPropertyValues("streamrune.outbox.enabled=false") // we don't need poller, just append
        .run(
            ctx -> {
              EventStore store = ctx.getBean(EventStore.class);
              store.append(
                  streamId,
                  List.of(envelope(streamId, new OrderPlaced("ord-1"))),
                  Version.initial());

              // Verify an outbox row was written for this stream
              int count = countOutboxRows(streamId);
              assertThat(count)
                  .as("expected one outbox row written transactionally with the event append")
                  .isEqualTo(1);
            });
  }

  @Test
  void pollerLogsTheStoresOrderingModeAtStart_andTheSkippedRetentionKnobBinds() {
    var strict =
        new PostgresOutboxStore(
            rawDataSource,
            java.time.Duration.ofMinutes(2),
            OutboxOrderingMode.STRICT_PER_AGGREGATE);
    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(OutboxPoller.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> rawDataSource)
          .withBean(EventTypeRegistry.class, () -> typeRegistry)
          .withBean(PostgresOutboxStore.class, () -> strict)
          .withBean(OutboxPublisher.class, () -> (OutboxPublisher) entry -> {})
          .withPropertyValues(
              "streamrune.outbox.enabled=true",
              "streamrune.outbox.flush-interval-ms=3600000",
              "streamrune.outbox.skipped-retention-max-age=45d")
          .run(
              ctx -> {
                assertThat(
                        ctx.getBean(StreamRuneProperties.class).outbox().skippedRetentionMaxAge())
                    .isEqualTo(java.time.Duration.ofDays(45));
                assertThat(appender.list)
                    .anySatisfy(
                        e ->
                            assertThat(e.getFormattedMessage())
                                .startsWith(
                                    "Outbox relay started: orderingMode=STRICT_PER_AGGREGATE"));
              });
    } finally {
      logger.detachAppender(appender);
    }
  }

  private static int countOutboxRows(StreamId streamId) throws Exception {
    try (var conn = rawDataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_type = ? AND aggregate_id = ?")) {
      ps.setString(1, streamId.aggregateType().value());
      ps.setString(2, streamId.aggregateId().value());
      try (var rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        return rs.getInt(1);
      }
    }
  }

  private static EventEnvelope envelope(StreamId streamId, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        new Version(1),
        new EventType("OrderPlaced"),
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("corr-outbox-it"),
            null,
            null,
            Instant.now()));
  }
}
