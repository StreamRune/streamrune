package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.test.SagaForwardFenceContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The forward fence of {@link PostgresEventStore} against the {@code saga_state} rows of a {@link
 * PostgresSagaStore} over the same {@code DataSource}. The event store writes outbox and audit rows
 * in its append transaction, so a refusal is shown to leave none of them either.
 */
@Testcontainers
class PostgresSagaForwardFenceTest extends SagaForwardFenceContract {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("saga_forward_fence");

  static PGSimpleDataSource dataSource;
  static EventTypeRegistry typeRegistry;

  /** Every contract event gets one outbox row, so a refused append would show one. */
  private static final OutboxEventMapper EVERY_EVENT =
      envelope ->
          List.of(
              OutboxEntry.pending(
                  OutboxEntryId.of(envelope.metadata().eventId().value()),
                  "{}",
                  envelope.eventType().name(),
                  envelope.streamId()));

  private PostgresEventStore events;
  private PostgresSagaStore rows;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return FenceEvent.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Object.class;
          }

          @Override
          public Collection<Class<?>> registeredTypes() {
            return List.of();
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void freshStores() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "TRUNCATE event_stream, command_inbox, outbox_events, event_audit_log, saga_state");
    }
    rows =
        new PostgresSagaStore(dataSource, new ObjectMapper().registerModule(new JavaTimeModule()));
    events =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .commandInbox(new PostgresCommandInbox(dataSource))
            .eventAuditStore(new PostgresEventAuditStore(dataSource))
            .outboxStore(new PostgresOutboxStore(dataSource))
            .outboxEventMapper(EVERY_EVENT)
            .build();
  }

  @Override
  protected EventStore eventStore() {
    return events;
  }

  @Override
  protected SagaStore sagaStore() {
    return rows;
  }

  @Test
  void aRefusedCommandWritesNoInboxOutboxOrAuditRow() throws SQLException {
    stampedRow(ID);

    assertThrows(
        SagaForwardFencedException.class,
        () -> append(TARGET, IdempotencyKey.of("fwd-refused"), FORWARD_OF));

    assertEquals(0, count("event_stream"));
    assertEquals(0, count("command_inbox"));
    assertEquals(0, count("outbox_events"));
    assertEquals(0, count("event_audit_log"));

    append(TARGET, IdempotencyKey.of("unfenced"), null);
    assertEquals(1, count("outbox_events"), "the same append unfenced writes its outbox row");
    assertEquals(1, count("event_audit_log"), "and its audit row");
    assertEquals(1, count("command_inbox"));
  }

  private static long count(String table) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM " + table)) {
      rs.next();
      return rs.getLong(1);
    }
  }
}
