package org.streamrune.postgres;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.types.EventType;
import org.streamrune.test.CommandInboxContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The shared command-inbox contract against {@code command_inbox}, written by the keyed append of a
 * {@link PostgresEventStore} over the same {@code DataSource}.
 */
@Testcontainers
class PostgresCommandInboxContractTest extends CommandInboxContract {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("command_inbox_contract");

  static PGSimpleDataSource dataSource;
  static EventTypeRegistry typeRegistry;

  private PostgresCommandInbox inbox;
  private PostgresEventStore events;

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
            return InboxEvent.class;
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
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("TRUNCATE event_stream, command_inbox");
    }
    inbox = new PostgresCommandInbox(dataSource);
    events =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .commandInbox(inbox)
            .build();
  }

  @Override
  protected CommandInbox inbox() {
    return inbox;
  }

  @Override
  protected EventStore eventStore() {
    return events;
  }
}
