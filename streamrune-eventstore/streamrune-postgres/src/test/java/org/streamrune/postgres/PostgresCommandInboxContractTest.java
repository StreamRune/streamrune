package org.streamrune.postgres;

import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.IdempotencyKey;
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

  @Override
  protected Inboxed newStore() {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("TRUNCATE event_stream, command_inbox");
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
    var inbox = new PostgresCommandInbox(dataSource);
    var events =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .commandInbox(inbox)
            .build();
    return new Inboxed(events, inbox);
  }

  @Override
  protected void setProcessedAt(Inboxed inboxed, IdempotencyKey key, Instant processedAt) {
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE command_inbox SET processed_at = ? WHERE idempotency_key = ?")) {
      stmt.setObject(1, OffsetDateTime.ofInstant(processedAt, ZoneOffset.UTC));
      stmt.setString(2, key.value());
      if (stmt.executeUpdate() != 1) {
        throw new IllegalStateException("no row is recorded under " + key);
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }
}
