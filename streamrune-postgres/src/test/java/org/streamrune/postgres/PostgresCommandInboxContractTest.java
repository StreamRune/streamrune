package org.streamrune.postgres;

import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.test.CommandInboxContract;
import org.streamrune.test.EventStoreContract.CrashPoint;
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
  static PostgresCrashPoints crashPoints;

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
    crashPoints = new PostgresCrashPoints(dataSource);
    crashPoints.install();
  }

  @Override
  protected Inboxed newStore() {
    crashPoints.disarm();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("TRUNCATE event_stream, command_inbox");
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
    return build();
  }

  /** A pair over the database, whose connections die after a commit when that point is armed. */
  private static Inboxed build() {
    DataSource connections = crashPoints.connections(dataSource);
    var inbox = new PostgresCommandInbox(connections);
    var events =
        PostgresEventStore.builder()
            .dataSource(connections)
            .typeRegistry(typeRegistry)
            .commandInbox(inbox)
            .build();
    return new Inboxed(events, inbox);
  }

  @Override
  protected boolean supportsCrashInjection() {
    return true;
  }

  @Override
  protected void crashAt(Inboxed inboxed, CrashPoint point) {
    crashPoints.arm(point);
  }

  /** A new pair over the same database; nothing is emptied. */
  @Override
  protected Inboxed reopen(Inboxed inboxed) {
    return build();
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
