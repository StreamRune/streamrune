package org.streamrune.postgres;

import java.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.types.EventType;
import org.streamrune.test.OutboxStoreContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** The shared ordering-mode contract against the real claim SQL on the V001 schema. */
@Testcontainers
class PostgresOutboxStoreContractTest extends OutboxStoreContract {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("outbox_store_contract");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    EventTypeRegistry typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Object.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Object.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return java.util.List.of();
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @Override
  protected OutboxStore newStore(OutboxOrderingMode mode) {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM outbox_events");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return new PostgresOutboxStore(dataSource, Duration.ofMinutes(4), mode);
  }

  @Override
  protected OutboxStore sameDataWithMode(OutboxStore store, OutboxOrderingMode mode) {
    // The same table, a second relay with a different mode — no DELETE here.
    return new PostgresOutboxStore(dataSource, Duration.ofMinutes(4), mode);
  }
}
