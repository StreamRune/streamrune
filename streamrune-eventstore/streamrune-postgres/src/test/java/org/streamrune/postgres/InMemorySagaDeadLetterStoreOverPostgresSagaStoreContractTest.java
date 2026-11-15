package org.streamrune.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeAll;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.EventType;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.SagaDeadLetterStoreContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The dead-letter contract for the test kit's {@link InMemorySagaDeadLetterStore} bound to a {@link
 * PostgresSagaStore}, a pairing the {@code SagaDeadLetterStore} wiring rule names as correct: the
 * dead-letter store reads and writes the saga rows the runner writes, through a saga store that
 * keeps the state as JSON and binds it to the class it is asked for. Every read of the row the
 * dead-letter store makes — the hold's applied check, the clear, retention and the backlog count —
 * is of the row's facts, never of its state.
 */
@Testcontainers
class InMemorySagaDeadLetterStoreOverPostgresSagaStoreContractTest
    extends SagaDeadLetterStoreContract {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("saga_dlq_pairing");

  static PGSimpleDataSource dataSource;
  static ObjectMapper objectMapper;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
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
  protected SagaStore newSagaStore() {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM saga_dead_letters");
      stmt.execute("DELETE FROM saga_state");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return new PostgresSagaStore(dataSource, objectMapper);
  }

  @Override
  protected SagaDeadLetterStore newStore(SagaStore sagaStore) {
    return new InMemorySagaDeadLetterStore(sagaStore);
  }
}
