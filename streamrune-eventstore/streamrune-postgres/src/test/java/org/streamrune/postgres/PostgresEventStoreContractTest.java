package org.streamrune.postgres;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.test.EventStoreContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The shared event-store contract against {@link PostgresEventStore} on the shipped baseline, with
 * a command inbox, and its snapshots purged by {@link PostgresSnapshotStorePurger}.
 */
@Testcontainers
class PostgresEventStoreContractTest extends EventStoreContract {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("event_store_contract");

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
            if (EVENT_TYPE.equals(eventType)) {
              return ContractEvent.class;
            }
            throw new UnknownEventTypeException(
                "event type", eventType.value(), List.of(EVENT_TYPE.value()));
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            if (ContractState.class.getSimpleName().equals(stateType)) {
              return ContractState.class;
            }
            throw new UnknownEventTypeException(
                "state type", stateType, List.of(ContractState.class.getSimpleName()));
          }

          @Override
          public Collection<Class<?>> registeredTypes() {
            return List.of(ContractEvent.class, ContractState.class);
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @Override
  protected EventStore newStore() {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("TRUNCATE event_stream, command_inbox, snapshot_store");
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
    return PostgresEventStore.builder()
        .dataSource(dataSource)
        .typeRegistry(typeRegistry)
        .commandInbox(new PostgresCommandInbox(dataSource))
        .build();
  }

  @Override
  protected boolean purgeSnapshot(StreamId streamId) {
    new PostgresSnapshotStorePurger(dataSource, subject -> List.of(streamId))
        .purge(SubjectId.of("contract-subject"));
    return true;
  }
}
