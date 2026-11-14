package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
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

  @BeforeAll
  static void initSchema() {
    dataSource = connections();
    new PostgresEventStoreFactory(dataSource, Setup.defaults().registry()).create();
  }

  /** A data source of its own over the container's database: one process's connections. */
  private static PGSimpleDataSource connections() {
    var connections = new PGSimpleDataSource();
    connections.setUrl(PG.getJdbcUrl());
    connections.setUser(PG.getUsername());
    connections.setPassword(PG.getPassword());
    return connections;
  }

  /** Empties the tables and rewinds the offset counter, as a freshly created database has it. */
  @Override
  protected EventStore newStore(Setup setup) {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("TRUNCATE event_stream, command_inbox, snapshot_store");
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
    return build(setup);
  }

  /**
   * A new store over the same database, with a data source of its own, as another process would
   * build it; nothing is emptied.
   */
  @Override
  protected EventStore reopen(EventStore store, Setup setup) {
    return build(connections(), setup);
  }

  private static EventStore build(Setup setup) {
    return build(dataSource, setup);
  }

  private static EventStore build(PGSimpleDataSource source, Setup setup) {
    return PostgresEventStore.builder()
        .dataSource(source)
        .typeRegistry(setup.registry())
        .cryptoEngine(setup.crypto())
        .upcasters(setup.upcasters())
        .migrations(setup.migrations())
        .commandInbox(new PostgresCommandInbox(source))
        .build();
  }

  @Override
  protected boolean purgeSnapshot(StreamId streamId) {
    new PostgresSnapshotStorePurger(dataSource, subject -> List.of(streamId))
        .purge(SubjectId.of("contract-subject"));
    return true;
  }

  @Test
  void aNewStoreHandsOutOffsetOneAgain() {
    store.append(A, events(A, 1, 2), Version.initial());

    EventStore fresh = newStore(Setup.defaults());

    assertEquals(
        List.of(GlobalOffset.of(1)),
        fresh.append(B, events(B, 1, 1), Version.initial()).globalOffsets(),
        "the first event of emptied storage takes offset 1, as in a freshly created database");
  }
}
