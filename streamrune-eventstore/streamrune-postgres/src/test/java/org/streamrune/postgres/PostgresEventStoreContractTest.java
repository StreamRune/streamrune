package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStore;
import org.streamrune.core.outbox.OutboxStore;
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

  static PostgresCrashPoints crashPoints;

  @BeforeAll
  static void initSchema() {
    dataSource = connections();
    new PostgresEventStoreFactory(dataSource, Setup.defaults().registry()).create();
    crashPoints = new PostgresCrashPoints(dataSource);
    crashPoints.install();
  }

  /** A data source of its own over the container's database: one process's connections. */
  private static PGSimpleDataSource connections() {
    var connections = new PGSimpleDataSource();
    connections.setUrl(PG.getJdbcUrl());
    connections.setUser(PG.getUsername());
    connections.setPassword(PG.getPassword());
    return connections;
  }

  /**
   * Empties the tables and rewinds the offset counter, as a freshly created database has it, with
   * no crash point armed.
   */
  @Override
  protected EventStore newStore(Setup setup) {
    crashPoints.disarm();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("TRUNCATE event_stream, command_inbox, snapshot_store, outbox_events");
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

  /** A store over {@code source}, whose connections die after a commit when that point is armed. */
  private static EventStore build(PGSimpleDataSource source, Setup setup) {
    DataSource connections = crashPoints.connections(source);
    return PostgresEventStore.builder()
        .dataSource(connections)
        .typeRegistry(setup.registry())
        .cryptoEngine(setup.crypto())
        .upcasters(setup.upcasters())
        .migrations(setup.migrations())
        .commandInbox(new PostgresCommandInbox(connections))
        .outboxStore(new PostgresOutboxStore(connections))
        .outboxEventMapper(OUTBOX_MAPPER)
        .build();
  }

  @Override
  protected boolean supportsCrashInjection() {
    return true;
  }

  @Override
  protected void crashAt(EventStore store, CrashPoint point) {
    crashPoints.arm(point);
  }

  @Override
  protected boolean supportsTransactionalOutbox() {
    return true;
  }

  @Override
  protected OutboxStore outboxOf(EventStore store) {
    return new PostgresOutboxStore(dataSource);
  }

  @Override
  protected boolean purgeSnapshot(StreamId streamId) {
    new PostgresSnapshotStorePurger(dataSource, subject -> List.of(streamId))
        .purge(SubjectId.of("contract-subject"));
    return true;
  }

  /**
   * The offset counter is gap-free (see the class javadoc of {@link PostgresEventStore}): the
   * reservation of an append that died before its commit is rolled back with it, and the next
   * append takes the offset right above the head.
   */
  @Test
  void theNextAppendTakesTheOffsetsOfAnAppendKilledBeforeItsCommit() {
    store.append(A, events(A, 1, 2), Version.initial());
    crashAt(store, CrashPoint.AFTER_FIRST_EVENT_WRITE);
    assertThrows(Throwable.class, () -> store.append(A, events(A, 3, 2), new Version(2)));

    EventStore restarted = reopen(store, Setup.defaults());

    assertEquals(
        List.of(GlobalOffset.of(3), GlobalOffset.of(4)),
        restarted.append(B, events(B, 1, 2), Version.initial()).globalOffsets());
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
