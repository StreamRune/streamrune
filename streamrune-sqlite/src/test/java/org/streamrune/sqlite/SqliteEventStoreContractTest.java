package org.streamrune.sqlite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStore;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.sqlite.SqliteCrashPoints.Kill;
import org.streamrune.test.EventStoreContract;

/**
 * The shared event-store contract against {@link SqliteEventStore}, built by {@link
 * SqliteEventStoreFactory} on a fresh database file per store, its snapshots purged by {@link
 * SqliteSnapshotStorePurger} and its writes killed by {@link SqliteCrashPoints}.
 *
 * <p>No case is skipped for being single-instance. The {@code …AcrossInstances} cases need a second
 * store over the same storage, not a second process: {@link #reopen} opens a second {@link
 * SqliteDatabase} handle and a data source of its own on the same file, as a second store of the
 * one process the instance guard admits does, and the two race on SQLite's write lock. Leases and
 * leadership are not part of this contract. Every store has a {@link SqliteCommandInbox} and a
 * {@link SqliteOutboxStore} with the contract's outbox mapper, built over the same handle, so the
 * keyed-append, transactional-outbox and their crash cases run too.
 */
class SqliteEventStoreContractTest extends EventStoreContract {

  @TempDir Path dir;

  private final SqliteCrashPoints crashPoints = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
  private final List<SqliteDatabase> handles = new ArrayList<>();
  private final Map<EventStore, SqliteDatabase> databases = new IdentityHashMap<>();
  private SqliteDatabase database;
  private int files;

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  /** A new database file, migrated by the factory: empty storage, the counter at 0. */
  @Override
  protected EventStore newStore(Setup setup) {
    crashPoints.disarm();
    database = open(dir.resolve("contract-" + files++ + ".db"));
    return build(database, setup);
  }

  /**
   * A new store over the same file, through a handle and a data source of its own, as a store built
   * anew would open it; nothing is emptied.
   */
  @Override
  protected EventStore reopen(EventStore store, Setup setup) {
    return build(open(database.file()), setup);
  }

  private SqliteDatabase open(Path file) {
    SqliteDatabase handle =
        SqliteDatabase.open(crashPoints.connections(TestDatabases.dataSource(file)));
    handles.add(handle);
    return handle;
  }

  private EventStore build(SqliteDatabase database, Setup setup) {
    EventStore store =
        new SqliteEventStoreFactory(database, setup.registry())
            .cryptoEngine(setup.crypto())
            .upcasters(setup.upcasters())
            .snapshotMigrations(setup.migrations())
            .commandInbox(new SqliteCommandInbox(database))
            .outboxStore(new SqliteOutboxStore(database))
            .outboxEventMapper(OUTBOX_MAPPER)
            .create();
    databases.put(store, database);
    return store;
  }

  @Override
  protected boolean supportsTransactionalOutbox() {
    return true;
  }

  /** A relay's store over the handle {@code store} was built on. */
  @Override
  protected OutboxStore outboxOf(EventStore store) {
    return new SqliteOutboxStore(databases.get(store));
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
  protected boolean purgeSnapshot(StreamId streamId) {
    new SqliteSnapshotStorePurger(database, subject -> List.of(streamId))
        .purge(SubjectId.of("contract-subject"));
    return true;
  }

  /**
   * The offset counter is gap-free: the reservation of an append that died before its commit is
   * rolled back with it, and the next append takes the offsets right above the head.
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
        "the first event of empty storage takes offset 1, as in a freshly created database");
  }
}
