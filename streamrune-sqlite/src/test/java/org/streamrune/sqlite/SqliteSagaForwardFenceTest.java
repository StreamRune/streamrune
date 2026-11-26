package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStore;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.sqlite.SqliteCrashPoints.Kill;
import org.streamrune.test.EventStoreContract.CrashPoint;
import org.streamrune.test.SagaForwardFenceContract;

/**
 * The forward fence of {@link SqliteEventStore} against the {@code saga_state} rows of a {@link
 * SqliteSagaStore} over the same database file. There is no fence lock: the database-wide write
 * lock that every write holds from its {@code BEGIN IMMEDIATE} to its commit is the fence lock, and
 * the fenced append reads the saga's row inside its own transaction. The event store writes outbox
 * rows in that transaction, so a refusal is shown to leave none either; claims racing fenced
 * appends show that every admitted command committed before the claim.
 */
class SqliteSagaForwardFenceTest extends SagaForwardFenceContract {

  /** Every contract event gets one outbox row, so a refused append would show one. */
  private static final OutboxEventMapper EVERY_EVENT =
      envelope ->
          List.of(
              OutboxEntry.pending(
                  OutboxEntryId.of(envelope.metadata().eventId().value()),
                  "{}",
                  envelope.eventType().value(),
                  envelope.streamId()));

  @TempDir Path dir;

  private final SqliteCrashPoints crashPoints = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
  private final List<SqliteDatabase> handles = new ArrayList<>();
  private Path file;
  private EventStore events;
  private SagaStore rows;

  @BeforeEach
  void freshStores() {
    file = dir.resolve("fence.db");
    SqliteDatabase database = open();
    events = events(database, Clock.systemUTC());
    rows = sagas(database, Clock.systemUTC());
  }

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  private SqliteDatabase open() {
    SqliteDatabase database =
        SqliteDatabase.open(crashPoints.connections(TestDatabases.dataSource(file)));
    handles.add(database);
    return database;
  }

  private static EventStore events(SqliteDatabase database, Clock clock) {
    return new SqliteEventStoreFactory(
            database,
            SimpleEventTypeRegistry.builder().registerEvent(EVENT_TYPE, FenceEvent.class).build())
        .clock(clock)
        .commandInbox(new SqliteCommandInbox(database, clock))
        .outboxStore(new SqliteOutboxStore(database))
        .outboxEventMapper(EVERY_EVENT)
        .create();
  }

  private static SagaStore sagas(SqliteDatabase database, Clock clock) {
    return new SqliteSagaStore(
        database, new ObjectMapper().registerModule(new JavaTimeModule()), clock);
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
  void aRefusedCommandWritesNoInboxOrOutboxRow() throws Exception {
    stampedRow(ID);

    assertThatThrownBy(() -> append(TARGET, IdempotencyKey.of("fwd-refused"), FORWARD_OF))
        .isExactlyInstanceOf(SagaForwardFencedException.class);

    assertThat(count("event_stream")).isZero();
    assertThat(count("command_inbox")).isZero();
    assertThat(count("outbox_events")).isZero();
    assertThat(count("global_offset_counter WHERE next_value > 0")).isZero();

    append(TARGET, IdempotencyKey.of("unfenced"), null);
    assertThat(count("outbox_events")).as("the same append unfenced writes its outbox row").isOne();
    assertThat(count("command_inbox")).isOne();
  }

  /** The storage identity is the database file, whatever handle either store was built on. */
  @Test
  void theIdentityIsTheDatabaseFile() {
    SqliteDatabase second = open();

    assertThat(events.sagaFenceStorageIdentity()).isEqualTo(handles.getFirst().file());
    assertThat(events(second, Clock.systemUTC()).sagaFenceStorageIdentity())
        .isEqualTo(sagas(second, Clock.systemUTC()).storageIdentity())
        .isEqualTo(events.sagaFenceStorageIdentity());
  }

  /**
   * A fenced append killed after its claim, its fence read and its offset reservation, before its
   * commit, leaves nothing; its rerun is fenced again and stores once. Killed after its commit, it
   * is stored whole and its rerun is answered from the inbox even after the claim, without reading
   * the fence.
   */
  @Test
  void aKilledFencedAppendLeavesNothingAndItsRerunConverges() throws Exception {
    forwardRow(ID);
    IdempotencyKey key = IdempotencyKey.of("fwd-killed");

    crashPoints.arm(CrashPoint.AFTER_OFFSET_RESERVATION);
    assertThatThrownBy(() -> append(TARGET, key, FORWARD_OF)).isInstanceOf(RuntimeException.class);
    crashPoints.disarm();
    assertThat(count("event_stream")).isZero();
    assertThat(count("command_inbox")).isZero();
    assertThat(count("outbox_events")).isZero();

    var rerun = append(TARGET, key, FORWARD_OF);
    assertThat(rerun.alreadyApplied()).isFalse();
    assertThat(stored(TARGET)).hasSize(1);

    IdempotencyKey durable = IdempotencyKey.of("fwd-durable");
    crashPoints.arm(CrashPoint.AFTER_COMMIT);
    assertThatThrownBy(() -> append(TARGET, durable, FORWARD_OF))
        .isInstanceOf(RuntimeException.class);
    crashPoints.disarm();
    assertThat(stored(TARGET)).as("durable whole").hasSize(2);
    sagaStore().claimCompensating(ID, TYPE, new FenceState(SagaStatus.COMPENSATING), 1L);

    var replay =
        eventStore()
            .appendWithKey(
                TARGET,
                List.of(event(TARGET, 2)),
                new Version(1),
                durable,
                COMMAND_TYPE,
                FORWARD_OF);
    assertThat(replay.alreadyApplied()).isTrue();
    assertThat(stored(TARGET)).hasSize(2);
  }

  /**
   * Saga claims race fenced appends of the same saga, round after round: six appenders append one
   * forward command after another until the fence refuses one, and the claim lands while they are
   * in flight. Every write reads the one logical clock inside its own transaction, under the write
   * lock, so the clock orders them as the lock did: every admitted forward command committed before
   * the claim stamped the episode, every command after it was refused, and a refused command stored
   * nothing and left its key unclaimed.
   */
  @Test
  @Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void sagaClaimsRacingFencedAppendsAdmitOnlyCommandsThatCommitBeforeTheClaim() throws Exception {
    Clock ticks = new TickClock();
    int appenders = 6;
    ExecutorService pool = Executors.newFixedThreadPool(appenders + 1);
    try {
      for (int round = 0; round < 8; round++) {
        SagaId sagaId = SagaId.of("raced-" + round);
        SagaOrigin origin = forwardOf(sagaId);
        SagaStore claimer = sagas(open(), ticks);
        claimer.create(sagaId, TYPE, new FenceState(SagaStatus.RUNNING), SagaStatus.RUNNING);
        CyclicBarrier start = new CyclicBarrier(appenders + 1);
        ConcurrentLinkedQueue<String> admitted = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> refused = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Throwable> unexpected = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int a = 0; a < appenders; a++) {
          String appenderId = "r" + round + "-a" + a;
          EventStore appender = events(open(), ticks);
          futures.add(
              pool.submit(
                  () -> {
                    start.await();
                    // Appends one forward command after another until the fence refuses one.
                    for (int n = 0; n < 2_000; n++) {
                      String id = appenderId + "-" + n;
                      StreamId target =
                          StreamId.of(AggregateType.of("fence_target"), AggregateId.of(id));
                      try {
                        appender.appendWithKey(
                            target,
                            List.of(event(target, 1)),
                            Version.initial(),
                            IdempotencyKey.of(id),
                            COMMAND_TYPE,
                            origin);
                        admitted.add(id);
                      } catch (SagaForwardFencedException e) {
                        refused.add(id);
                        return null;
                      } catch (Throwable t) {
                        unexpected.add(t);
                        return null;
                      }
                    }
                    return null;
                  }));
        }
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  // Claims while the appenders are in flight: after the first command is admitted.
                  while (admitted.isEmpty() && unexpected.isEmpty()) {
                    Thread.onSpinWait();
                  }
                  claimer.claimCompensating(
                      sagaId, TYPE, new FenceState(SagaStatus.COMPENSATING), 1L);
                  return null;
                }));
        for (Future<?> future : futures) {
          future.get();
        }

        assertThat(unexpected).as("round " + round).isEmpty();
        assertThat(admitted).as("commands admitted before the claim").isNotEmpty();
        assertThat(refused).as("each appender ends on a refusal").hasSize(appenders);
        long claimedAt =
            scalar("SELECT episode_claimed_at FROM saga_state WHERE saga_id = ?", sagaId.value());
        for (String id : admitted) {
          assertThat(scalar("SELECT created_at FROM event_stream WHERE aggregate_id = ?", id))
              .as(id + " committed before the claim")
              .isLessThan(claimedAt);
          assertThat(scalar("SELECT count(*) FROM command_inbox WHERE idempotency_key = ?", id))
              .isOne();
        }
        for (String id : refused) {
          assertThat(scalar("SELECT count(*) FROM event_stream WHERE aggregate_id = ?", id))
              .as(id + " refused, stored nothing")
              .isZero();
          assertThat(scalar("SELECT count(*) FROM command_inbox WHERE idempotency_key = ?", id))
              .as(id + " left its key unclaimed")
              .isZero();
        }
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /** A clock that moves one microsecond each time it is read: the order of the reads. */
  private static final class TickClock extends Clock {
    private final AtomicLong ticks = new AtomicLong();

    @Override
    public Instant instant() {
      return Instant.EPOCH.plusNanos(ticks.incrementAndGet() * 1_000);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }
  }

  private long count(String tableAndFilter) throws SQLException {
    try (Connection conn = TestDatabases.dataSource(file).getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT count(*) FROM " + tableAndFilter)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private long scalar(String sql, String argument) throws SQLException {
    try (Connection conn = TestDatabases.dataSource(file).getConnection();
        var ps = conn.prepareStatement(sql)) {
      ps.setString(1, argument);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }
}
