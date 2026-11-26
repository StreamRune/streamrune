package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.DeadLetterQueue.DeadLetterPublishRequest;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.sqlite.SqliteCrashPoints.Kill;
import org.streamrune.sqlite.SqliteCrashPoints.Point;
import org.streamrune.test.EventStoreContract;

/**
 * What {@link SqliteDeadLetterQueue} adds to the shared contract: {@code published_at} is the
 * injected clock's instant, every write is durable on return and refuses a connection that carries
 * an application's open transaction or is in manual-commit mode, every write waits for the write
 * lock at most {@code busy_timeout}, a write killed at any point leaves the queue as before or as
 * after it and its rerun converges, retention drains in batches, and the key lookup reads the
 * partial index.
 */
class SqliteDeadLetterQueueTest {

  private static final Instant T0 = Instant.parse("2026-11-23T10:00:00.123456Z");
  private static final String TYPE = "com.example.PlaceOrder";
  private static final StreamId ORDER =
      StreamId.of(AggregateType.of("order"), AggregateId.of("order-1"));
  private static final long BUSY_MILLIS = 200;
  private static final long SLACK_MILLIS = 1_500;
  private static final Instant FAR_FUTURE = Instant.parse("9999-01-01T00:00:00Z");

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();
  private final List<SingleConnection> connections = new ArrayList<>();

  @AfterEach
  void close() throws SQLException {
    handles.forEach(SqliteDatabase::close);
    for (SingleConnection connection : connections) {
      connection.close();
    }
  }

  private SqliteDatabase open(DataSource dataSource) {
    SqliteDatabase database = SqliteDatabase.open(dataSource);
    handles.add(database);
    return database;
  }

  private SqliteDatabase migrated(String file, String override) {
    SqliteDatabase database =
        open(
            override.isEmpty()
                ? TestDatabases.dataSource(dir.resolve(file))
                : TestDatabases.dataSource(dir.resolve(file), override));
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return database;
  }

  private static DeadLetterPublishRequest request(String commandId, IdempotencyKey key) {
    return new DeadLetterPublishRequest(
        "{\"amount\":42}",
        TYPE,
        CommandId.of(commandId),
        ORDER,
        "java.lang.IllegalStateException",
        "boom",
        3,
        T0,
        null,
        null,
        null,
        key);
  }

  @Test
  void rejectsANullDatabaseOrClock() {
    assertThatThrownBy(() -> new SqliteDeadLetterQueue(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("database");
    SqliteDatabase database = migrated("ctor.db", "");
    assertThatThrownBy(() -> new SqliteDeadLetterQueue(database, null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("clock");
  }

  /** {@code published_at} is the injected clock's instant, rounded to the microsecond. */
  @Test
  void publishedAtIsTheInjectedClocksInstant() {
    Instant now = Instant.parse("2030-05-06T07:08:09.123456789Z");
    var queue =
        new SqliteDeadLetterQueue(migrated("clock.db", ""), Clock.fixed(now, ZoneOffset.UTC));

    queue.publish(request("c-1", null));

    assertThat(queue.find(CommandId.of("c-1")).orElseThrow().publishedAt())
        .isEqualTo(Instant.parse("2030-05-06T07:08:09.123457Z"));
    assertThat(queue.find(CommandId.of("c-1")).orElseThrow().firstAttemptAt()).isEqualTo(T0);
  }

  /** The key lookup is served by the unique partial index, as on Postgres. */
  @Test
  void theKeyLookupReadsThePartialIndex() throws SQLException {
    SqliteDatabase database = migrated("plan.db", "");
    try (Connection conn = database.dataSource().getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "EXPLAIN QUERY PLAN SELECT command_id FROM dead_letter_queue"
                    + " WHERE idempotency_key = ? ORDER BY published_at ASC")) {
      ps.setString(1, "k1");
      StringBuilder plan = new StringBuilder();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          plan.append(rs.getString("detail")).append('\n');
        }
      }
      assertThat(plan.toString()).contains("USING INDEX idx_dlq_idempotency_key");
    }
  }

  /**
   * A write on a connection that carries a transaction the application opened is refused, not
   * joined: the application's later COMMIT commits its own write only, and the refusal leaves the
   * application's transaction open.
   */
  @Test
  void everyWriteRefusesAConnectionCarryingAnApplicationTransaction() throws SQLException {
    SqliteDatabase plain = migrated("foreign.db", "");
    var queue = new SqliteDeadLetterQueue(plain);
    queue.publish(request("stored", null));
    // One physical connection, so the queue borrows the one the application left a BEGIN on.
    SingleConnection single = new SingleConnection(TestDatabases.dataSource(plain.file()));
    connections.add(single);
    var onShared = new SqliteDeadLetterQueue(open(single));
    Map<String, Runnable> writes = new LinkedHashMap<>();
    writes.put("publish", () -> onShared.publish(request("c-1", null)));
    writes.put("discard", () -> onShared.discard(CommandId.of("stored")));
    writes.put("updateAttempts", () -> onShared.updateAttempts(CommandId.of("stored"), 7, T0));
    writes.put("deleteOlderThan", () -> onShared.deleteOlderThan(FAR_FUTURE));
    try (Connection app = single.getConnection();
        Statement st = app.createStatement()) {
      st.execute("BEGIN");
      st.execute("UPDATE dead_letter_queue SET error_message = 'app' WHERE command_id = 'stored'");
      for (var write : writes.entrySet()) {
        assertThatThrownBy(write.getValue()::run)
            .as(write.getKey())
            .isExactlyInstanceOf(EventStoreException.class)
            .rootCause()
            .hasMessageContaining("cannot start a transaction within a transaction");
      }
      st.execute("COMMIT"); // still the application's transaction: the refusal left it open
    }

    assertThat(queue.countPending()).isEqualTo(1);
    var stored = queue.find(CommandId.of("stored")).orElseThrow();
    assertThat(stored.errorMessage()).isEqualTo("app");
    assertThat(stored.dlqAttempts()).isZero();
  }

  @Test
  void aConnectionInManualCommitModeIsRefusedBeforeAnythingIsWritten() {
    SqliteDatabase database = migrated("manual.db", "");
    var source = new SwitchableDataSource(TestDatabases.dataSource(database.file()));
    var queue = new SqliteDeadLetterQueue(open(source));
    new SqliteDeadLetterQueue(database).publish(request("stored", null));
    source.manualCommit();

    List<Runnable> writes =
        List.of(
            () -> queue.publish(request("c-1", null)),
            () -> queue.discard(CommandId.of("stored")),
            () -> queue.updateAttempts(CommandId.of("stored"), 1, T0),
            () -> queue.deleteOlderThan(FAR_FUTURE));
    for (Runnable write : writes) {
      assertThatThrownBy(write::run)
          .isExactlyInstanceOf(IllegalStateException.class)
          .hasMessage(SqliteTransactions.MANUAL_COMMIT_CONNECTION);
    }
    var check = new SqliteDeadLetterQueue(database);
    assertThat(check.countPending()).isEqualTo(1);
    assertThat(check.find(CommandId.of("stored")).orElseThrow().dlqAttempts()).isZero();
  }

  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void everyWriteWaitsAtMostBusyTimeoutForTheWriteLock_andReadsDoNotWait() throws Exception {
    SqliteDatabase database = migrated("busy.db", "busy_timeout=" + BUSY_MILLIS);
    var queue = new SqliteDeadLetterQueue(database);
    queue.publish(request("stored", IdempotencyKey.of("k1")));
    Map<String, Runnable> writes = new LinkedHashMap<>();
    writes.put("publish", () -> queue.publish(request("c-1", null)));
    writes.put("discard", () -> queue.discard(CommandId.of("stored")));
    writes.put("updateAttempts", () -> queue.updateAttempts(CommandId.of("stored"), 1, T0));
    writes.put("deleteOlderThan", () -> queue.deleteOlderThan(FAR_FUTURE));

    try (Connection holder = TestDatabases.dataSource(database.file()).getConnection();
        Statement st = holder.createStatement()) {
      st.execute("BEGIN IMMEDIATE");
      for (var write : writes.entrySet()) {
        long started = System.nanoTime();
        assertThatThrownBy(write.getValue()::run)
            .as(write.getKey())
            .isExactlyInstanceOf(EventStoreException.class)
            .hasMessageContaining("busy_timeout");
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))
            .as(write.getKey() + " waited")
            .isLessThan(BUSY_MILLIS + SLACK_MILLIS);
      }
      assertThat(queue.read(10)).hasSize(1);
      assertThat(queue.find(CommandId.of("stored"))).isPresent();
      assertThat(queue.findByIdempotencyKey(IdempotencyKey.of("k1"))).hasSize(1);
      assertThat(queue.readRetryable(3, 10, true, null)).hasSize(1);
      assertThat(queue.countPending()).isEqualTo(1);
      assertThat(queue.countAwaitingOperator(3, true, null)).isZero();
      st.execute("ROLLBACK");
    }

    assertThat(queue.read(10)).as("nothing was written").hasSize(1);
    assertThat(queue.find(CommandId.of("stored")).orElseThrow().dlqAttempts()).isZero();
  }

  /**
   * A publish killed inside its row write leaves nothing; killed right after its COMMIT, the entry.
   * A keyed rerun then stores nothing and returns normally; an unkeyed rerun is refused, and the
   * caller finds the stored entry by its command id.
   */
  @Test
  void aPublishKilledAtAnyPointLeavesNothingOrTheEntry_andItsRerunConverges() {
    Path file = dir.resolve("crash-publish.db");
    SqliteDatabase plain = migrated("crash-publish.db", "");
    var check = new SqliteDeadLetterQueue(plain);
    SqliteCrashPoints crash = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
    var queue = new SqliteDeadLetterQueue(open(crash.connections(TestDatabases.dataSource(file))));

    for (IdempotencyKey key : new IdempotencyKey[] {IdempotencyKey.of("k1"), null}) {
      String id = key == null ? "unkeyed" : "keyed";
      crash.arm(new Point("dead_letter_queue", 1));
      assertThatThrownBy(() -> queue.publish(request(id, key)))
          .isExactlyInstanceOf(EventStoreException.class);
      assertThat(check.find(CommandId.of(id))).as(id + " killed inside its row").isEmpty();

      crash.arm(Point.AFTER_COMMIT);
      assertThatThrownBy(() -> queue.publish(request(id, key)))
          .isExactlyInstanceOf(EventStoreException.class);
      assertThat(check.find(CommandId.of(id))).as(id + " killed after its COMMIT").isPresent();
    }

    queue.publish(request("keyed", IdempotencyKey.of("k1")));
    assertThat(check.findByIdempotencyKey(IdempotencyKey.of("k1"))).hasSize(1);
    assertThatThrownBy(() -> queue.publish(request("unkeyed", null)))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(check.countPending()).isEqualTo(2);
  }

  /**
   * The retry runner's attempt update, a discard and a retention batch killed inside their row
   * write change nothing; killed after their COMMIT they are done, and their rerun changes nothing
   * more.
   */
  @Test
  void anUpdateDiscardOrRetentionKilledAtAnyPointConverges() {
    Path file = dir.resolve("crash-writes.db");
    SqliteDatabase plain = migrated("crash-writes.db", "");
    var check = new SqliteDeadLetterQueue(plain);
    SqliteCrashPoints crash = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
    var queue = new SqliteDeadLetterQueue(open(crash.connections(TestDatabases.dataSource(file))));
    check.publish(request("c-1", null));
    check.publish(request("c-2", null));
    check.publish(request("c-3", null));
    Instant at = T0.plusSeconds(5);

    crash.arm(new Point("dead_letter_queue", 1));
    assertThatThrownBy(() -> queue.updateAttempts(CommandId.of("c-1"), 1, at))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(check.find(CommandId.of("c-1")).orElseThrow().dlqAttempts()).isZero();
    crash.arm(Point.AFTER_COMMIT);
    assertThatThrownBy(() -> queue.updateAttempts(CommandId.of("c-1"), 1, at))
        .isExactlyInstanceOf(EventStoreException.class);
    queue.updateAttempts(CommandId.of("c-1"), 1, at);
    assertThat(check.find(CommandId.of("c-1")).orElseThrow().dlqAttempts()).isEqualTo(1);
    assertThat(check.find(CommandId.of("c-1")).orElseThrow().lastAttemptAt()).isEqualTo(at);

    crash.arm(new Point("dead_letter_queue", 1));
    assertThatThrownBy(() -> queue.discard(CommandId.of("c-1")))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(check.find(CommandId.of("c-1"))).isPresent();
    crash.arm(Point.AFTER_COMMIT);
    assertThatThrownBy(() -> queue.discard(CommandId.of("c-1")))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(check.find(CommandId.of("c-1"))).isEmpty();
    queue.discard(CommandId.of("c-1"));
    assertThat(check.countPending()).isEqualTo(2);

    Instant later = FAR_FUTURE;
    crash.arm(new Point("dead_letter_queue", 2));
    assertThatThrownBy(() -> queue.deleteOlderThan(later))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(check.countPending()).as("the batch killed in its second row").isEqualTo(2);
    assertThat(queue.deleteOlderThan(later)).isEqualTo(2);
    assertThat(check.countPending()).isZero();
  }

  /** Retention deletes in batches, oldest first, until a batch deletes nothing. */
  @Test
  void retentionDrainsEveryOldEntryInBatches_andKeepsTheNewerOnes() throws Exception {
    SqliteDatabase database = migrated("retention.db", "");
    var queue = new SqliteDeadLetterQueue(database);
    int old = SqliteDeadLetterQueue.DELETE_BATCH_SIZE * 2 + 500;
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement();
        PreparedStatement ps =
            conn.prepareStatement(
                "INSERT INTO dead_letter_queue (command_id, command_type, payload, error_type,"
                    + " attempts, first_attempt_at, published_at) VALUES (?, 'T', '{}', 'E', 0,"
                    + " ?, ?)")) {
      st.execute("BEGIN IMMEDIATE");
      for (int i = 0; i < old; i++) {
        ps.setString(1, "old-" + i);
        ps.setLong(2, SqliteTimestamps.toMicros(T0));
        ps.setLong(3, SqliteTimestamps.toMicros(T0.plusNanos(i * 1_000L)));
        ps.addBatch();
      }
      ps.executeBatch();
      st.execute("COMMIT");
    }
    new SqliteDeadLetterQueue(database, Clock.fixed(T0.plusSeconds(3_600), ZoneOffset.UTC))
        .publish(request("new", null));

    assertThat(queue.deleteOlderThan(T0.plusSeconds(60))).isEqualTo(old);
    assertThat(queue.read(10))
        .singleElement()
        .extracting(e -> e.commandId().value())
        .isEqualTo("new");
  }

  @Test
  void aStorageFailureIsAnEventStoreExceptionForEveryReadAndWrite() throws SQLException {
    SqliteDatabase database = migrated("missing.db", "");
    var source = new SwitchableDataSource(TestDatabases.dataSource(database.file()));
    var queue = new SqliteDeadLetterQueue(open(source));
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute("DROP TABLE dead_letter_queue");
    }
    List<Runnable> calls =
        List.of(
            () -> queue.publish(request("c-1", null)),
            () -> queue.read(1),
            () -> queue.find(CommandId.of("c-1")),
            () -> queue.findByIdempotencyKey(IdempotencyKey.of("k1")),
            () -> queue.discard(CommandId.of("c-1")),
            () -> queue.updateAttempts(CommandId.of("c-1"), 1, T0),
            () -> queue.readRetryable(3, 1, true, T0),
            () -> queue.countPending(),
            () -> queue.countAwaitingOperator(3, true, T0),
            () -> queue.deleteOlderThan(T0));
    for (Runnable call : calls) {
      assertThatThrownBy(call::run)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasCauseInstanceOf(SQLException.class);
    }

    source.failWith(() -> new IllegalStateException("pool closed"));
    for (Runnable call : calls) {
      assertThatThrownBy(call::run)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasCauseInstanceOf(IllegalStateException.class);
    }
  }

  @Test
  void aPayloadThatIsNotJsonIsAStorageFailure() {
    var queue = new SqliteDeadLetterQueue(migrated("json.db", ""));

    assertThatThrownBy(
            () ->
                queue.publish(
                    new DeadLetterPublishRequest(
                        "not json",
                        TYPE,
                        CommandId.of("c-1"),
                        null,
                        "E",
                        null,
                        0,
                        T0,
                        null,
                        null,
                        null,
                        null)))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("c-1");
    assertThat(queue.countPending()).isZero();
  }
}
