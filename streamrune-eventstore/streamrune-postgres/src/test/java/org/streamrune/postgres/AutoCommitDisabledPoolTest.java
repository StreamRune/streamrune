package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Every Postgres store must make its writes durable on a pool configured with {@code
 * auto-commit=false} (the usual Hibernate tuning, {@code
 * spring.datasource.hikari.auto-commit=false}). On such a pool a connection opens a transaction
 * implicitly on its first statement, and a store that assumes {@code autoCommit=true} never commits
 * it: HikariCP rolls the connection back on return, so the write is lost silently. Each test here
 * writes through a store built on an {@code autoCommit=false} HikariCP pool and reads the result
 * back through a separate plain connection, so an uncommitted write shows up as a missing row,
 * never as a false pass.
 */
@Testcontainers
@Timeout(value = 60, unit = TimeUnit.SECONDS) // an uncommitted retention DELETE is re-run forever
class AutoCommitDisabledPoolTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_autocommit_off_test");

  record SnapshotState(String value) implements AggregateState {}

  static final class TestSagaState implements SagaState {
    @JsonProperty("orderId")
    private final String orderId;

    @JsonCreator
    TestSagaState(@JsonProperty("orderId") String orderId) {
      this.orderId = orderId;
    }

    @Override
    public SagaStatus status() {
      return SagaStatus.RUNNING;
    }

    public String orderId() {
      return orderId;
    }
  }

  /** The schema owner and the verification path: a plain, autoCommit=true connection per call. */
  static PGSimpleDataSource plain;

  /** The pool under test: HikariCP with {@code autoCommit=false}. */
  static HikariDataSource pool;

  static EventTypeRegistry typeRegistry;
  static ObjectMapper objectMapper;

  @BeforeAll
  static void init() {
    plain = new PGSimpleDataSource();
    plain.setUrl(PG.getJdbcUrl());
    plain.setUser(PG.getUsername());
    plain.setPassword(PG.getPassword());

    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Object.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return SnapshotState.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return List.of(SnapshotState.class);
          }
        };
    new PostgresEventStoreFactory(plain, typeRegistry).create();

    var config = new HikariConfig();
    config.setJdbcUrl(PG.getJdbcUrl());
    config.setUsername(PG.getUsername());
    config.setPassword(PG.getPassword());
    config.setAutoCommit(false);
    config.setMaximumPoolSize(4);
    config.setPoolName("streamrune-autocommit-off-under-test");
    pool = new HikariDataSource(config);

    objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
  }

  @AfterAll
  static void closePool() {
    pool.close();
  }

  @BeforeEach
  void cleanTables() throws Exception {
    try (var conn = plain.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "DELETE FROM outbox_events; DELETE FROM projection_offset; DELETE FROM dead_letter_queue;"
              + " DELETE FROM command_inbox; DELETE FROM audit_log; DELETE FROM event_audit_log;"
              + " DELETE FROM snapshot_store; DELETE FROM saga_state; DELETE FROM saga_dead_letters;"
              + " DELETE FROM subscription_leases; DELETE FROM projection_dead_letters");
    }
  }

  @Test
  void poolUnderTestHandsOutManualCommitConnections() throws Exception {
    try (var conn = pool.getConnection()) {
      assertFalse(conn.getAutoCommit(), "the fixture must exercise autoCommit=false");
    }
  }

  // --- outbox -------------------------------------------------------------------------------

  @Test
  void outboxStore_saveClaimDeliverAndSweepAreDurable() {
    var store = outboxStore();
    var id = OutboxEntryId.of("obx-happy-1");

    store.save(OutboxEntry.pending(id, "{}", "OrderPlaced", TestStreams.stream("agg-1")));
    assertEquals("PENDING", outboxStatus(id));

    List<OutboxEntry> claimed = store.loadPending(10);
    assertEquals(1, claimed.size());
    assertEquals("IN_PROGRESS", outboxStatus(id));

    assertTrue(store.markDelivered(id, store.claimedBy()));
    assertEquals("DELIVERED", outboxStatus(id));

    assertEquals(1, store.deleteDelivered(Instant.now().plus(1, ChronoUnit.HOURS)));
    assertEquals(0, count("SELECT COUNT(*) FROM outbox_events"));
  }

  @Test
  void outboxStore_failureRetryAndOperatorPathsAreDurable() {
    var store = outboxStore();
    var id = OutboxEntryId.of("obx-fail-1");

    store.save(OutboxEntry.pending(id, "{}", "OrderPlaced", TestStreams.stream("agg-2")));
    assertEquals(1, store.loadPending(10).size());
    assertTrue(store.markFailed(id, 1, "broker down", store.claimedBy()));
    assertEquals("FAILED", outboxStatus(id));

    assertTrue(store.resetFailedToPending(id));
    assertEquals("PENDING", outboxStatus(id));

    assertEquals(1, store.loadPending(10).size());
    assertTrue(store.markRetry(id, 2, "broker still down", Duration.ofHours(1), store.claimedBy()));
    assertEquals("PENDING", outboxStatus(id));
    assertEquals(2, count("SELECT attempts FROM outbox_events WHERE entry_id = ?", id.value()));

    var skipped = OutboxEntryId.of("obx-skip-1");
    store.save(OutboxEntry.pending(skipped, "{}", "OrderPlaced", TestStreams.stream("agg-3")));
    assertEquals(1, store.loadPending(10).size()); // agg-2 is backed off; only agg-3 is claimable
    assertTrue(store.markFailed(skipped, 1, "poison", store.claimedBy()));
    assertTrue(store.skipFailed(skipped, "operator", "poison payload"));
    assertEquals("SKIPPED", outboxStatus(skipped));
    assertEquals(1, store.deleteSkipped(Instant.now().plus(1, ChronoUnit.HOURS)));
    assertEquals(
        0, count("SELECT COUNT(*) FROM outbox_events WHERE entry_id = ?", skipped.value()));

    var deleted = OutboxEntryId.of("obx-delete-1");
    store.save(OutboxEntry.pending(deleted, "{}", "OrderPlaced", TestStreams.stream("agg-4")));
    store.delete(deleted);
    assertEquals(
        0, count("SELECT COUNT(*) FROM outbox_events WHERE entry_id = ?", deleted.value()));
  }

  @Test
  void outboxStore_batchReleaseAndBatchDeliverAreDurable() {
    var store = outboxStore();
    var a = OutboxEntryId.of("obx-batch-a");
    var b = OutboxEntryId.of("obx-batch-b");
    store.save(OutboxEntry.pending(a, "{}", "OrderPlaced", TestStreams.stream("agg-a")));
    store.save(OutboxEntry.pending(b, "{}", "OrderPlaced", TestStreams.stream("agg-b")));

    List<OutboxEntry> claimed = store.loadPending(10);
    assertEquals(2, claimed.size());
    assertEquals(2, store.releaseClaims(claimed, store.claimedBy()));
    assertEquals("PENDING", outboxStatus(a));
    assertEquals("PENDING", outboxStatus(b));

    assertEquals(2, store.loadPending(10).size());
    assertEquals(2, store.markDeliveredAll(List.of(a, b), store.claimedBy()));
    assertEquals("DELIVERED", outboxStatus(a));
    assertEquals("DELIVERED", outboxStatus(b));
  }

  // --- projection offsets -------------------------------------------------------------------

  @Test
  void offsetStore_saveAndResetAreDurable() {
    var store = new PostgresOffsetStore(pool);
    var verify = new PostgresOffsetStore(plain);
    var name = ProjectionName.of("orders-view");

    store.saveOffset(name, GlobalOffset.of(42));
    assertEquals(GlobalOffset.of(42), verify.getLastOffset(name));

    store.reset(name);
    assertEquals(GlobalOffset.initial(), verify.getLastOffset(name));
  }

  // --- command dead-letter queue ------------------------------------------------------------

  @Test
  void deadLetterQueue_publishUpdateDiscardAndSweepAreDurable() {
    var dlq = new PostgresDeadLetterQueue(pool);
    var commandId = CommandId.of("11111111-1111-1111-1111-111111111111");
    dlq.publish(deadLetter(commandId));
    assertEquals(1, count("SELECT COUNT(*) FROM dead_letter_queue"));

    dlq.updateAttempts(commandId, 3, Instant.now());
    assertEquals(
        3,
        count(
            "SELECT dlq_attempts FROM dead_letter_queue WHERE command_id = ?", commandId.value()));

    dlq.discard(commandId);
    assertEquals(0, count("SELECT COUNT(*) FROM dead_letter_queue"));

    dlq.publish(deadLetter(CommandId.of("22222222-2222-2222-2222-222222222222")));
    assertEquals(1, dlq.deleteOlderThan(Instant.now().plus(1, ChronoUnit.HOURS)));
    assertEquals(0, count("SELECT COUNT(*) FROM dead_letter_queue"));
  }

  // --- command inbox retention --------------------------------------------------------------

  @Test
  void commandInbox_retentionSweepIsDurable() throws Exception {
    try (var conn = plain.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO command_inbox (idempotency_key, command_type, aggregate_type,"
                    + " aggregate_id, final_version, processed_at) VALUES (?, 'PlaceOrder', 'test',"
                    + " 'order-1', 1,"
                    + " now() - interval '2 days')")) {
      ps.setString(1, "key-old");
      ps.executeUpdate();
    }

    var inbox = new PostgresCommandInbox(pool);
    assertEquals(1, inbox.deleteProcessedBefore(Instant.now().minus(1, ChronoUnit.DAYS)));
    assertEquals(0, count("SELECT COUNT(*) FROM command_inbox"));
  }

  // --- audit --------------------------------------------------------------------------------

  @Test
  void auditStores_savesAreDurable() {
    new PostgresAuditStore(pool)
        .save(
            new AuditEntry(
                CommandId.of("33333333-3333-3333-3333-333333333333"),
                "PlaceOrder",
                TestStreams.TYPE,
                AggregateId.of("order-1"),
                UserId.of("user-1"),
                Instant.now(),
                AuditOutcome.SUCCESS,
                null,
                1,
                CorrelationId.of("corr-1")));
    assertEquals(1, count("SELECT COUNT(*) FROM audit_log"));

    new PostgresEventAuditStore(pool)
        .saveAll(List.of(eventAudit("ev-1", 1), eventAudit("ev-2", 2)));
    assertEquals(2, count("SELECT COUNT(*) FROM event_audit_log"));
  }

  // --- snapshots ----------------------------------------------------------------------------

  @Test
  void snapshot_saveAndPurgeAreDurable() {
    var stream = TestStreams.stream("order-snap-1");
    var store = PostgresEventStore.builder().dataSource(pool).typeRegistry(typeRegistry).build();

    store.saveSnapshot(stream, new Version(3), new SnapshotState("v3"));
    assertEquals(
        1,
        count(
            "SELECT COUNT(*) FROM snapshot_store WHERE aggregate_type = ? AND aggregate_id = ?",
            stream.aggregateType().value(),
            stream.aggregateId().value()));

    new PostgresSnapshotStorePurger(pool, subject -> List.of(stream))
        .purge(SubjectId.of("customer-1"));
    assertEquals(0, count("SELECT COUNT(*) FROM snapshot_store"));
  }

  // --- saga state ---------------------------------------------------------------------------

  @Test
  void sagaStore_everyWriteIsDurable() {
    var store = new PostgresSagaStore(pool, objectMapper);
    var id = SagaId.of("saga-ac-1");
    var type = SagaType.of("OrderFulfillment");
    var state = new TestSagaState("order-1");

    store.create(id, type, state, SagaStatus.RUNNING);
    assertEquals(1, sagaVersion(id));

    store.update(id, type, state, SagaStatus.RUNNING, 1);
    assertEquals(2, sagaVersion(id));

    store.applyEvent(
        id,
        type,
        state,
        SagaStatus.RUNNING,
        2,
        new SagaStore.AppliedEvent(GlobalOffset.of(7), false, false));
    assertEquals(
        7, count("SELECT last_applied_offset FROM saga_state WHERE saga_id = ?", id.value()));

    assertTrue(store.markFaulted(id, type, sagaVersion(id)));
    assertEquals("FAULTED", scalar("SELECT status FROM saga_state WHERE saga_id = ?", id.value()));

    store.setDeadLetterPending(id, type, true);
    assertEquals(
        "true",
        scalar("SELECT dead_letter_pending::text FROM saga_state WHERE saga_id = ?", id.value()));

    store.delete(id, type);
    assertEquals(0, count("SELECT COUNT(*) FROM saga_state WHERE saga_id = ?", id.value()));

    var faulted = SagaId.of("saga-ac-faulted");
    store.createFaulted(faulted, type, state);
    assertEquals(
        "FAULTED", scalar("SELECT status FROM saga_state WHERE saga_id = ?", faulted.value()));
  }

  // --- saga dead letters --------------------------------------------------------------------

  @Test
  void sagaDeadLetterStore_everyWriteIsDurable() {
    var store = new PostgresSagaDeadLetterStore(pool);
    var type = SagaType.of("OrderFulfillment");
    var named = SagaId.of("saga-dl-1");

    store.publish(sagaDeadLetter(named, type, 10));
    assertEquals(
        1, count("SELECT COUNT(*) FROM saga_dead_letters WHERE saga_id = ?", named.value()));

    store.publish(sagaDeadLetter(null, type, 20));
    assertEquals(1, count("SELECT COUNT(*) FROM saga_dead_letters WHERE saga_id IS NULL"));

    store.establishFirstReplayAnchor(null, type, GlobalOffset.of(20), Instant.now());
    assertEquals(
        1,
        count(
            "SELECT COUNT(*) FROM saga_dead_letters WHERE saga_id IS NULL AND event_offset = 20"
                + " AND first_replay_started_at IS NOT NULL"));

    store.setResolvedTarget(type, GlobalOffset.of(20), named);
    assertEquals(
        named.value(),
        scalar(
            "SELECT target_saga_id FROM saga_dead_letters WHERE saga_id IS NULL AND event_offset = 20"));

    assertTrue(store.discard(null, type, GlobalOffset.of(20)));
    assertEquals(0, count("SELECT COUNT(*) FROM saga_dead_letters WHERE saga_id IS NULL"));

    // An unresolved null-saga entry is the one kind of row the retention sweep prunes.
    store.publish(sagaDeadLetter(null, type, 30));
    assertEquals(1, store.deleteOlderThan(Instant.now().plus(1, ChronoUnit.HOURS)));
    assertEquals(0, count("SELECT COUNT(*) FROM saga_dead_letters WHERE saga_id IS NULL"));

    assertTrue(store.discard(named, type, GlobalOffset.of(10)));
    assertEquals(0, count("SELECT COUNT(*) FROM saga_dead_letters"));
  }

  // --- leadership ---------------------------------------------------------------------------

  @Test
  void leadership_acquireRenewAndResignAreDurable() throws Exception {
    Duration ttl = Duration.ofMillis(1500);
    try (var leadership = new LeaseBasedLeadership(pool, ttl)) {
      assertTrue(leadership.tryAcquire("consumer-a").isPresent());
      assertEquals(
          1, count("SELECT epoch FROM subscription_leases WHERE consumer_name = ?", "consumer-a"));

      // The heartbeat renews every ttl/3 on its own connection; two TTLs later the lease is still
      // live at the database only if those renews were committed.
      Thread.sleep(ttl.multipliedBy(2).toMillis());
      assertEquals(
          "true",
          scalar(
              "SELECT (lease_until > now())::text FROM subscription_leases WHERE consumer_name = ?",
              "consumer-a"));

      leadership.resign("consumer-a");
      assertEquals(
          "true",
          scalar(
              "SELECT (lease_until <= now())::text FROM subscription_leases WHERE consumer_name = ?",
              "consumer-a"));
    }
  }

  // --- projection dead letters --------------------------------------------------------------

  @Test
  void projectionDeadLetterStore_saveAndDiscardAreDurable() {
    var store = new PostgresProjectionDeadLetterStore(pool);
    var name = ProjectionName.of("orders-view");
    store.save(
        new ProjectionDeadLetterEntry(
            name,
            GlobalOffset.of(5),
            GlobalOffset.of(9),
            5,
            "java.lang.RuntimeException",
            "connection refused",
            3,
            Instant.now()));
    assertEquals(1, count("SELECT COUNT(*) FROM projection_dead_letters"));

    store.discard(name, GlobalOffset.of(5));
    assertEquals(0, count("SELECT COUNT(*) FROM projection_dead_letters"));
  }

  // --- projection read models -----------------------------------------------------------------

  @Test
  void projectionRepository_tableCreationAndStandaloneWritesAreDurable() {
    var repository = new JdbcProjectionRepository(pool);
    var name = ProjectionName.of("autocommit_off_items");
    String byId = "SELECT COUNT(*) FROM autocommit_off_items_view WHERE id = ?";

    repository.save(name, "a", Map.of("value", "first")); // creates the table, then upserts
    assertEquals(1, count(byId, "a"));

    repository.save(name, "a", Map.of("value", "second"), 0L);
    assertEquals(1, count("SELECT version FROM autocommit_off_items_view WHERE id = ?", "a"));

    repository.delete(name, "a");
    assertEquals(0, count(byId, "a"));
  }

  @Test
  void projectionRepository_batchOnATableCreatedOutsideItCommits() {
    var repository = new JdbcProjectionRepository(pool);
    var name = ProjectionName.of("autocommit_off_batch");

    // The first write the repository sees for this projection is the batch itself: the table is
    // created before the batch's transaction opens, and the batch must find it there.
    repository.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(7),
        0L,
        txRepository -> txRepository.save(name, "b", Map.of("value", "batched")),
        new PostgresOffsetStore(pool));

    assertEquals(1, count("SELECT COUNT(*) FROM autocommit_off_batch_view WHERE id = ?", "b"));
    assertEquals(
        7,
        count("SELECT last_offset FROM projection_offset WHERE projection_name = ?", name.value()));
  }

  @Test
  void projectionRepository_fencingStampAndOffsetResetAreDurable() {
    var repository = new JdbcProjectionRepository(pool);
    var name = ProjectionName.of("autocommit_off_fenced");
    String epoch = "SELECT epoch FROM projection_offset WHERE projection_name = ?";
    String offset = "SELECT last_offset FROM projection_offset WHERE projection_name = ?";

    repository.stampFencingEpoch(name, 3L);
    assertEquals(3, count(epoch, name.value()));

    repository.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(11),
        3L,
        txRepository -> {},
        new PostgresOffsetStore(pool));
    assertEquals(11, count(offset, name.value()));

    repository.resetOffset(name);
    assertEquals(0, count(offset, name.value()));
  }

  // --- LISTEN -------------------------------------------------------------------------------

  @Test
  void notificationListener_receivesNotificationsOnAManualCommitConnection() throws Exception {
    // A non-HikariCP DataSource is used for LISTEN as-is (no dedicated pool is derived from it),
    // so the LISTEN runs on whatever autoCommit mode the pool hands out. LISTEN takes effect only
    // at commit: left in an open transaction, the backend never registers the channel.
    String channel = "streamrune_autocommit_off_listen";
    var fired = new CountDownLatch(1);
    try (var listener =
        new PgNotificationListener(new DelegatingDataSource(pool), fired::countDown, channel)) {
      listener.start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      boolean received = false;
      while (!received && System.nanoTime() < deadline) {
        try (var conn = plain.getConnection();
            var stmt = conn.createStatement()) {
          stmt.execute("NOTIFY " + channel);
        }
        received = fired.await(200, TimeUnit.MILLISECONDS);
      }
      assertTrue(received, "a LISTEN issued on an autoCommit=false connection must be committed");
    }
  }

  // --- fixtures -----------------------------------------------------------------------------

  private static PostgresOutboxStore outboxStore() {
    return new PostgresOutboxStore(
        pool, PostgresOutboxStore.DEFAULT_CLAIM_LEASE, OutboxOrderingMode.AVAILABILITY_FIRST);
  }

  private static DeadLetterQueue.DeadLetterPublishRequest deadLetter(CommandId commandId) {
    return new DeadLetterQueue.DeadLetterPublishRequest(
        "{\"amount\":42}",
        "PlaceOrder",
        commandId,
        TestStreams.stream("order-1"),
        "java.lang.IllegalStateException",
        "boom",
        3,
        Instant.now(),
        null,
        null,
        null,
        null);
  }

  private static EventAuditEntry eventAudit(String eventId, long version) {
    return new EventAuditEntry(
        EventId.of(eventId),
        EventType.of("OrderPlaced"),
        TestStreams.stream("order-1"),
        new Version(version),
        CommandId.of("44444444-4444-4444-4444-444444444444"),
        CorrelationId.of("corr-1"),
        null,
        null,
        Instant.now());
  }

  private static SagaDeadLetterStore.SagaDeadLetterEntry sagaDeadLetter(
      SagaId sagaId, SagaType type, long offset) {
    return new SagaDeadLetterStore.SagaDeadLetterEntry(
        sagaId,
        type,
        GlobalOffset.of(offset),
        EventType.of("OrderPlaced"),
        "java.lang.RuntimeException",
        "handler failed",
        Instant.now());
  }

  private static String outboxStatus(OutboxEntryId id) {
    return scalar("SELECT status FROM outbox_events WHERE entry_id = ?", id.value());
  }

  private static long sagaVersion(SagaId id) {
    return count("SELECT version FROM saga_state WHERE saga_id = ?", id.value());
  }

  /** Reads one number through a plain autoCommit=true connection; -1 when the row is absent. */
  private static long count(String sql, String... params) {
    String value = scalar(sql, params);
    return value == null ? -1 : Long.parseLong(value);
  }

  /** Reads one value as text through a plain autoCommit=true connection; null when absent. */
  private static String scalar(String sql, String... params) {
    try (var conn = plain.getConnection();
        var ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < params.length; i++) {
        ps.setString(i + 1, params[i]);
      }
      try (var rs = ps.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        String value = rs.getString(1);
        assertNotNull(value, "unexpected NULL from: " + sql);
        return value;
      }
    } catch (SQLException e) {
      throw new AssertionError("verification query failed: " + sql, e);
    }
  }

  /** Hides the HikariCP type so the listener uses the pool's connections directly. */
  private static final class DelegatingDataSource implements DataSource {
    private final DataSource delegate;

    DelegatingDataSource(DataSource delegate) {
      this.delegate = delegate;
    }

    @Override
    public Connection getConnection() throws SQLException {
      return delegate.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return delegate.getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
      delegate.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
      delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
      return delegate.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return delegate.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return delegate.isWrapperFor(iface);
    }
  }
}
