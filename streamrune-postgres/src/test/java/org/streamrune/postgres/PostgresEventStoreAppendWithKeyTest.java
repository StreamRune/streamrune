package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore.IdempotentAppendResult;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class PostgresEventStoreAppendWithKeyTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("appendwithkey_test");

  static PGSimpleDataSource dataSource;
  static ObjectMapper objectMapper;
  static EventTypeRegistry typeRegistry;

  PostgresCommandInbox commandInbox;
  PostgresEventStore store;

  record TestEvent(String value) implements DomainEvent {}

  @BeforeAll
  static void setUpSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return TestEvent.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return null;
          }
        };
    Flyway.configure()
        .dataSource(dataSource)
        .locations("classpath:db/streamrune-migration")
        .table("flyway_schema_history_streamrune")
        .load()
        .migrate();
  }

  @BeforeEach
  void setUp() throws Exception {
    commandInbox = new PostgresCommandInbox(dataSource);
    store =
        new PostgresEventStore(
            dataSource,
            objectMapper,
            typeRegistry,
            org.streamrune.core.upcasting.UpcasterChain.empty(),
            null,
            null,
            null,
            commandInbox);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("DELETE FROM command_inbox");
    }
  }

  private EventEnvelope envelope(StreamId streamId, long version) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        new Version(version),
        new EventType("TestEvent"),
        new TestEvent("v" + version),
        new EventMetadata(
            EventId.of("evt-v" + version),
            CommandId.of("cmd-v" + version),
            null,
            CorrelationId.of("corr-1"),
            null,
            null,
            Instant.now()));
  }

  @Test
  void multiEventConflict_throwsOptimisticLock_andBurnsNoOffset() throws Exception {
    // Same as the unkeyed path: a real aggregate emits 2+ events. With a DIFFERENT idempotency
    // key (so the inbox claim succeeds) but a conflicting (stale) expectedVersion, appendWithKey
    // must map the conflict to OptimisticLockException (retryable) NOT EventStoreException
    // (dead-lettered → an unkeyed DLQ replay re-appends duplicate events), and burn no global
    // offset. Every pre-existing keyed conflict test used a single event; this locks the
    // multi-event batch path.
    //
    // Before the strict head check, this conflict was detected only when executeBatch's
    // FIRST row collided on UNIQUE (aggregate_type, aggregate_id, version) (pgjdbc surfacing a
    // BatchUpdateException).
    // The strict head check (insertEvents, right after RESERVE_OFFSETS) now catches this same
    // stale expectedVersion earlier, before any INSERT runs — this test still pins the observable
    // contract (OptimisticLockException, not EventStoreException; no offset burned), just via the
    // new interception point; a genuine mid-batch 23505 is no longer reachable through this
    // store's own API (see insertEvents' javadoc) and the mapping below is defense-in-depth only.
    StreamId streamId = TestStreams.stream("keyed-multi-conflict");
    // Seed to version 1 under key-seed (records the offset it took).
    IdempotentAppendResult seed =
        store.appendWithKey(
            streamId,
            List.of(envelope(streamId, 1)),
            Version.initial(),
            IdempotencyKey.of("key-seed"),
            "TestCommand");
    long seedOffset = seed.globalOffsets().getFirst().value();

    // A 3-event batch at the SAME expectedVersion=initial under a NEW key: the claim succeeds, then
    // insertEvents' first row (version 1) collides.
    assertThrows(
        OptimisticLockException.class,
        () ->
            store.appendWithKey(
                streamId,
                List.of(envelope(streamId, 1), envelope(streamId, 2), envelope(streamId, 3)),
                Version.initial(),
                IdempotencyKey.of("key-conflict"),
                "TestCommand"),
        "a multi-event keyed batch conflict must map to OptimisticLockException, not"
            + " EventStoreException");

    // The failed batch reserved 3 offsets then rolled back; they must be reused, so the next append
    // takes exactly seedOffset+1 — the conflict burned no global offset.
    IdempotentAppendResult next =
        store.appendWithKey(
            streamId,
            List.of(envelope(streamId, 2)),
            new Version(1),
            IdempotencyKey.of("key-next"),
            "TestCommand");
    assertEquals(
        seedOffset + 1,
        next.globalOffsets().getFirst().value(),
        "the conflicting keyed batch must burn no global offset");

    // The rolled-back claim must not persist either: exactly the two committed events for this
    // stream.
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT COUNT(*) FROM event_stream WHERE aggregate_type = ? AND aggregate_id = ?")) {
      ps.setString(1, streamId.aggregateType().value());
      ps.setString(2, streamId.aggregateId().value());
      try (var rs = ps.executeQuery()) {
        rs.next();
        assertEquals(
            2,
            rs.getLong(1),
            "only the two successful appends committed; the conflict rolled back");
      }
    }
  }

  @Test
  @Timeout(60)
  void concurrentAppendsWithSameKey_appendOnce_loserReturnsWinnersOffsets() throws Exception {
    // The headline exactly-once idempotency guarantee, RACED for real against Postgres (every other
    // appendWithKey test drives the loser branch with a PRE-COMMITTED inbox row — sequential, never
    // two concurrent appendWithKey calls). Two threads released simultaneously by a latch call
    // appendWithKey with the SAME key/stream/event. The production semantics (PostgresEventStore
    // .appendWithKey + PostgresCommandInbox: INSERT ... ON CONFLICT DO NOTHING blocks on the
    // winner's uncommitted claim row, then loses once it commits, and the loser reads back the
    // winner's durable offsets on a fresh connection) must yield: EXACTLY ONE winner
    // (alreadyApplied==false with real offsets), EXACTLY ONE loser (alreadyApplied==true carrying
    // the SAME globalOffsets()/finalVersion() as the winner), one stream row, and no exception.
    StreamId streamId = TestStreams.stream("stream-race");
    IdempotencyKey key = IdempotencyKey.of("cmd-race-1");
    int threads = 2;

    var startLatch = new CountDownLatch(1);
    var readyLatch = new CountDownLatch(threads);
    var results = new ConcurrentLinkedQueue<IdempotentAppendResult>();
    var errors = new ConcurrentLinkedQueue<Throwable>();
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      for (int i = 0; i < threads; i++) {
        pool.submit(
            () -> {
              readyLatch.countDown();
              try {
                startLatch.await();
                results.add(
                    store.appendWithKey(
                        streamId,
                        List.of(envelope(streamId, 1)),
                        Version.initial(),
                        key,
                        "TestCommand"));
              } catch (Throwable t) {
                errors.add(t);
              }
            });
      }
      assertTrue(readyLatch.await(10, TimeUnit.SECONDS), "both threads should be ready");
      startLatch.countDown(); // release both into the appendWithKey race simultaneously
      pool.shutdown();
      assertTrue(
          pool.awaitTermination(30, TimeUnit.SECONDS), "both appendWithKey calls must finish");
    } finally {
      pool.shutdownNow();
    }

    assertTrue(
        errors.isEmpty(), () -> "no exception expected from a same-key race, got: " + errors);
    assertEquals(threads, results.size());

    List<IdempotentAppendResult> winners =
        results.stream().filter(r -> !r.alreadyApplied()).toList();
    List<IdempotentAppendResult> losers =
        results.stream().filter(IdempotentAppendResult::alreadyApplied).toList();
    assertEquals(1, winners.size(), "exactly one thread must win the key (alreadyApplied==false)");
    assertEquals(1, losers.size(), "exactly one thread must lose the key (alreadyApplied==true)");

    IdempotentAppendResult winner = winners.getFirst();
    IdempotentAppendResult loser = losers.getFirst();
    assertEquals(1, winner.globalOffsets().size(), "the winner appended exactly one event");
    assertEquals(new Version(1), winner.finalVersion());
    assertEquals(
        winner.globalOffsets(),
        loser.globalOffsets(),
        "the loser must return the winner's durable global offsets, read on a fresh connection");
    assertEquals(
        winner.finalVersion(), loser.finalVersion(), "the loser must return the winner's version");

    assertEquals(
        1, streamRowCount(streamId), "exactly one stream row — the event was appended once");
  }

  @Test
  @Timeout(60)
  void concurrentAppendsSameKey_winnerRollsBack_loserWins() throws Exception {
    // The inverse branch: a winner claims the key but its transaction ABORTS (e.g. its event insert
    // hits a 23505, rolling the inbox claim back with it), so the blocked loser must then claim the
    // key and append successfully — the key is NOT permanently blocked, and the loser never hits
    // the
    // "Inbox conflict but no row found" dead-end. Proves the claim/append atomicity holds under
    // contention: an uncommitted-then-rolled-back claim leaves the key acquirable.
    //
    // The winner is a manually-controlled transaction: it tryClaims the key (leaving the row
    // uncommitted), the loser's real appendWithKey blocks on that row (INSERT ... ON CONFLICT DO
    // NOTHING waits on the uncommitted duplicate), then the winner rolls back — exactly the DB
    // state
    // a 23505-aborted appendWithKey leaves behind — and the loser's INSERT now succeeds.
    StreamId streamId = TestStreams.stream("stream-rollback-race");
    IdempotencyKey key = IdempotencyKey.of("cmd-rollback-race-1");

    try (Connection winnerConn = dataSource.getConnection()) {
      winnerConn.setAutoCommit(false);
      assertTrue(
          commandInbox.tryClaim(key, "TestCommand", streamId, new Version(1), winnerConn),
          "the winner claims the key first (uncommitted)");

      ExecutorService pool = Executors.newSingleThreadExecutor();
      try {
        Future<IdempotentAppendResult> loser =
            pool.submit(
                () ->
                    store.appendWithKey(
                        streamId,
                        List.of(envelope(streamId, 1)),
                        Version.initial(),
                        key,
                        "TestCommand"));

        // The loser's tryClaim INSERT must be blocked on the winner's uncommitted claim row before
        // we roll the winner back — so we deterministically exercise the block-then-win path.
        awaitBlockedOnLock(Duration.ofSeconds(15));

        // Winner aborts: its whole tx (including the claim) rolls back — the 23505-rollback case.
        winnerConn.rollback();

        // The loser's INSERT now succeeds (no committed conflicting row): it claims the key and
        // appends via the winner branch — NOT the claim-lost path, and never the dead-end
        // "Inbox conflict but no row found for key" EventStoreException.
        IdempotentAppendResult loserResult = loser.get(15, TimeUnit.SECONDS);
        assertFalse(
            loserResult.alreadyApplied(), "the loser wins the key after the winner rolled back");
        assertEquals(1, loserResult.globalOffsets().size(), "the loser appended exactly one event");
        assertEquals(new Version(1), loserResult.finalVersion());
      } finally {
        pool.shutdownNow();
      }
    }

    // The key is NOT permanently blocked: a subsequent append with the same key is now deduped
    // against the loser's now-committed result (alreadyApplied==true, carrying its offsets).
    IdempotentAppendResult dedup =
        store.appendWithKey(
            streamId, List.of(envelope(streamId, 1)), Version.initial(), key, "TestCommand");
    assertTrue(dedup.alreadyApplied(), "the committed key must dedup a later same-key append");
    assertFalse(dedup.globalOffsets().isEmpty(), "dedup returns the loser's committed offsets");

    assertEquals(1, streamRowCount(streamId), "exactly one stream row — the loser appended once");
  }

  @Test
  void appendWithKeyForcedConflictReturnsAlreadyApplied() throws Exception {
    StreamId streamId = TestStreams.stream("stream-conflict");
    IdempotencyKey key = IdempotencyKey.of("cmd-conflict-1");

    // Pre-insert the key directly (simulates the winner finishing first)
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO command_inbox"
                    + " (idempotency_key, command_type, aggregate_type, aggregate_id, final_version)"
                    + " VALUES (?, 'TestCommand', ?, ?, 1)")) {
      ps.setString(1, key.value());
      ps.setString(2, streamId.aggregateType().value());
      ps.setString(3, streamId.aggregateId().value());
      ps.executeUpdate();
    }

    IdempotentAppendResult result =
        store.appendWithKey(
            streamId, List.of(envelope(streamId, 1)), Version.initial(), key, "TestCommand");

    assertTrue(result.alreadyApplied());
    assertTrue(result.globalOffsets().isEmpty());

    // No events appended
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT COUNT(*) FROM event_stream WHERE aggregate_type = ? AND aggregate_id = ?")) {
      ps.setString(1, streamId.aggregateType().value());
      ps.setString(2, streamId.aggregateId().value());
      try (var rs = ps.executeQuery()) {
        rs.next();
        assertEquals(0, rs.getLong(1));
      }
    }
  }

  @Test
  void claimLoser_releasesAppendConnection_beforeInboxLookup() throws Exception {
    StreamId streamId = TestStreams.stream("stream-conflict-pool-starve");
    IdempotencyKey key = IdempotencyKey.of("cmd-conflict-pool-starve-1");

    // Pre-insert the key directly (simulates the winner finishing first) — forces
    // appendWithKey down the claim-loser path, mirroring
    // appendWithKeyForcedConflictReturnsAlreadyApplied.
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO command_inbox"
                    + " (idempotency_key, command_type, aggregate_type, aggregate_id, final_version)"
                    + " VALUES (?, 'TestCommand', ?, ?, 1)")) {
      ps.setString(1, key.value());
      ps.setString(2, streamId.aggregateType().value());
      ps.setString(3, streamId.aggregateId().value());
      ps.executeUpdate();
    }

    // A single-connection pool: if appendWithKey still held the append connection while
    // commandInbox.find() (which grabs its own connection from the SAME pool) ran, the pool
    // would have zero connections left and find() would block until connectionTimeout, then
    // throw. With the fix, the loser releases its connection before find() runs, so a pool of
    // size 1 is enough for both to complete.
    var hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(PG.getJdbcUrl());
    hikariConfig.setUsername(PG.getUsername());
    hikariConfig.setPassword(PG.getPassword());
    hikariConfig.setMaximumPoolSize(1);
    hikariConfig.setMinimumIdle(1);
    hikariConfig.setConnectionTimeout(2_000);
    hikariConfig.setPoolName("starve-test-" + System.nanoTime());

    try (HikariDataSource smallPool = new HikariDataSource(hikariConfig)) {
      PostgresCommandInbox smallPoolInbox = new PostgresCommandInbox(smallPool);
      PostgresEventStore smallPoolStore =
          new PostgresEventStore(
              smallPool,
              objectMapper,
              typeRegistry,
              org.streamrune.core.upcasting.UpcasterChain.empty(),
              null,
              null,
              null,
              smallPoolInbox);

      IdempotentAppendResult result =
          smallPoolStore.appendWithKey(
              streamId, List.of(envelope(streamId, 1)), Version.initial(), key, "TestCommand");

      assertTrue(result.alreadyApplied());
      assertTrue(result.globalOffsets().isEmpty());
    }
  }

  @Test
  void appendWithKeyWithoutCommandInboxThrows() {
    PostgresEventStore storeNoInbox =
        new PostgresEventStore(
            dataSource,
            objectMapper,
            typeRegistry,
            org.streamrune.core.upcasting.UpcasterChain.empty(),
            null,
            null,
            null,
            null);
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            storeNoInbox.appendWithKey(
                TestStreams.stream("s"),
                List.of(),
                Version.initial(),
                IdempotencyKey.of("k"),
                "T"));
  }

  /**
   * A RACED key collision across two command types must fail closed, exactly as the sequential path
   * already does.
   *
   * <p>Two threads present the SAME key with DIFFERENT command types and are released together, so
   * both miss any pre-check and both reach {@code tryClaim}. One wins and commits; the loser's
   * {@code ON CONFLICT DO NOTHING} blocks on the winner's uncommitted row and then loses. Before
   * the fix the loser took the claim-lost branch and returned {@code IdempotentAppendResult(winner
   * offsets, winner finalVersion, alreadyApplied=true)} without ever comparing {@code
   * prior.commandType()} to the presented one — the caller was told its command had succeeded while
   * its events were never appended and never would be, and it received the winner's offsets and
   * version. The sequential presentation of the same two inputs throws {@code
   * IllegalArgumentException} (VirtualThreadCommandBus.buildReplayResult), so the same inputs
   * produced either a loud rejection or a silent false success depending purely on timing.
   */
  @Test
  @Timeout(60)
  void concurrentAppendsSameKey_differentCommandTypes_loserIsRejected() throws Exception {
    StreamId streamId = TestStreams.stream("stream-race-type-mismatch");
    IdempotencyKey key = IdempotencyKey.of("cmd-race-type-mismatch-1");
    List<String> commandTypes = List.of("com.example.CancelOrder", "com.example.RefundPayment");

    var startLatch = new CountDownLatch(1);
    var readyLatch = new CountDownLatch(commandTypes.size());
    var results = new ConcurrentLinkedQueue<IdempotentAppendResult>();
    var errors = new ConcurrentLinkedQueue<Throwable>();
    ExecutorService pool = Executors.newFixedThreadPool(commandTypes.size());
    try {
      for (String commandType : commandTypes) {
        pool.submit(
            () -> {
              readyLatch.countDown();
              try {
                startLatch.await();
                results.add(
                    store.appendWithKey(
                        streamId,
                        List.of(envelope(streamId, 1)),
                        Version.initial(),
                        key,
                        commandType));
              } catch (Throwable t) {
                errors.add(t);
              }
            });
      }
      assertTrue(readyLatch.await(10, TimeUnit.SECONDS), "both threads should be ready");
      startLatch.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "both calls must finish");
    } finally {
      pool.shutdownNow();
    }

    assertEquals(
        1,
        results.size(),
        () -> "exactly one thread may succeed on a colliding key, got results: " + results);
    IdempotentAppendResult winner = results.peek();
    assertFalse(winner.alreadyApplied(), "the sole surviving call is the claim winner");
    assertEquals(1, winner.globalOffsets().size(), "the winner appended exactly one event");

    assertEquals(1, errors.size(), () -> "the claim loser must be rejected, got: " + errors);
    Throwable loserFailure = errors.peek();
    assertInstanceOf(
        IllegalArgumentException.class,
        loserFailure,
        "the racing loser must fail the same way the sequential path does");
    String message = loserFailure.getMessage();
    assertTrue(message.contains(key.value()), "message must name the key, got: " + message);
    assertTrue(
        message.contains("CancelOrder") && message.contains("RefundPayment"),
        "message must name both the stored and the presented command type, got: " + message);

    assertEquals(1, streamRowCount(streamId), "only the winner's event may be committed");
  }

  /**
   * The same race with the same command type on DIFFERENT streams. The claim-lost branch returned
   * the winner's offsets and final version for a completely different aggregate, leaking one
   * stream's identity into the other caller's result while silently dropping its command.
   */
  @Test
  @Timeout(60)
  void concurrentAppendsSameKey_differentStreams_loserIsRejected() throws Exception {
    List<StreamId> streams =
        List.of(TestStreams.stream("stream-race-alice"), TestStreams.stream("stream-race-bob"));
    IdempotencyKey key = IdempotencyKey.of("cmd-race-stream-mismatch-1");

    var startLatch = new CountDownLatch(1);
    var readyLatch = new CountDownLatch(streams.size());
    var results = new ConcurrentLinkedQueue<IdempotentAppendResult>();
    var errors = new ConcurrentLinkedQueue<Throwable>();
    ExecutorService pool = Executors.newFixedThreadPool(streams.size());
    try {
      for (StreamId streamId : streams) {
        pool.submit(
            () -> {
              readyLatch.countDown();
              try {
                startLatch.await();
                results.add(
                    store.appendWithKey(
                        streamId,
                        List.of(envelope(streamId, 1)),
                        Version.initial(),
                        key,
                        "TestCommand"));
              } catch (Throwable t) {
                errors.add(t);
              }
            });
      }
      assertTrue(readyLatch.await(10, TimeUnit.SECONDS), "both threads should be ready");
      startLatch.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "both calls must finish");
    } finally {
      pool.shutdownNow();
    }

    assertEquals(
        1,
        results.size(),
        () -> "exactly one thread may succeed on a colliding key, got results: " + results);
    assertFalse(results.peek().alreadyApplied(), "the sole surviving call is the claim winner");

    assertEquals(1, errors.size(), () -> "the claim loser must be rejected, got: " + errors);
    Throwable loserFailure = errors.peek();
    assertInstanceOf(IllegalArgumentException.class, loserFailure);
    String message = loserFailure.getMessage();
    assertTrue(message.contains(key.value()), "message must name the key, got: " + message);
    assertTrue(
        message.contains("stream-race-alice") && message.contains("stream-race-bob"),
        "message must name both the stored and the presented stream, got: " + message);

    long total = streamRowCount(streams.getFirst()) + streamRowCount(streams.get(1));
    assertEquals(1, total, "only the winner's stream may hold an event");
  }

  private long streamRowCount(StreamId streamId) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT COUNT(*) FROM event_stream WHERE aggregate_type = ? AND aggregate_id = ?")) {
      ps.setString(1, streamId.aggregateType().value());
      ps.setString(2, streamId.aggregateId().value());
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  /**
   * Polls {@code pg_stat_activity} until at least one backend is actively waiting on a lock for a
   * {@code command_inbox} INSERT — i.e. the loser's {@code ON CONFLICT DO NOTHING} claim is blocked
   * on the winner's uncommitted duplicate row. Deterministic replacement for a fixed sleep.
   */
  private void awaitBlockedOnLock(Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    String sql =
        "SELECT count(*) FROM pg_stat_activity"
            + " WHERE wait_event_type = 'Lock' AND state = 'active'"
            + " AND query ILIKE '%command_inbox%'";
    while (System.nanoTime() < deadline) {
      try (var conn = dataSource.getConnection();
          var ps = conn.prepareStatement(sql);
          var rs = ps.executeQuery()) {
        rs.next();
        if (rs.getLong(1) >= 1) {
          return;
        }
      }
      Thread.sleep(25);
    }
    fail("timed out waiting for the loser's claim INSERT to block on the winner's uncommitted row");
  }
}
