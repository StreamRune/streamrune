package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Testcontainers-backed tests for {@link PostgresDeadLetterQueue}. Exercises the full
 * publish/read/discard surface against a real PostgreSQL instance, on the schema the shipped
 * event-store baseline provisions.
 */
@Testcontainers
class PostgresDeadLetterQueueTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_dlq_test");

  static PGSimpleDataSource dataSource;
  PostgresDeadLetterQueue dlq;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    org.flywaydb.core.Flyway.configure()
        .dataSource(dataSource)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
  }

  @BeforeEach
  void setUp() throws Exception {
    dlq = new PostgresDeadLetterQueue(dataSource);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM dead_letter_queue");
    }
  }

  @Test
  void publishAndReadRoundTrip() {
    var req =
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{\"amount\":42}",
            "PlaceOrder",
            CommandId.of("11111111-1111-1111-1111-111111111111"),
            TestStreams.stream("order-1"),
            "OptimisticLockException",
            "version conflict",
            3,
            Instant.parse("2026-01-01T00:00:00Z"),
            null,
            null,
            null,
            null);

    dlq.publish(req);
    var entries = dlq.read(10);

    assertEquals(1, entries.size());
    var e = entries.getFirst();
    assertEquals("PlaceOrder", e.commandType());
    assertNotNull(e.streamId());
    assertEquals(TestStreams.stream("order-1"), e.streamId());
    assertNotNull(e.aggregateId());
    assertEquals("order-1", e.aggregateId().value());
    assertEquals("OptimisticLockException", e.errorType());
    assertEquals("version conflict", e.errorMessage());
    assertEquals(3, e.attempts());
    // Postgres JSONB normalises whitespace; just assert the key/value round-tripped.
    assertTrue(
        e.commandPayload().contains("\"amount\"") && e.commandPayload().contains("42"),
        "payload should round-trip via JSONB: " + e.commandPayload());
    assertEquals(Instant.parse("2026-01-01T00:00:00Z"), e.firstAttemptAt());
    assertNotNull(e.publishedAt());
  }

  /**
   * Why the ingress bound on {@code userId} is 255 and not a round number someone liked. {@code
   * dead_letter_queue.user_id} is {@code VARCHAR(255)}, so an identity longer than that cannot be
   * written to the recovery channel AT ALL: the INSERT fails, {@code VirtualThreadCommandBus}
   * catches the failure and logs it (the original command failure is what propagates to the
   * caller), and an already-accepted business command ends up with no dead-letter row and therefore
   * no replay.
   *
   * <p>This test pins the coupling in the direction that matters: {@code IdConstraints.MAX_LENGTH}
   * is not an arbitrary ceiling, it is this column's width, and widening one without the other
   * re-opens the silent-loss chain. The ingress door now rejects such an identity before a command
   * is ever accepted, which is the only point at which anything can be done about it.
   */
  @Test
  void anIdentityLongerThanTheColumnCannotBeDeadLetteredAtAll() {
    var overLong = "u".repeat(org.streamrune.core.types.IdConstraints.MAX_LENGTH + 1);
    var req =
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "PlaceOrder",
            CommandId.of("44444444-4444-4444-4444-444444444444"),
            TestStreams.stream("order-4"),
            "Err",
            "boom",
            1,
            Instant.parse("2026-01-01T00:00:00Z"),
            null,
            UserId.of(overLong),
            null,
            null);

    assertThrows(
        org.streamrune.core.EventStoreException.class,
        () -> dlq.publish(req),
        "an over-column identity must not be silently storable");
    assertEquals(
        0L,
        dlq.countPending(),
        "no recovery row exists — this is the silent-loss chain the ingress bound closes");

    // Exactly at the bound it stores, so the ingress ceiling is the largest identity the recovery
    // channel can actually carry — not one character more, not one fewer.
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "PlaceOrder",
            CommandId.of("55555555-5555-5555-5555-555555555555"),
            TestStreams.stream("order-5"),
            "Err",
            "boom",
            1,
            Instant.parse("2026-01-01T00:00:00Z"),
            null,
            UserId.of("u".repeat(org.streamrune.core.types.IdConstraints.MAX_LENGTH)),
            null,
            null));
    assertEquals(1L, dlq.countPending());
  }

  /**
   * The same coupling for the aggregate id. {@code dead_letter_queue.aggregate_id} is {@code
   * VARCHAR(255)}, so a routed id longer than that cannot be dead-lettered at all: {@code
   * AggregateId.of} refuses one at the id extractor, before the command runs, and a stream refuses
   * one built through the lenient door, so it never reaches the column. Every id {@code of} accepts
   * is storable here: the bound counts UTF-16 code units, and the column counts characters, of
   * which a supplementary-plane id at the bound has fewer.
   */
  @Test
  void anAggregateIdAtTheIngressBoundIsDeadLetterableAndOneCharacterMoreIsNot() {
    int bound = org.streamrune.core.types.IdConstraints.MAX_LENGTH;
    var overLong = "o".repeat(bound + 1);
    assertThrows(
        IllegalArgumentException.class,
        () -> publishRequest("66666666-6666-6666-6666-666666666666", overLong),
        "an over-column aggregate id cannot form a stream, so it is never storable");
    assertEquals(0L, dlq.countPending());

    var ascii = "o".repeat(bound);
    var supplementary = "\uD83D\uDE00".repeat(127) + "o";
    dlq.publish(
        publishRequest("77777777-7777-7777-7777-777777777777", AggregateId.of(ascii).value()));
    dlq.publish(
        publishRequest(
            "88888888-8888-8888-8888-888888888888", AggregateId.of(supplementary).value()));

    var stored = dlq.read(10).stream().map(e -> e.aggregateId().value()).toList();
    assertEquals(2, stored.size());
    assertTrue(stored.containsAll(List.of(ascii, supplementary)), "stored verbatim: " + stored);
  }

  @Test
  void aRequestWithoutAStream_writesTwoNulls_andAHalfPairIsRefusedByTheCheck() throws Exception {
    dlq.publish(requestWithStream("c-no-stream", null));
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT aggregate_type, aggregate_id FROM dead_letter_queue")) {
      assertTrue(rs.next());
      assertNull(rs.getString(1));
      assertNull(rs.getString(2));
    }
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      var ex =
          assertThrows(
              java.sql.SQLException.class,
              () ->
                  stmt.execute(
                      "INSERT INTO dead_letter_queue (command_id, command_type, payload,"
                          + " aggregate_type, error_type, attempts, first_attempt_at) VALUES"
                          + " ('c-half', 'T', '{}', 'inventory', 'E', 1, NOW())"));
      assertEquals("23514", ex.getSQLState());
    }
  }

  @Test
  void aTypeThatBreaksTheSyntax_isRefusedByTheCheck() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      var ex =
          assertThrows(
              java.sql.SQLException.class,
              () ->
                  stmt.execute(
                      "INSERT INTO dead_letter_queue (command_id, command_type, payload,"
                          + " aggregate_type, aggregate_id, error_type, attempts,"
                          + " first_attempt_at) VALUES ('c-bad-type', 'T', '{}', 'Inventory',"
                          + " 'p-1', 'E', 1, NOW())"));
      assertEquals("23514", ex.getSQLState());
    }
  }

  @Test
  void aTypedStreamRoundTrips_withBothColumnsAndTheDerivedAggregateId() throws Exception {
    var inventory = StreamId.of(AggregateType.of("inventory"), AggregateId.of("p-1"));
    dlq.publish(requestWithStream("c-typed", inventory));

    var entries = dlq.read(10);

    assertEquals(1, entries.size());
    assertEquals(inventory, entries.getFirst().streamId());
    assertEquals("p-1", entries.getFirst().aggregateId().value());
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT aggregate_type, aggregate_id FROM dead_letter_queue")) {
      assertTrue(rs.next());
      assertEquals("inventory", rs.getString(1));
      assertEquals("p-1", rs.getString(2));
    }
  }

  private static DeadLetterQueue.DeadLetterPublishRequest requestWithStream(
      String commandId, StreamId streamId) {
    return new DeadLetterQueue.DeadLetterPublishRequest(
        "{}",
        "PlaceOrder",
        CommandId.of(commandId),
        streamId,
        "Err",
        "boom",
        1,
        Instant.parse("2026-01-01T00:00:00Z"),
        null,
        null,
        null,
        null);
  }

  private static DeadLetterQueue.DeadLetterPublishRequest publishRequest(
      String commandId, String aggregateId) {
    return new DeadLetterQueue.DeadLetterPublishRequest(
        "{}",
        "PlaceOrder",
        CommandId.of(commandId),
        TestStreams.stream(aggregateId),
        "Err",
        "boom",
        1,
        Instant.parse("2026-01-01T00:00:00Z"),
        null,
        null,
        null,
        null);
  }

  /**
   * {@code IdempotencyKey.of} now refuses control characters — it is the INGRESS door. The row
   * mapper is the DECODE door: a key a command presented through the canonical constructor (which
   * keeps no charset rule) is stored verbatim, and must read back verbatim. Through {@code of},
   * that one row would throw inside every {@code read} page that reached it, wedging the recovery
   * channel for every other entry behind it, and the replay could no longer present the original
   * key to the inbox.
   */
  @Test
  void aStoredKeyCarryingAControlCharacterReadsBackVerbatim() {
    var key = new IdempotencyKey("client\u0001key\nx");
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "PlaceOrder",
            CommandId.of("22222222-2222-2222-2222-222222222222"),
            TestStreams.stream("order-1"),
            "RuntimeException",
            "infra",
            1,
            Instant.parse("2026-01-01T00:00:00Z"),
            null,
            null,
            null,
            key));

    var entries = dlq.read(10);

    assertEquals(1, entries.size());
    assertEquals(key, entries.getFirst().idempotencyKey());
    assertEquals(
        key,
        dlq.find(CommandId.of("22222222-2222-2222-2222-222222222222"))
            .orElseThrow()
            .idempotencyKey());
  }

  /**
   * {@code AggregateId.of} now refuses control characters — it is the INGRESS door. The row mapper
   * is the DECODE door: an id a command was routed under through the canonical constructor (which
   * keeps no charset rule) is stored verbatim in {@code aggregate_id}, and must read back verbatim.
   * Through {@code of}, that one row would throw inside every {@code read} page that reached it,
   * wedging the recovery channel for every entry behind it.
   */
  @Test
  void aStoredAggregateIdCarryingAControlCharacterReadsBackVerbatim() {
    var raw = "order\r\n\u0085-1";
    var commandId = CommandId.of("33333333-3333-3333-3333-333333333333");
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "PlaceOrder",
            commandId,
            TestStreams.stream(raw),
            "RuntimeException",
            "infra",
            1,
            Instant.parse("2026-01-01T00:00:00Z"),
            null,
            null,
            null,
            null));

    var entries = dlq.read(10);

    assertEquals(1, entries.size());
    assertEquals(new AggregateId(raw), entries.getFirst().aggregateId());
    assertEquals(TestStreams.stream(raw), entries.getFirst().streamId());
    assertEquals(new AggregateId(raw), dlq.find(commandId).orElseThrow().aggregateId());
  }

  @Test
  void countPendingReflectsQueuedEntries() {
    // The command DLQ depth gauge samples countPending(); it must count queued (not yet
    // discarded) entries and drop as entries are discarded.
    assertEquals(0L, dlq.countPending(), "empty queue counts 0");

    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "PlaceOrder",
            CommandId.of("22222222-2222-2222-2222-222222222222"),
            TestStreams.stream("order-2"),
            "Err",
            "boom",
            1,
            Instant.parse("2026-01-01T00:00:00Z"),
            null,
            null,
            null,
            null));
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "PlaceOrder",
            CommandId.of("33333333-3333-3333-3333-333333333333"),
            TestStreams.stream("order-3"),
            "Err",
            "boom",
            1,
            Instant.parse("2026-01-01T00:00:00Z"),
            null,
            null,
            null,
            null));

    assertEquals(2L, dlq.countPending());

    dlq.discard(CommandId.of("22222222-2222-2222-2222-222222222222"));
    assertEquals(1L, dlq.countPending(), "discarding an entry drops the depth");
  }

  @Test
  void metadataOnlyNullPayloadSurvivesJsonbCastWhileToStringIsDropped() {
    // The fixed VirtualThreadCommandBus records a JSON-null payload (never command.toString()) when
    // a command cannot be serialized for the DLQ (e.g. crypto fail-fast). This proves that shape is
    // a valid jsonb value that round-trips — the failed command is retained, not silently dropped —
    // while the former command.toString() payload is rejected by the ?::jsonb cast (the silent-drop
    // mechanism this fix removes).
    var metadataOnly =
        new DeadLetterQueue.DeadLetterPublishRequest(
            "null", // JSON-null literal — the metadata-only payload the fixed bus emits
            "RegisterCustomer",
            CommandId.of("d1a11111-0000-0000-0000-000000000001"),
            TestStreams.stream("cust-1"),
            "org.streamrune.core.crypto.CryptoOperationException",
            "no encryption key for subject",
            3,
            Instant.now(),
            null,
            null,
            null,
            null);

    dlq.publish(metadataOnly);

    var entries = dlq.read(10);
    assertEquals(1, entries.size(), "metadata-only entry must be persisted, not silently dropped");
    var e = entries.getFirst();
    assertEquals("RegisterCustomer", e.commandType());
    assertEquals("org.streamrune.core.crypto.CryptoOperationException", e.errorType());
    assertEquals("no encryption key for subject", e.errorMessage());
    // Postgres stores JSON null verbatim.
    assertEquals("null", e.commandPayload());

    // Control (the removed behavior): a command.toString() payload is invalid JSON, so the ?::jsonb
    // cast rejects it — exactly how the failed command used to be silently dropped from the DLQ.
    var toStringShape =
        new DeadLetterQueue.DeadLetterPublishRequest(
            "RegisterCustomer[id=cust-2, email=alice@example.com]",
            "RegisterCustomer",
            CommandId.of("d1a11111-0000-0000-0000-000000000002"),
            TestStreams.stream("cust-2"),
            "org.streamrune.core.crypto.CryptoOperationException",
            "no encryption key for subject",
            3,
            Instant.now(),
            null,
            null,
            null,
            null);
    assertThrows(
        RuntimeException.class,
        () -> dlq.publish(toStringShape),
        "a command.toString() payload is invalid JSON → jsonb cast rejects it → silent DLQ drop");
  }

  @Test
  void discardRemovesEntry() {
    var cmdId = CommandId.of("22222222-2222-2222-2222-222222222222");
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Cmd",
            cmdId,
            TestStreams.stream("s1"),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));

    assertEquals(1, dlq.read(10).size());
    dlq.discard(cmdId);
    assertTrue(dlq.read(10).isEmpty());
  }

  @Test
  void discardUnknownIdIsNoop() {
    assertDoesNotThrow(() -> dlq.discard(CommandId.of("99999999-9999-9999-9999-999999999999")));
    assertTrue(dlq.read(10).isEmpty());
  }

  @Test
  void findReturnsMatchingEntryByIndexedLookup() {
    // find(CommandId) must be an override backed by an indexed WHERE command_id = ?
    // lookup, not the inherited DeadLetterQueue default (which scans read(Integer.MAX_VALUE)).
    // Publishing a second, unrelated entry proves find() targets the requested row specifically.
    var cmdId = CommandId.of("e2000000-0000-0000-0000-000000000001");
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{\"amount\":7}",
            "PlaceOrder",
            cmdId,
            TestStreams.stream("order-find-1"),
            "OptimisticLockException",
            "version conflict",
            2,
            Instant.parse("2026-01-01T00:00:00Z"),
            null,
            null,
            null,
            null));
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Other",
            CommandId.of("e2000000-0000-0000-0000-000000000002"),
            TestStreams.stream("s"),
            "Err",
            "other",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));

    var found = dlq.find(cmdId);

    assertTrue(found.isPresent());
    assertEquals(cmdId, found.get().commandId());
    assertEquals("PlaceOrder", found.get().commandType());
    assertEquals(2, found.get().attempts());
    assertEquals("version conflict", found.get().errorMessage());
  }

  @Test
  void findReturnsEmptyOptionalForUnknownCommandId() {
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Cmd",
            CommandId.of("e2000000-0000-0000-0000-000000000003"),
            TestStreams.stream("s"),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));

    assertTrue(dlq.find(CommandId.of("e2000000-0000-0000-0000-000000000099")).isEmpty());
  }

  @Test
  void publishWithNullStreamAndAggregateIds() {
    var req =
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Cmd",
            CommandId.of("33333333-3333-3333-3333-333333333333"),
            null,
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null);

    dlq.publish(req);
    var entry = dlq.read(10).getFirst();
    assertNull(entry.streamId());
    assertNull(entry.aggregateId());
  }

  @Test
  void readRespectsLimit() {
    for (int i = 0; i < 5; i++) {
      dlq.publish(
          new DeadLetterQueue.DeadLetterPublishRequest(
              "{}",
              "Cmd",
              CommandId.of("4000000" + i + "-0000-0000-0000-000000000000"),
              TestStreams.stream("s"),
              "Err",
              "msg",
              1,
              Instant.now(),
              null,
              null,
              null,
              null));
    }
    assertEquals(5, dlq.read(10).size());
    assertEquals(3, dlq.read(3).size());
  }

  @Test
  void readReturnsEntriesOrderedByPublishedAtDesc() throws Exception {
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "First",
            CommandId.of("50000000-0000-0000-0000-000000000001"),
            TestStreams.stream("s"),
            "Err",
            "first",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));
    // Small gap to make published_at distinct
    Thread.sleep(10);
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Second",
            CommandId.of("50000000-0000-0000-0000-000000000002"),
            TestStreams.stream("s"),
            "Err",
            "second",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));

    var entries = dlq.read(10);
    assertEquals(2, entries.size());
    // Newest first (descending)
    assertEquals("Second", entries.get(0).commandType());
    assertEquals("First", entries.get(1).commandType());
  }

  @Test
  void deleteOlderThanRemovesStrictlyOlderEntriesAndReturnsCount() throws Exception {
    Instant tMinus2d = Instant.now().minus(2, java.time.temporal.ChronoUnit.DAYS);
    Instant tMinus1d = Instant.now().minus(1, java.time.temporal.ChronoUnit.DAYS);

    insertWithPublishedAt("e0000000-0000-0000-0000-000000000001", tMinus2d);
    insertWithPublishedAt("e0000000-0000-0000-0000-000000000002", tMinus1d);
    // fresh entry via the normal publish path (published_at = NOW())
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Fresh",
            CommandId.of("e0000000-0000-0000-0000-000000000003"),
            TestStreams.stream("s"),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));

    int deleted = dlq.deleteOlderThan(tMinus1d);

    assertEquals(1, deleted, "only the strictly-older entry must be removed");
    var remaining = dlq.read(10);
    assertEquals(2, remaining.size());
    assertTrue(remaining.stream().noneMatch(e -> e.commandId().value().endsWith("000000000001")));
  }

  @Test
  void deleteOlderThanKeepsEntriesExactlyAtCutoff() throws Exception {
    Instant cutoff = Instant.now().minus(1, java.time.temporal.ChronoUnit.DAYS);
    insertWithPublishedAt("e1000000-0000-0000-0000-000000000001", cutoff);

    int deleted = dlq.deleteOlderThan(cutoff);

    assertEquals(0, deleted, "strictly-before semantics: entry exactly at cutoff must be kept");
    assertEquals(1, dlq.read(10).size());
  }

  @Test
  void deleteOlderThanBatchesAcrossMoreThanOneThousandRows() throws Exception {
    Instant old = Instant.now().minus(2, java.time.temporal.ChronoUnit.DAYS);
    int total = 1500;
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO dead_letter_queue "
                    + "(command_id, command_type, payload, error_type, attempts, first_attempt_at,"
                    + " published_at) VALUES (?, 'Cmd', '{}'::jsonb, 'Err', 1, NOW(), ?)")) {
      for (int i = 0; i < total; i++) {
        ps.setString(1, String.format("f0000000-0000-0000-0000-%012d", i));
        ps.setObject(2, java.sql.Timestamp.from(old));
        ps.addBatch();
      }
      ps.executeBatch();
    }

    int deleted = dlq.deleteOlderThan(Instant.now());

    assertEquals(
        total, deleted, "batched delete must remove every eligible row, not just one batch");
    assertTrue(dlq.read(10).isEmpty());
  }

  /**
   * The retry runner's attempt bookkeeping ({@code updateAttempts}) rewrites an entry's row while
   * it stays aged, and it targets the least-recently-active entries — the same oldest rows the
   * retention sweep selects. The sweep's DELETE matches its candidates by {@code ctid}; a row
   * updated inside the DELETE window has moved to a new ctid, so READ COMMITTED's EvalPlanQual
   * re-check skips it and the chunk deletes fewer than its LIMIT although aged rows remain. Under
   * the former {@code batch == DELETE_BATCH_SIZE} termination the sweep stopped there and left the
   * updated entry plus every aged entry past the first chunk behind.
   */
  @Test
  void deleteOlderThan_keepsDrainingWhenAConcurrentAttemptUpdateShortensAChunk() throws Exception {
    Instant old = Instant.now().minus(2, java.time.temporal.ChronoUnit.DAYS);
    int aged = 1500;
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO dead_letter_queue "
                    + "(command_id, command_type, payload, error_type, attempts, first_attempt_at,"
                    + " published_at) VALUES (?, 'Cmd', '{}'::jsonb, 'Err', 1, NOW(), ?)")) {
      for (int i = 0; i < aged; i++) {
        ps.setString(1, String.format("f1000000-0000-0000-0000-%012d", i));
        ps.setObject(2, java.sql.Timestamp.from(old.plusMillis(i)));
        ps.addBatch();
      }
      ps.executeBatch();
    }

    int deleted =
        RowLockRaces.sweepWhileAWriterHoldsARow(
            dataSource,
            "UPDATE dead_letter_queue SET dlq_attempts = dlq_attempts + 1, last_attempt_at = NOW()"
                + " WHERE command_id = ?",
            () -> dlq.deleteOlderThan(Instant.now()),
            String.format("f1000000-0000-0000-0000-%012d", 0));

    assertEquals(aged, deleted, "the concurrently updated entry is still aged and must be swept");
    assertEquals(0L, dlq.countPending(), "no aged entry may outlive the sweep");
  }

  private void insertWithPublishedAt(String commandId, Instant publishedAt) throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "INSERT INTO dead_letter_queue "
              + "(command_id, command_type, payload, error_type, attempts, first_attempt_at,"
              + " published_at) VALUES ('"
              + commandId
              + "', 'Cmd', '{}'::jsonb, 'Err', 1, NOW(), '"
              + java.sql.Timestamp.from(publishedAt)
              + "')");
    }
  }

  @Test
  void constructorRejectsANullDataSource() {
    assertThrows(IllegalArgumentException.class, () -> new PostgresDeadLetterQueue(null));
  }

  @Test
  void updateAttemptsPersistsDlqAttempts() {
    var cmdId = CommandId.of("60000000-0000-0000-0000-000000000001");
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Cmd",
            cmdId,
            TestStreams.stream("s1"),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));

    Instant retryTime = Instant.parse("2026-03-01T12:00:00Z");
    dlq.updateAttempts(cmdId, 2, retryTime);

    var entry = dlq.read(10).getFirst();
    assertEquals(2, entry.dlqAttempts());
    assertEquals(retryTime, entry.lastAttemptAt());
  }

  @Test
  void readRetryableReturnsOnlyEligibleEntries() {
    var cmd1 = CommandId.of("70000000-0000-0000-0000-000000000001");
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Eligible",
            cmd1,
            TestStreams.stream("s1"),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));

    var cmd2 = CommandId.of("70000000-0000-0000-0000-000000000002");
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Exhausted",
            cmd2,
            TestStreams.stream("s1"),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));
    dlq.updateAttempts(cmd2, 3, Instant.now());

    var retryable = dlq.readRetryable(3, 10);
    assertEquals(1, retryable.size());
    assertEquals("Eligible", retryable.getFirst().commandType());
  }

  @Test
  void readRetryableOrdersOldestFirst() throws Exception {
    var cmd1 = CommandId.of("80000000-0000-0000-0000-000000000001");
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "First",
            cmd1,
            TestStreams.stream("s1"),
            "Err",
            "first",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));
    Thread.sleep(10);
    var cmd2 = CommandId.of("80000000-0000-0000-0000-000000000002");
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Second",
            cmd2,
            TestStreams.stream("s1"),
            "Err",
            "second",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));

    var retryable = dlq.readRetryable(5, 10);
    assertEquals(2, retryable.size());
    assertEquals("First", retryable.get(0).commandType());
    assertEquals("Second", retryable.get(1).commandType());
  }

  @Test
  void readRetryableOrdersByLeastRecentlyActiveNotPublishOrder() throws Exception {
    // Ordering is COALESCE(last_attempt_at, published_at) ASC, not published_at
    // ASC.
    // A recently-retried entry (recent last_attempt_at) must sort AFTER a never-attempted entry
    // that
    // was published later, so a page of still-backing-off entries cannot head-of-line block due
    // entries behind them. Publish "Older" first, "Newer" second, then retry "Older" — its
    // last_attempt_at becomes the most recent activity of the two, so it must sort last.
    var older = CommandId.of("a1000000-0000-0000-0000-000000000001");
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Older",
            older,
            TestStreams.stream("s1"),
            "Err",
            "older",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));
    Thread.sleep(10);
    var newer = CommandId.of("a1000000-0000-0000-0000-000000000002");
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Newer",
            newer,
            TestStreams.stream("s1"),
            "Err",
            "newer",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));
    // Retry "Older": its last_attempt_at (an hour ahead of both publish times) is now the most
    // recent activity, so under COALESCE ordering it sorts last despite being published first.
    dlq.updateAttempts(older, 1, Instant.now().plus(java.time.Duration.ofHours(1)));

    var retryable = dlq.readRetryable(5, 10);
    assertEquals(2, retryable.size());
    assertEquals(
        "Newer",
        retryable.get(0).commandType(),
        "the never-attempted entry (older activity) sorts first");
    assertEquals(
        "Older",
        retryable.get(1).commandType(),
        "the recently-retried entry (recent last_attempt_at) sorts last, not by publish order");
  }

  @Test
  void readRetryableRespectsLimit() {
    for (int i = 0; i < 5; i++) {
      dlq.publish(
          new DeadLetterQueue.DeadLetterPublishRequest(
              "{}",
              "Cmd",
              CommandId.of("9000000" + i + "-0000-0000-0000-000000000000"),
              TestStreams.stream("s"),
              "Err",
              "msg",
              1,
              Instant.now(),
              null,
              null,
              null,
              null));
    }
    assertEquals(3, dlq.readRetryable(5, 3).size());
  }

  @Test
  void readIncludesDlqRetryFields() {
    var cmdId = CommandId.of("a0000000-0000-0000-0000-000000000001");
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Cmd",
            cmdId,
            TestStreams.stream("s1"),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));

    var entry = dlq.read(10).getFirst();
    assertEquals(0, entry.dlqAttempts());
    assertNull(entry.lastAttemptAt());
  }

  @Test
  void publishAndReadRoundTripsRequestContext() {
    var req =
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "PlaceOrder",
            CommandId.of("c0000000-0000-0000-0000-000000000001"),
            TestStreams.stream("order-9"),
            "Err",
            "msg",
            1,
            Instant.now(),
            CorrelationId.of("corr-ctx-1"),
            UserId.of("user-ctx-1"),
            TraceId.of("trace-ctx-1"),
            null);

    dlq.publish(req);

    var entry = dlq.read(10).getFirst();
    assertNotNull(entry.correlationId());
    assertEquals("corr-ctx-1", entry.correlationId().value());
    assertNotNull(entry.userId());
    assertEquals("user-ctx-1", entry.userId().value());
    assertNotNull(entry.traceId());
    assertEquals("trace-ctx-1", entry.traceId().value());
  }

  @Test
  void publishWithNullRequestContextReadsBackNull() {
    // A command that ran with no context bound persists null context and reads back null (not
    // blank value-type objects), so the runner replays it unbound.
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Cmd",
            CommandId.of("c0000000-0000-0000-0000-000000000002"),
            TestStreams.stream("s1"),
            "Err",
            "msg",
            1,
            Instant.now(),
            null,
            null,
            null,
            null));

    var entry = dlq.read(10).getFirst();
    assertNull(entry.correlationId());
    assertNull(entry.userId());
    assertNull(entry.traceId());
  }

  @Test
  void readRetryableIncludesRequestContext() {
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Cmd",
            CommandId.of("c0000000-0000-0000-0000-000000000003"),
            TestStreams.stream("s1"),
            "Err",
            "msg",
            1,
            Instant.now(),
            CorrelationId.of("corr-retry-1"),
            UserId.of("user-retry-1"),
            TraceId.of("trace-retry-1"),
            null));

    var entry = dlq.readRetryable(5, 10).getFirst();
    assertNotNull(entry.correlationId());
    assertEquals("corr-retry-1", entry.correlationId().value());
    assertEquals("user-retry-1", entry.userId().value());
    assertEquals("trace-retry-1", entry.traceId().value());
  }

  /**
   * Competing retry-schedulers guarantee: K schedulers run {@code SELECT ... FOR UPDATE SKIP
   * LOCKED} inside <em>overlapping</em> open transactions, so the row locks held by one are visible
   * to the others. Each must claim a disjoint slice of the retryable rows — no command_id is
   * returned to two schedulers at once.
   *
   * <p>Drives the production {@code SELECT_RETRYABLE} SQL directly because {@link
   * PostgresDeadLetterQueue#readRetryable} commits immediately (releasing its locks), which would
   * not keep the transactions overlapping long enough to observe SKIP LOCKED.
   */
  @Test
  void concurrentReadRetryableDoesNotReturnSameRowTwice() throws Exception {
    int total = 100;
    int threads = 6;
    int limitPerScheduler = 25;
    for (int i = 0; i < total; i++) {
      dlq.publish(
          new DeadLetterQueue.DeadLetterPublishRequest(
              "{}",
              "Retryable",
              CommandId.of(String.format("d0000000-0000-0000-0000-%012d", i)),
              TestStreams.stream("s"),
              "Err",
              "msg",
              1,
              Instant.now(),
              null,
              null,
              null,
              null));
    }

    // Mirrors PostgresDeadLetterQueue.SELECT_RETRYABLE: dlq_attempts gate + ORDER BY + SKIP LOCKED.
    String selectRetryable =
        "SELECT command_id FROM dead_letter_queue WHERE dlq_attempts < ? "
            + "ORDER BY COALESCE(last_attempt_at, published_at) ASC LIMIT ? FOR UPDATE SKIP LOCKED";

    var barrier = new CyclicBarrier(threads);
    // Per-thread claimed sets, so we can assert pairwise disjointness.
    var perThreadClaims = new ConcurrentHashMap<Integer, Set<String>>();
    var errors = new ConcurrentLinkedQueue<Throwable>();
    var workers = new ArrayList<Thread>();

    for (int t = 0; t < threads; t++) {
      final int tid = t;
      var worker =
          new Thread(
              () -> {
                Set<String> mine = new HashSet<>();
                perThreadClaims.put(tid, mine);
                try (var conn = dataSource.getConnection()) {
                  conn.setAutoCommit(false);
                  try (var ps = conn.prepareStatement(selectRetryable)) {
                    ps.setInt(1, 5); // maxRetries
                    ps.setInt(2, limitPerScheduler);
                    barrier.await(); // open all transactions, then run the locking SELECT together
                    try (var rs = ps.executeQuery()) {
                      while (rs.next()) {
                        mine.add(rs.getString("command_id"));
                      }
                    }
                    // Hold the transaction (and its row locks) open until every scheduler has run
                    // its SELECT, so the SKIP LOCKED visibility window actually overlaps.
                    barrier.await();
                  }
                  conn.commit();
                } catch (Throwable th) {
                  errors.add(th);
                }
              },
              "dlq-scheduler-" + t);
      workers.add(worker);
      worker.start();
    }
    for (Thread w : workers) {
      w.join(30_000);
    }

    assertTrue(errors.isEmpty(), "no scheduler should error: " + errors);

    var all = new ArrayList<String>();
    for (Set<String> claims : perThreadClaims.values()) {
      all.addAll(claims);
    }
    var unique = new HashSet<>(all);
    assertEquals(
        all.size(),
        unique.size(),
        "a row was returned to more than one scheduler (SKIP LOCKED visibility violated)");
    // With overlapping locks each scheduler claims a distinct slice; combined they take up to
    // threads * limit rows but never the same row twice.
    assertEquals(
        Math.min(total, threads * limitPerScheduler),
        unique.size(),
        "schedulers should collectively claim min(total, threads*limit) distinct rows");
  }
}
