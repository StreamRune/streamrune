package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * {@code event_stream} carries TWO unique constraints and only one of them is an
 * optimistic-concurrency conflict. {@code UNIQUE (aggregate_type, aggregate_id, version)} is the
 * version conflict a retry resolves; {@code event_stream_pkey} on the explicitly-assigned {@code
 * global_offset} is a desynced {@code global_offset_sequence} counter, which no retry can ever
 * resolve — the reservation is rolled back with the failed append, so every attempt reserves the
 * same already-committed offsets and fails identically.
 *
 * <p>Classifying the second as a retryable {@code OptimisticLockException} makes EVERY command in
 * the system burn its full retry budget with backoff before dead-lettering, and reports "version
 * conflict on stream X" for a stream that has no version conflict at all — the desynced counter
 * appears in no message, metric or DLQ row.
 *
 * <p>Reachable by restoring {@code event_stream} from a newer dump than {@code
 * global_offset_sequence}, promoting a logical replica whose two tables replicated with different
 * lag, or re-seeding the counter row by hand while traffic is live.
 */
@Testcontainers
class PostgresEventStoreGlobalOffsetDesyncIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("offset_desync");

  static PGSimpleDataSource dataSource;
  static ObjectMapper objectMapper;
  static EventTypeRegistry typeRegistry;

  PostgresEventStore store;
  PostgresCommandInbox commandInbox;

  record TestEvent(String value) implements DomainEvent {}

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return TestEvent.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return TestEvent.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return List.of(TestEvent.class);
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void setUp() throws Exception {
    commandInbox = new PostgresCommandInbox(dataSource);
    store =
        new PostgresEventStore(
            dataSource, objectMapper, typeRegistry, null, null, null, null, commandInbox);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("DELETE FROM command_inbox");
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
  }

  /** Rewinds the counter below max(global_offset) — the restored-from-mismatched-dump state. */
  private void desyncCounter() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
  }

  @Test
  void append_withADesyncedCounter_failsWithACounterDiagnosis_notAVersionConflict()
      throws Exception {
    StreamId seeded = TestStreams.stream("desync-seed");
    store.append(seeded, List.of(envelope(seeded, 1)), Version.initial()); // global_offset 1
    desyncCounter();

    StreamId fresh =
        TestStreams.stream("desync-fresh"); // brand new stream — no version conflict possible
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> store.append(fresh, List.of(envelope(fresh, 1)), Version.initial()));

    assertFalse(
        thrown instanceof OptimisticLockException,
        "a global_offset PRIMARY KEY collision is not an optimistic-concurrency conflict: the bus"
            + " retries OptimisticLockException, and every retry reserves the same used offsets");
    assertInstanceOf(EventStoreException.class, thrown);
    assertTrue(
        thrown.getMessage().contains("global_offset_sequence"),
        "the operator-facing message must name the desynced counter, not a version conflict on a"
            + " stream that has none — was: "
            + thrown.getMessage());
  }

  @Test
  void appendWithKey_withADesyncedCounter_failsWithTheSameCounterDiagnosis() throws Exception {
    StreamId seeded = TestStreams.stream("desync-keyed-seed");
    store.append(seeded, List.of(envelope(seeded, 1)), Version.initial());
    desyncCounter();

    StreamId fresh = TestStreams.stream("desync-keyed-fresh");
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                store.appendWithKey(
                    fresh,
                    List.of(envelope(fresh, 1)),
                    Version.initial(),
                    IdempotencyKey.of("desync-key-1"),
                    "TestCommand"));

    assertFalse(
        thrown instanceof OptimisticLockException,
        "the keyed append path must reach the same verdict as the unkeyed one");
    assertInstanceOf(EventStoreException.class, thrown);
    assertTrue(thrown.getMessage().contains("global_offset_sequence"), thrown.getMessage());
  }

  @Test
  void appendWithKey_withADesyncedCounter_leavesNoInboxClaimBehind() throws Exception {
    // The claim and the events share one transaction: a failed append must not leave the key
    // claimed, or the client's retry (once the counter is re-seeded) would replay against a row
    // whose events were never written.
    StreamId seeded = TestStreams.stream("desync-claim-seed");
    store.append(seeded, List.of(envelope(seeded, 1)), Version.initial());
    desyncCounter();

    StreamId fresh = TestStreams.stream("desync-claim-fresh");
    assertThrows(
        RuntimeException.class,
        () ->
            store.appendWithKey(
                fresh,
                List.of(envelope(fresh, 1)),
                Version.initial(),
                IdempotencyKey.of("desync-key-2"),
                "TestCommand"));

    assertTrue(
        commandInbox.find(IdempotencyKey.of("desync-key-2")).isEmpty(),
        "the rolled-back claim must not survive the failed append");
  }

  @Test
  void genuineVersionConflict_isStillARetryableOptimisticLockException() throws Exception {
    StreamId streamId = TestStreams.stream("real-conflict");
    store.append(streamId, List.of(envelope(streamId, 1)), Version.initial());

    assertThrows(
        OptimisticLockException.class,
        () -> store.append(streamId, List.of(envelope(streamId, 1)), Version.initial()),
        "UNIQUE (aggregate_type, aggregate_id, version) is the optimistic-concurrency check and stays retryable — "
            + "misclassifying it would dead-letter routine conflicts, and an unkeyed DLQ replay "
            + "would re-append duplicate domain events");
  }

  @Test
  void appendSucceedsAgainOnceTheCounterIsReSeeded() throws Exception {
    StreamId seeded = TestStreams.stream("desync-reseed-seed");
    store.append(seeded, List.of(envelope(seeded, 1)), Version.initial());
    desyncCounter();

    StreamId fresh = TestStreams.stream("desync-reseed-fresh");
    assertThrows(
        RuntimeException.class,
        () -> store.append(fresh, List.of(envelope(fresh, 1)), Version.initial()));

    // The remediation the message prescribes.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "UPDATE global_offset_sequence SET next_value = (SELECT COALESCE(max(global_offset), 0)"
              + " FROM event_stream) WHERE id = 1");
    }

    var result = store.append(fresh, List.of(envelope(fresh, 1)), Version.initial());
    assertEquals(
        2L,
        result.globalOffsets().get(0).value(),
        "after re-seeding, the next append continues the dense sequence");
  }

  @Test
  void schemaValidation_reportsAnErrorForACounterBehindMaxOffset() throws Exception {
    StreamId seeded = TestStreams.stream("desync-validator");
    store.append(seeded, List.of(envelope(seeded, 1)), Version.initial());

    assertTrue(
        SchemaValidator.validate(dataSource).isValid(),
        "a healthy counter must not be reported as drift");

    desyncCounter();

    var result = SchemaValidator.validate(dataSource);
    assertFalse(
        result.isValid(),
        "a desynced counter breaks every append, so it must fail boot rather than fail each"
            + " command with a misdiagnosed version conflict");
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i ->
                    i.severity() == SchemaIssue.Severity.ERROR
                        && "global_offset_sequence".equals(i.table())
                        && i.message().contains("BEHIND")),
        "the issue must name the counter and its remediation: " + result.issues());
  }

  @Test
  void schemaValidation_toleratesACounterAheadOfMaxOffset() throws Exception {
    // next_value > max(global_offset) is the normal in-flight state (a reservation taken but not
    // yet committed) and is also what an over-seeded counter looks like: it burns offsets but
    // breaks nothing.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("UPDATE global_offset_sequence SET next_value = 1000 WHERE id = 1");
    }
    assertTrue(SchemaValidator.validate(dataSource).isValid());
  }

  @Test
  void schemaValidation_toleratesAnAppendCommittingWhileTheCheckRuns() throws Exception {
    // The counter UPDATE and the event INSERT commit in ONE append transaction, so
    // the pair only ever advances together and next_value >= max(global_offset) holds at every
    // commit boundary. Reading them in two separate statements gives each its own READ COMMITTED
    // snapshot, and an append that commits in between makes the SECOND read see rows the FIRST
    // one could not: a healthy database reports "the offset counter is desynced" and boot is
    // refused, with a remediation that would re-seed a perfectly correct counter.
    //
    // This is the HA rolling-deploy shape: the booting replica validates while its sibling serves
    // writes. The window is milliseconds, so it presents as flaky boots under load rather than as
    // a reproducible failure — which is why it is pinned here deterministically: the append is
    // committed from inside the validator's own statement sequence, at exactly the instant the
    // race requires.
    StreamId seeded = TestStreams.stream("desync-race-seed");
    store.append(seeded, List.of(envelope(seeded, 1)), Version.initial());

    var fired = new java.util.concurrent.atomic.AtomicBoolean(false);
    var result = SchemaValidator.validate(appendingBetweenStatements(fired));

    assertTrue(fired.get(), "the concurrent append must have landed inside the check");
    assertTrue(
        result.isValid(),
        "a concurrent append is the healthy state, not counter drift — validation must not"
            + " refuse boot for it: "
            + result.issues());
  }

  /**
   * A {@link javax.sql.DataSource} facade that commits one real append transaction the first time
   * the validator queries {@code global_offset_sequence}, right after that statement's result has
   * been read. Deterministically places a concurrent writer in the window between the counter read
   * and the max-offset read.
   */
  private javax.sql.DataSource appendingBetweenStatements(
      java.util.concurrent.atomic.AtomicBoolean fired) {
    return (javax.sql.DataSource)
        java.lang.reflect.Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {javax.sql.DataSource.class},
            (proxy, method, args) -> {
              Object result = method.invoke(dataSource, args);
              return "getConnection".equals(method.getName())
                  ? wrapConnection((java.sql.Connection) result, fired)
                  : result;
            });
  }

  private java.sql.Connection wrapConnection(
      java.sql.Connection real, java.util.concurrent.atomic.AtomicBoolean fired) {
    return (java.sql.Connection)
        java.lang.reflect.Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {java.sql.Connection.class},
            (proxy, method, args) -> {
              Object result = method.invoke(real, args);
              return "createStatement".equals(method.getName())
                  ? wrapStatement((java.sql.Statement) result, fired)
                  : result;
            });
  }

  private java.sql.Statement wrapStatement(
      java.sql.Statement real, java.util.concurrent.atomic.AtomicBoolean fired) {
    return (java.sql.Statement)
        java.lang.reflect.Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {java.sql.Statement.class},
            (proxy, method, args) -> {
              Object result = method.invoke(real, args);
              if ("executeQuery".equals(method.getName())
                  && args != null
                  && args.length == 1
                  && String.valueOf(args[0]).contains("global_offset_sequence")
                  && fired.compareAndSet(false, true)) {
                commitOneConcurrentAppend();
              }
              return result;
            });
  }

  /** One append transaction exactly as {@code PostgresEventStore.append} shapes it. */
  private void commitOneConcurrentAppend() throws Exception {
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      long offset;
      try (var reserve =
          conn.prepareStatement(
              "UPDATE global_offset_sequence SET next_value = next_value + 1 WHERE id = 1"
                  + " RETURNING next_value")) {
        try (var rs = reserve.executeQuery()) {
          rs.next();
          offset = rs.getLong(1);
        }
      }
      try (var insert =
          conn.prepareStatement(
              "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version,"
                  + " event_type, payload, metadata, schema_version)"
                  + " VALUES (?, 'test', ?, ?, ?, ?::jsonb, ?::jsonb, ?)")) {
        insert.setLong(1, offset);
        insert.setString(2, "desync-race-concurrent");
        insert.setLong(3, offset);
        insert.setString(4, "TestEvent");
        insert.setString(5, "{\"value\":\"concurrent\"}");
        insert.setString(
            6,
            "{\"eventId\":\"evt-race\",\"commandId\":\"cmd-race\",\"correlationId\":\"corr-race\","
                + "\"timestamp\":\"2026-01-01T00:00:00Z\"}");
        insert.setInt(7, 1);
        insert.executeUpdate();
      }
      conn.commit();
    }
  }

  // --- classifier unit cases: the discrimination itself, without a database ---

  @Test
  void classifier_recognisesTheEventStreamPrimaryKeyThroughABatchUpdateException() {
    // pgjdbc links the failing sub-statement's PSQLException via getNextException(); the
    // BatchUpdateException's own SQLState may be null. A multi-event append always batches.
    BatchUpdateException batch =
        new BatchUpdateException("batch failed", null, new int[] {Statement.EXECUTE_FAILED});
    batch.setNextException(
        new SQLException(
            "ERROR: duplicate key value violates unique constraint \"event_stream_pkey\"",
            "23505"));
    assertTrue(PostgresEventStore.isGlobalOffsetCollision(batch));
  }

  @Test
  void classifier_ignoresTheVersionUniqueConstraint() {
    SQLException versionConflict =
        new SQLException(
            "ERROR: duplicate key value violates unique constraint"
                + " \"event_stream_stream_version_key\"",
            "23505");
    assertFalse(
        PostgresEventStore.isGlobalOffsetCollision(versionConflict),
        "the version conflict must keep its retryable classification");
    assertTrue(PostgresEventStore.isRetryableConflict(versionConflict));
  }

  @Test
  void classifier_ignoresAnUnidentifiable23505() {
    // Fail SAFE: when the constraint cannot be identified, the 23505 keeps today's retryable
    // classification. Calling it permanent would dead-letter a routine version conflict, and an
    // unkeyed DLQ replay of that would re-append duplicate domain events.
    assertFalse(PostgresEventStore.isGlobalOffsetCollision(new SQLException("dup", "23505")));
    assertTrue(PostgresEventStore.isRetryableConflict(new SQLException("dup", "23505")));
  }

  @Test
  void classifier_ignoresANonUniqueViolationMentioningTheConstraintName() {
    assertFalse(
        PostgresEventStore.isGlobalOffsetCollision(
            new SQLException("relation \"event_stream_pkey\" does not exist", "42P01")));
  }

  private EventEnvelope envelope(StreamId streamId, long version) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        new Version(version),
        new EventType("TestEvent"),
        new TestEvent("v" + version),
        new EventMetadata(
            EventId.of("evt-" + streamId.value() + "-" + version),
            CommandId.of("cmd-" + streamId.value() + "-" + version),
            null,
            null,
            CorrelationId.of("corr-1"),
            null,
            null,
            Instant.now()));
  }
}
