package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore.AppendResult;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Gate test for the gapless, fully-live global reader (the gapless-global-offset fix): {@link
 * PostgresEventStore#readGlobalStream(GlobalOffset, int)} does not gate reads on {@code
 * pg_snapshot_xmin(pg_current_snapshot())} — it selects committed rows above the checkpoint in
 * offset order and stops at the first non-contiguous offset inside a page, relying on the gapless
 * commit-ordered counter to guarantee that a committed offset N implies every offset below N is
 * committed.
 *
 * <p>The star property is liveness: an unrelated long-running transaction anywhere else in the
 * database must never delay delivery of already-committed events — the old {@code transaction_id <
 * pg_snapshot_xmin(...)} gate depended on the oldest in-flight transaction database-wide, so a long
 * transaction on an unrelated table stalled every reader. This class proves that stall is gone.
 */
@Testcontainers
class PostgresGaplessReaderIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_gapless_reader");

  static PGSimpleDataSource dataSource;
  static ObjectMapper objectMapper;
  static EventTypeRegistry typeRegistry;
  PostgresEventStore store;

  record TestEvent(String value) implements DomainEvent {}

  @BeforeAll
  static void initSchema() throws Exception {
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
            return java.util.List.of(TestEvent.class);
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
    // Test-only scaffolding table for unrelatedLongTransaction_doesNotDelayDelivery — a write
    // target unrelated to event_stream, so a long-running transaction against it exercises
    // "unrelated" concurrent activity without needing a real migration.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE IF NOT EXISTS gapless_reader_unrelated (note text)");
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    store = PostgresEventStore.builder().dataSource(dataSource).typeRegistry(typeRegistry).build();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      // RESTART IDENTITY + reset the application counter: every test in this class starts each
      // checkpoint at GlobalOffset.initial() (0) and expects offsets 1, 2, 3, ... to be
      // predictable. A plain TRUNCATE does not touch global_offset_sequence.
      stmt.execute("TRUNCATE TABLE event_stream RESTART IDENTITY");
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
      stmt.execute("TRUNCATE TABLE gapless_reader_unrelated");
    }
  }

  private EventEnvelope envelope(StreamId streamId, String value) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        Version.initial(),
        new EventType("TestEvent"),
        new TestEvent(value),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("gapless-reader"),
            null,
            null,
            Instant.now()));
  }

  private static final String RESERVE_OFFSET =
      "UPDATE global_offset_sequence SET next_value = next_value + 1 WHERE id = 1"
          + " RETURNING next_value";

  private static final String INSERT =
      "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version, event_type,"
          + " payload, metadata, schema_version)"
          + " VALUES (?, 'test', ?, 1, 'TestEvent', ?::jsonb, ?::jsonb, 1)";

  /**
   * Reserves the next global_offset (the same counter the production append path uses) and inserts
   * one event on the given (possibly uncommitted) connection; returns its assigned global_offset.
   * Used to hold an append open across a commit boundary, which the public {@code append} API
   * cannot do since it commits atomically.
   */
  private long insert(java.sql.Connection conn, String stream, String value) throws Exception {
    long offset;
    try (var res = conn.prepareStatement(RESERVE_OFFSET);
        var rs = res.executeQuery()) {
      rs.next();
      offset = rs.getLong(1);
    }
    String payload = objectMapper.writeValueAsString(new TestEvent(value));
    String metadata =
        objectMapper.writeValueAsString(
            new EventMetadata(
                IdGenerator.generateEventId(),
                IdGenerator.generateCommandId(),
                null,
                null,
                CorrelationId.of("gapless-reader"),
                null,
                null,
                Instant.now()));
    try (var ps = conn.prepareStatement(INSERT)) {
      ps.setLong(1, offset);
      ps.setString(2, stream);
      ps.setString(3, payload);
      ps.setString(4, metadata);
      ps.executeUpdate();
    }
    return offset;
  }

  @Test
  void checkpointBelowStreamStart_deliversFromFirstAvailableOffset() throws Exception {
    // A projection at checkpoint 0 whose stream's lowest committed offset is > 1 (offsets begin
    // above 1 — e.g. a restore or import left the counter ahead of the data, or the earliest rows
    // were deleted out of band) must still be delivered. The reader seeds its contiguity
    // expectation from the FIRST returned row, so the
    // legitimate gap between the checkpoint and where data resumes is paged over, not stalled.
    // Regression for the ProjectionCrashRecoveryIT stall.
    try (var conn = dataSource.getConnection();
        var st = conn.createStatement()) {
      st.execute("UPDATE global_offset_sequence SET next_value = 40 WHERE id = 1");
    }
    StreamId s1 = TestStreams.stream("gapless-reader-late-1");
    StreamId s2 = TestStreams.stream("gapless-reader-late-2");
    AppendResult r1 = store.append(s1, List.of(envelope(s1, "v1")), Version.initial()); // offset 41
    AppendResult r2 = store.append(s2, List.of(envelope(s2, "v1")), Version.initial()); // offset 42

    List<EventEnvelope> batch = store.readGlobalStream(GlobalOffset.of(0), 10);

    assertEquals(
        List.of(41L, 42L),
        batch.stream().map(e -> e.globalOffset().value()).toList(),
        "a checkpoint below the stream start must deliver from the first available offset, not"
            + " stall");
    assertEquals(41L, r1.globalOffsets().get(0).value());
    assertEquals(42L, r2.globalOffsets().get(0).value());
  }

  @Test
  void tailInFlightAppend_isWithheldThenDeliveredInOrder() throws Exception {
    StreamId s1 = TestStreams.stream("gapless-reader-1");
    StreamId s2 = TestStreams.stream("gapless-reader-2");
    store.append(s1, List.of(envelope(s1, "v1")), Version.initial()); // offset 1
    store.append(s2, List.of(envelope(s2, "v1")), Version.initial()); // offset 2

    try (var raw = dataSource.getConnection()) {
      raw.setAutoCommit(false);
      long offset3 = insert(raw, "gapless-reader-3", "v3"); // reserves offset 3, holds open
      assertEquals(3L, offset3, "the raw append must reserve the next dense offset");

      List<EventEnvelope> firstRead = store.readGlobalStream(GlobalOffset.of(0), 10);
      assertEquals(
          List.of(1L, 2L),
          firstRead.stream().map(e -> e.globalOffset().value()).toList(),
          "offset 3 must be withheld while its append is still uncommitted — reader never returns"
              + " it early and never skips it");

      raw.commit(); // offset 3 settles.
    }

    List<EventEnvelope> secondRead = store.readGlobalStream(GlobalOffset.of(2), 10);
    assertEquals(
        List.of(3L),
        secondRead.stream().map(e -> e.globalOffset().value()).toList(),
        "once committed, offset 3 is delivered exactly once, in order");
  }

  @Test
  void noMiddleHoleEverObserved() throws Exception {
    int writers = 6;
    int perWriter = 8;
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      CountDownLatch ready = new CountDownLatch(writers);
      CountDownLatch go = new CountDownLatch(1);
      List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();

      for (int w = 0; w < writers; w++) {
        int idx = w;
        futures.add(
            pool.submit(
                () -> {
                  ready.countDown();
                  try {
                    go.await();
                  } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                  }
                  for (int i = 0; i < perWriter; i++) {
                    StreamId streamId =
                        TestStreams.stream("gapless-reader-interleave-" + idx + "-" + i);
                    store.append(streamId, List.of(envelope(streamId, "v" + i)), Version.initial());
                  }
                }));
      }

      ready.await(10, TimeUnit.SECONDS);
      go.countDown();

      // Poll repeatedly WHILE the concurrent appends are still running: every batch returned must
      // be contiguous starting at afterOffset+1, and the checkpoint must only ever advance by
      // exactly the size of the batch it returned (no gap, no skip-ahead).
      List<Long> allDelivered = new CopyOnWriteArrayList<>();
      long checkpoint = 0;
      int totalExpected = writers * perWriter;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (allDelivered.size() < totalExpected && System.nanoTime() < deadline) {
        List<EventEnvelope> batch = store.readGlobalStream(GlobalOffset.of(checkpoint), 5);
        long expectedNext = checkpoint + 1;
        for (EventEnvelope e : batch) {
          assertEquals(
              expectedNext,
              e.globalOffset().value(),
              "every returned batch must be contiguous from afterOffset+1");
          expectedNext++;
        }
        if (!batch.isEmpty()) {
          checkpoint = batch.getLast().globalOffset().value();
          batch.forEach(e -> allDelivered.add(e.globalOffset().value()));
        } else {
          Thread.sleep(10);
        }
      }

      for (var f : futures) {
        f.get(10, TimeUnit.SECONDS);
      }

      // Drain whatever remains after all writers finished.
      List<EventEnvelope> tail = store.readGlobalStream(GlobalOffset.of(checkpoint), totalExpected);
      tail.forEach(e -> allDelivered.add(e.globalOffset().value()));

      assertEquals(totalExpected, allDelivered.size(), "every appended event must be delivered");
      for (int i = 0; i < allDelivered.size(); i++) {
        assertEquals(
            i + 1L,
            allDelivered.get(i),
            "the fully-drained sequence must be exactly 1..N, no gaps");
      }
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void unrelatedLongTransaction_doesNotDelayDelivery() throws Exception {
    try (var longConn = dataSource.getConnection()) {
      longConn.setAutoCommit(false);
      // An unrelated long-running WRITE transaction on a separate table, held open for the
      // duration of this test. Under the old xmin-gated READ_GLOBAL this would withhold delivery
      // of everything committed afterward, because the reader's watermark depended on the oldest
      // in-flight transaction ANYWHERE in the database, not just on event_stream. The gapless
      // contiguous-prefix reader must not have this liveness problem: it only cares about
      // contiguity of committed global_offset rows, never about unrelated open transactions.
      try (var stmt = longConn.createStatement()) {
        stmt.execute("INSERT INTO gapless_reader_unrelated (note) VALUES ('long-running')");
      }

      StreamId s1 = TestStreams.stream("gapless-reader-live-1");
      StreamId s2 = TestStreams.stream("gapless-reader-live-2");
      AppendResult r1 = store.append(s1, List.of(envelope(s1, "e1")), Version.initial());
      AppendResult r2 = store.append(s2, List.of(envelope(s2, "e2")), Version.initial());

      List<EventEnvelope> batch = store.readGlobalStream(GlobalOffset.of(0), 10);

      assertEquals(
          List.of(1L, 2L),
          batch.stream().map(e -> e.globalOffset().value()).toList(),
          "E1,E2 must be delivered immediately despite the unrelated open transaction — no added"
              + " latency, no withholding");
      assertTrue(
          r1.globalOffsets().get(0).value() == 1 && r2.globalOffsets().get(0).value() == 2,
          "sanity: the two appends took offsets 1 and 2");

      longConn.rollback();
    }
  }
}
