package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
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
import org.streamrune.core.OptimisticLockException;
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
 * Gate test for gapless, commit-ordered {@code global_offset} assignment: offsets are reserved from
 * {@code global_offset_sequence} inside the append transaction, so an aborted or version-conflicted
 * append releases its reservation on rollback and the next append reuses it — unlike a
 * sequence-backed default, which would burn an offset (a permanent hole) on every {@code UNIQUE
 * (aggregate_type, aggregate_id, version)} conflict.
 */
@Testcontainers
class PostgresGaplessOffsetIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_gapless");

  static PGSimpleDataSource dataSource;
  static ObjectMapper objectMapper;
  static EventTypeRegistry typeRegistry;
  PostgresEventStore store;

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
            return java.util.List.of(TestEvent.class);
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void setUp() throws Exception {
    store = PostgresEventStore.builder().dataSource(dataSource).typeRegistry(typeRegistry).build();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      // Deterministic 1-based offsets per test: reset the application counter too, since a plain
      // DELETE on event_stream does not touch global_offset_sequence.
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
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
            CorrelationId.of("gapless"),
            null,
            null,
            Instant.now()));
  }

  private List<Long> allOffsetsOrdered() throws Exception {
    List<Long> offsets = new ArrayList<>();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT global_offset FROM event_stream ORDER BY 1")) {
      while (rs.next()) {
        offsets.add(rs.getLong(1));
      }
    }
    return offsets;
  }

  @Test
  void concurrentCrossAggregateAppends_produceDenseContiguousOffsets() throws Exception {
    int n = 16;
    ExecutorService pool = Executors.newFixedThreadPool(n);
    try {
      CountDownLatch ready = new CountDownLatch(n);
      CountDownLatch go = new CountDownLatch(1);
      List<java.util.concurrent.Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < n; i++) {
        int idx = i;
        futures.add(
            pool.submit(
                () -> {
                  StreamId streamId = TestStreams.stream("gapless-cross-" + idx);
                  ready.countDown();
                  try {
                    go.await();
                  } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                  }
                  AppendResult result =
                      store.append(
                          streamId, List.of(envelope(streamId, "v" + idx)), Version.initial());
                  return result.globalOffsets().get(0).value();
                }));
      }

      ready.await(10, TimeUnit.SECONDS);
      go.countDown();

      Set<Long> assigned =
          futures.stream()
              .map(
                  f -> {
                    try {
                      return (Long) f.get(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  })
              .collect(Collectors.toSet());

      Set<Long> expected =
          java.util.stream.LongStream.rangeClosed(1, n).boxed().collect(Collectors.toSet());
      assertEquals(
          expected, assigned, "N concurrent cross-aggregate appends must yield exactly 1..N");
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void versionConflictReleasesOffset_noHole() throws Exception {
    StreamId streamS = TestStreams.stream("gapless-conflict-s");

    // v1 -> offset 1
    AppendResult first = store.append(streamS, List.of(envelope(streamS, "v1")), Version.initial());
    assertEquals(1L, first.globalOffsets().get(0).value());

    // Two concurrent appends both at expectedVersion=1: one wins (offset 2), one loses with
    // OptimisticLockException and must NOT burn an offset.
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      CountDownLatch ready = new CountDownLatch(2);
      CountDownLatch go = new CountDownLatch(1);
      AtomicReference<Long> winnerOffset = new AtomicReference<>();
      AtomicReference<Exception> loserException = new AtomicReference<>();

      var f1 =
          pool.submit(
              () -> {
                ready.countDown();
                await(go);
                try {
                  AppendResult r =
                      store.append(streamS, List.of(envelope(streamS, "v2-a")), new Version(1));
                  winnerOffset.set(r.globalOffsets().get(0).value());
                } catch (OptimisticLockException e) {
                  loserException.set(e);
                }
              });
      var f2 =
          pool.submit(
              () -> {
                ready.countDown();
                await(go);
                try {
                  AppendResult r =
                      store.append(streamS, List.of(envelope(streamS, "v2-b")), new Version(1));
                  winnerOffset.set(r.globalOffsets().get(0).value());
                } catch (OptimisticLockException e) {
                  loserException.set(e);
                }
              });

      ready.await(10, TimeUnit.SECONDS);
      go.countDown();
      f1.get(10, TimeUnit.SECONDS);
      f2.get(10, TimeUnit.SECONDS);

      assertNotNull(loserException.get(), "exactly one of the two concurrent appends must lose");
      assertEquals(
          2L, winnerOffset.get(), "the winner must take offset 2 — no offset burned by v1");
    } finally {
      pool.shutdownNow();
    }

    // A fresh append to another stream must get offset 3 — the loser's reservation was released.
    StreamId streamOther = TestStreams.stream("gapless-conflict-other");
    AppendResult third =
        store.append(streamOther, List.of(envelope(streamOther, "v1")), Version.initial());
    assertEquals(3L, third.globalOffsets().get(0).value());

    assertEquals(
        List.of(1L, 2L, 3L),
        allOffsetsOrdered(),
        "offsets must be exactly 1,2,3 — the loser burned nothing");
  }

  @Test
  void rolledBackReservation_isReusedByNextAppend() throws Exception {
    // Emulates a version-conflict rollback deterministically: manually reserve via
    // RESERVE_OFFSETS-equivalent SQL on a raw connection, then ROLLBACK — the reservation must be
    // undone so the next real append reuses the same offset instead of leaving a hole.
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try (var ps =
          conn.prepareStatement(
              "UPDATE global_offset_sequence SET next_value = next_value + 1 WHERE id = 1"
                  + " RETURNING next_value")) {
        try (var rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(1L, rs.getLong(1), "the manual reservation takes offset 1");
        }
      }
      conn.rollback();
    }

    StreamId streamId = TestStreams.stream("gapless-reuse");
    AppendResult result =
        store.append(streamId, List.of(envelope(streamId, "v1")), Version.initial());
    assertEquals(
        1L,
        result.globalOffsets().get(0).value(),
        "the rolled-back reservation must be reused, not skipped");
  }

  @Test
  void multiEventConflict_throwsOptimisticLock_andBurnsNoOffset() throws Exception {
    // Real aggregates emit 2+ events. On a version conflict the FIRST row of the batch
    // hits UNIQUE (aggregate_type, aggregate_id, version); pgjdbc surfaces the whole executeBatch
    // failure as a
    // BatchUpdateException. The store must map it to OptimisticLockException (retryable — the bus
    // retries it) NOT EventStoreException (which the bus dead-letters, and an unkeyed DLQ replay
    // then re-appends DUPLICATE domain events). Every pre-existing conflict test used a SINGLE
    // event; this locks the multi-event batch path.
    StreamId s = TestStreams.stream("multi-conflict-append");
    // Seed the stream to version 1 (offset 1).
    store.append(s, List.of(envelope(s, "v1")), Version.initial());

    // A 3-event batch at the SAME expectedVersion=initial: its first row (version 1) collides.
    assertThrows(
        OptimisticLockException.class,
        () ->
            store.append(
                s,
                List.of(envelope(s, "a"), envelope(s, "b"), envelope(s, "c")),
                Version.initial()),
        "a multi-event batch conflict must map to OptimisticLockException, not EventStoreException");

    // The failed batch reserved 3 offsets then rolled back — they must be REUSED, no hole burned.
    // A fresh append to another stream must get offset 2, and the global stream is dense 1,2.
    StreamId other = TestStreams.stream("multi-conflict-other");
    AppendResult next = store.append(other, List.of(envelope(other, "v1")), Version.initial());
    assertEquals(
        2L,
        next.globalOffsets().get(0).value(),
        "the conflicting batch must burn no global offset");
    assertEquals(
        List.of(1L, 2L),
        allOffsetsOrdered(),
        "offsets must be dense 1,2 — the conflict burned none");
  }

  @Test
  void concurrentAppends_underRepeatableReadDefault_stillSucceed() throws Exception {
    // append() pins its transaction to READ COMMITTED. Without that pin, a DataSource whose
    // connections default to REPEATABLE READ (a legitimate server/pool hardening choice) aborts a
    // blocked-then-unblocked append on the shared global_offset_sequence counter row with SQLState
    // 40001 (serialization_failure) — neither 23505 nor retryable — so it becomes an
    // EventStoreException and the command is dead-lettered under ANY append concurrency. With the
    // pin, concurrent cross-aggregate appends against a REPEATABLE READ DataSource still succeed
    // with dense 1..N offsets.
    PostgresEventStore rrStore =
        PostgresEventStore.builder()
            .dataSource(new RepeatableReadDataSource(dataSource))
            .typeRegistry(typeRegistry)
            .build();

    int n = 12;
    ExecutorService pool = Executors.newFixedThreadPool(n);
    try {
      CountDownLatch ready = new CountDownLatch(n);
      CountDownLatch go = new CountDownLatch(1);
      List<java.util.concurrent.Future<Long>> futures = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        int idx = i;
        futures.add(
            pool.submit(
                () -> {
                  StreamId streamId = TestStreams.stream("rr-cross-" + idx);
                  ready.countDown();
                  go.await();
                  AppendResult result =
                      rrStore.append(
                          streamId, List.of(envelope(streamId, "v" + idx)), Version.initial());
                  return result.globalOffsets().get(0).value();
                }));
      }
      ready.await(10, TimeUnit.SECONDS);
      go.countDown();

      Set<Long> assigned =
          futures.stream()
              .map(
                  f -> {
                    try {
                      return f.get(20, TimeUnit.SECONDS);
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  })
              .collect(Collectors.toSet());

      Set<Long> expected =
          java.util.stream.LongStream.rangeClosed(1, n).boxed().collect(Collectors.toSet());
      assertEquals(
          expected,
          assigned,
          "under a REPEATABLE READ DataSource, N concurrent appends must all succeed with dense 1..N"
              + " (the READ COMMITTED pin removes the isolation dependency)");
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void multiEventAppend_getsDenseBlock() throws Exception {
    StreamId streamId = TestStreams.stream("gapless-multi");
    AppendResult result =
        store.append(
            streamId,
            List.of(envelope(streamId, "a"), envelope(streamId, "b"), envelope(streamId, "c")),
            Version.initial());

    List<Long> offsets = result.globalOffsets().stream().map(GlobalOffset::value).toList();
    assertEquals(List.of(1L, 2L, 3L), offsets, "one append of 3 events yields a dense block");
  }

  private static void await(CountDownLatch go) {
    try {
      go.await();
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * A {@link DataSource} that hands out connections whose default isolation is REPEATABLE READ,
   * modeling a server ({@code default_transaction_isolation='repeatable read'}) or pool configured
   * that way. Every other call delegates to the wrapped source. Used to prove {@code
   * PostgresEventStore.append} pins READ COMMITTED per-transaction and so no longer depends on the
   * ambient default.
   */
  private static final class RepeatableReadDataSource implements javax.sql.DataSource {
    private final javax.sql.DataSource delegate;

    RepeatableReadDataSource(javax.sql.DataSource delegate) {
      this.delegate = delegate;
    }

    private static java.sql.Connection force(java.sql.Connection c) throws java.sql.SQLException {
      c.setTransactionIsolation(java.sql.Connection.TRANSACTION_REPEATABLE_READ);
      return c;
    }

    @Override
    public java.sql.Connection getConnection() throws java.sql.SQLException {
      return force(delegate.getConnection());
    }

    @Override
    public java.sql.Connection getConnection(String username, String password)
        throws java.sql.SQLException {
      return force(delegate.getConnection(username, password));
    }

    @Override
    public java.io.PrintWriter getLogWriter() throws java.sql.SQLException {
      return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(java.io.PrintWriter out) throws java.sql.SQLException {
      delegate.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws java.sql.SQLException {
      delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws java.sql.SQLException {
      return delegate.getLoginTimeout();
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
      return java.util.logging.Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws java.sql.SQLException {
      return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws java.sql.SQLException {
      return delegate.isWrapperFor(iface);
    }
  }
}
