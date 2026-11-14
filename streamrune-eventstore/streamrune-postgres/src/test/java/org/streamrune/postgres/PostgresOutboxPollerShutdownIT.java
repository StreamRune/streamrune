package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.types.EventType;
import org.streamrune.runtime.OutboxPoller;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Stopping a relay in the middle of a batch against a real PostgreSQL store. The relay runs on its
 * virtual thread, a publish is waiting on the transport when {@link OutboxPoller#close()}
 * interrupts it, and the batch's outcome must still reach the database: on a virtual thread a JDBC
 * round trip made with the interrupt flag set fails at the socket.
 */
@Testcontainers
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class PostgresOutboxPollerShutdownIT {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_shutdown");

  /** Schema owner and verification path: a plain connection per call. */
  static PGSimpleDataSource plain;

  /** The pool the relay's store uses, configured as in production. */
  static HikariDataSource pool;

  @BeforeAll
  static void init() {
    plain = new PGSimpleDataSource();
    plain.setUrl(PG.getJdbcUrl());
    plain.setUser(PG.getUsername());
    plain.setPassword(PG.getPassword());
    var typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Object.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Object.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return List.of();
          }
        };
    new PostgresEventStoreFactory(plain, typeRegistry).create();

    var config = new HikariConfig();
    config.setJdbcUrl(PG.getJdbcUrl());
    config.setUsername(PG.getUsername());
    config.setPassword(PG.getPassword());
    config.setMaximumPoolSize(4);
    config.setPoolName("streamrune-outbox-shutdown-under-test");
    pool = new HikariDataSource(config);
  }

  @AfterAll
  static void closePool() {
    pool.close();
  }

  @BeforeEach
  void cleanTable() throws SQLException {
    try (var conn = plain.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM outbox_events");
    }
  }

  @Test
  void closeMidBatch_confirmedRowsAreDelivered_interruptedRowIsHeld_unattemptedRowsArePending()
      throws Exception {
    var store =
        new PostgresOutboxStore(
            pool, PostgresOutboxStore.DEFAULT_CLAIM_LEASE, OutboxOrderingMode.STRICT_PER_AGGREGATE);
    for (String id : List.of("e1", "e2", "e3", "e4", "e5")) {
      store.save(
          OutboxEntry.pending(
              OutboxEntryId.of(id), "{}", "Event", TestStreams.stream("agg-" + id)));
    }
    var parked = new CountDownLatch(1);
    var attempted = new CopyOnWriteArrayList<String>();
    OutboxPublisher publisher =
        new OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) throws Exception {
            attempted.add(entry.id().value());
            if ("e3".equals(entry.id().value())) {
              // Handed to the transport, waiting for its acknowledgement when close() comes.
              parked.countDown();
              new CountDownLatch(1).await();
            }
          }

          @Override
          public Duration inFlightHorizon() {
            return Duration.ofSeconds(10);
          }
        };
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(publisher)
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .build();

    poller.start();
    assertTrue(parked.await(10, TimeUnit.SECONDS), "the relay must reach e3");
    // As in production, the interrupted publish has been waiting a while: the pool's connections
    // are idle long enough to be validated on the next borrow.
    Thread.sleep(700);
    poller.close();

    assertFalse(poller.isAlive(), "the poll thread has stopped");
    assertEquals(List.of("e1", "e2", "e3"), attempted);
    assertEquals("DELIVERED", status("e1"));
    assertEquals("DELIVERED", status("e2"));
    assertEquals("IN_PROGRESS", status("e3"), "e3 may still be delivered: its claim is held");
    assertEquals("0", scalar("SELECT attempts FROM outbox_events WHERE entry_id = 'e3'"));
    assertEquals("PENDING", status("e4"));
    assertEquals("PENDING", status("e5"));
    assertEquals(0, poller.consecutiveFailures(), "stopping is not a failed cycle");
  }

  @Test
  void aBacklogOfSeveralBatches_isDeliveredInOnePollCycle_inStreamOrder() throws Exception {
    var store =
        new PostgresOutboxStore(
            pool, PostgresOutboxStore.DEFAULT_CLAIM_LEASE, OutboxOrderingMode.STRICT_PER_AGGREGATE);
    // 30 streams with two entries each: six full batches of ten under one-head-per-stream claims.
    for (int s = 0; s < 30; s++) {
      for (int n = 1; n <= 2; n++) {
        store.save(
            OutboxEntry.pending(
                OutboxEntryId.of("s" + s + "-" + n),
                "{}",
                "Event",
                TestStreams.stream("agg-" + s)));
      }
    }
    var published = new CopyOnWriteArrayList<String>();
    var batches = new CopyOnWriteArrayList<Integer>();
    OutboxPublisher publisher =
        new OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) {
            published.add(entry.id().value());
          }

          @Override
          public BatchResult publishBatch(
              List<OutboxEntry> entries,
              java.time.Instant deadline,
              org.streamrune.core.outbox.OutboxEntrySpans spans) {
            batches.add(entries.size());
            return OutboxPublisher.super.publishBatch(entries, deadline, spans);
          }
        };
    // A poll interval far longer than the test: everything must go out in the first cycle.
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(publisher)
            .batchSize(10)
            .pollInterval(Duration.ofMinutes(10))
            .build();

    poller.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
      while (!"60".equals(scalar("SELECT count(*) FROM outbox_events WHERE status = 'DELIVERED'"))
          && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
    } finally {
      poller.close();
    }

    assertEquals("60", scalar("SELECT count(*) FROM outbox_events WHERE status = 'DELIVERED'"));
    assertEquals(List.of(10, 10, 10, 10, 10, 10), batches);
    for (int s = 0; s < 30; s++) {
      assertTrue(
          published.indexOf("s" + s + "-1") < published.indexOf("s" + s + "-2"),
          "stream agg-" + s + " is delivered in order");
    }
  }

  /** A publisher that confirms every entry at once and records the order it saw them in. */
  private static OutboxPublisher recordingPublisher(List<String> published) {
    return new OutboxPublisher() {
      @Override
      public void publish(OutboxEntry entry) {
        published.add(entry.id().value());
      }
    };
  }

  /**
   * Writes {@code perStream} entries in each of {@code streams} streams in one statement, ids
   * {@code s<stream>-<n>}, the n-th entry of every stream before the (n+1)-th of any.
   */
  private static void insertBacklog(int streams, int perStream) throws SQLException {
    try (var conn = plain.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO outbox_events (entry_id, payload, payload_type, aggregate_type,"
                    + " aggregate_id) SELECT 's' || s || '-' || n, '{}'::jsonb, 'Event', ?,"
                    + " 'agg-' || s FROM generate_series(1, ?) AS n,"
                    + " generate_series(0, ? - 1) AS s ORDER BY n, s")) {
      ps.setString(1, TestStreams.TYPE.value());
      ps.setInt(2, perStream);
      ps.setInt(3, streams);
      ps.executeUpdate();
    }
  }

  /** Whether every stream's entries were published in order, each exactly once. */
  private static void assertEachStreamInOrderOnce(List<String> published, int streams, int each) {
    assertEquals(streams * each, published.size(), "every entry is published");
    assertEquals(streams * each, Set.copyOf(published).size(), "no entry is published twice");
    for (int s = 0; s < streams; s++) {
      for (int n = 2; n <= each; n++) {
        assertTrue(
            published.indexOf("s" + s + "-" + (n - 1)) < published.indexOf("s" + s + "-" + n),
            "stream agg-" + s + " is delivered in order");
      }
    }
  }

  private static void awaitDelivered(int expected, Duration atMost) throws Exception {
    long deadline = System.nanoTime() + atMost.toNanos();
    while (!String.valueOf(expected).equals(countWithStatus("DELIVERED"))
        && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
  }

  private static String countWithStatus(String status) throws SQLException {
    return scalar("SELECT count(*) FROM outbox_events WHERE status = '" + status + "'");
  }

  @Test
  void aQueueOnOneStream_isDeliveredInOnePollCycle_inStreamOrder() throws Exception {
    var store =
        new PostgresOutboxStore(
            pool, PostgresOutboxStore.DEFAULT_CLAIM_LEASE, OutboxOrderingMode.STRICT_PER_AGGREGATE);
    insertBacklog(1, 30);
    var published = new CopyOnWriteArrayList<String>();
    // A poll interval far longer than the test: every entry must go out in the first cycle,
    // although each claim finds one entry, the stream's head.
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(recordingPublisher(published))
            .batchSize(10)
            .pollInterval(Duration.ofMinutes(10))
            .build();

    poller.start();
    try {
      awaitDelivered(30, Duration.ofSeconds(20));
    } finally {
      poller.close();
    }

    assertEquals("30", countWithStatus("DELIVERED"));
    assertEachStreamInOrderOnce(published, 1, 30);
  }

  @Test
  void twoRelaysDrainingOneChannel_keepEveryStreamInOrder_andPublishEachEntryOnce()
      throws Exception {
    insertBacklog(50, 20);
    var published = Collections.synchronizedList(new ArrayList<String>());
    var relays = new ArrayList<OutboxPoller>();
    for (int r = 0; r < 2; r++) {
      relays.add(
          OutboxPoller.builder()
              .outboxStore(
                  new PostgresOutboxStore(
                      pool,
                      PostgresOutboxStore.DEFAULT_CLAIM_LEASE,
                      OutboxOrderingMode.STRICT_PER_AGGREGATE))
              .publisher(recordingPublisher(published))
              .batchSize(20)
              .pollInterval(Duration.ofMillis(100))
              .build());
    }

    relays.forEach(OutboxPoller::start);
    try {
      awaitDelivered(1000, Duration.ofSeconds(30));
    } finally {
      relays.forEach(OutboxPoller::close);
    }

    assertEquals("1000", countWithStatus("DELIVERED"));
    assertEquals("0", countWithStatus("IN_PROGRESS"));
    assertEachStreamInOrderOnce(List.copyOf(published), 50, 20);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void closingTheRelayAnywhereInADrain_leavesNoEntryClaimed() throws Exception {
    // On a strict backlog the claim is most of each batch, so a stop at a random moment of a drain
    // usually lands in a claim. Whatever the claim had committed must be released, not left
    // claimed until the lease expires.
    var random = new Random();
    int stoppedMidDrain = 0;
    for (int iteration = 0; iteration < 10; iteration++) {
      cleanTable();
      insertBacklog(400, 5);
      var store =
          new PostgresOutboxStore(
              pool,
              PostgresOutboxStore.DEFAULT_CLAIM_LEASE,
              OutboxOrderingMode.STRICT_PER_AGGREGATE);
      var poller =
          OutboxPoller.builder()
              .outboxStore(store)
              .publisher(recordingPublisher(new CopyOnWriteArrayList<>()))
              .batchSize(50)
              .pollInterval(Duration.ofMinutes(10))
              .build();

      poller.start();
      try {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while ("0".equals(countWithStatus("DELIVERED")) && System.nanoTime() < deadline) {
          Thread.onSpinWait();
        }
        Thread.sleep(random.nextInt(40));
      } finally {
        poller.close();
      }

      assertFalse(poller.isAlive(), "the poll thread has stopped");
      assertEquals(
          "0",
          countWithStatus("IN_PROGRESS"),
          "iteration " + iteration + ": no entry stays claimed");
      if (!"2000".equals(countWithStatus("DELIVERED"))) {
        stoppedMidDrain++;
      }
    }
    assertTrue(stoppedMidDrain > 0, "at least one stop landed in the middle of a drain");
  }

  private static String status(String id) throws SQLException {
    return scalar("SELECT status FROM outbox_events WHERE entry_id = '" + id + "'");
  }

  private static String scalar(String sql) throws SQLException {
    try (var conn = plain.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(sql)) {
      return rs.next() ? rs.getString(1) : null;
    }
  }
}
