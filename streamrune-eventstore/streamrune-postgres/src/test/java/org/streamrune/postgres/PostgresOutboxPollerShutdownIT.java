package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_shutdown");

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
            if (entry.id().value().equals("e3")) {
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
