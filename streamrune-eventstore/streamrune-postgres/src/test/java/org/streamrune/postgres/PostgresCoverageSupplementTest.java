package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.LockException;
import org.streamrune.core.types.EventType;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Supplementary coverage tests for {@link PgAdvisoryLocker} retry/timeout paths, {@link
 * PgNotificationListener} LISTEN loop, {@link PostgresEventStoreFactory} configuration, and {@link
 * PostgresEventStore.Builder} surface area.
 */
@Testcontainers
class PostgresCoverageSupplementTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_supp_test");

  static PGSimpleDataSource dataSource;

  record NoopEvent(String id) implements DomainEvent {}

  record NoopState(String id) implements AggregateState {}

  static final EventTypeRegistry TYPE_REGISTRY =
      new EventTypeRegistry() {
        @Override
        public Class<?> resolveEventType(EventType eventType) {
          return NoopEvent.class;
        }

        @Override
        public Class<?> resolveStateType(String stateType) {
          return NoopState.class;
        }

        @Override
        public java.util.Collection<Class<?>> registeredTypes() {
          return java.util.List.of(NoopEvent.class, NoopState.class);
        }
      };

  @BeforeAll
  static void initDataSource() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
  }

  // ---------- PgAdvisoryLocker ----------

  @Test
  void advisoryLockerRejectsNullDataSource() {
    assertThrows(IllegalArgumentException.class, () -> new PgAdvisoryLocker(null));
  }

  @Test
  void advisoryLockerAcquiresWithNullTimeoutUsingDefault() throws Exception {
    var locker = new PgAdvisoryLocker(dataSource);
    try (var ignored =
        (AutoCloseable) locker.acquireLock(TestStreams.stream("null-timeout"), null)) {
      assertNotNull(ignored);
    }
  }

  /**
   * A NEGATIVE {@code streamrune.lock-timeout} used to be silently replaced by this locker's
   * hardcoded 30-second default and then BLOCK, while {@code LocalStripedLocker} — the
   * auto-configured default — did {@code tryLock(negativeMillis)} and did not wait at all. One
   * knob, two behaviours, switched by which locker happened to be wired. The frozen rule is "zero
   * means do not wait; a negative duration is never a valid StreamRune value": both lockers now
   * reject it, and all three {@code StreamRuneConfigValidator}s reject it at boot so it never
   * reaches here.
   */
  @Test
  void advisoryLockerRejectsNegativeTimeout() {
    var locker = new PgAdvisoryLocker(dataSource);
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                locker.acquireLock(TestStreams.stream("negative-timeout"), Duration.ofSeconds(-5)));
    assertTrue(
        ex.getMessage().contains("negative"),
        "message must name the offending contract, got: " + ex.getMessage());
  }

  /** The other half of the frozen contract: {@link Duration#ZERO} is valid and means "try once". */
  @Test
  void advisoryLockerAcquiresWithZeroTimeoutWhenUncontended() throws Exception {
    var locker = new PgAdvisoryLocker(dataSource);
    try (var ignored =
        (AutoCloseable) locker.acquireLock(TestStreams.stream("zero-timeout"), Duration.ZERO)) {
      assertNotNull(ignored);
    }
  }

  @Test
  void advisoryLockerTimesOutWhenLockHeldByOtherSession() throws Exception {
    var locker = new PgAdvisoryLocker(dataSource);
    var aggregateId = TestStreams.stream("contended-aggregate");

    var holderReady = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var holderFailed = new AtomicBoolean(false);

    Thread holder =
        Thread.ofVirtual()
            .name("lock-holder")
            .start(
                () -> {
                  try (var _ = locker.acquireLock(aggregateId, Duration.ofSeconds(5))) {
                    holderReady.countDown();
                    release.await(10, TimeUnit.SECONDS);
                  } catch (Exception _) {
                    holderFailed.set(true);
                  }
                });

    try {
      assertTrue(holderReady.await(5, TimeUnit.SECONDS), "holder did not acquire");

      // Now try to acquire with a short timeout — should exhaust retries and throw
      long start = System.nanoTime();
      LockException ex =
          assertThrows(
              LockException.class, () -> locker.acquireLock(aggregateId, Duration.ofMillis(300)));
      long elapsedMs = (System.nanoTime() - start) / 1_000_000;
      assertTrue(
          ex.getMessage().contains("stream: test:contended-aggregate"),
          "message should include the stream: " + ex.getMessage());
      // Retry loop had to spin at least a few hundred ms before timing out.
      assertTrue(elapsedMs >= 200, "should have waited at least 200ms, was " + elapsedMs);
      // Upper bound guards against a runaway retry loop.
      assertTrue(elapsedMs < 3_000, "should have timed out well under 3s, was " + elapsedMs);
    } finally {
      release.countDown();
      holder.join(5000);
      assertFalse(holderFailed.get(), "holder thread failed");
    }
  }

  // ---------- PgNotificationListener ----------

  @Test
  void notificationListenerReceivesAndDispatches() throws Exception {
    var notified = new CountDownLatch(1);
    // Pool size 4: 1 for the LISTEN loop + 1 for NOTIFY + slack
    try (var hds = newHikari(4);
        var listener = new PgNotificationListener(hds, notified::countDown, "streamrune_events")) {
      listener.start();
      assertTrue(listener.isRunning());

      // Give the listener a moment to actually LISTEN on its connection before we notify.
      Thread.sleep(500);

      // Fire NOTIFY from a separate pooled connection — retry a few times in case the
      // listener thread hasn't issued LISTEN yet.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (notified.getCount() > 0 && System.nanoTime() < deadline) {
        try (var conn = hds.getConnection();
            var stmt = conn.createStatement()) {
          stmt.execute("NOTIFY streamrune_events, 'hello'");
        }
        if (notified.await(500, TimeUnit.MILLISECONDS)) {
          break;
        }
      }
      assertEquals(0, notified.getCount(), "listener did not receive NOTIFY in time");
    }
  }

  @Test
  void notificationListenerWithPlainDataSourceReceivesNotify() throws Exception {
    // Non-pooling DataSource path: used directly, no dedicated listen pool is created.
    var notified = new CountDownLatch(1);
    try (var listener =
        new PgNotificationListener(dataSource, notified::countDown, "streamrune_events")) {
      listener.start();

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (notified.getCount() > 0 && System.nanoTime() < deadline) {
        try (var conn = dataSource.getConnection();
            var stmt = conn.createStatement()) {
          stmt.execute("NOTIFY streamrune_events, 'plain'");
        }
        if (notified.await(500, TimeUnit.MILLISECONDS)) {
          break;
        }
      }
      assertEquals(0, notified.getCount(), "listener did not receive NOTIFY in time");
    }
  }

  @Test
  void notificationListenerDoesNotParkASharedPoolConnection() throws Exception {
    // With a pool of exactly one connection, the listener must not occupy the only slot —
    // it creates its own dedicated listen data source instead.
    var notified = new CountDownLatch(1);
    try (var hds = newHikari(1);
        var listener = new PgNotificationListener(hds, notified::countDown, "streamrune_events")) {
      listener.start();

      // Prove LISTEN is established before checking the slot: otherwise the check passes
      // vacuously when the listener has not connected yet. The NOTIFY goes through the plain
      // DataSource so the proof never borrows the one shared pool slot under test.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (notified.getCount() > 0 && System.nanoTime() < deadline) {
        try (var conn = dataSource.getConnection();
            var stmt = conn.createStatement()) {
          stmt.execute("NOTIFY streamrune_events, 'slot-probe'");
        }
        if (notified.await(500, TimeUnit.MILLISECONDS)) {
          break;
        }
      }
      assertEquals(0, notified.getCount(), "listener did not establish LISTEN in time");

      try (var conn = hds.getConnection()) {
        assertNotNull(conn, "shared pool slot must remain available while listener runs");
      }
    }
  }

  @Test
  void notificationListenerCloseIsIdempotentAndStopsRunning() throws Exception {
    try (var hds = newHikari(1)) {
      var listener = new PgNotificationListener(hds, () -> {}, "streamrune_events");
      listener.start();
      assertTrue(listener.isRunning());
      listener.close();
      assertFalse(listener.isRunning());
      // Second close should not throw
      assertDoesNotThrow(listener::close);
    }
  }

  @Test
  void notificationListenerRejectsInvalidChannelPattern() {
    try (var hds = newHikari(1)) {
      assertThrows(
          IllegalArgumentException.class,
          () -> new PgNotificationListener(hds, () -> {}, "invalid-channel-name!"));
    }
  }

  // ---------- PostgresEventStoreFactory ----------

  @Test
  void factoryCreatesEventStoreWithAutoSchema() {
    var factory = new PostgresEventStoreFactory(dataSource, TYPE_REGISTRY);
    EventStore store = factory.create();
    assertNotNull(store);

    // A second call builds another store on the same DataSource.
    EventStore store2 = factory.create();
    assertNotNull(store2);
  }

  @Test
  void factoryCreatesEventStoreWithUpcastersAndCrypto() {
    var upcaster =
        new EventUpcaster() {
          @Override
          public EventType eventType() {
            return new EventType("Noop");
          }

          @Override
          public int currentVersion() {
            return 2;
          }

          @Override
          public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
            return eventData;
          }
        };

    var factory = new PostgresEventStoreFactory(dataSource, TYPE_REGISTRY);
    factory.autoInitializeSchema(true).upcasters(List.of(upcaster)).cryptoEngine(null);
    EventStore store = factory.create();
    assertNotNull(store);
  }

  @Test
  void factoryCreatesProjectionRepository() {
    var factory = new PostgresEventStoreFactory(dataSource, TYPE_REGISTRY);
    factory.initializeSchema();
    var repo = factory.createProjectionRepository();
    assertNotNull(repo);
  }

  @Test
  void factoryBuildsOnAnApplicationHikariPool() {
    try (var hds = newHikari(4)) {
      EventStore store =
          new PostgresEventStoreFactory(hds, TYPE_REGISTRY).autoInitializeSchema(false).create();
      assertNotNull(store);
      assertNotNull(store.lastGlobalOffset());
    }
  }

  @Test
  void factoryRejectsNullConstructorArgs() {
    assertThrows(
        IllegalArgumentException.class, () -> new PostgresEventStoreFactory(null, TYPE_REGISTRY));
    assertThrows(
        IllegalArgumentException.class, () -> new PostgresEventStoreFactory(dataSource, null));
  }

  @Test
  void factoryRejectsInvalidStatementTimeout() {
    var factory = new PostgresEventStoreFactory(dataSource, TYPE_REGISTRY);
    assertThrows(IllegalArgumentException.class, () -> factory.statementTimeout(null));
    assertThrows(
        IllegalArgumentException.class, () -> factory.statementTimeout(Duration.ofSeconds(-1)));
    // Zero is valid: no bound of the store's own.
    factory.statementTimeout(Duration.ZERO);
  }

  /**
   * {@code Duration.ZERO} means "no bound", and a positive sub-millisecond value would truncate to
   * {@code statement_timeout = 0} — PostgreSQL's spelling of DISABLED. Asking for the tightest
   * possible statement bound must not remove the bound entirely, so such a value is refused.
   */
  @Test
  void factoryRejectsSubMillisecondStatementTimeout() {
    var factory = new PostgresEventStoreFactory(dataSource, TYPE_REGISTRY);
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> factory.statementTimeout(Duration.ofNanos(500_000)));
    assertTrue(
        ex.getMessage().contains("at least 1ms"),
        "message must name the millisecond floor, got: " + ex.getMessage());
    // The floor itself stays valid.
    factory.statementTimeout(Duration.ofMillis(1));
  }

  // ---------- PostgresEventStore.Builder ----------

  @Test
  void postgresEventStoreBuilderBuildsStore() throws Exception {
    // Ensure schema exists first.
    new PostgresEventStoreFactory(dataSource, TYPE_REGISTRY).initializeSchema();

    var store =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(TYPE_REGISTRY)
            .cryptoEngine(null)
            .upcasters(List.of())
            .build();
    assertNotNull(store);
  }

  @Test
  void postgresEventStoreConstructorRejectsNullArgs() {
    assertThrows(
        IllegalArgumentException.class,
        () -> PostgresEventStore.builder().dataSource(null).typeRegistry(TYPE_REGISTRY).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> PostgresEventStore.builder().dataSource(dataSource).typeRegistry(null).build());
  }

  // ---------- helpers ----------

  private static HikariDataSource newHikari(int maxPool) {
    var cfg = new HikariConfig();
    cfg.setJdbcUrl(PG.getJdbcUrl());
    cfg.setUsername(PG.getUsername());
    cfg.setPassword(PG.getPassword());
    cfg.setMaximumPoolSize(maxPool);
    cfg.setMinimumIdle(1);
    cfg.setPoolName("supp-" + System.nanoTime());
    return new HikariDataSource(cfg);
  }
}
