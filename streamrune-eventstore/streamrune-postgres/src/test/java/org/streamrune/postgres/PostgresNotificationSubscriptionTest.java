package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Testcontainers-backed tests for {@link PostgresNotificationSubscription}. Covers LISTEN/NOTIFY
 * lifecycle, {@code createListenDataSource} credential extraction, and constructor validation.
 */
@Testcontainers
class PostgresNotificationSubscriptionTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_notify_test");

  record Added(String sku) implements DomainEvent {}

  record CartState(int n) implements AggregateState {}

  static HikariDataSource eventDataSource;
  static ObjectMapper objectMapper;
  static EventTypeRegistry typeRegistry;
  PostgresEventStore store;

  private static final SubscriptionConfig FAST_CONFIG =
      new SubscriptionConfig(true, Duration.ofMillis(100), Duration.ofMillis(10));

  @BeforeAll
  static void initSchema() throws Exception {
    var config = new HikariConfig();
    config.setJdbcUrl(PG.getJdbcUrl());
    config.setUsername(PG.getUsername());
    config.setPassword(PG.getPassword());
    config.setMaximumPoolSize(4);
    eventDataSource = new HikariDataSource(config);

    objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Added.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return CartState.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return java.util.List.of(Added.class, CartState.class);
          }
        };

    // The shipped event-store baseline, applied the way the factory applies it.
    org.flywaydb.core.Flyway.configure()
        .dataSource(eventDataSource)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
  }

  @AfterAll
  static void closePool() {
    if (eventDataSource != null) eventDataSource.close();
  }

  @BeforeEach
  void setUp() throws Exception {
    store =
        PostgresEventStore.builder().dataSource(eventDataSource).typeRegistry(typeRegistry).build();
    try (var conn = eventDataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("DELETE FROM projection_offset");
      // Deterministic 1-based offsets per test: a plain DELETE on event_stream does not reset
      // global_offset_sequence, and readGlobalStream's contiguous-prefix guard requires an
      // accurate checkpoint — tests here read from GlobalOffset.initial() (0) expecting freshly
      // appended events to start at offset 1.
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
  }

  private EventEnvelope envelope(StreamId s, long v) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        s,
        new Version(v),
        new EventType("Added"),
        new Added("X"),
        new EventMetadata(
            EventId.of("evt_" + v),
            CommandId.of("cmd_" + v),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now()));
  }

  static class InMemoryOffsetStore implements OffsetStore {
    final ConcurrentHashMap<String, GlobalOffset> offsets = new ConcurrentHashMap<>();

    @Override
    public GlobalOffset getLastOffset(ProjectionName n) {
      return offsets.getOrDefault(n.value(), GlobalOffset.initial());
    }

    @Override
    public void saveOffset(ProjectionName n, GlobalOffset o) {
      offsets.put(n.value(), o);
    }
  }

  /**
   * Proves REAL server-side LISTEN/NOTIFY end to end: appending an event must fire {@code
   * pg_notify('streamrune_events', …)} from {@link PostgresEventStore}'s post-commit hook, the
   * subscription's {@code LISTEN streamrune_events} connection must receive it, and delivery must
   * happen within a short bounded wait — <em>without</em> any in-process shortcut. There is
   * deliberately NO {@code sub.notifyNewEvents()} call here: that in-process flag would fire the
   * poll directly and make this test pass even if the server NOTIFY channel were renamed or the
   * post-commit NOTIFY hook were removed.
   *
   * <p><b>Closing the catch-up loophole.</b> The subscription runs exactly ONE catch-up poll right
   * after LISTEN is established, then delivers only when a notification arrives (there is no
   * periodic poll fallback). Merely appending "after {@code isRunning()}" is not enough: {@code
   * isRunning()} flips true when the connection opens, which can be slightly before that catch-up
   * poll runs — so a late catch-up poll could still deliver the asserted event even if server
   * NOTIFY were broken. To eliminate that race, a <em>warm-up</em> event is appended BEFORE {@code
   * start()} (its NOTIFY fires before LISTEN exists, so it is lost and can ONLY reach us via the
   * catch-up poll). Observing the warm-up's delivery is a hard barrier proving the catch-up poll
   * has already run and completed. The asserted event is appended strictly AFTER that barrier, so
   * its delivery can ONLY come via a live server-side NOTIFY — this test FAILS if the channel/hook
   * is broken.
   *
   * <p>In-process-shortcut and catch-up coverage live in separate tests ({@link
   * #notifyNewEventsInProcessShortcutTriggersDelivery} and {@link
   * #startCatchesUpOnEventsAppendedWhileDown}).
   */
  @Test
  void serverSideNotifyTriggersDelivery() throws Exception {
    HikariDataSource listenDs =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource);
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var offsetStore = new InMemoryOffsetStore();
    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("test-sub"))
            .listenDataSource(listenDs)
            .eventStore(store)
            .offsetStore(offsetStore)
            .listener(received::addAll)
            .config(FAST_CONFIG)
            .fetchSize(10)
            .build();

    // Warm-up event appended BEFORE start(): its NOTIFY fires before the LISTEN connection exists,
    // so it is lost and can reach the subscription ONLY through the start-time catch-up poll.
    StreamId warmup = TestStreams.stream("cart-notify-warmup");
    store.append(warmup, List.of(envelope(warmup, 1)), Version.initial());

    sub.start();
    try {
      // Barrier: wait until the single catch-up poll delivers the warm-up event. Once seen, the
      // catch-up poll has run to completion, so NOTHING but a live server NOTIFY can drive the next
      // delivery (there is no periodic poll and no in-process shortcut is called here).
      long catchupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (received.isEmpty() && System.nanoTime() < catchupDeadline) {
        Thread.sleep(20);
      }
      assertEquals(
          1, received.size(), "catch-up poll on start must deliver the pre-start warm-up event");
      assertEquals(TestStreams.stream("cart-notify-warmup"), received.get(0).streamId());

      // Now append the asserted event, strictly AFTER the catch-up poll has completed. Its only
      // possible delivery path is the server-side pg_notify('streamrune_events', …) from the
      // post-commit hook reaching the live LISTEN connection.
      StreamId s = TestStreams.stream("cart-notify-1");
      store.append(s, List.of(envelope(s, 1)), Version.initial());
      // NO belt-and-braces sub.notifyNewEvents() — delivery must come from the real server NOTIFY.

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (received.size() < 2 && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      assertEquals(
          2,
          received.size(),
          "the post-catch-up event must be delivered via the server-side LISTEN/NOTIFY round-trip"
              + " (pg_notify on the 'streamrune_events' channel from the post-commit hook) — the"
              + " only remaining delivery path once the catch-up poll has completed");
      assertEquals(TestStreams.stream("cart-notify-1"), received.get(1).streamId());
      // Offset should have been advanced by pollAndDeliver
      assertNotEquals(
          GlobalOffset.initial(), offsetStore.getLastOffset(ProjectionName.of("test-sub")));
    } finally {
      sub.close();
    }
    // close() should also close the listen data source
    assertTrue(listenDs.isClosed(), "listen data source must be closed after sub.close()");
  }

  /**
   * Keeps coverage of the in-process {@link PostgresNotificationSubscription#notifyNewEvents()}
   * shortcut (the append-path fast trigger that does not wait for a server NOTIFY). This is the
   * legitimate half of the old belt-and-braces test, split out so the real-NOTIFY test above can
   * fail if server-side delivery breaks. Here the shortcut alone must drive delivery.
   */
  @Test
  void notifyNewEventsInProcessShortcutTriggersDelivery() throws Exception {
    HikariDataSource listenDs =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource);
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var offsetStore = new InMemoryOffsetStore();
    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("shortcut-sub"))
            .listenDataSource(listenDs)
            .eventStore(store)
            .offsetStore(offsetStore)
            .listener(received::addAll)
            .config(FAST_CONFIG)
            .fetchSize(10)
            .build();

    sub.start();
    try {
      long start = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
      while (!sub.isRunning() && System.nanoTime() < start) {
        Thread.sleep(20);
      }

      StreamId s = TestStreams.stream("cart-shortcut-1");
      store.append(s, List.of(envelope(s, 1)), Version.initial());
      // Drive delivery purely through the in-process flag.
      sub.notifyNewEvents();

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (received.isEmpty() && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      assertEquals(1, received.size(), "the in-process notifyNewEvents() shortcut must deliver");
      assertEquals(TestStreams.stream("cart-shortcut-1"), received.get(0).streamId());
    } finally {
      sub.close();
    }
  }

  @Test
  void startCatchesUpOnEventsAppendedWhileDown() throws Exception {
    // Append BEFORE the subscription starts — its NOTIFY is gone forever. The catch-up poll
    // on start must deliver it anyway, without any notification arriving.
    StreamId s = TestStreams.stream("cart-catchup-1");
    store.append(s, List.of(envelope(s, 1)), Version.initial());

    HikariDataSource listenDs =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource);
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var offsetStore = new InMemoryOffsetStore();
    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("catchup-sub"))
            .listenDataSource(listenDs)
            .eventStore(store)
            .offsetStore(offsetStore)
            .listener(received::addAll)
            .config(FAST_CONFIG)
            .fetchSize(10)
            .build();

    sub.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (received.isEmpty() && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      assertEquals(1, received.size(), "catch-up poll on start should deliver the missed event");
      assertEquals(TestStreams.stream("cart-catchup-1"), received.get(0).streamId());
    } finally {
      sub.close();
    }
  }

  /**
   * One append of N &gt; fetchSize events fires exactly ONE {@code pg_notify}. The subscription
   * must DRAIN the whole backlog on that single notification, not deliver only the first {@code
   * fetchSize} page and strand the rest until some later append happens to notify again (which may
   * never come). A deliberately long polling interval keeps the periodic fallback dormant so the
   * ONLY delivery path for the burst is the single server-side NOTIFY driving the drain — this test
   * fails against the pre-fix single-page {@code pollAndDeliver}.
   */
  @Test
  void oneNotificationDrainsBacklogLargerThanFetchSize() throws Exception {
    int fetchSize = 5;
    int burst = 12; // spans three pages: 5 + 5 + 2
    // Long polling interval: the periodic fallback must NOT fire within the test window, so the
    // burst can only be delivered by the single server NOTIFY draining to the head.
    var slowPoll = new SubscriptionConfig(true, Duration.ofSeconds(60), Duration.ZERO);

    HikariDataSource listenDs =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource);
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var offsetStore = new InMemoryOffsetStore();
    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("drain-sub"))
            .listenDataSource(listenDs)
            .eventStore(store)
            .offsetStore(offsetStore)
            .listener(received::addAll)
            .config(slowPoll)
            .fetchSize(fetchSize)
            .build();

    // Warm-up appended BEFORE start(): its NOTIFY is lost, so it can only reach us via the
    // start-time
    // catch-up poll. Observing it is a barrier proving catch-up completed — so the burst below is
    // delivered strictly by the live server NOTIFY, not by the catch-up poll.
    StreamId warmup = TestStreams.stream("drain-warmup");
    store.append(warmup, List.of(envelope(warmup, 1)), Version.initial());

    sub.start();
    try {
      long barrier = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (received.isEmpty() && System.nanoTime() < barrier) {
        Thread.sleep(20);
      }
      assertEquals(1, received.size(), "catch-up poll must deliver the pre-start warm-up event");

      // A single append of `burst` events → exactly ONE pg_notify on 'streamrune_events'.
      StreamId s = TestStreams.stream("drain-burst");
      var batch = new java.util.ArrayList<EventEnvelope>();
      for (int v = 1; v <= burst; v++) {
        batch.add(envelope(s, v));
      }
      store.append(s, batch, Version.initial());

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
      while (received.size() < 1 + burst && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      assertEquals(
          1 + burst,
          received.size(),
          "one NOTIFY must drain the entire burst to the head (warm-up + all "
              + burst
              + " burst events), not just the first fetchSize page");
      assertEquals(
          GlobalOffset.of(1 + burst),
          offsetStore.getLastOffset(ProjectionName.of("drain-sub")),
          "offset must advance to the true tail after draining");
    } finally {
      sub.close();
    }
  }

  /**
   * {@code pg_notify} is best-effort — a dropped, failed, or coalesced NOTIFY has no other recovery
   * trigger. The subscription must poll periodically as a fallback. Here the subscription reads
   * from an in-memory event store (whose appends never fire any server NOTIFY), so with no
   * notification ever arriving the events can be delivered ONLY by the periodic fallback poll —
   * which must also drain past {@code fetchSize}. Fails against the pre-fix code, which had no
   * periodic poll at all.
   */
  @Test
  void periodicPollFallbackDeliversWhenNoNotificationArrives() throws Exception {
    int fetchSize = 5;
    int burst = 12;
    var fastPoll = new SubscriptionConfig(true, Duration.ofMillis(200), Duration.ZERO);

    var memStore = new MutableInMemoryEventStore();
    // Warm-up seeded BEFORE start: the catch-up poll delivers it (barrier that catch-up completed).
    memStore.append(1, TestStreams.stream("fallback-warmup"));

    HikariDataSource listenDs =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource);
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var offsetStore = new InMemoryOffsetStore();
    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("fallback-sub"))
            .listenDataSource(listenDs)
            .eventStore(memStore)
            .offsetStore(offsetStore)
            .listener(received::addAll)
            .config(fastPoll)
            .fetchSize(fetchSize)
            .build();

    sub.start();
    try {
      long barrier = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (received.isEmpty() && System.nanoTime() < barrier) {
        Thread.sleep(20);
      }
      assertEquals(1, received.size(), "catch-up poll must deliver the pre-start warm-up event");

      // Add the burst AFTER catch-up, WITHOUT any notification (in-memory store fires no pg_notify
      // and we never call notifyNewEvents). Only the periodic fallback poll can deliver these.
      for (int v = 1; v <= burst; v++) {
        memStore.append(1 + v, TestStreams.stream("fallback-burst"));
      }

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
      while (received.size() < 1 + burst && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      assertEquals(
          1 + burst,
          received.size(),
          "the periodic poll fallback must catch up (and drain) all events even with no NOTIFY");
    } finally {
      sub.close();
    }
  }

  @Test
  void createListenDataSourceCopiesHikariCredentials() {
    try (HikariDataSource listen =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource)) {
      assertEquals(eventDataSource.getJdbcUrl(), listen.getJdbcUrl());
      assertEquals(eventDataSource.getUsername(), listen.getUsername());
      assertEquals(1, listen.getMaximumPoolSize());
      assertEquals(1, listen.getMinimumIdle());
      assertEquals("streamrune-listen", listen.getPoolName());
    }
  }

  @Test
  void createListenDataSourceTurnsOnTcpKeepAliveAndInheritsTheMainPoolsDriverProperties() {
    // Driver properties set on the main pool (TLS, application name, timeouts) must reach the
    // LISTEN
    // connection too, and the LISTEN connection — idle for long stretches — asks the OS to probe
    // its peer.
    try (var main = new HikariDataSource()) {
      main.setJdbcUrl(PG.getJdbcUrl());
      main.setUsername(PG.getUsername());
      main.setPassword(PG.getPassword());
      main.addDataSourceProperty("ApplicationName", "orders-app");
      try (HikariDataSource listen =
          PostgresNotificationSubscription.createListenDataSource(main)) {
        assertEquals("true", listen.getDataSourceProperties().getProperty("tcpKeepAlive"));
        assertEquals("orders-app", listen.getDataSourceProperties().getProperty("ApplicationName"));
      }
    }
  }

  @Test
  void createListenDataSourceKeepsAnExplicitTcpKeepAliveSetting() {
    try (var main = new HikariDataSource()) {
      main.setJdbcUrl(PG.getJdbcUrl());
      main.addDataSourceProperty("tcpKeepAlive", "false");
      try (HikariDataSource listen =
          PostgresNotificationSubscription.createListenDataSource(main)) {
        assertEquals("false", listen.getDataSourceProperties().getProperty("tcpKeepAlive"));
      }
    }
  }

  @Test
  void createListenDataSourceConnectsForAMainPoolConfiguredByDataSourceClassName()
      throws Exception {
    // A pool configured by data source class and properties has no JDBC URL to copy; the LISTEN
    // pool must be built the same way rather than from a null URL.
    try (var main = new HikariDataSource()) {
      main.setDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
      main.addDataSourceProperty("serverName", PG.getHost());
      main.addDataSourceProperty("portNumber", PG.getFirstMappedPort());
      main.addDataSourceProperty("databaseName", PG.getDatabaseName());
      main.setUsername(PG.getUsername());
      main.setPassword(PG.getPassword());
      try (HikariDataSource listen = PostgresNotificationSubscription.createListenDataSource(main);
          var conn = listen.getConnection();
          var stmt = conn.createStatement()) {
        assertFalse(stmt.execute("LISTEN streamrune_events"), "LISTEN returns no result set");
      }
    }
  }

  /**
   * {@link PGSimpleDataSource} exposes {@code getUrl()} / {@code getUser()} rather than {@code
   * getJdbcUrl()} / {@code getUsername()}; the LISTEN pool reads those too, as the crypto forget
   * signal does.
   */
  @Test
  void createListenDataSourceReadsTheUrlAndUserOfPGSimpleDataSource() throws Exception {
    var pg = new PGSimpleDataSource();
    pg.setUrl(PG.getJdbcUrl());
    pg.setUser(PG.getUsername());
    pg.setPassword(PG.getPassword());

    try (HikariDataSource listen = PostgresNotificationSubscription.createListenDataSource(pg);
        var conn = listen.getConnection();
        var rs = conn.createStatement().executeQuery("SELECT 1")) {
      // getUrl() spells out every driver property after the database name.
      assertTrue(
          listen.getJdbcUrl().startsWith(PG.getJdbcUrl().split("\\?")[0]), listen.getJdbcUrl());
      assertEquals(PG.getUsername(), listen.getUsername());
      assertTrue(rs.next());
    }
  }

  /**
   * A pool that keeps its URL in a configuration object (Agroal on Quarkus) or a proxy around one
   * exposes no URL accessor; the error names the way out instead of only the missing accessor.
   */
  @Test
  void createListenDataSourceNamesTheWayOutForADataSourceWithoutAUrlAccessor() {
    var opaque = new OpaqueDataSource(eventDataSource);

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> PostgresNotificationSubscription.createListenDataSource(opaque));

    assertTrue(ex.getMessage().contains(OpaqueDataSource.class.getName()), ex.getMessage());
    assertTrue(ex.getMessage().contains("listenDataSource(DataSource)"), ex.getMessage());
  }

  /**
   * The LISTEN connection can come from any DataSource dedicated to the subscription — a pool built
   * from the application's own settings when they cannot be read off its DataSource. The
   * subscription switches the connection to auto-commit itself and closes the DataSource with it.
   */
  @Test
  void listensOnANonHikariDataSourceAndClosesItWithTheSubscription() throws Exception {
    var listenSource = new ClosingDataSource(eventDataSource);
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("non-hikari-listen"))
            .listenDataSource(listenSource)
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(received::addAll)
            .config(new SubscriptionConfig(true, Duration.ofSeconds(60), Duration.ofMillis(10)))
            .fetchSize(10)
            .build();
    // Appended before start(): only the catch-up poll that follows the LISTEN can deliver it, so
    // seeing it proves the LISTEN is registered.
    StreamId warmup = TestStreams.stream("cart-non-hikari-warmup");
    store.append(warmup, List.of(envelope(warmup, 1)), Version.initial());
    sub.start();
    try {
      long listening = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (received.isEmpty() && System.nanoTime() < listening) {
        Thread.sleep(20);
      }
      assertEquals(1, received.size(), "the catch-up poll after LISTEN delivers the warm-up event");
      // The fallback poll is a minute away: only the server NOTIFY can deliver this one in time.
      StreamId s = TestStreams.stream("cart-non-hikari");
      store.append(s, List.of(envelope(s, 1)), Version.initial());
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (received.size() < 2 && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertEquals(2, received.size(), "delivered through the NOTIFY on the supplied connection");
    } finally {
      sub.close();
    }
    assertTrue(listenSource.closed, "the subscription closes the LISTEN DataSource it was given");
  }

  /** A DataSource with no URL accessor, like an Agroal pool: it only hands out connections. */
  static final class OpaqueDataSource implements javax.sql.DataSource {
    private final javax.sql.DataSource delegate;

    OpaqueDataSource(javax.sql.DataSource delegate) {
      this.delegate = delegate;
    }

    @Override
    public java.sql.Connection getConnection() throws java.sql.SQLException {
      return delegate.getConnection();
    }

    @Override
    public java.sql.Connection getConnection(String u, String p) throws java.sql.SQLException {
      return delegate.getConnection(u, p);
    }

    @Override
    public java.io.PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(java.io.PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
      return java.util.logging.Logger.getGlobal();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws java.sql.SQLException {
      throw new java.sql.SQLException("not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }

  /**
   * A dedicated non-Hikari LISTEN source: each connection is a physical one opened from the test
   * database's settings, switched to manual commit the way a pool configured with auto-commit off
   * hands them out; records its own close.
   */
  static final class ClosingDataSource implements javax.sql.DataSource, AutoCloseable {
    private final PGSimpleDataSource physical = new PGSimpleDataSource();
    volatile boolean closed;

    ClosingDataSource(HikariDataSource settings) {
      physical.setUrl(settings.getJdbcUrl());
      physical.setUser(settings.getUsername());
      physical.setPassword(settings.getPassword());
    }

    @Override
    public java.sql.Connection getConnection() throws java.sql.SQLException {
      var conn = physical.getConnection();
      conn.setAutoCommit(false);
      return conn;
    }

    @Override
    public java.sql.Connection getConnection(String u, String p) throws java.sql.SQLException {
      return physical.getConnection(u, p);
    }

    @Override
    public java.io.PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(java.io.PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
      return java.util.logging.Logger.getGlobal();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws java.sql.SQLException {
      throw new java.sql.SQLException("not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  @Test
  void createListenDataSourceWorksWithCustomGetJdbcUrlDataSource() {
    // A custom DataSource that exposes getJdbcUrl() via reflection (no getUsername/getPassword)
    // exercises the reflection fallback path for credential extraction.
    var custom = new JdbcUrlOnlyDataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    try (HikariDataSource listen =
        PostgresNotificationSubscription.createListenDataSource(custom)) {
      assertNotNull(listen);
      assertEquals(1, listen.getMaximumPoolSize());
      assertEquals(PG.getJdbcUrl(), listen.getJdbcUrl());
    }
  }

  @Test
  void startTwiceThrows() {
    HikariDataSource listenDs =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource);
    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("dup"))
            .listenDataSource(listenDs)
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(events -> {})
            .config(FAST_CONFIG)
            .fetchSize(10)
            .build();

    sub.start();
    try {
      assertTrue(sub.isRunning());
      assertThrows(IllegalStateException.class, sub::start);
    } finally {
      sub.close();
    }
  }

  @Test
  void isRunningReturnsFalseBeforeStart() {
    HikariDataSource listenDs =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource);
    try (listenDs) {
      var sub =
          PostgresNotificationSubscription.builder(SubscriptionName.of("pre-start"))
              .listenDataSource(listenDs)
              .eventStore(store)
              .offsetStore(new InMemoryOffsetStore())
              .listener(events -> {})
              .config(FAST_CONFIG)
              .fetchSize(10)
              .build();
      assertFalse(sub.isRunning());
    }
  }

  @Test
  void closeIsIdempotent() {
    HikariDataSource listenDs =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource);
    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("idempotent-close"))
            .listenDataSource(listenDs)
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(events -> {})
            .config(FAST_CONFIG)
            .fetchSize(10)
            .build();

    sub.start();
    sub.close();
    assertDoesNotThrow(sub::close);
    assertTrue(listenDs.isClosed());
  }

  /**
   * {@code close()} interrupts the listen thread to cut its waits short. On a virtual thread a JDBC
   * call made while the flag is set fails at the socket, so the offset store here refuses every
   * call made on an interrupted thread, the way the PostgreSQL stores do. A delivery in flight when
   * {@code close()} is called must still save its checkpoint.
   */
  @Test
  void closeDuringADelivery_savesItsOffset() throws Exception {
    StreamId s = TestStreams.stream("cart-close-held");
    store.append(s, List.of(envelope(s, 1)), Version.initial());
    var callsOnInterruptedThread = new CopyOnWriteArrayList<String>();
    var offsetStore =
        new InMemoryOffsetStore() {
          @Override
          public GlobalOffset getLastOffset(ProjectionName n) {
            refuseIfInterrupted("getLastOffset");
            return super.getLastOffset(n);
          }

          @Override
          public void saveOffset(ProjectionName n, GlobalOffset o) {
            refuseIfInterrupted("saveOffset");
            super.saveOffset(n, o);
          }

          private void refuseIfInterrupted(String call) {
            if (Thread.currentThread().isInterrupted()) {
              callsOnInterruptedThread.add(call);
              throw new IllegalStateException(call + " on an interrupted thread");
            }
          }
        };
    var entered = new java.util.concurrent.CountDownLatch(1);
    var release = new java.util.concurrent.atomic.AtomicBoolean();
    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("close-held"))
            .listenDataSource(
                PostgresNotificationSubscription.createListenDataSource(eventDataSource))
            .eventStore(store)
            .offsetStore(offsetStore)
            .listener(
                events -> {
                  entered.countDown();
                  while (!release.get()) {
                    Thread.onSpinWait(); // ignores interrupts, like a JDBC write in progress
                  }
                })
            .config(FAST_CONFIG)
            .fetchSize(10)
            .build();
    sub.start();
    assertTrue(entered.await(10, TimeUnit.SECONDS), "the delivery must be in flight");

    Thread closer = Thread.ofVirtual().start(sub::close);
    Thread.sleep(300); // long enough for an immediate interrupt to land on the listen thread
    release.set(true);
    closer.join(TimeUnit.SECONDS.toMillis(20));

    assertFalse(closer.isAlive(), "close() must return");
    assertEquals(List.of(), callsOnInterruptedThread, "no store call on the interrupted thread");
    assertEquals(
        1L,
        offsetStore.getLastOffset(ProjectionName.of("close-held")).value(),
        "the delivery in flight saves its offset");
  }

  @Test
  void pollAndDeliverSwallowsException() throws Exception {
    // Exercises the catch (Exception) branch in pollAndDeliver().
    // We use a subclass that overrides readGlobalStream to throw.
    // NOTE: This test is marked flaky due to virtual thread scheduling uncertainty.
    HikariDataSource listenDs =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource);
    var readCalled = new java.util.concurrent.atomic.AtomicBoolean(false);
    org.streamrune.core.EventStore errorStore =
        new org.streamrune.core.EventStore() {
          @Override
          public org.streamrune.core.AggregateHistory load(
              org.streamrune.core.types.StreamId streamId) {
            return null;
          }

          @Override
          public org.streamrune.core.EventStore.AppendResult append(
              org.streamrune.core.types.StreamId streamId,
              java.util.List<org.streamrune.core.EventEnvelope> events,
              org.streamrune.core.types.Version expectedVersion) {
            return null;
          }

          @Override
          public void saveSnapshot(
              org.streamrune.core.types.StreamId streamId,
              org.streamrune.core.types.Version version,
              org.streamrune.core.AggregateState state) {}

          @Override
          public java.util.List<org.streamrune.core.EventEnvelope> readGlobalStream(
              org.streamrune.core.types.GlobalOffset afterOffset, int maxCount) {
            readCalled.set(true);
            throw new RuntimeException("simulated read failure");
          }

          @Override
          public java.util.List<org.streamrune.core.EventEnvelope> readStream(
              org.streamrune.core.types.StreamId streamId,
              org.streamrune.core.types.Version afterVersion,
              int maxCount) {
            return List.of();
          }
        };

    var offsetStore = new InMemoryOffsetStore();
    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("ex-delivery"))
            .listenDataSource(listenDs)
            .eventStore(errorStore)
            .offsetStore(offsetStore)
            .listener(events -> {})
            .config(FAST_CONFIG)
            .fetchSize(10)
            .build();

    sub.start();
    sub.notifyNewEvents();
    // Wait for the virtual thread to process at least one notification cycle.
    // getNotifications() blocks for 500ms, so we need to wait longer than that.
    Thread.sleep(1200);
    assertTrue(sub.isRunning(), "subscription should still be running despite exception");
    sub.close();
  }

  /**
   * A DETERMINISTIC read poison must stop this subscription terminally and be exposed via {@link
   * org.streamrune.runtime.ReadPoisonAware}, exactly as on the default polling path.
   *
   * <p>Before the fix the read failure was caught by {@code pollAndDeliver}'s blanket {@code catch
   * (Exception)}, logged, and re-attempted on every NOTIFY and every fallback tick forever, with no
   * bound and no state change — {@code isRunning()} stayed {@code true}, the class was not {@code
   * ReadPoisonAware}, and the owning {@code ContinuousProjectionRunner} therefore had NO way to
   * observe the freeze: state LIVE, /health UP, read model stuck at the checkpoint.
   */
  @Test
  void stopsTerminallyAfterBoundedDeterministicReadPoison() throws Exception {
    HikariDataSource listenDs =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource);
    var reads = new java.util.concurrent.atomic.AtomicInteger();
    org.streamrune.core.EventStore poisonStore =
        new org.streamrune.core.EventStore() {
          @Override
          public org.streamrune.core.AggregateHistory load(StreamId streamId) {
            throw new UnsupportedOperationException();
          }

          @Override
          public AppendResult append(
              StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
            throw new UnsupportedOperationException();
          }

          @Override
          public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
            reads.incrementAndGet();
            // The canonical deterministic poison: the stored event's type is not in THIS replica's
            // registry (rolling deploy). It fails identically at this checkpoint forever.
            throw new org.streamrune.core.UnknownEventTypeException(
                "event type", "NotInThisReplicasRegistry", List.of("Added"));
          }

          @Override
          public List<EventEnvelope> readStream(
              StreamId streamId, Version afterVersion, int maxCount) {
            throw new UnsupportedOperationException();
          }
        };

    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("poison-sub"))
            .listenDataSource(listenDs)
            .eventStore(poisonStore)
            .offsetStore(new InMemoryOffsetStore())
            .listener(events -> {})
            .config(FAST_CONFIG)
            .fetchSize(10)
            .readPoisonBound(3)
            .build();

    try (sub) {
      sub.start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (sub.isRunning() && System.nanoTime() < deadline) {
        sub.notifyNewEvents();
        Thread.sleep(50);
      }
      assertFalse(sub.isRunning(), "the subscription must stop itself once the bound is exhausted");
      assertNotNull(
          sub.readPoisonError(),
          "the terminal poison must be exposed so ContinuousProjectionRunner can HALT with health"
              + " DOWN instead of reporting LIVE/UP over a frozen read model");
      assertTrue(
          reads.get() >= 3, "the bound must be reached by repeated reads at the same checkpoint");
    }
  }

  /**
   * A TRANSIENT read failure must NOT trip the bound: the previous unbounded-retry behaviour is
   * what keeps a DB outage from permanently killing the subscription.
   */
  @Test
  void transientReadFailureNeverTripsTheReadPoisonBound() throws Exception {
    HikariDataSource listenDs =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource);
    var reads = new java.util.concurrent.atomic.AtomicInteger();
    org.streamrune.core.EventStore flakyStore =
        new org.streamrune.core.EventStore() {
          @Override
          public org.streamrune.core.AggregateHistory load(StreamId streamId) {
            throw new UnsupportedOperationException();
          }

          @Override
          public AppendResult append(
              StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
            throw new UnsupportedOperationException();
          }

          @Override
          public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
            reads.incrementAndGet();
            throw new org.streamrune.core.EventStoreException(
                "connection reset", new java.sql.SQLException("08006"));
          }

          @Override
          public List<EventEnvelope> readStream(
              StreamId streamId, Version afterVersion, int maxCount) {
            throw new UnsupportedOperationException();
          }
        };

    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("flaky-sub"))
            .listenDataSource(listenDs)
            .eventStore(flakyStore)
            .offsetStore(new InMemoryOffsetStore())
            .listener(events -> {})
            .config(FAST_CONFIG)
            .fetchSize(10)
            .readPoisonBound(3)
            .build();

    try (sub) {
      sub.start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (reads.get() < 6 && System.nanoTime() < deadline) {
        sub.notifyNewEvents();
        Thread.sleep(50);
      }
      assertTrue(reads.get() >= 6, "expected repeated transient read attempts");
      assertTrue(sub.isRunning(), "a transient read failure must never stop the subscription");
      assertNull(sub.readPoisonError());
    }
  }

  @Test
  void buildRejectsInvalidArgs() {
    HikariDataSource listenDs =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource);
    try (listenDs) {
      // Null subscription name (a blank one is refused by SubscriptionName itself)
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PostgresNotificationSubscription.builder(null)
                  .listenDataSource(listenDs)
                  .eventStore(store)
                  .offsetStore(new InMemoryOffsetStore())
                  .listener(events -> {})
                  .config(FAST_CONFIG)
                  .fetchSize(10)
                  .build());
      // Null listen data source
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PostgresNotificationSubscription.builder(SubscriptionName.of("n"))
                  .listenDataSource(null)
                  .eventStore(store)
                  .offsetStore(new InMemoryOffsetStore())
                  .listener(events -> {})
                  .config(FAST_CONFIG)
                  .fetchSize(10)
                  .build());
      // Null event store
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PostgresNotificationSubscription.builder(SubscriptionName.of("n"))
                  .listenDataSource(listenDs)
                  .eventStore(null)
                  .offsetStore(new InMemoryOffsetStore())
                  .listener(events -> {})
                  .config(FAST_CONFIG)
                  .fetchSize(10)
                  .build());
      // Null offset store
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PostgresNotificationSubscription.builder(SubscriptionName.of("n"))
                  .listenDataSource(listenDs)
                  .eventStore(store)
                  .offsetStore(null)
                  .listener(events -> {})
                  .config(FAST_CONFIG)
                  .fetchSize(10)
                  .build());
      // Null listener
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PostgresNotificationSubscription.builder(SubscriptionName.of("n"))
                  .listenDataSource(listenDs)
                  .eventStore(store)
                  .offsetStore(new InMemoryOffsetStore())
                  .listener(null)
                  .config(FAST_CONFIG)
                  .fetchSize(10)
                  .build());
      // Null config
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PostgresNotificationSubscription.builder(SubscriptionName.of("n"))
                  .listenDataSource(listenDs)
                  .eventStore(store)
                  .offsetStore(new InMemoryOffsetStore())
                  .listener(events -> {})
                  .config(null)
                  .fetchSize(10)
                  .build());
      // Non-positive fetch size
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PostgresNotificationSubscription.builder(SubscriptionName.of("n"))
                  .listenDataSource(listenDs)
                  .eventStore(store)
                  .offsetStore(new InMemoryOffsetStore())
                  .listener(events -> {})
                  .config(FAST_CONFIG)
                  .fetchSize(0)
                  .build());
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PostgresNotificationSubscription.builder(SubscriptionName.of("n"))
                  .listenDataSource(listenDs)
                  .eventStore(store)
                  .offsetStore(new InMemoryOffsetStore())
                  .listener(events -> {})
                  .config(FAST_CONFIG)
                  .fetchSize(-1)
                  .build());
    }
  }

  /**
   * {@code close()} must JOIN the listen thread before returning and before tearing down the LISTEN
   * pool, and must be terminal.
   *
   * <p>{@code close()} does not interrupt a delivery in flight, so without the join {@code close()}
   * returned while the drain was still reading, delivering and saving its checkpoint on the SHARED
   * event DataSource — after the caller had moved on to closing it. And because {@code start()}
   * only CAS-ed {@code running} back to true, a restart was accepted while that thread was still
   * live, putting two listeners on one checkpoint (both read the same offset, both deliver, both
   * save) and breaking this class's own single-thread assumption for its plain read-poison streak
   * fields.
   */
  @Test
  void closeJoinsTheListenThreadAndIsTerminal() throws Exception {
    HikariDataSource listenDs =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource);
    var listenThread = new java.util.concurrent.atomic.AtomicReference<Thread>();
    var entered = new java.util.concurrent.CountDownLatch(1);
    var release = new java.util.concurrent.CountDownLatch(1);

    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("conc1-lifecycle-sub"))
            .listenDataSource(listenDs)
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(
                events -> {
                  listenThread.set(Thread.currentThread());
                  entered.countDown();
                  try {
                    assertTrue(release.await(20, TimeUnit.SECONDS));
                    // Deliberately slow finish so the listen thread is provably still alive when
                    // a NON-joining close() returns — the window a correct join must wait out.
                    Thread.sleep(500);
                  } catch (InterruptedException e) {
                    throw new AssertionError("close() must not interrupt a delivery", e);
                  }
                })
            .config(FAST_CONFIG)
            .fetchSize(10)
            .build();

    // Appended BEFORE start(): the start-time catch-up poll delivers it, parking the listen thread
    // inside pollAndDeliver.
    StreamId s = TestStreams.stream("cart-conc1-lifecycle");
    store.append(s, List.of(envelope(s, 1)), Version.initial());

    sub.start();
    assertTrue(
        entered.await(10, TimeUnit.SECONDS), "the listen thread must reach the blocking listener");

    var aliveWhenCloseReturned = new java.util.concurrent.atomic.AtomicBoolean(true);
    Thread closer =
        Thread.ofVirtual()
            .start(
                () -> {
                  sub.close();
                  aliveWhenCloseReturned.set(listenThread.get().isAlive());
                });
    Thread.sleep(200);
    release.countDown();
    closer.join(TimeUnit.SECONDS.toMillis(20));

    assertFalse(closer.isAlive(), "close() must return");
    assertFalse(
        aliveWhenCloseReturned.get(),
        "close() must join the listen thread before returning: close() does not interrupt a"
            + " delivery, so the in-flight drain would otherwise keep saving its checkpoint after"
            + " close() returned");
    assertTrue(listenDs.isClosed(), "the LISTEN pool is torn down only after the join");
    assertThrows(
        IllegalStateException.class,
        sub::start,
        "close() is terminal — a restart would put a second listener on the same checkpoint");
    sub.close(); // idempotent
  }

  /**
   * Minimal mutable in-memory {@link org.streamrune.core.EventStore} for the periodic-fallback
   * test. Appending to it fires NO server {@code pg_notify}, so the subscription can only deliver
   * its events via the periodic poll fallback. {@code readGlobalStream} returns events with a
   * global offset greater than {@code afterOffset}, in offset order, capped at {@code maxCount}.
   */
  static final class MutableInMemoryEventStore implements org.streamrune.core.EventStore {
    private final java.util.concurrent.ConcurrentSkipListMap<Long, EventEnvelope> events =
        new java.util.concurrent.ConcurrentSkipListMap<>();

    void append(long globalOffset, StreamId streamId) {
      events.put(
          globalOffset,
          new EventEnvelope(
              GlobalOffset.of(globalOffset),
              streamId,
              new Version(1),
              new EventType("Added"),
              new Added("X"),
              new EventMetadata(
                  EventId.of("evt_" + globalOffset),
                  CommandId.of("cmd_" + globalOffset),
                  null,
                  null,
                  CorrelationId.of("corr"),
                  null,
                  null,
                  Instant.now())));
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      return events.tailMap(afterOffset.value(), false).values().stream().limit(maxCount).toList();
    }

    @Override
    public org.streamrune.core.AggregateHistory load(StreamId streamId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AppendResult append(
        StreamId streamId, List<EventEnvelope> evts, Version expectedVersion) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      throw new UnsupportedOperationException();
    }
  }

  static final class JdbcUrlOnlyDataSource implements javax.sql.DataSource {
    private final String jdbcUrl;
    private final String username;
    private final String password;

    JdbcUrlOnlyDataSource(String jdbcUrl, String username, String password) {
      this.jdbcUrl = jdbcUrl;
      this.username = username;
      this.password = password;
    }

    public String getJdbcUrl() {
      return jdbcUrl;
    }

    public String getUsername() {
      return username;
    }

    public String getPassword() {
      return password;
    }

    @Override
    public java.sql.Connection getConnection() {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.sql.Connection getConnection(String u, String p) {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.io.PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(java.io.PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
      return java.util.logging.Logger.getLogger("test");
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }

  // ── Nothing escaping the listen loop may leave a dead thread behind isRunning()==true
  // ─

  private static ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>
      attachSubscriptionLogAppender() {
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(PostgresNotificationSubscription.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    return appender;
  }

  private static void detachSubscriptionLogAppender(
      ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender) {
    ((ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(PostgresNotificationSubscription.class))
        .detachAppender(appender);
    appender.stop();
  }

  private static long countMessages(
      ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender,
      ch.qos.logback.classic.Level level,
      String fragment) {
    return appender.list.stream()
        .filter(e -> e.getLevel() == level && e.getFormattedMessage().contains(fragment))
        .count();
  }

  @Test
  void listenThreadDyingOnUncaughtErrorFlipsIsRunningFalse() throws Exception {
    // listenLoop caught only SQLException and pollAndDeliver only
    // Exception, so a java.lang.Error from the listener (AssertionError, NoClassDefFoundError /
    // LinkageError from a missing optional dependency, StackOverflowError on pathological data,
    // OutOfMemoryError) escaped both and killed the virtual listen thread with `running` still
    // true. isRunning() then lied forever: the owning ContinuousProjectionRunner's
    // !subscription.isRunning() halt check never fired, the read model silently froze and health
    // stayed UP. Sibling PollingEventSubscription.runPollLoop guards exactly this.
    HikariDataSource listenDs =
        PostgresNotificationSubscription.createListenDataSource(eventDataSource);
    var appender = attachSubscriptionLogAppender();
    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("error-sub"))
            .listenDataSource(listenDs)
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(
                events -> {
                  throw new AssertionError("boom — an unrecoverable Error, not a RuntimeException");
                })
            .config(FAST_CONFIG)
            .fetchSize(10)
            .build();
    // Appended BEFORE start(): the start-time catch-up poll hands it to the throwing listener.
    StreamId s = TestStreams.stream("cart-error");
    store.append(s, List.of(envelope(s, 1)), Version.initial());

    sub.start();
    try {
      assertTrue(sub.isRunning(), "subscription must be running right after start()");
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (sub.isRunning() && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertFalse(
          sub.isRunning(),
          "an uncaught Error that kills the listen thread must flip isRunning() to false so the"
              + " owning runner surfaces a terminal ERROR — not leave it stuck running");
      assertEquals(
          1,
          countMessages(appender, ch.qos.logback.classic.Level.ERROR, "unrecoverable error"),
          "the terminal stop must be surfaced as one ERROR naming the subscription");
    } finally {
      detachSubscriptionLogAppender(appender);
      sub.close();
    }
  }

  /**
   * A LISTEN pool whose {@code getConnection()} fails with a RuntimeException — what Hikari's lazy
   * pool initialisation raises for a non-SQL cause (a driver that cannot be loaded, an invalid pool
   * configuration; a SQL cause is unwrapped back into a SQLException) and what a misbehaving
   * DataSource wrapper can raise at any time.
   */
  static final class ThrowingListenDataSource extends HikariDataSource {
    final java.util.concurrent.atomic.AtomicInteger attempts =
        new java.util.concurrent.atomic.AtomicInteger();

    @Override
    public java.sql.Connection getConnection() {
      attempts.incrementAndGet();
      throw new IllegalStateException("simulated LISTEN pool initialisation failure");
    }
  }

  @Test
  void listenLoopRetriesWhenTheListenPoolThrowsARuntimeExceptionOnConnect() throws Exception {
    // The connect-phase catch was SQLException-only, so a
    // RuntimeException from the LISTEN pool killed the listen thread on the FIRST attempt —
    // silently,
    // with running=true. A pool that cannot start is as transient as a connection that cannot be
    // established: the loop must log, sleep and retry, exactly like the SQLException path.
    var listenDs = new ThrowingListenDataSource();
    var appender = attachSubscriptionLogAppender();
    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("connect-retry-sub"))
            .listenDataSource(listenDs)
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(events -> {})
            .config(FAST_CONFIG)
            .fetchSize(10)
            .build();

    sub.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (listenDs.attempts.get() < 2 && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      assertTrue(
          listenDs.attempts.get() >= 2,
          "a RuntimeException from the LISTEN pool must be retried like a SQLException, not kill"
              + " the listen thread on the first attempt");
      assertTrue(
          countMessages(
                  appender,
                  ch.qos.logback.classic.Level.WARN,
                  "failed to establish LISTEN connection")
              >= 1,
          "each failed connect attempt is surfaced as a WARN");
      assertTrue(sub.isRunning(), "the subscription keeps running while it retries the connect");
    } finally {
      detachSubscriptionLogAppender(appender);
      sub.close();
    }
  }
}
