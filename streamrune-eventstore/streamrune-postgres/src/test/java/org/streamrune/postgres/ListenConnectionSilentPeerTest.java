package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.PushHealthAware.PushPathStatus;
import org.streamrune.runtime.SubscriptionHealthContributor;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * A database host that dies without closing its connections — a crashed or replaced node behind a
 * virtual IP or DNS name — leaves every LISTEN connection to it ESTABLISHED on the client side, and
 * a timed wait for notifications on it simply returns nothing, forever. These tests put a {@link
 * SilentPeerProxy} between the LISTEN connections and PostgreSQL, silence it, and require the push
 * path to notice, reconnect, and deliver notifications again; while it cannot, a hybrid
 * subscription must not report its push path live.
 */
@Testcontainers
@Timeout(120)
class ListenConnectionSilentPeerTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_silent_peer");

  private static final long PROBE_INTERVAL_MS = 200;
  private static final long PROBE_TIMEOUT_MS = 500;
  private static final String CHANNEL = "streamrune_events";

  record Added(String sku) implements DomainEvent {}

  record CartState(int n) implements AggregateState {}

  static HikariDataSource directPool;
  SilentPeerProxy proxy;
  HikariDataSource proxiedPool;

  @BeforeAll
  static void initSchema() {
    directPool = pool(PG.getJdbcUrl());
    org.flywaydb.core.Flyway.configure()
        .dataSource(directPool)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
  }

  @AfterAll
  static void closeDirectPool() {
    if (directPool != null) {
      directPool.close();
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    proxy = new SilentPeerProxy(PG.getHost(), PG.getFirstMappedPort());
    proxiedPool = pool(proxy.jdbcUrl(PG.getDatabaseName()));
    try (var conn = directPool.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
  }

  @AfterEach
  void tearDown() {
    proxiedPool.close();
    proxy.close();
  }

  @Test
  void theListenerReplacesAConnectionWhoseServerVanishedWithoutClosingIt() throws Exception {
    var notified = new AtomicInteger();
    var metrics = new CountingMetrics();
    var listener =
        PgNotificationListener.builder()
            .dataSource(proxiedPool) // pooled: the listener draws from its own dedicated pool
            .onNotification(notified::incrementAndGet)
            .metrics(metrics)
            .backoff(50, 200)
            .livenessProbe(PROBE_INTERVAL_MS, PROBE_TIMEOUT_MS)
            .build();
    try (listener) {
      listener.start();
      awaitNotified(notified, 1, "the first NOTIFY was not delivered");
      assertEquals(1, proxy.forwardedConnections(), "one LISTEN connection so far");

      proxy.silenceOpenConnections();

      await(
          () -> metrics.reconnects.get() >= 1,
          "a LISTEN connection whose server vanished without closing it was never noticed");
      int afterLoss = notified.get();
      awaitNotified(
          notified,
          afterLoss + 1,
          "a NOTIFY sent after the server vanished must arrive over a replacement connection");
      assertEquals(2, proxy.forwardedConnections(), "the listener opened one replacement");
      assertTrue(listener.isRunning());
    }
  }

  @Test
  void theSubscriptionDeliversByNotifyAgainAfterItsServerVanished() throws Exception {
    var store = eventStore();
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    // The fallback poll is far away: within this test only a NOTIFY — or the catch-up poll that
    // follows every new LISTEN connection — can deliver an event.
    var config = new SubscriptionConfig(true, Duration.ofMinutes(10), Duration.ofMillis(1));
    var sub =
        PostgresNotificationSubscription.builder(SubscriptionName.of("silent-peer-sub"))
            .listenDataSource(PostgresNotificationSubscription.createListenDataSource(proxiedPool))
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(received::addAll)
            .config(config)
            .fetchSize(10)
            .livenessProbe(PROBE_INTERVAL_MS, PROBE_TIMEOUT_MS)
            .build();

    sub.start();
    try {
      // The start-time catch-up poll delivers the first event once LISTEN is registered.
      append(store, "cart-before");
      await(() -> received.size() == 1, "the first event was not delivered");

      proxy.silenceOpenConnections();
      append(store, "cart-after");

      await(
          () -> received.size() == 2,
          "an event appended after the server vanished must be delivered once the subscription"
              + " has replaced its LISTEN connection, not at the next fallback poll");
      assertEquals(TestStreams.stream("cart-after"), received.get(1).streamId());
      assertTrue(sub.isRunning());
    } finally {
      sub.close();
    }
  }

  @Test
  void aHybridSubscriptionReportsItsPushPathDeadUntilItHasReplacedAVanishedConnection()
      throws Exception {
    var eventStore = new InMemoryEventStore();
    OffsetStore offsetStore = mock(OffsetStore.class);
    when(offsetStore.getLastOffset(any())).thenReturn(GlobalOffset.initial());
    var health = new SubscriptionHealthContributor(eventStore, offsetStore);
    var sub =
        new HybridEventSubscription(
            SubscriptionName.of("silent-peer-hybrid"),
            proxiedPool,
            eventStore,
            offsetStore,
            events -> {},
            new SubscriptionConfig(true, Duration.ofSeconds(1), Duration.ofMillis(1)),
            100,
            StreamRuneMetrics.NOOP,
            0,
            listener ->
                listener
                    .backoff(50, 200)
                    .livenessProbe(PROBE_INTERVAL_MS, PROBE_TIMEOUT_MS)
                    .reconnectGrace(1_000));
    sub.start();
    try {
      health.register("silent-peer-hybrid", sub);
      await(() -> sub.notificationListener().isListening(), "the listener never connected");
      assertEquals(PushPathStatus.LIVE, sub.pushPathStatus());

      // The host vanishes and its replacement is not reachable yet.
      proxy.refuseNewConnections(true);
      proxy.silenceOpenConnections();

      await(
          () -> sub.pushPathStatus() == PushPathStatus.DEAD,
          "a push path whose server vanished, and that cannot reconnect, must not report LIVE");
      assertTrue(sub.isRunning(), "polling still delivers — the subscription is not down");
      assertEquals(SubscriptionHealth.Status.DEGRADED, health.health().getFirst().status());

      // The replacement becomes reachable.
      proxy.refuseNewConnections(false);

      await(
          () -> sub.pushPathStatus() == PushPathStatus.LIVE,
          "a push path that reconnected must report LIVE again");
      assertTrue(sub.notificationListener().isListening());
      assertEquals(SubscriptionHealth.Status.UP, health.health().getFirst().status());
    } finally {
      sub.close();
    }
  }

  private static HikariDataSource pool(String jdbcUrl) {
    var config = new HikariConfig();
    config.setJdbcUrl(jdbcUrl);
    config.setUsername(PG.getUsername());
    config.setPassword(PG.getPassword());
    config.setMaximumPoolSize(2);
    // The proxied pool only lends its URL and credentials to the LISTEN pools built from it; no
    // idle connections of its own, so the proxy counts LISTEN connections only.
    config.setMinimumIdle(0);
    config.setInitializationFailTimeout(-1);
    return new HikariDataSource(config);
  }

  private static void awaitNotified(AtomicInteger notified, int atLeast, String failure)
      throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
    while (System.nanoTime() < deadline) {
      try (var conn = directPool.getConnection();
          var stmt = conn.createStatement()) {
        stmt.execute("NOTIFY " + CHANNEL);
      }
      if (notified.get() >= atLeast) {
        return;
      }
      Thread.sleep(100);
    }
    fail(failure);
  }

  private static void await(BooleanSupplier condition, String failure) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(20);
    }
    fail(failure);
  }

  private static PostgresEventStore eventStore() {
    var typeRegistry =
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
            return List.of(Added.class, CartState.class);
          }
        };
    return PostgresEventStore.builder().dataSource(directPool).typeRegistry(typeRegistry).build();
  }

  private static void append(PostgresEventStore store, String stream) {
    StreamId streamId = TestStreams.stream(stream);
    var envelope =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId,
            new Version(1),
            new EventType("Added"),
            new Added("X"),
            new EventMetadata(
                EventId.of("evt_" + stream),
                CommandId.of("cmd_" + stream),
                null,
                null,
                CorrelationId.of("corr"),
                null,
                null,
                Instant.now()));
    store.append(streamId, List.of(envelope), Version.initial());
  }

  private static final class CountingMetrics implements StreamRuneMetrics {
    final AtomicInteger reconnects = new AtomicInteger();

    @Override
    public void recordListenerReconnect(String channel) {
      reconnects.incrementAndGet();
    }
  }

  private static final class InMemoryOffsetStore implements OffsetStore {
    final ConcurrentHashMap<String, GlobalOffset> offsets = new ConcurrentHashMap<>();

    @Override
    public GlobalOffset getLastOffset(ProjectionName name) {
      return offsets.getOrDefault(name.value(), GlobalOffset.initial());
    }

    @Override
    public void saveOffset(ProjectionName name, GlobalOffset offset) {
      offsets.put(name.value(), offset);
    }
  }
}
