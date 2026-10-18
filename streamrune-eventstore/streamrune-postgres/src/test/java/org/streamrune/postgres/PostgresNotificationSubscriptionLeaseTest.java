package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.EventListener;
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
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SubscriptionLease;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * A {@link PostgresNotificationSubscription} built with a lease delivers only while this process
 * holds it: the gate before the checkpoint read, the drain ended after a page once the lease left,
 * the anchor closed after the listen thread left its page, and one anchor per start.
 */
@Testcontainers
class PostgresNotificationSubscriptionLeaseTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_lease_test");

  record Added(String sku) implements DomainEvent {}

  record CartState(int n) implements AggregateState {}

  private static final String NAME = "leased-sub";
  private static final SubscriptionConfig FAST =
      new SubscriptionConfig(true, Duration.ofMillis(100), Duration.ofMillis(10));
  private static final SubscriptionConfig SLOW =
      new SubscriptionConfig(true, Duration.ofHours(1), Duration.ZERO);

  static HikariDataSource eventDataSource;
  static EventTypeRegistry typeRegistry;

  PostgresEventStore store;
  OffsetStore offsets;
  final RecordingLeadership leadership = spy(new RecordingLeadership());
  final List<EventEnvelope> received = new CopyOnWriteArrayList<>();
  final List<PostgresNotificationSubscription> subscriptions = new CopyOnWriteArrayList<>();
  final CountDownLatch release = new CountDownLatch(1);

  @BeforeAll
  static void initSchema() {
    var config = new HikariConfig();
    config.setJdbcUrl(PG.getJdbcUrl());
    config.setUsername(PG.getUsername());
    config.setPassword(PG.getPassword());
    config.setMaximumPoolSize(6);
    eventDataSource = new HikariDataSource(config);
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
            return List.of(Added.class, CartState.class);
          }
        };
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
    offsets = spy(new PostgresOffsetStore(eventDataSource));
    try (var conn = eventDataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("DELETE FROM projection_offset");
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
  }

  @AfterEach
  void closeEverySubscription() {
    release.countDown();
    subscriptions.forEach(PostgresNotificationSubscription::close);
  }

  @Test
  void aStandbyDeliversNothing() throws Exception {
    append(3);
    var sub = leased(FAST, 10, received::addAll);

    sub.start();
    await().atMost(10, TimeUnit.SECONDS).until(() -> leadership.tryAcquires(NAME) >= 2);
    Thread.sleep(2 * FAST.pollingInterval().toMillis());

    assertThat(received).isEmpty();
    assertThat(checkpointRows()).isZero();
    assertThat(sub.isRunning()).isTrue();
  }

  @Test
  void aLeaderDelivers() {
    leadership.grant(NAME);
    append(3);
    var sub = leased(FAST, 10, received::addAll);

    sub.start();

    await().atMost(10, TimeUnit.SECONDS).until(() -> received.size() == 3);
    await()
        .atMost(10, TimeUnit.SECONDS)
        .until(() -> offsets.getLastOffset(ProjectionName.of(NAME)).value() == 3);
    assertThat(leadership.tryAcquires(NAME)).isEqualTo(1);
  }

  @Test
  void aLeaseLostBetweenPages_endsTheDrainAfterThePage() throws Exception {
    leadership.grant(NAME);
    append(3);
    var sub =
        leased(
            SLOW,
            1,
            events -> {
              received.addAll(events);
              leadership.revoke(NAME);
            });

    sub.start();

    await().atMost(10, TimeUnit.SECONDS).until(() -> received.size() == 1);
    await()
        .atMost(10, TimeUnit.SECONDS)
        .until(() -> offsets.getLastOffset(ProjectionName.of(NAME)).value() == 1);
    Thread.sleep(300);
    assertThat(received).hasSize(1);
  }

  @Test
  void closeResignsAfterTheListenThreadLeftThePage() {
    leadership.grant(NAME);
    append(1);
    CountDownLatch parked = new CountDownLatch(1);
    var sub =
        leased(
            SLOW,
            10,
            events -> {
              received.addAll(events);
              parked.countDown();
              awaitQuietly(release);
            });
    sub.start();
    awaitQuietly(parked);

    CompletableFuture<Void> closing = CompletableFuture.runAsync(sub::close);
    await()
        .pollDelay(Duration.ofMillis(200))
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> !sub.isRunning());
    assertThat(closing).isNotDone();
    assertThat(leadership.resigns(NAME)).isZero();

    release.countDown();
    closing.join();
    assertThat(leadership.resigns(NAME)).isEqualTo(1);
    InOrder order = inOrder(offsets, leadership);
    order.verify(offsets).saveOffset(eq(ProjectionName.of(NAME)), eq(GlobalOffset.of(1)));
    order.verify(leadership).resign(NAME);
  }

  @Test
  void theResignOfACloseRunsWithTheInterruptHeldBack() {
    leadership.grant(NAME);
    var sub = leased(FAST, 10, received::addAll);
    sub.start();
    await().atMost(10, TimeUnit.SECONDS).until(() -> leadership.tryAcquires(NAME) == 1);

    sub.close();

    assertThat(leadership.resigns(NAME)).isEqualTo(1);
    assertThat(leadership.resignsOnAnInterruptedThread()).isZero();
  }

  @Test
  void aSecondStartAfterAnErrorExitAnchorsAgain() {
    leadership.grant(NAME);
    append(1);
    AtomicBoolean failed = new AtomicBoolean();
    var sub =
        leased(
            FAST,
            10,
            events -> {
              if (failed.compareAndSet(false, true)) {
                throw new AssertionError("the listener fails once, for good");
              }
              received.addAll(events);
            });
    sub.start();
    await().atMost(10, TimeUnit.SECONDS).until(() -> leadership.resigns(NAME) == 1);
    assertThat(sub.isRunning()).isFalse();

    await().atMost(10, TimeUnit.SECONDS).until(() -> startedAgain(sub));

    await().atMost(10, TimeUnit.SECONDS).until(() -> received.size() == 1);
    assertThat(leadership.tryAcquires(NAME)).isEqualTo(2);
  }

  @Test
  void aSecondSubscriptionOfTheSameLeaseIsRefusedAtStart() {
    SubscriptionLease lease = SubscriptionLease.of(leadership, NAME);
    var first = track(builder(FAST, 10, received::addAll).lease(lease).build());
    var second = track(builder(FAST, 10, received::addAll).lease(lease).build());
    first.start();

    assertThatThrownBy(second::start)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("already has a delivering subscription in this process");
    assertThat(second.isRunning()).isFalse();
  }

  @Test
  void leadershipNamesTheLeaseAfterTheSubscription() {
    var sub = track(builder(FAST, 10, received::addAll).leadership(leadership).build());

    sub.start();

    await().atMost(10, TimeUnit.SECONDS).until(() -> leadership.tryAcquires(NAME) >= 1);
  }

  @Test
  void sagaRunnerTogetherWithListenerIsRefused() {
    assertThatThrownBy(
            () -> builder(FAST, 10, received::addAll).sagaRunner(mock(SagaRunner.class)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "PostgresNotificationSubscription.Builder: sagaRunner(...) and listener(...) are both"
                + " set — sagaRunner(runner) delivers to runner.asEventListener(); name one of"
                + " them");
  }

  @Test
  void sagaRunnerTogetherWithLeaseOrLeadershipIsRefused() {
    assertThatThrownBy(
            () ->
                builder(FAST, 10, null)
                    .sagaRunner(mock(SagaRunner.class))
                    .lease(SubscriptionLease.ungated())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "PostgresNotificationSubscription.Builder: sagaRunner(...) and lease(...) are both set"
                + " — sagaRunner(runner) takes runner.lease(); name one of them");
    assertThatThrownBy(
            () ->
                builder(FAST, 10, null)
                    .sagaRunner(mock(SagaRunner.class))
                    .leadership(leadership)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "PostgresNotificationSubscription.Builder: sagaRunner(...) and leadership(...) are"
                + " both set — sagaRunner(runner) takes runner.lease(); name one of them");
    assertThatThrownBy(
            () ->
                builder(FAST, 10, received::addAll)
                    .lease(SubscriptionLease.ungated())
                    .leadership(leadership)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "PostgresNotificationSubscription.Builder: lease(...) and leadership(...) are both set"
                + " — leadership(l) is lease(SubscriptionLease.of(l, subscriptionName)); name one"
                + " of them");
  }

  // ---- helpers ----

  private boolean startedAgain(PostgresNotificationSubscription sub) {
    try {
      sub.start();
      return true;
    } catch (IllegalStateException stillWindingDown) {
      assertThat(stillWindingDown.getMessage()).contains("Previous listen thread");
      return false;
    }
  }

  private PostgresNotificationSubscription.Builder builder(
      SubscriptionConfig config, int fetchSize, EventListener listener) {
    return PostgresNotificationSubscription.builder(SubscriptionName.of(NAME))
        .listenDataSource(PostgresNotificationSubscription.createListenDataSource(eventDataSource))
        .eventStore(store)
        .offsetStore(offsets)
        .listener(listener)
        .config(config)
        .fetchSize(fetchSize);
  }

  private PostgresNotificationSubscription leased(
      SubscriptionConfig config, int fetchSize, EventListener listener) {
    return track(
        builder(config, fetchSize, listener).lease(SubscriptionLease.of(leadership, NAME)).build());
  }

  private PostgresNotificationSubscription track(PostgresNotificationSubscription sub) {
    subscriptions.add(sub);
    return sub;
  }

  private int checkpointRows() throws Exception {
    try (var conn = eventDataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "SELECT count(*) FROM projection_offset WHERE projection_name = ?")) {
      stmt.setString(1, NAME);
      try (var rs = stmt.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private void append(int count) {
    for (int i = 1; i <= count; i++) {
      StreamId s = TestStreams.stream("cart-lease-" + i);
      store.append(
          s,
          List.of(
              new EventEnvelope(
                  GlobalOffset.initial(),
                  s,
                  new Version(1),
                  new EventType("Added"),
                  new Added("X"),
                  new EventMetadata(
                      EventId.of("evt_" + i),
                      CommandId.of("cmd_" + i),
                      null,
                      null,
                      CorrelationId.of("corr"),
                      null,
                      null,
                      Instant.now()))),
          Version.initial());
    }
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      if (!latch.await(20, TimeUnit.SECONDS)) {
        throw new AssertionError("the latch never opened");
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new AssertionError(interrupted);
    }
  }
}
