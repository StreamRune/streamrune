package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;

/** Unit tests for HybridEventSubscription. */
class HybridEventSubscriptionTest {

  private final SubscriptionConfig config =
      new SubscriptionConfig(true, Duration.ofMillis(100), Duration.ofMillis(10));

  @Test
  void constructorThrowsOnNullSubscriptionName() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new HybridEventSubscription(
                null,
                mockDataSource(),
                mockEventStore(),
                mockOffsetStore(),
                mockListener(),
                config,
                100));
  }

  @Test
  void blankSubscriptionNameIsRejectedBySubscriptionNameBeforeTheConstructorSeesIt() {
    // HybridEventSubscription checks only for null: SubscriptionName's own constructor rejects a
    // blank value, so a blank subscription name cannot be constructed, let alone passed in.
    assertThrows(IllegalArgumentException.class, () -> SubscriptionName.of("   "));
  }

  @Test
  void constructorThrowsOnNullDataSource() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new HybridEventSubscription(
                SubscriptionName.of("test-sub"),
                null,
                mockEventStore(),
                mockOffsetStore(),
                mockListener(),
                config,
                100));
  }

  @Test
  void constructorThrowsOnNullEventStore() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new HybridEventSubscription(
                SubscriptionName.of("test-sub"),
                mockDataSource(),
                null,
                mockOffsetStore(),
                mockListener(),
                config,
                100));
  }

  @Test
  void constructorThrowsOnNullOffsetStore() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new HybridEventSubscription(
                SubscriptionName.of("test-sub"),
                mockDataSource(),
                mockEventStore(),
                null,
                mockListener(),
                config,
                100));
  }

  @Test
  void constructorThrowsOnNullListener() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new HybridEventSubscription(
                SubscriptionName.of("test-sub"),
                mockDataSource(),
                mockEventStore(),
                mockOffsetStore(),
                null,
                config,
                100));
  }

  @Test
  void constructorThrowsOnNullConfig() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new HybridEventSubscription(
                SubscriptionName.of("test-sub"),
                mockDataSource(),
                mockEventStore(),
                mockOffsetStore(),
                mockListener(),
                null,
                100));
  }

  @Test
  void startThrowsWhenAlreadyRunning() {
    var subscription = createSubscription();

    subscription.start();
    assertTrue(subscription.isRunning());

    assertThrows(IllegalStateException.class, subscription::start);

    subscription.close();
  }

  @Test
  void isRunningReturnsFalseBeforeStart() {
    var subscription = createSubscription();

    assertFalse(subscription.isRunning());
  }

  @Test
  void builderCreatesSubscription() {
    var subscription =
        HybridEventSubscription.builder()
            .subscriptionName("test-sub")
            .dataSource(mockDataSource())
            .eventStore(mockEventStore())
            .offsetStore(mockOffsetStore())
            .listener(mockListener())
            .config(config)
            .fetchSize(50)
            .build();

    assertFalse(subscription.isRunning());
  }

  @Test
  void builderUsesDefaultConfig() {
    var subscription =
        HybridEventSubscription.builder()
            .subscriptionName("test-sub")
            .dataSource(mockDataSource())
            .eventStore(mockEventStore())
            .offsetStore(mockOffsetStore())
            .listener(mockListener())
            .build();

    assertFalse(subscription.isRunning());
  }

  @Test
  void builderThrowsOnMissingSubscriptionName() {
    var builder =
        HybridEventSubscription.builder()
            .dataSource(mockDataSource())
            .eventStore(mockEventStore())
            .offsetStore(mockOffsetStore())
            .listener(mockListener());

    var exception = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(exception.getMessage().contains("subscriptionName"));
  }

  @Test
  void builderThrowsOnBlankSubscriptionName() {
    // SubscriptionName.of rejects blank values at the setter site, not at build() time.
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> HybridEventSubscription.builder().subscriptionName("   "));
    assertTrue(exception.getMessage().contains("subscriptionName"));
  }

  @Test
  void builderThrowsOnMissingDataSource() {
    var builder =
        HybridEventSubscription.builder()
            .subscriptionName("test-sub")
            .eventStore(mockEventStore())
            .offsetStore(mockOffsetStore())
            .listener(mockListener());

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void builderThrowsOnMissingEventStore() {
    var builder =
        HybridEventSubscription.builder()
            .subscriptionName("test-sub")
            .dataSource(mockDataSource())
            .offsetStore(mockOffsetStore())
            .listener(mockListener());

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void builderThrowsOnMissingOffsetStore() {
    var builder =
        HybridEventSubscription.builder()
            .subscriptionName("test-sub")
            .dataSource(mockDataSource())
            .eventStore(mockEventStore())
            .listener(mockListener());

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void builderThrowsOnMissingListener() {
    var builder =
        HybridEventSubscription.builder()
            .subscriptionName("test-sub")
            .dataSource(mockDataSource())
            .eventStore(mockEventStore())
            .offsetStore(mockOffsetStore());

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void subscriptionNameIsUsedAsOffsetKey() throws InterruptedException {
    var offsetStore = new RecordingOffsetStore();
    var subscription =
        HybridEventSubscription.builder()
            .subscriptionName("orders-projection")
            .dataSource(mockDataSource())
            .eventStore(new EmptyEventStore())
            .offsetStore(offsetStore)
            .listener(mockListener())
            .config(config)
            .build();

    subscription.start();
    try {
      assertTrue(
          offsetStore.firstLookup.await(5, TimeUnit.SECONDS),
          "poll loop should look up the offset within 5s");
      assertEquals(ProjectionName.of("orders-projection"), offsetStore.lastQueriedName);
    } finally {
      subscription.close();
    }
  }

  @Test
  void onNotification_swallowsPollException_soListenLoopIsNeverTornDown()
      throws InterruptedException {
    // A wrapped-listener/read exception on the notify path must NOT propagate out of the
    // notification callback — otherwise it reaches PgNotificationListener's listen loop, which
    // treats it as a LISTEN-connection failure (closes the healthy LISTEN connection + bumps the
    // reconnect meter + backs off). The polling path owns retry/backoff; the notify-triggered poll
    // must swallow it. A large polling interval frees the internal poll lock for the whole test, so
    // the direct onNotificationReceived() call deterministically reaches the throwing read.
    var boom = new RuntimeException("notify-path transient blip (e.g. DLQ-TRANSIENT rethrow)");
    var reads = new java.util.concurrent.atomic.AtomicInteger(0);
    EventStore throwingStore =
        new MockEventStore() {
          @Override
          public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
            reads.incrementAndGet();
            throw boom;
          }
        };
    // listenNotifyEnabled=false: no PgNotificationListener is built (no real LISTEN needed); we
    // drive the notify handler directly. A 30s interval makes the background loop back off long
    // enough that the internal poll lock stays free for the direct call.
    var pollingOnly = new SubscriptionConfig(false, Duration.ofSeconds(30), Duration.ZERO);
    var subscription =
        new HybridEventSubscription(
            SubscriptionName.of("proj-r1"),
            mockDataSource(),
            throwingStore,
            mockOffsetStore(),
            mockListener(),
            pollingOnly,
            100);
    subscription.start();
    try {
      // Let the background loop take its first (throwing) poll and enter its long backoff, freeing
      // the poll lock — then the direct notify call reliably reaches the throwing read.
      long deadline = System.currentTimeMillis() + 5_000;
      while (reads.get() < 1 && System.currentTimeMillis() < deadline) {
        Thread.sleep(20);
      }
      int before = reads.get();
      assertDoesNotThrow(
          subscription::onNotificationReceived,
          "a poll-path exception on the notify path must be swallowed, not propagated to the "
              + "LISTEN loop");
      assertTrue(
          reads.get() > before,
          "the notify handler must actually have reached the throwing read (poll lock was free)");
    } finally {
      subscription.close();
    }
  }

  private HybridEventSubscription createSubscription() {
    return new HybridEventSubscription(
        SubscriptionName.of("test-sub"),
        mockDataSource(),
        mockEventStore(),
        mockOffsetStore(),
        mockListener(),
        config,
        100);
  }

  private DataSource mockDataSource() {
    return new MockDataSource();
  }

  private EventStore mockEventStore() {
    return new MockEventStore();
  }

  private OffsetStore mockOffsetStore() {
    return new MockOffsetStore();
  }

  private EventListener mockListener() {
    return event -> {};
  }

  /** Simple mock DataSource for testing. */
  private static class MockDataSource implements DataSource {
    @Override
    public java.sql.Connection getConnection() {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.sql.Connection getConnection(String username, String password) {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.io.PrintWriter getLogWriter() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLogWriter(java.io.PrintWriter out) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLoginTimeout(int seconds) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
      throw new UnsupportedOperationException();
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

  private static class MockEventStore implements EventStore {
    @Override
    public AggregateHistory load(StreamId streamId) {
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
      throw new UnsupportedOperationException();
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      throw new UnsupportedOperationException();
    }
  }

  /** Event store whose global stream is always empty — keeps the poll loop alive. */
  private static class EmptyEventStore extends MockEventStore {
    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      return List.of();
    }
  }

  private static class MockOffsetStore implements OffsetStore {
    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      return GlobalOffset.initial();
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {}
  }

  /** Records the projection name the poll loop uses as the offset key. */
  private static class RecordingOffsetStore implements OffsetStore {
    final CountDownLatch firstLookup = new CountDownLatch(1);
    volatile ProjectionName lastQueriedName;

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      lastQueriedName = projectionName;
      firstLookup.countDown();
      return GlobalOffset.initial();
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {}
  }
}
