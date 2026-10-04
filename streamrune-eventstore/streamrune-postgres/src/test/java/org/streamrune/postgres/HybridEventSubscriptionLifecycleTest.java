package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;

/** Lifecycle state-machine tests for HybridEventSubscription. */
class HybridEventSubscriptionLifecycleTest {

  private final SubscriptionConfig config =
      new SubscriptionConfig(true, Duration.ofMillis(100), Duration.ofMillis(10));

  // ── State assertions ──────────────────────────────────────────────────────

  @Test
  void state_isCreated_beforeStart() {
    var sub = createSubscription();

    assertEquals(SubscriptionLifecycleState.CREATED, sub.state());
  }

  @Test
  void state_isRunning_afterStart() {
    var sub = createSubscription();

    sub.start();
    try {
      assertEquals(SubscriptionLifecycleState.RUNNING, sub.state());
      assertTrue(sub.isRunning());
    } finally {
      sub.close();
    }
  }

  @Test
  void state_isPaused_afterPause() {
    var sub = createSubscription();
    sub.start();
    try {
      sub.pause();
      assertEquals(SubscriptionLifecycleState.PAUSED, sub.state());
      assertFalse(sub.isRunning());
    } finally {
      sub.close();
    }
  }

  @Test
  void state_isRunning_afterResume() {
    var sub = createSubscription();
    sub.start();
    try {
      sub.pause();
      sub.resume();
      assertEquals(SubscriptionLifecycleState.RUNNING, sub.state());
      assertTrue(sub.isRunning());
    } finally {
      sub.close();
    }
  }

  @Test
  void state_isStopped_afterClose() {
    var sub = createSubscription();
    sub.start();

    sub.close();

    assertEquals(SubscriptionLifecycleState.STOPPED, sub.state());
    assertFalse(sub.isRunning());
  }

  @Test
  void state_isStopped_afterClose_fromPaused() {
    var sub = createSubscription();
    sub.start();
    sub.pause();

    sub.close();

    assertEquals(SubscriptionLifecycleState.STOPPED, sub.state());
    assertFalse(sub.isRunning());
  }

  // ── Illegal-state guards ──────────────────────────────────────────────────

  @Test
  void pause_throwsIllegalStateException_whenNotRunning() {
    var sub = createSubscription();
    // still CREATED — pause must reject

    assertThrows(IllegalStateException.class, sub::pause);
  }

  @Test
  void resume_throwsIllegalStateException_whenNotPaused() {
    var sub = createSubscription();
    sub.start();
    try {
      // RUNNING — resume must reject
      assertThrows(IllegalStateException.class, sub::resume);
    } finally {
      sub.close();
    }
  }

  @Test
  void resume_throwsIllegalStateException_whenStopped() {
    var sub = createSubscription();
    sub.start();
    sub.close();

    // STOPPED — resume must reject
    assertThrows(IllegalStateException.class, sub::resume);
  }

  @Test
  void start_throwsIllegalStateException_whenAlreadyRunning() {
    var sub = createSubscription();
    sub.start();
    try {
      assertThrows(IllegalStateException.class, sub::start);
    } finally {
      sub.close();
    }
  }

  @Test
  void start_throwsIllegalStateException_whenStopped() {
    var sub = createSubscription();
    sub.start();
    sub.close();

    // STOPPED — start must reject
    assertThrows(IllegalStateException.class, sub::start);
  }

  @Test
  void pause_throwsIllegalStateException_whenStopped() {
    var sub = createSubscription();
    sub.start();
    sub.close();

    // STOPPED — pause must reject
    assertThrows(IllegalStateException.class, sub::pause);
  }

  // ── Push-path liveness (FIX 5) ────────────────────────────────────────────

  @Test
  void pushHealthy_trueWhileStarted_despiteFailingConnection() throws Exception {
    // MockDataSource.getConnection() always throws. Before FIX 5 the listener gave up after 5
    // reconnect attempts and pushHealthy() would flip to false within seconds. With unbounded
    // reconnect the listener is kept alive, so the push path stays observable as healthy while it
    // keeps retrying (delivery meanwhile continues via polling).
    var sub = createSubscription();
    sub.start();
    try {
      assertTrue(sub.pushHealthy(), "listener kept alive by unbounded reconnect while started");
      // Still healthy after a short spell (would have exceeded the old 5-attempt cap by now).
      Thread.sleep(200);
      assertTrue(
          sub.pushHealthy(), "listener must not permanently give up on a failing connection");
    } finally {
      sub.close();
    }
  }

  @Test
  void pushHealthy_falseAfterClose() {
    var sub = createSubscription();
    sub.start();
    sub.close();

    assertFalse(sub.pushHealthy(), "closed subscription reports the push path as not healthy");
  }

  // ── pollingOnly disables LISTEN/NOTIFY ──────────────────────

  @Test
  void pollingOnly_opensNoListenConnection() throws Exception {
    // SubscriptionConfig.pollingOnly(...) is documented as the PgBouncer mitigation
    // ("disable LISTEN/NOTIFY and fall back to polling"), but HybridEventSubscription ignored the
    // flag and unconditionally built + started PgNotificationListener — thrashing its unbounded
    // reconnect loop behind the pooler (a dedicated single-connection pool, WARN spam) while the
    // operator believed push was off. Honoring the flag means NO LISTEN connection is ever opened.
    var connectionAttempts = new AtomicInteger(0);
    DataSource counting = new CountingDataSource(connectionAttempts);
    var sub =
        new HybridEventSubscription(
            SubscriptionName.of("polling-only-sub"),
            counting,
            mockEventStore(),
            mockOffsetStore(),
            mockListener(),
            SubscriptionConfig.pollingOnly(Duration.ofMillis(50)),
            100);
    sub.start();
    try {
      // Give a would-be reconnect loop ample time to try opening LISTEN connections.
      Thread.sleep(300);
      assertEquals(
          0,
          connectionAttempts.get(),
          "pollingOnly must open NO LISTEN connection (LISTEN/NOTIFY disabled)");
      assertTrue(sub.isRunning(), "polling delivery must still be running");
      assertFalse(sub.pushHealthy(), "with LISTEN/NOTIFY disabled the push path is not active");
    } finally {
      sub.close();
    }
  }

  // ── Idempotent close ─────────────────────────────────────────────────────

  @Test
  void close_isIdempotent() {
    var sub = createSubscription();
    sub.start();

    sub.close();
    // Second close must not throw
    assertDoesNotThrow(sub::close);
    assertEquals(SubscriptionLifecycleState.STOPPED, sub.state());
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private HybridEventSubscription createSubscription() {
    return new HybridEventSubscription(
        SubscriptionName.of("lifecycle-test-sub"),
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

  /**
   * Mock DataSource that throws on getConnection() — safe because the listener runs on a daemon
   * thread that never blocks the test.
   */
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

  /**
   * DataSource that counts every {@code getConnection()} attempt (then throws, like {@link
   * MockDataSource}). A LISTEN connection is opened only through this method, so a count of zero
   * proves LISTEN/NOTIFY was never activated.
   */
  private static final class CountingDataSource extends MockDataSource {
    private final AtomicInteger attempts;

    CountingDataSource(AtomicInteger attempts) {
      this.attempts = attempts;
    }

    @Override
    public java.sql.Connection getConnection() {
      attempts.incrementAndGet();
      return super.getConnection();
    }

    @Override
    public java.sql.Connection getConnection(String username, String password) {
      attempts.incrementAndGet();
      return super.getConnection(username, password);
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
      return List.of();
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      throw new UnsupportedOperationException();
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
}
