package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.runtime.SubscriptionHealthContributor;
import org.streamrune.test.InMemoryEventStore;

/**
 * A dead LISTEN/NOTIFY push path must be observable.
 *
 * <p>When {@code resolveListenSource()} fails at startup — the dedicated single-connection pool
 * cannot be created because {@code max_connections} is reached, the credentials are scoped to the
 * main pool only, or the TLS handshake fails — the listener logs one ERROR at boot, goes CLOSED and
 * returns before the reconnect loop is ever entered. {@code
 * streamrune.subscriptions.listener.reconnects} therefore stays at 0 forever, which is also exactly
 * what a perfectly healthy listener reports, and no health component covered the listener at all:
 * every subscription silently degraded from millisecond push delivery to poll-interval delivery for
 * the life of the process while {@code /health} reported UP and every metric read healthy.
 */
class HybridEventSubscriptionPushHealthTest {

  private static final SubscriptionConfig CONFIG =
      new SubscriptionConfig(true, Duration.ofMillis(50), Duration.ofMillis(10));

  @Test
  void aListenerThatDiesAtStartupDegradesTheSubscriptionHealth() throws Exception {
    var eventStore = new InMemoryEventStore();
    OffsetStore offsetStore = mock(OffsetStore.class);
    when(offsetStore.getLastOffset(any())).thenReturn(GlobalOffset.initial());
    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore);

    // A pooled DataSource with no JDBC URL: createListenDataSource copies the (null) url into a new
    // HikariConfig, whose validation rejects it — so resolveListenSource throws on the listener
    // thread, which is the startup-failure path above. Polling delivery is unaffected.
    HikariDataSource brokenPool = new HikariDataSource();
    var subscription =
        new HybridEventSubscription(
            SubscriptionName.of("orders"),
            brokenPool,
            eventStore,
            offsetStore,
            events -> {},
            CONFIG,
            100);
    try {
      subscription.start();
      contributor.register("orders", subscription);

      awaitPushPathDead(subscription);

      assertTrue(subscription.isRunning(), "polling still delivers — the subscription is not down");
      SubscriptionHealth health = contributor.health().getFirst();
      assertEquals(
          SubscriptionHealth.Status.DEGRADED,
          health.status(),
          "a subscription whose push path is dead must not report UP: delivery has silently"
              + " degraded to the poll interval for the life of the process");
      assertNotEquals(
          SubscriptionHealth.Status.DOWN,
          health.status(),
          "and it must not report DOWN either — polling is still delivering");
    } finally {
      subscription.close();
      brokenPool.close();
    }
  }

  @Test
  void aPollingOnlySubscriptionIsNotDegraded() {
    // With LISTEN/NOTIFY disabled there is no push path to be dead. "Not configured"
    // must never be reported as a fault, or every pollingOnly deployment reads DEGRADED forever.
    var eventStore = new InMemoryEventStore();
    OffsetStore offsetStore = mock(OffsetStore.class);
    when(offsetStore.getLastOffset(any())).thenReturn(GlobalOffset.initial());
    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore);

    var pollingOnly = new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10));
    var subscription =
        new HybridEventSubscription(
            SubscriptionName.of("orders-polling-only"),
            new HikariDataSource(),
            eventStore,
            offsetStore,
            events -> {},
            pollingOnly,
            100);
    try {
      subscription.start();
      contributor.register("orders-polling-only", subscription);

      assertEquals(SubscriptionHealth.Status.UP, contributor.health().getFirst().status());
    } finally {
      subscription.close();
    }
  }

  private static void awaitPushPathDead(HybridEventSubscription subscription)
      throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline) {
      if (!subscription.pushHealthy()) {
        return;
      }
      Thread.sleep(20);
    }
    fail("the listener never reported a dead push path");
  }
}
