package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.runtime.SubscriptionHealthContributor;
import org.streamrune.runtime.SubscriptionLease;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * A hybrid subscription built with {@code healthContributor(...)} registers itself, not the polling
 * subscription inside it, under its name when it starts and unregisters when it closes, so health
 * reads its push path and its lease role.
 */
class HybridEventSubscriptionHealthRegistrationTest {

  private static final String NAME = "hybrid-health";
  private static final SubscriptionConfig POLLING_ONLY =
      SubscriptionConfig.pollingOnly(Duration.ofMillis(50));

  private final InMemoryEventStore eventStore = new InMemoryEventStore();
  private final InMemoryOffsetStore offsetStore = new InMemoryOffsetStore();
  private final RecordingLeadership leadership = new RecordingLeadership();
  private final SubscriptionHealthContributor contributor =
      new SubscriptionHealthContributor(eventStore, offsetStore);
  private HybridEventSubscription subscription;
  private HikariDataSource deadPool;

  @AfterEach
  void closeEverything() {
    if (subscription != null) {
      subscription.close();
    }
    if (deadPool != null) {
      deadPool.close();
    }
  }

  @Test
  void startRegistersUnderItsName_closeUnregisters() {
    subscription =
        HybridEventSubscription.builder()
            .subscriptionName(NAME)
            .dataSource(deadPool())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .listener(events -> {})
            .config(POLLING_ONLY)
            .healthContributor(contributor)
            .build();
    assertThat(contributor.health()).isEmpty();

    subscription.start();

    SubscriptionHealth health = entry(NAME).orElseThrow();
    assertThat(health.state()).isEqualTo(SubscriptionLifecycleState.RUNNING);
    assertThat(health.role()).isEqualTo(LeaseRole.UNGATED);

    subscription.close();

    assertThat(contributor.health()).isEmpty();
  }

  @Test
  void itsRoleIsTheInnerSubscriptionsLeaseRole() {
    leadership.grant(NAME);
    subscription =
        HybridEventSubscription.builder()
            .subscriptionName(NAME)
            .dataSource(deadPool())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .listener(events -> {})
            .config(POLLING_ONLY)
            .leadership(leadership)
            .healthContributor(contributor)
            .build();

    subscription.start();

    await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> assertThat(entry(NAME).orElseThrow().role()).isEqualTo(LeaseRole.LEADING));
    assertThat(subscription.leaseRole()).isEqualTo(LeaseRole.LEADING);
    leadership.revoke(NAME);
    assertThat(entry(NAME).orElseThrow().role()).isEqualTo(LeaseRole.STANDBY);
  }

  @Test
  void itRegistersItself_soADeadPushPathReadsDegraded() {
    UnaryOperator<PgNotificationListener.Builder> quickGrace =
        b -> b.reconnectGrace(200).backoff(50, 100);
    subscription =
        new HybridEventSubscription(
            SubscriptionName.of(NAME),
            deadPool(),
            eventStore,
            offsetStore,
            events -> {},
            new SubscriptionConfig(true, Duration.ofMillis(50), Duration.ofMillis(10)),
            100,
            StreamRuneMetrics.NOOP,
            0,
            quickGrace,
            SubscriptionLease.ungated(),
            contributor);

    subscription.start();

    await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              SubscriptionHealth health = entry(NAME).orElseThrow();
              assertThat(health.state()).isEqualTo(SubscriptionLifecycleState.RUNNING);
              assertThat(health.status()).isEqualTo(SubscriptionHealth.Status.DEGRADED);
            });
  }

  private Optional<SubscriptionHealth> entry(String name) {
    return contributor.health().stream().filter(h -> h.name().equals(name)).findFirst();
  }

  /** A pool whose database nothing listens on: the push path cannot connect. */
  private DataSource deadPool() {
    var config = new HikariConfig();
    config.setJdbcUrl("jdbc:postgresql://127.0.0.1:1/nowhere");
    config.setUsername("nobody");
    config.setPassword("nothing");
    config.setInitializationFailTimeout(-1);
    config.setMaximumPoolSize(1);
    deadPool = new HikariDataSource(config);
    return deadPool;
  }
}
