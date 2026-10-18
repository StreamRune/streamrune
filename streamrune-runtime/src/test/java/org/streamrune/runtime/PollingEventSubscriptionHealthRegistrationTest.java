package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * A polling subscription built with {@code healthContributor(...)} registers itself under its name
 * when it starts and unregisters when it closes; its health entry carries its role under the lease.
 */
class PollingEventSubscriptionHealthRegistrationTest {

  private static final String NAME = "orders";

  private final InMemoryEventStore eventStore = new InMemoryEventStore();
  private final InMemoryOffsetStore offsetStore = new InMemoryOffsetStore();
  private final RecordingLeadership leadership = new RecordingLeadership();
  private final SubscriptionHealthContributor contributor =
      new SubscriptionHealthContributor(eventStore, offsetStore);
  private final List<PollingEventSubscription> subscriptions = new ArrayList<>();

  @AfterEach
  void closeEverySubscription() {
    subscriptions.forEach(PollingEventSubscription::close);
  }

  @Test
  void startRegisters_closeUnregisters() {
    PollingEventSubscription subscription = subscription(b -> b.healthContributor(contributor));
    assertThat(contributor.health()).isEmpty();

    subscription.start();

    SubscriptionHealth health = entry(NAME).orElseThrow();
    assertThat(health.state()).isEqualTo(SubscriptionLifecycleState.RUNNING);

    subscription.close();

    assertThat(contributor.health()).isEmpty();
  }

  @Test
  void aPausedSubscriptionStaysRegistered() {
    PollingEventSubscription subscription = subscription(b -> b.healthContributor(contributor));
    subscription.start();

    subscription.pause();

    SubscriptionHealth health = entry(NAME).orElseThrow();
    assertThat(health.state()).isEqualTo(SubscriptionLifecycleState.PAUSED);
  }

  @Test
  void withoutAContributorNothingIsRegistered() {
    PollingEventSubscription subscription = subscription(b -> b);

    subscription.start();
    subscription.close();

    assertThat(contributor.health()).isEmpty();
  }

  @Test
  void itsRoleIsItsLeasesRole() {
    PollingEventSubscription ungated = subscription(b -> b.healthContributor(contributor));
    assertThat(ungated.leaseRole()).isEqualTo(LeaseRole.UNGATED);
    ungated.start();
    assertThat(entry(NAME).orElseThrow().role()).isEqualTo(LeaseRole.UNGATED);
    ungated.close();

    leadership.grant("gated");
    PollingEventSubscription leading =
        subscription(
            b -> b.subscriptionName("gated").leadership(leadership).healthContributor(contributor));
    leading.start();
    await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> assertThat(entry("gated").orElseThrow().role()).isEqualTo(LeaseRole.LEADING));
    leading.close();

    PollingEventSubscription standby =
        subscription(
            b ->
                b.subscriptionName("standing-by")
                    .leadership(leadership)
                    .healthContributor(contributor));
    standby.start();
    await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertThat(entry("standing-by").orElseThrow().role()).isEqualTo(LeaseRole.STANDBY));
  }

  private Optional<SubscriptionHealth> entry(String name) {
    return contributor.health().stream().filter(h -> h.name().equals(name)).findFirst();
  }

  private PollingEventSubscription subscription(
      java.util.function.UnaryOperator<PollingEventSubscription.Builder> more) {
    PollingEventSubscription subscription =
        more.apply(
                PollingEventSubscription.builder()
                    .subscriptionName(NAME)
                    .eventStore(eventStore)
                    .offsetStore(offsetStore)
                    .listener(events -> {})
                    .config(SubscriptionConfig.pollingOnly(Duration.ofMillis(50))))
            .build();
    subscriptions.add(subscription);
    return subscription;
  }
}
