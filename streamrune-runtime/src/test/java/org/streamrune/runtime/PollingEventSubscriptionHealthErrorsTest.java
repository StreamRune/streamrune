package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * A polling subscription built with {@code healthContributor(...)} records each page whose listener
 * threw as a delivery error, and a page that was delivered clears them — the semantics of {@link
 * HealthTrackingEventListener} — without counting twice when the application already wrapped its
 * listener in one for the same contributor and name.
 */
class PollingEventSubscriptionHealthErrorsTest {

  private static final String NAME = "orders";
  private static final AggregateType TYPE = AggregateType.of("cart");

  record Added(String sku) implements DomainEvent {}

  private final InMemoryEventStore eventStore = new InMemoryEventStore();
  private final InMemoryOffsetStore offsetStore = new InMemoryOffsetStore();
  private final SubscriptionHealthContributor contributor =
      new SubscriptionHealthContributor(eventStore, offsetStore);
  private final List<PollingEventSubscription> subscriptions = new ArrayList<>();
  private final AtomicBoolean failing = new AtomicBoolean(true);
  private final AtomicInteger calls = new AtomicInteger();

  @AfterEach
  void closeEverySubscription() {
    subscriptions.forEach(PollingEventSubscription::close);
  }

  @Test
  void eachFailedPageIsADeliveryError_andTheStatusIsDegraded() {
    appendOne();
    PollingEventSubscription subscription = startedAndFirstPollFailed(failingListener());

    poll(subscription);
    poll(subscription);

    SubscriptionHealth health = entry().orElseThrow();
    assertThat(health.errorCount()).isEqualTo(3);
    assertThat(health.status()).isEqualTo(SubscriptionHealth.Status.DEGRADED);
  }

  @Test
  void aDeliveredPageClearsTheErrors() {
    appendOne();
    PollingEventSubscription subscription = startedAndFirstPollFailed(failingListener());
    poll(subscription);
    assertThat(entry().orElseThrow().errorCount()).isEqualTo(2);

    failing.set(false);
    poll(subscription);

    SubscriptionHealth health = entry().orElseThrow();
    assertThat(health.errorCount()).isZero();
    assertThat(health.status()).isEqualTo(SubscriptionHealth.Status.UP);
  }

  @Test
  void aListenerAlreadyWrappedForTheSameContributorAndNameIsNotCountedTwice() {
    appendOne();
    EventListener wrapped =
        new HealthTrackingEventListener(failingListener(), contributor, SubscriptionName.of(NAME));
    PollingEventSubscription subscription = startedAndFirstPollFailed(wrapped);

    poll(subscription);
    poll(subscription);

    assertThat(entry().orElseThrow().errorCount()).isEqualTo(3);
  }

  @Test
  void aListenerWrappedForAnotherContributorIsStillTracked() {
    appendOne();
    var other = new SubscriptionHealthContributor(eventStore, offsetStore);
    EventListener wrapped =
        new HealthTrackingEventListener(failingListener(), other, SubscriptionName.of(NAME));
    PollingEventSubscription subscription = startedAndFirstPollFailed(wrapped);

    poll(subscription);

    assertThat(entry().orElseThrow().errorCount()).isEqualTo(2);
    assertThat(other.health()).isEmpty();
  }

  @Test
  void withoutAContributorNothingIsRecorded() {
    appendOne();
    PollingEventSubscription subscription = subscription(failingListener(), false);
    subscription.start();
    contributor.register(NAME, subscription);

    poll(subscription);

    assertThat(entry().orElseThrow().errorCount()).isZero();
  }

  @Test
  void theSubscriptionIsRegisteredBeforeItsFirstPoll() {
    appendOne();
    AtomicBoolean registeredAtFirstCall = new AtomicBoolean();
    AtomicBoolean firstCall = new AtomicBoolean(true);
    EventListener listener =
        events -> {
          if (firstCall.compareAndSet(true, false)) {
            registeredAtFirstCall.set(entry().isPresent());
          }
          throw new IllegalStateException("the listener cannot handle the page");
        };
    subscription(listener, true).start();
    await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(firstCall).isFalse());

    assertThat(registeredAtFirstCall).isTrue();
    await()
        .atMost(10, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(entry().orElseThrow().errorCount()).isEqualTo(1));
  }

  @Test
  void aStandbyThatLostItsLeaseWhileFailing_readsUpWithNoErrors_andCountsAfresh() {
    String lease = "saga:orders";
    RecordingLeadership leadership = new RecordingLeadership();
    leadership.grant(lease);
    AtomicLong clock = new AtomicLong(1_000_000_000L);
    appendOne();
    PollingEventSubscription subscription =
        PollingEventSubscription.builder()
            .subscriptionName(NAME)
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .listener(failingListener())
            .config(SubscriptionConfig.pollingOnly(Duration.ofHours(1)))
            .lease(SubscriptionLease.of(leadership, lease, clock::get))
            .healthContributor(contributor)
            .build();
    subscriptions.add(subscription);
    subscription.start();
    await()
        .atMost(10, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(entry().orElseThrow().errorCount()).isEqualTo(1));
    poll(subscription);
    assertThat(entry().orElseThrow().errorCount()).isEqualTo(2);
    assertThat(entry().orElseThrow().role()).isEqualTo(LeaseRole.LEADING);

    leadership.revoke(lease);

    SubscriptionHealth standby = entry().orElseThrow();
    assertThat(standby.role()).isEqualTo(LeaseRole.STANDBY);
    assertThat(standby.errorCount()).isZero();
    assertThat(standby.status()).isEqualTo(SubscriptionHealth.Status.UP);

    leadership.grant(lease);
    clock.addAndGet(Duration.ofHours(2).toNanos());
    poll(subscription);

    SubscriptionHealth leading = entry().orElseThrow();
    assertThat(leading.role()).isEqualTo(LeaseRole.LEADING);
    assertThat(leading.errorCount()).isEqualTo(1);
    assertThat(leading.status()).isEqualTo(SubscriptionHealth.Status.DEGRADED);
  }

  /** Starts a subscription that polls once an hour and waits for its first poll to fail. */
  private PollingEventSubscription startedAndFirstPollFailed(EventListener listener) {
    PollingEventSubscription subscription = subscription(listener, true);
    subscription.start();
    await()
        .atMost(10, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(entry().orElseThrow().errorCount()).isEqualTo(1));
    return subscription;
  }

  /** One poll on this thread; a failing listener's exception is the expected outcome. */
  private static void poll(PollingEventSubscription subscription) {
    try {
      subscription.pollOnce();
    } catch (RuntimeException expected) {
      // the page failed; the checkpoint stays and the health contributor was told
    }
  }

  private EventListener failingListener() {
    return events -> {
      calls.incrementAndGet();
      if (failing.get()) {
        throw new IllegalStateException("the listener cannot handle the page");
      }
    };
  }

  private Optional<SubscriptionHealth> entry() {
    return contributor.health().stream().filter(h -> h.name().equals(NAME)).findFirst();
  }

  private PollingEventSubscription subscription(EventListener listener, boolean withContributor) {
    PollingEventSubscription.Builder builder =
        PollingEventSubscription.builder()
            .subscriptionName(NAME)
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .listener(listener)
            .config(SubscriptionConfig.pollingOnly(Duration.ofHours(1)));
    if (withContributor) {
      builder.healthContributor(contributor);
    }
    PollingEventSubscription subscription = builder.build();
    subscriptions.add(subscription);
    return subscription;
  }

  private void appendOne() {
    StreamId stream = StreamId.of(TYPE, AggregateId.of("cart-1"));
    eventStore.append(
        stream,
        List.of(
            new EventEnvelope(
                GlobalOffset.of(1),
                stream,
                new Version(1),
                new EventType("Added"),
                new Added("X"),
                new EventMetadata(
                    EventId.of("evt_1"),
                    CommandId.of("cmd_1"),
                    null,
                    null,
                    CorrelationId.of("corr"),
                    null,
                    null,
                    Instant.now()))),
        Version.initial());
  }
}
