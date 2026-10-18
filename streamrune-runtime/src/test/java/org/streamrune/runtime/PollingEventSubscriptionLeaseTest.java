package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * A subscription built with a lease: the gate before the checkpoint read, the anchor's lifetime
 * across pause, resume, close and a terminal exit, and the builder's sources of the lease.
 */
@ExtendWith(MockitoExtension.class)
class PollingEventSubscriptionLeaseTest {

  private static final String NAME = "orders";
  private static final ProjectionName CHECKPOINT = ProjectionName.of(NAME);
  private static final Duration POLLING = Duration.ofMillis(50);

  record Tick(int n) implements DomainEvent {}

  @Mock EventStore eventStore;

  private final List<EventEnvelope> stream = new CopyOnWriteArrayList<>();
  private final List<EventEnvelope> delivered = new CopyOnWriteArrayList<>();
  private final OffsetStore offsetStore = spy(new InMemoryOffsetStore());
  private final RecordingLeadership leadership = spy(new RecordingLeadership());
  private final List<PollingEventSubscription> subscriptions = new ArrayList<>();
  private final CountDownLatch release = new CountDownLatch(1);

  @BeforeEach
  void readTheStreamFromTheList() {
    lenient()
        .when(eventStore.readGlobalStream(any(), anyInt()))
        .thenAnswer(
            call -> {
              GlobalOffset after = call.getArgument(0);
              int max = call.getArgument(1);
              return stream.stream()
                  .filter(e -> e.globalOffset().value() > after.value())
                  .limit(max)
                  .toList();
            });
  }

  @AfterEach
  void closeEverySubscription() {
    release.countDown();
    subscriptions.forEach(PollingEventSubscription::close);
  }

  @Test
  void aStandbyReadsNeitherTheCheckpointNorTheStream() throws Exception {
    append(3);
    PollingEventSubscription subscription = leased(b -> b);

    subscription.start();
    await().atMost(5, TimeUnit.SECONDS).until(() -> leadership.tryAcquires(NAME) >= 1);
    Thread.sleep(300);

    verifyNoInteractions(eventStore, offsetStore);
    assertThat(delivered).isEmpty();
    assertThat(subscription.consecutiveFailures()).isZero();
    assertThat(subscription.lease().role()).isEqualTo(LeaseRole.STANDBY);
    assertThat(subscription.state()).isEqualTo(SubscriptionLifecycleState.RUNNING);
  }

  @Test
  void aLeaderDrains() {
    leadership.grant(NAME);
    append(3);
    PollingEventSubscription subscription = leased(b -> b);

    subscription.start();

    await().atMost(5, TimeUnit.SECONDS).until(() -> delivered.size() == 3);
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> offsetStore.getLastOffset(CHECKPOINT).value() == 3);
    assertThat(subscription.lease().role()).isEqualTo(LeaseRole.LEADING);
  }

  @Test
  void aLeaseLostBetweenPages_endsTheDrainAfterThePageInFlight() throws Exception {
    leadership.grant(NAME);
    append(3);
    PollingEventSubscription subscription =
        leased(
            b ->
                b.fetchSize(1)
                    .config(SubscriptionConfig.pollingOnly(Duration.ofHours(1)))
                    .listener(
                        events -> {
                          delivered.addAll(events);
                          leadership.revoke(NAME);
                        }));

    subscription.start();

    await().atMost(5, TimeUnit.SECONDS).until(() -> delivered.size() == 1);
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> offsetStore.getLastOffset(CHECKPOINT).value() == 1);
    Thread.sleep(300);
    assertThat(delivered).hasSize(1);
    verify(eventStore, times(1)).readGlobalStream(any(), anyInt());
  }

  @Test
  void pauseResignsAfterThePageInFlight_andResumeAnchorsAgain() {
    leadership.grant(NAME);
    append(1);
    CountDownLatch parked = new CountDownLatch(1);
    PollingEventSubscription subscription = leased(b -> b.listener(parkingOnce(parked)));
    subscription.start();
    SagaStepLockFixture.await(parked);

    CompletableFuture<Void> pausing = SagaStepLockFixture.onAnotherThread(subscription::pause);
    assertThat(SagaStepLockFixture.outcomeOf(pausing)).isNull();
    assertThat(leadership.resigns(NAME)).isZero();

    release.countDown();
    await().atMost(5, TimeUnit.SECONDS).until(() -> leadership.resigns(NAME) == 1);
    InOrder order = inOrder(offsetStore, leadership);
    order.verify(offsetStore).saveOffset(eq(CHECKPOINT), eq(GlobalOffset.of(1)));
    order.verify(leadership).resign(NAME);

    subscription.resume();
    await().atMost(5, TimeUnit.SECONDS).until(() -> leadership.tryAcquires(NAME) == 2);
    await().atMost(5, TimeUnit.SECONDS).until(() -> subscription.lease().isLeadingHere());
    assertThat(leadership.resigns(NAME)).isEqualTo(1);
  }

  @Test
  void closeResignsAfterThePollThreadLeftThePage() {
    leadership.grant(NAME);
    append(1);
    CountDownLatch parked = new CountDownLatch(1);
    PollingEventSubscription subscription = leased(b -> b.listener(parkingOnce(parked)));
    subscription.start();
    SagaStepLockFixture.await(parked);

    CompletableFuture<Void> closing = SagaStepLockFixture.onAnotherThread(subscription::close);
    await()
        .pollDelay(Duration.ofMillis(200))
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> subscription.state() == SubscriptionLifecycleState.STOPPED);
    assertThat(closing).isNotDone();
    assertThat(leadership.resigns(NAME)).isZero();

    release.countDown();
    assertThat(SagaStepLockFixture.outcomeOf(closing)).isNull();
    assertThat(leadership.resigns(NAME)).isEqualTo(1);
    InOrder order = inOrder(offsetStore, leadership);
    order.verify(offsetStore).saveOffset(eq(CHECKPOINT), eq(GlobalOffset.of(1)));
    order.verify(leadership).resign(NAME);
    assertThat(subscription.lease().isLeadingHere()).isFalse();
  }

  @Test
  void theResignOfAPauseOrACloseRunsWithTheInterruptHeldBack() {
    leadership.grant(NAME);
    PollingEventSubscription subscription = leased(b -> b);
    subscription.start();
    await().atMost(5, TimeUnit.SECONDS).until(() -> subscription.lease().isLeadingHere());

    subscription.pause();
    await().atMost(5, TimeUnit.SECONDS).until(() -> leadership.resigns(NAME) == 1);
    subscription.resume();
    await().atMost(5, TimeUnit.SECONDS).until(() -> subscription.lease().isLeadingHere());
    subscription.close();

    assertThat(leadership.resigns(NAME)).isEqualTo(2);
    assertThat(leadership.resignsOnAnInterruptedThread()).isZero();
  }

  @Test
  void anErrorThatStopsThePollThreadResigns() {
    leadership.grant(NAME);
    append(1);
    PollingEventSubscription subscription =
        leased(
            b ->
                b.listener(
                    events -> {
                      throw new AssertionError("the listener fails for good");
                    }));

    subscription.start();

    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> subscription.state() == SubscriptionLifecycleState.STOPPED);
    await().atMost(5, TimeUnit.SECONDS).until(() -> leadership.resigns(NAME) == 1);
    verify(offsetStore, never()).saveOffset(any(), any());
  }

  @Test
  void wakeUpsOnAStandbyAskForTheLeaseOncePerInterval() {
    append(3);
    PollingEventSubscription subscription =
        leased(b -> b.config(SubscriptionConfig.pollingOnly(Duration.ofHours(1))));
    subscription.start();
    await().atMost(5, TimeUnit.SECONDS).until(() -> leadership.tryAcquires(NAME) == 1);

    for (int i = 0; i < 50; i++) {
      assertThat(subscription.triggerImmediatePoll()).isZero();
    }

    assertThat(leadership.tryAcquires(NAME)).isEqualTo(1);
    verifyNoInteractions(eventStore, offsetStore);
  }

  @Test
  void aNeverStartedGatedSubscriptionDeliversNothingOnAWakeUp() {
    leadership.grant(NAME);
    append(3);
    PollingEventSubscription subscription = leased(b -> b);

    assertThat(subscription.triggerImmediatePoll()).isZero();

    verifyNoInteractions(eventStore, offsetStore);
    assertThat(leadership.tryAcquires(NAME)).isZero();
    assertThat(delivered).isEmpty();
  }

  @Test
  void aSecondSubscriptionOfTheSameLeaseIsRefusedAtStart() {
    SubscriptionLease lease = SubscriptionLease.of(leadership, NAME);
    PollingEventSubscription first = track(builder().lease(lease).build());
    PollingEventSubscription second = track(builder().lease(lease).build());
    first.start();

    assertThatThrownBy(second::start)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("already has a delivering subscription in this process");
    assertThat(second.state()).isEqualTo(SubscriptionLifecycleState.CREATED);
    assertThat(second.lease()).isSameAs(lease);
  }

  @Test
  void sagaRunnerTogetherWithListenerIsRefused() {
    try (var process = gatedProcess()) {
      assertThatThrownBy(
              () ->
                  PollingEventSubscription.builder()
                      .subscriptionName(NAME)
                      .eventStore(eventStore)
                      .offsetStore(offsetStore)
                      .sagaRunner(process.runner)
                      .listener(delivered::addAll)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              "PollingEventSubscription.Builder: sagaRunner(...) and listener(...) are both set"
                  + " — sagaRunner(runner) delivers to runner.asEventListener(); name one of"
                  + " them");
    }
  }

  @Test
  void sagaRunnerTogetherWithLeaseIsRefused() {
    try (var process = gatedProcess()) {
      assertThatThrownBy(
              () ->
                  PollingEventSubscription.builder()
                      .subscriptionName(NAME)
                      .eventStore(eventStore)
                      .offsetStore(offsetStore)
                      .sagaRunner(process.runner)
                      .lease(SubscriptionLease.ungated())
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              "PollingEventSubscription.Builder: sagaRunner(...) and lease(...) are both set —"
                  + " sagaRunner(runner) takes runner.lease(); name one of them");
    }
  }

  @Test
  void sagaRunnerTogetherWithLeadershipIsRefused() {
    try (var process = gatedProcess()) {
      assertThatThrownBy(
              () ->
                  PollingEventSubscription.builder()
                      .subscriptionName(NAME)
                      .eventStore(eventStore)
                      .offsetStore(offsetStore)
                      .sagaRunner(process.runner)
                      .leadership(leadership)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              "PollingEventSubscription.Builder: sagaRunner(...) and leadership(...) are both"
                  + " set — sagaRunner(runner) takes runner.lease(); name one of them");
    }
  }

  @Test
  void leaseTogetherWithLeadershipIsRefused() {
    assertThatThrownBy(
            () ->
                builder()
                    .lease(SubscriptionLease.of(leadership, NAME))
                    .leadership(leadership)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "PollingEventSubscription.Builder: lease(...) and leadership(...) are both set —"
                + " leadership(l) is lease(SubscriptionLease.of(l, subscriptionName)); name one"
                + " of them");
  }

  @Test
  void aLeaseIsRequired() {
    assertThatThrownBy(
            () ->
                new PollingEventSubscription(
                    org.streamrune.core.types.SubscriptionName.of(NAME),
                    eventStore,
                    offsetStore,
                    delivered::addAll,
                    SubscriptionConfig.pollingOnly(POLLING),
                    10,
                    null,
                    0,
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("lease is required");
  }

  @Test
  void sagaRunnerSetsTheListenerAndTheLease() {
    String leaseName = SagaRunner.LEASE_NAME_PREFIX + SagaStepLockFixture.TYPE.value();
    leadership.grant(leaseName);
    try (var process = new SagaStepLockFixture(leadership, Duration.ofHours(1))) {
      PollingEventSubscription subscription =
          track(
              PollingEventSubscription.builder()
                  .subscriptionName("orders-saga")
                  .eventStore(process.eventStore)
                  .offsetStore(new InMemoryOffsetStore())
                  .config(SubscriptionConfig.pollingOnly(POLLING))
                  .sagaRunner(process.runner)
                  .build());

      assertThat(subscription.lease()).isSameAs(process.runner.lease());
      subscription.start();
      process.append(new Opened("1"));
      process.awaitRow("1");
      assertThat(leadership.tryAcquires(leaseName)).isEqualTo(1);
      subscription.close();
    }
  }

  @Test
  void leadershipNamesTheLeaseAfterTheSubscription() {
    PollingEventSubscription subscription =
        track(
            PollingEventSubscription.builder()
                .subscriptionName("payments")
                .eventStore(eventStore)
                .offsetStore(offsetStore)
                .listener(delivered::addAll)
                .config(SubscriptionConfig.pollingOnly(Duration.ofHours(1)))
                .leadership(leadership)
                .build());

    subscription.start();

    await().atMost(5, TimeUnit.SECONDS).until(() -> leadership.tryAcquires("payments") == 1);
    assertThat(subscription.lease().name()).isEqualTo("payments");
    assertThat(subscription.lease().isGated()).isTrue();
  }

  @Test
  void anUngatedSubscriptionCallsNoLeadership() {
    append(2);
    PollingEventSubscription subscription = track(builder().build());

    subscription.start();

    await().atMost(5, TimeUnit.SECONDS).until(() -> delivered.size() == 2);
    assertThat(subscription.lease().isGated()).isFalse();
    assertThat(subscription.lease().role()).isEqualTo(LeaseRole.UNGATED);
    verifyNoInteractions(leadership);
    subscription.pause();
    subscription.resume();
    append(1);
    await().atMost(5, TimeUnit.SECONDS).until(() -> delivered.size() == 3);
    verifyNoInteractions(leadership);
  }

  @Test
  void aNullLeaseOrLeadershipIsNoSource() {
    PollingEventSubscription subscription = track(builder().lease(null).leadership(null).build());

    assertThat(subscription.lease().isGated()).isFalse();
  }

  @Test
  void aWakeUpDrainInFlight_holdsTheResignUntilItsPageIsSaved() throws Exception {
    leadership.grant(NAME);
    CountDownLatch parked = new CountDownLatch(1);
    PollingEventSubscription subscription =
        leased(
            b ->
                b.config(SubscriptionConfig.pollingOnly(Duration.ofHours(1)))
                    .listener(parkingOnce(parked)));
    subscription.start();
    // The poll thread's first drain finds nothing and sleeps for the hour.
    await().atMost(5, TimeUnit.SECONDS).until(() -> subscription.lease().isLeadingHere());
    await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(() -> verify(eventStore).readGlobalStream(any(), anyInt()));
    append(1);
    CompletableFuture<Void> wakeUp =
        SagaStepLockFixture.onAnotherThread(subscription::triggerImmediatePoll);
    SagaStepLockFixture.await(parked);

    subscription.pause();
    Thread.sleep(200);
    assertThat(leadership.resigns(NAME)).isZero();

    release.countDown();
    assertThat(SagaStepLockFixture.outcomeOf(wakeUp)).isNull();
    await().atMost(5, TimeUnit.SECONDS).until(() -> leadership.resigns(NAME) == 1);
    InOrder order = inOrder(offsetStore, leadership);
    order.verify(offsetStore).saveOffset(eq(CHECKPOINT), eq(GlobalOffset.of(1)));
    order.verify(leadership).resign(NAME);
  }

  @Test
  void resumeWhileThePausedThreadIsInItsPage_waitsForItThenAnchorsAgain() throws Exception {
    leadership.grant(NAME);
    append(1);
    CountDownLatch parked = new CountDownLatch(1);
    List<Thread> deliveringThreads = new CopyOnWriteArrayList<>();
    PollingEventSubscription subscription =
        leased(
            b ->
                b.listener(
                    events -> {
                      deliveringThreads.add(Thread.currentThread());
                      parkingOnce(parked).onEvents(events);
                    }));
    subscription.start();
    SagaStepLockFixture.await(parked);
    Thread paused = deliveringThreads.getFirst();

    subscription.pause();
    CompletableFuture<Void> resuming = SagaStepLockFixture.onAnotherThread(subscription::resume);
    Thread.sleep(200);
    assertThat(resuming).isNotDone();
    assertThat(leadership.resigns(NAME)).isZero();

    release.countDown();
    assertThat(SagaStepLockFixture.outcomeOf(resuming)).isNull();
    assertThat(leadership.resigns(NAME)).isEqualTo(1);
    assertThat(paused.isAlive()).isFalse();
    assertThat(subscription.state()).isEqualTo(SubscriptionLifecycleState.RUNNING);
    append(1);
    await().atMost(5, TimeUnit.SECONDS).until(() -> deliveringThreads.size() == 2);
    assertThat(deliveringThreads.get(1)).isNotSameAs(paused);
    assertThat(leadership.tryAcquires(NAME)).isEqualTo(2);
    assertThat(subscription.lease().isLeadingHere()).isTrue();
    assertThatThrownBy(subscription.lease()::anchor).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void aCloseFromInsideTheListener_resignsWhenThePollThreadExits() {
    leadership.grant(NAME);
    append(2);
    PollingEventSubscription[] self = new PollingEventSubscription[1];
    self[0] =
        leased(
            b ->
                b.listener(
                    events -> {
                      delivered.addAll(events);
                      self[0].close();
                    }));

    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          self[0].start();
          await().atMost(4, TimeUnit.SECONDS).until(() -> leadership.resigns(NAME) == 1);
        });

    assertThat(self[0].state()).isEqualTo(SubscriptionLifecycleState.STOPPED);
    InOrder order = inOrder(offsetStore, leadership);
    order.verify(offsetStore).saveOffset(eq(CHECKPOINT), eq(GlobalOffset.of(2)));
    order.verify(leadership).resign(NAME);
    assertThat(leadership.resigns(NAME)).isEqualTo(1);
  }

  @Test
  void aPauseThenResumeFromInsideTheListener_keepsOneAnchor() {
    leadership.grant(NAME);
    append(1);
    PollingEventSubscription[] self = new PollingEventSubscription[1];
    self[0] =
        leased(
            b ->
                b.listener(
                    events -> {
                      boolean first = delivered.isEmpty();
                      delivered.addAll(events);
                      if (first) {
                        self[0].pause();
                        self[0].resume();
                      }
                    }));

    self[0].start();

    await().atMost(5, TimeUnit.SECONDS).until(() -> leadership.resigns(NAME) == 1);
    await().atMost(5, TimeUnit.SECONDS).until(() -> leadership.tryAcquires(NAME) == 2);
    append(1);
    await().atMost(5, TimeUnit.SECONDS).until(() -> delivered.size() == 2);
    assertThat(self[0].state()).isEqualTo(SubscriptionLifecycleState.RUNNING);
    assertThat(self[0].lease().isLeadingHere()).isTrue();
    assertThatThrownBy(self[0].lease()::anchor).isInstanceOf(IllegalStateException.class);
    assertThat(leadership.resigns(NAME)).isEqualTo(1);
  }

  @Test
  void aCloseFromInsideTheListenerOnAWakeUpThread_resignsWhenThePollThreadExits() {
    leadership.grant(NAME);
    PollingEventSubscription[] self = new PollingEventSubscription[1];
    self[0] =
        leased(
            b ->
                b.config(SubscriptionConfig.pollingOnly(Duration.ofHours(1)))
                    .listener(
                        events -> {
                          delivered.addAll(events);
                          self[0].close();
                        }));
    idleLeaderOnItsPollThread(self[0]);
    append(1);

    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          CompletableFuture<Void> wakeUp =
              SagaStepLockFixture.onAnotherThread(self[0]::triggerImmediatePoll);
          assertThat(wakeUp.get(4, TimeUnit.SECONDS)).isNull();
          await().atMost(4, TimeUnit.SECONDS).until(() -> leadership.resigns(NAME) == 1);
        });

    assertThat(self[0].state()).isEqualTo(SubscriptionLifecycleState.STOPPED);
    InOrder order = inOrder(offsetStore, leadership);
    order.verify(offsetStore).saveOffset(eq(CHECKPOINT), eq(GlobalOffset.of(1)));
    order.verify(leadership).resign(NAME);
    assertThat(leadership.resigns(NAME)).isEqualTo(1);
  }

  @Test
  void aPauseThenResumeFromInsideTheListenerOnAWakeUpThread_keepsOneAnchor() {
    leadership.grant(NAME);
    PollingEventSubscription[] self = new PollingEventSubscription[1];
    self[0] =
        leased(
            b ->
                b.config(SubscriptionConfig.pollingOnly(Duration.ofHours(1)))
                    .listener(
                        events -> {
                          boolean first = delivered.isEmpty();
                          delivered.addAll(events);
                          if (first) {
                            self[0].pause();
                            self[0].resume();
                          }
                        }));
    idleLeaderOnItsPollThread(self[0]);
    append(1);

    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          CompletableFuture<Void> wakeUp =
              SagaStepLockFixture.onAnotherThread(self[0]::triggerImmediatePoll);
          assertThat(wakeUp.get(4, TimeUnit.SECONDS)).isNull();
        });

    await().atMost(5, TimeUnit.SECONDS).until(() -> leadership.tryAcquires(NAME) == 2);
    append(1);
    self[0].triggerImmediatePoll();
    await().atMost(5, TimeUnit.SECONDS).until(() -> delivered.size() == 2);
    assertThat(self[0].state()).isEqualTo(SubscriptionLifecycleState.RUNNING);
    assertThat(self[0].lease().isLeadingHere()).isTrue();
    assertThatThrownBy(self[0].lease()::anchor).isInstanceOf(IllegalStateException.class);
    assertThat(leadership.resigns(NAME)).isEqualTo(1);
  }

  // ---- helpers ----

  /** Starts {@code subscription} and waits until its poll thread led, read nothing and sleeps. */
  private void idleLeaderOnItsPollThread(PollingEventSubscription subscription) {
    subscription.start();
    await().atMost(5, TimeUnit.SECONDS).until(() -> subscription.lease().isLeadingHere());
    await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(() -> verify(eventStore).readGlobalStream(any(), anyInt()));
  }

  /** Delivers into {@link #delivered}; the first page parks until {@link #release} opens. */
  private EventListener parkingOnce(CountDownLatch parked) {
    return events -> {
      boolean first = parked.getCount() > 0;
      delivered.addAll(events);
      if (first) {
        parked.countDown();
        SagaStepLockFixture.park(release);
      }
    };
  }

  private PollingEventSubscription.Builder builder() {
    return PollingEventSubscription.builder()
        .subscriptionName(NAME)
        .eventStore(eventStore)
        .offsetStore(offsetStore)
        .listener(delivered::addAll)
        .config(SubscriptionConfig.pollingOnly(POLLING));
  }

  /** A subscription over the lease {@link #NAME} of {@link #leadership}. */
  private PollingEventSubscription leased(UnaryOperator<PollingEventSubscription.Builder> more) {
    return track(more.apply(builder().lease(SubscriptionLease.of(leadership, NAME))).build());
  }

  private PollingEventSubscription track(PollingEventSubscription subscription) {
    subscriptions.add(subscription);
    return subscription;
  }

  private SagaStepLockFixture gatedProcess() {
    return new SagaStepLockFixture(leadership, Duration.ofHours(1));
  }

  private void append(int count) {
    for (int i = 0; i < count; i++) {
      long offset = stream.size() + 1L;
      StreamId streamId =
          StreamId.of(AggregateType.of("tick"), AggregateId.of("t-" + UUID.randomUUID()));
      stream.add(
          new EventEnvelope(
              GlobalOffset.of(offset),
              streamId,
              Version.initial(),
              EventType.fromClass(Tick.class),
              new Tick((int) offset),
              new EventMetadata(
                  EventId.of("evt-" + offset),
                  CommandId.of("cmd-" + offset),
                  null,
                  null,
                  CorrelationId.of("c-" + offset),
                  null,
                  null,
                  Instant.now())));
    }
  }
}
