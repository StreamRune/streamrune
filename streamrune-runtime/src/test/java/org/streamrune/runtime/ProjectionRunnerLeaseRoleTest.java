package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.subscription.SubscriptionLifecycle;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreFixture;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionDeadLetterStore;
import org.streamrune.test.ScriptedSubscriptionLeadership;

/**
 * The role a projection runner reports in its health entry is the role it plays under its own
 * lease: {@code UNGATED} without one, {@code LEADING} while it holds it, {@code STANDBY} otherwise
 * — whichever view of it the entry holds (the runner's own while it stands by or catches up, the
 * live subscription's while it is live). The live subscription takes no lease of its own: the
 * runner holds it.
 */
class ProjectionRunnerLeaseRoleTest {

  record Placed(String orderId) implements DomainEvent {}

  private static final ProjectionName ORDERS = ProjectionName.of("orders");
  private static final StreamId ORDER_1 =
      StreamId.of(AggregateType.of("order"), AggregateId.of("1"));

  private final InMemoryEventStore eventStore = new InMemoryEventStore();
  private final InMemoryOffsetStore offsetStore = new InMemoryOffsetStore();
  private final SubscriptionHealthContributor contributor =
      new SubscriptionHealthContributor(eventStore, offsetStore, 100);

  private void appendOne() {
    eventStore.append(
        ORDER_1,
        List.of(
            EventStoreFixture.event(
                ORDER_1, new Version(1), EventType.fromClass(Placed.class), new Placed("1"))),
        Version.initial());
  }

  private ContinuousProjectionRunner.Builder runner(SubscriptionLeadership leadership) {
    return ContinuousProjectionRunner.builder()
        .eventStore(eventStore)
        .offsetStore(offsetStore)
        .subscriptionConfig(SubscriptionConfig.pollingOnly(Duration.ofMillis(20)))
        .leadership(leadership)
        .atomicProcessor(
            leadership == SubscriptionLeadership.NOOP
                ? AtomicBatchProcessor.nonAtomicAtLeastOnce()
                : TestFencingProcessor.fencingClaimOnly())
        .healthContributor(contributor);
  }

  private LeaseRole role() {
    return entry().role();
  }

  private SubscriptionHealth entry() {
    return contributor.health().stream()
        .filter(health -> health.name().equals(ORDERS.value()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no health entry for orders"));
  }

  private static Thread run(ContinuousProjectionRunner runner, Projection projection) {
    return Thread.ofVirtual().start(() -> runner.run(ORDERS, projection, AT_LEAST_ONCE_IDEMPOTENT));
  }

  private static void awaitState(ContinuousProjectionRunner runner, ProjectionState state) {
    await().atMost(Duration.ofSeconds(10)).until(() -> runner.state() == state);
  }

  @Test
  void aRunnerWithoutALease_isUngated_whileLive() throws Exception {
    var runner = runner(SubscriptionLeadership.NOOP).build();
    Thread thread = run(runner, batch -> {});
    try {
      awaitState(runner, ProjectionState.LIVE);
      assertThat(role()).isEqualTo(LeaseRole.UNGATED);
    } finally {
      runner.close();
      thread.join(10_000);
    }
  }

  @Test
  void aRunnerThatHoldsItsLease_isLeading_whileLive() throws Exception {
    var leadership = new ScriptedSubscriptionLeadership().grant();
    var runner = runner(leadership).build();
    Thread thread = run(runner, batch -> {});
    try {
      awaitState(runner, ProjectionState.LIVE);
      assertThat(role()).isEqualTo(LeaseRole.LEADING);
    } finally {
      runner.close();
      thread.join(10_000);
    }
  }

  @Test
  void aRunnerThatDoesNotHoldItsLease_isStandby() throws Exception {
    var leadership = new ScriptedSubscriptionLeadership();
    var runner = runner(leadership).build();
    Thread thread = run(runner, batch -> {});
    try {
      awaitState(runner, ProjectionState.STANDBY);
      assertThat(role()).isEqualTo(LeaseRole.STANDBY);
    } finally {
      runner.close();
      thread.join(10_000);
    }
  }

  /**
   * A live runner whose lease is taken drops to standby and says so; the batch it dead-lettered
   * while it led is still a hole in the read model, so a scrape while it stands by keeps its entry
   * {@code DEGRADED}, and so does the next leadership.
   */
  @Test
  void aRunnerThatLosesItsLease_turnsStandby_andKeepsTheHoleItDeadLettered() throws Exception {
    appendOne();
    var leadership = new ScriptedSubscriptionLeadership().grant();
    var deadLetters = new InMemoryProjectionDeadLetterStore();
    var runner =
        runner(leadership)
            .errorStrategy(ProjectionErrorStrategy.DLQ)
            .deadLetterStore(deadLetters)
            .build();
    Thread thread =
        run(
            runner,
            batch -> {
              throw new IllegalArgumentException("poison");
            });
    try {
      awaitState(runner, ProjectionState.LIVE);
      assertThat(deadLetters.all()).hasSize(1);
      assertThat(entry().status()).isEqualTo(SubscriptionHealth.Status.DEGRADED);

      leadership.revoke();
      awaitState(runner, ProjectionState.STANDBY);
      await().atMost(Duration.ofSeconds(10)).until(() -> role() == LeaseRole.STANDBY);
      assertThat(entry().status()).isEqualTo(SubscriptionHealth.Status.DEGRADED);

      leadership.grant();
      awaitState(runner, ProjectionState.LIVE);
      await().atMost(Duration.ofSeconds(10)).until(() -> role() == LeaseRole.LEADING);
      assertThat(entry().status()).isEqualTo(SubscriptionHealth.Status.DEGRADED);
    } finally {
      runner.close();
      thread.join(10_000);
    }
  }

  /**
   * Every view a projection runner registers keeps its errors through a standby, not only the one
   * the lease-loss test above scrapes: the live view while the runner gives up its lease, and the
   * scheduled runner's view between fires.
   */
  @Test
  void everyHealthViewOfAProjectionRunner_keepsItsErrorsThroughAStandby() {
    List<Class<?>> views =
        Stream.of(ContinuousProjectionRunner.class, ScheduledProjectionRunner.class)
            .flatMap(runner -> Arrays.stream(runner.getDeclaredClasses()))
            .filter(SubscriptionLifecycle.class::isAssignableFrom)
            .toList();

    assertThat(views)
        .extracting(Class::getSimpleName)
        .containsExactlyInAnyOrder(
            "RunnerHealthView", "LiveHealthView", "ScheduledSubscriptionView");
    assertThat(views).allSatisfy(view -> assertThat(view).isAssignableTo(StandbyKeepsErrors.class));
  }

  @Test
  void aScheduledRunner_reportsTheRoleItPlaysUnderItsLease() throws Exception {
    var leadership = new ScriptedSubscriptionLeadership();
    var scheduled =
        ScheduledProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .leadership(leadership)
            .healthContributor(contributor)
            .register(ORDERS.value(), batch -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    var ungated =
        ScheduledProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .healthContributor(contributor)
            .register("ungated", batch -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduled.start();
    ungated.start();
    try {
      assertThat(role()).isEqualTo(LeaseRole.STANDBY);
      leadership.grant();
      await().atMost(Duration.ofSeconds(10)).until(() -> role() == LeaseRole.LEADING);
      assertThat(
              contributor.health().stream()
                  .filter(health -> health.name().equals("ungated"))
                  .findFirst()
                  .orElseThrow()
                  .role())
          .isEqualTo(LeaseRole.UNGATED);
    } finally {
      scheduled.stop();
      ungated.stop();
    }
  }
}
