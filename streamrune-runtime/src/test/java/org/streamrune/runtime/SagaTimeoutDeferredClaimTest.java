package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.SagaStepLockFixture.TIMEOUT;
import static org.streamrune.runtime.SagaStepLockFixture.TYPE;
import static org.streamrune.runtime.SagaStepLockFixture.await;
import static org.streamrune.runtime.SagaStepLockFixture.awaitUntil;
import static org.streamrune.runtime.SagaStepLockFixture.onAnotherThread;
import static org.streamrune.runtime.SagaStepLockFixture.outcomeOf;
import static org.streamrune.runtime.SagaStepLockFixture.park;
import static org.streamrune.runtime.SagaStepLockFixture.sagaOf;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.runtime.BackgroundRelayHealthContributor.RelayStatusSource;
import org.streamrune.runtime.BackgroundRelayHealthContributor.Status;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.Reserve;
import org.streamrune.runtime.SagaStepLockFixture.State;

/**
 * What a deferred timeout claim costs the other sagas, and how it shows.
 *
 * <p>A saga whose claim is deferred — a step of it is in flight — stays a forward row past its
 * deadline and is selected by every poll. It takes no slot of the timeout runner's batch: other
 * sagas past their deadline are claimed in the same poll, however many deferred sagas come before
 * them, and the store's two populations keep their share. A deferred claim is counted, and the
 * runner's health component reads {@code DEGRADED} while a claim it waited for is still deferred.
 *
 * <p>The steps that do not return are deliveries parked before their forward command by a latch;
 * the timeout runner polls on the test's thread.
 */
class SagaTimeoutDeferredClaimTest {

  /** The steps of the given orders, each parked before its forward command until released. */
  private static final class StepsThatDoNotReturn {
    private final CountDownLatch release = new CountDownLatch(1);
    private final List<CompletableFuture<Void>> deliveries = new ArrayList<>();

    StepsThatDoNotReturn(SagaStepLockFixture p, String... orders) {
      Set<String> parked = Set.of(orders);
      var reached = new CountDownLatch(orders.length);
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Reserve reserve && parked.contains(reserve.orderId())) {
              reached.countDown();
              park(release);
            }
          };
      for (String order : orders) {
        // Each saga starts a second after the one before it: the store orders them by that.
        var opened = p.append(new Opened(order));
        deliveries.add(onAnotherThread(() -> p.deliver(opened)));
        p.awaitRow(order);
        p.clock.advance(Duration.ofSeconds(1));
      }
      await(reached);
    }

    void returnAtLast() {
      release.countDown();
      for (CompletableFuture<Void> delivery : deliveries) {
        assertThat(outcomeOf(delivery)).isNull();
      }
    }
  }

  private static SagaTimeoutRunner<State> timeoutRunner(
      SagaStepLockFixture p, int batchSize, Duration claimWait) {
    return SagaTimeoutRunner.<State>builder()
        .decider(p.saga)
        .sagaStore(p.sagaStore)
        .commandBus(p.bus)
        .sagaType(TYPE)
        .clock(p.clock)
        .sagaRunner(p.runner)
        .batchSize(batchSize)
        .claimWait(claimWait)
        .metrics(p.metrics)
        .build();
  }

  @Test
  void
      twoSagasInStepsThatDoNotReturnFillTheBatch_anIdleSagaPastItsDeadlineIsClaimedInTheSamePoll() {
    try (var p = new SagaStepLockFixture()) {
      SagaTimeoutRunner<State> timeouts = timeoutRunner(p, 2, Duration.ofMillis(20));
      var hung = new StepsThatDoNotReturn(p, "1", "2");
      p.deliver(p.append(new Opened("3")));
      p.clock.advance(TIMEOUT.plusSeconds(1));

      timeouts.pollOnce();

      assertThat(p.row("3").status())
          .as("the third saga is claimed by the poll that first meets the two in a step")
          .isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.storedOrder("3")).containsExactly("Reserved", "Released", "Refunded");
      assertThat(p.storedOrder("1")).isEmpty();
      assertThat(p.storedOrder("2")).isEmpty();
      assertThat(p.sagaStore.timeoutQueryLimits)
          .as("batchSize ids, then two more for each of the two sagas passed over")
          .containsExactly(2, 6);

      // Later polls: the two sagas are counted before the query, and tried on every poll.
      p.deliver(p.append(new Opened("4")));
      p.clock.advance(TIMEOUT.plusSeconds(1));
      p.sagaStore.timeoutQueryLimits.clear();
      for (int poll = 0; poll < 6; poll++) {
        timeouts.pollOnce();
      }

      assertThat(p.row("4").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.sagaStore.timeoutQueryLimits).containsOnly(6);
      assertThat(timeouts.deferralsOf(sagaOf("1"))).as("tried on every poll").isEqualTo(7);
      assertThat(timeouts.deferralsOf(sagaOf("2"))).isEqualTo(7);
      assertThat(timeouts.claimsDeferredPastTheWait()).isEqualTo(2);
      assertThat(p.row("1").episodeVersion()).as("never claimed under its step").isNull();

      hung.returnAtLast();
      timeouts.pollOnce();

      assertThat(p.storedOrder("1")).containsExactly("Reserved", "Released", "Refunded");
      assertThat(p.storedOrder("2")).containsExactly("Reserved", "Released", "Refunded");
      assertThat(timeouts.claimsDeferredPastTheWait()).isZero();
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  @Test
  void aSagaInAStepAndABacklogOfRePicksFillTheBatch_theForwardHalfStillClaimsAnIdleSaga() {
    try (var p = new SagaStepLockFixture()) {
      // Two episodes whose compensation keeps failing: re-picked on every poll, never done.
      p.deliver(p.append(new Opened("5")));
      p.deliver(p.append(new Opened("6")));
      p.refund.failure = () -> new IllegalStateException("connection reset");
      p.timeOut();
      assertThat(p.row("5").status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(p.row("6").status()).isEqualTo(SagaStatus.COMPENSATING);
      SagaTimeoutRunner<State> timeouts = timeoutRunner(p, 2, Duration.ofMillis(20));
      var hung = new StepsThatDoNotReturn(p, "1");
      p.deliver(p.append(new Opened("3")));
      p.clock.advance(TIMEOUT.plusSeconds(1));
      p.sagaStore.timeoutQueryLimits.clear();
      int releasesBefore = p.storedOrder("5").size() + p.storedOrder("6").size();

      timeouts.pollOnce();

      assertThat(p.row("3").episodeVersion())
          .as("the idle saga is claimed in the same poll: the saga in a step holds no forward slot")
          .isNotNull();
      assertThat(p.storedOrder("3")).containsExactly("Reserved", "Released");
      assertThat(p.storedOrder("1")).isEmpty();
      assertThat(p.sagaStore.timeoutQueryLimits).containsExactly(2, 4);
      assertThat(p.storedOrder("5").size() + p.storedOrder("6").size())
          .as("the re-picks answer from the inbox; nothing of theirs is stored twice")
          .isEqualTo(releasesBefore);
      assertThat(p.row("5").status()).isEqualTo(SagaStatus.COMPENSATING);

      p.refund.failure = null;
      hung.returnAtLast();
      p.clock.advance(TIMEOUT.plusSeconds(1));
      timeouts.pollOnce();
      timeouts.pollOnce();

      for (String order : List.of("1", "3", "5", "6")) {
        assertThat(p.row(order).status()).as("order %s", order).isEqualTo(SagaStatus.COMPENSATED);
      }
    }
  }

  @Test
  void aPollReadsPastAtMostBatchSizeSagasInSteps_noQueryAsksForMoreThanThreeTimesTheBatch() {
    try (var p = new SagaStepLockFixture()) {
      SagaTimeoutRunner<State> timeouts = timeoutRunner(p, 1, Duration.ofMillis(1));
      var hung = new StepsThatDoNotReturn(p, "1", "2", "3");
      p.deliver(p.append(new Opened("4")));
      p.clock.advance(TIMEOUT.plusSeconds(1));

      timeouts.pollOnce();
      timeouts.pollOnce();

      assertThat(p.sagaStore.timeoutQueryLimits)
          .as("one id; then the bound, one saga read past; the next poll starts at the bound")
          .containsExactly(1, 3, 3);
      assertThat(p.row("4").status())
          .as("beyond the bound a saga in a step does take a slot")
          .isEqualTo(SagaStatus.RUNNING);

      hung.returnAtLast();
      for (int poll = 0; poll < 4; poll++) {
        timeouts.pollOnce();
      }
      assertThat(p.row("4").status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }

  @Test
  void aSagaThePollHasToWaitForIsTriedAfterTheOthers_itsWaitDelaysNoOtherClaim() {
    try (var p = new SagaStepLockFixture()) {
      SagaTimeoutRunner<State> timeouts = timeoutRunner(p, 10, Duration.ofSeconds(20));
      var hung = new StepsThatDoNotReturn(p, "1");
      p.clock.advance(TIMEOUT.plusSeconds(1));
      timeouts.pollOnce();
      timeouts.pollOnce();
      timeouts.pollOnce();
      p.deliver(p.append(new Opened("2")));
      p.clock.advance(TIMEOUT.plusSeconds(1));

      // The fourth poll waits for the first saga — after it has claimed the second.
      CompletableFuture<Void> waitingPoll = onAnotherThread(timeouts::pollOnce);
      p.awaitWaitingFor("1", 1);

      assertThat(p.row("2").status())
          .as("claimed before the poll started to wait")
          .isEqualTo(SagaStatus.COMPENSATED);

      hung.returnAtLast();
      assertThat(outcomeOf(waitingPoll)).isNull();
      assertThat(p.storedOrder("1")).containsExactly("Reserved", "Released", "Refunded");
    }
  }

  @Test
  void everyDeferredClaimIsCounted_taggedWithTheSagaTypeOnly() {
    try (var p = new SagaStepLockFixture()) {
      SagaTimeoutRunner<State> timeouts = timeoutRunner(p, 10, Duration.ofMillis(20));
      var hung = new StepsThatDoNotReturn(p, "1");
      p.clock.advance(TIMEOUT.plusSeconds(1));

      for (int poll = 0; poll < 5; poll++) {
        timeouts.pollOnce();
      }

      assertThat(p.deferredClaimSamples)
          .as("three polls that asked once and two that waited")
          .hasSize(5)
          .containsOnly(TYPE.value());

      hung.returnAtLast();
      timeouts.pollOnce();
      assertThat(p.deferredClaimSamples).as("a claim that is made is not counted").hasSize(5);
    }
  }

  @Test
  void theRunnersHealthIsDegradedWhileAClaimItWaitedForIsDeferred_andUpOnceItIsMade() {
    try (var p = new SagaStepLockFixture()) {
      SagaTimeoutRunner<State> timeouts =
          SagaTimeoutRunner.<State>builder()
              .decider(p.saga)
              .sagaStore(p.sagaStore)
              .commandBus(p.bus)
              .sagaType(TYPE)
              .clock(p.clock)
              .sagaRunner(p.runner)
              .claimWait(Duration.ofMillis(5))
              .pollInterval(Duration.ofMillis(5))
              .build();
      var health = new BackgroundRelayHealthContributor();
      health.registerSagaTimeoutRunner(timeouts);
      var hung = new StepsThatDoNotReturn(p, "1");
      p.clock.advance(TIMEOUT.plusSeconds(1));
      timeouts.start();
      try {
        awaitUntil(
            "a poll waited for the saga in a step and deferred its claim",
            () -> timeouts.claimsDeferredPastTheWait() == 1);

        assertThat(health.components())
            .singleElement()
            .satisfies(
                component -> {
                  assertThat(component.name()).isEqualTo("saga-timeout:" + TYPE.value());
                  assertThat(component.status()).isEqualTo(Status.DEGRADED);
                  assertThat(component.alive()).isTrue();
                  assertThat(component.consecutiveFailures()).isZero();
                });
        assertThat(health.overallStatus()).isEqualTo(Status.DEGRADED);

        hung.returnAtLast();
        awaitUntil(
            "the claim is made once the step has returned",
            () -> p.row("1").status() == SagaStatus.COMPENSATED);
        awaitUntil("the poll that made it ended", () -> timeouts.claimsDeferredPastTheWait() == 0);

        assertThat(health.overallStatus()).isEqualTo(Status.UP);
      } finally {
        timeouts.stop();
      }
    }
  }

  @Test
  void aRelayThatLeavesWorkUndoneIsDegradedOnlyWhileItIsStartedAndAlive() {
    record Source(
        boolean started, boolean alive, int consecutiveFailures, boolean leavingWorkUndone)
        implements RelayStatusSource {
      @Override
      public String name() {
        return "relay";
      }
    }
    var health = new BackgroundRelayHealthContributor();
    health.register(new Source(true, true, 0, true));
    health.register(new Source(true, true, 0, false));
    health.register(new Source(false, false, 0, true));
    health.register(new Source(true, false, 0, true));
    health.register(new Source(true, true, 2, false));

    assertThat(health.components())
        .extracting(BackgroundRelayHealthContributor.ComponentHealth::status)
        .containsExactly(Status.DEGRADED, Status.UP, Status.UP, Status.DOWN, Status.DEGRADED);
  }
}
