package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.SagaStepLockFixture.await;
import static org.streamrune.runtime.SagaStepLockFixture.onAnotherThread;
import static org.streamrune.runtime.SagaStepLockFixture.outcomeOf;
import static org.streamrune.runtime.SagaStepLockFixture.park;
import static org.streamrune.runtime.SagaStepLockFixture.sagaOf;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.Ship;
import org.streamrune.runtime.SagaStepLockFixture.State;

/**
 * A step of a saga that does not return, in the process of the timeout runner, past the saga's
 * deadline: the timeout runner passes the saga over on its first polls and then waits for the step
 * for its bounded wait. What it does once the wait has passed with the step still in flight depends
 * on the forward fence of the runner it was built from: with the fence on, the claim is made
 * underneath the step and the step's later forward command is refused at its commit; with the fence
 * off, or with a timeout runner built from a step lock alone, the claim is never made underneath a
 * step.
 *
 * <p>The step is parked right before its second forward command; four polls follow (three that ask
 * the lock once and one that waits 100 ms).
 */
class SagaTimeoutRunnerFencedClaimTest {

  private static final String ORDER = "1";
  private static final Duration CLAIM_WAIT = Duration.ofMillis(100);

  @Test
  void aStepThatDoesNotReturn_isClaimedUnderAfterTheBoundedWait_whenTheFenceIsOn() {
    try (var p = new SagaStepLockFixture(new SagaStepLock(), CLAIM_WAIT, true)) {
      p.saga.startAlsoShips = true;
      var parked = parkTheStepBeforeItsSecondCommand(p);

      List<ILoggingEvent> lines = timeoutRunnerLinesDuring(() -> fourPolls(p, p.timeoutRunner));

      assertThat(p.storedOrder(ORDER))
          .as("the fourth poll claims and compensates underneath the step")
          .containsExactly("Reserved", "Released", "Refunded");
      assertThat(p.timeoutRunner.claimsDeferredPastTheWait()).isZero();
      assertThat(p.timeoutRunner.deferralsOf(sagaOf(ORDER))).isZero();
      assertThat(p.deferredClaimSamples).as("the three polls that passed it over").hasSize(3);
      assertThat(lines)
          .as("no WARN; one INFO naming the saga")
          .noneMatch(e -> e.getLevel() == Level.WARN)
          .filteredOn(e -> e.getLevel() == Level.INFO)
          .singleElement()
          .satisfies(
              e -> {
                assertThat(e.getFormattedMessage()).contains(sagaOf(ORDER).value());
                assertThat(e.getFormattedMessage()).contains("the claim is made underneath it");
              });

      parked.letTheStepGo().countDown();
      assertThat(outcomeOf(parked.step())).as("the step ends without a failure").isNull();
      assertThat(p.storedOrder(ORDER))
          .as("the step's second command is refused at its commit")
          .containsExactly("Reserved", "Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }

  @Test
  void aStepThatDoesNotReturn_isNeverClaimedUnder_whenTheFenceIsOff() {
    try (var p = new SagaStepLockFixture(new SagaStepLock(), CLAIM_WAIT, false)) {
      p.saga.startAlsoShips = true;
      var parked = parkTheStepBeforeItsSecondCommand(p);

      List<ILoggingEvent> lines = timeoutRunnerLinesDuring(() -> fourPolls(p, p.timeoutRunner));

      assertThat(p.storedOrder(ORDER))
          .as("never claimed under the step")
          .containsExactly("Reserved");
      assertThat(p.row(ORDER).episodeVersion()).isNull();
      assertThat(p.timeoutRunner.claimsDeferredPastTheWait()).isEqualTo(1);
      assertThat(lines)
          .filteredOn(e -> e.getLevel() == Level.WARN)
          .singleElement()
          .satisfies(e -> assertThat(e.getFormattedMessage()).contains("does not return"));

      parked.letTheStepGo().countDown();
      assertThat(outcomeOf(parked.step())).isNull();
      p.timeoutRunner.pollOnce();
      assertThat(p.storedOrder(ORDER))
          .containsExactly("Reserved", "Shipped", "Released", "Refunded");
    }
  }

  @Test
  void aStepThatDoesNotReturn_isNeverClaimedUnder_byARunnerBoundWithStepLock() {
    try (var p = new SagaStepLockFixture(new SagaStepLock(), CLAIM_WAIT, true)) {
      p.saga.startAlsoShips = true;
      SagaTimeoutRunner<State> lockBound =
          SagaTimeoutRunner.<State>builder()
              .decider(p.saga)
              .sagaStore(p.sagaStore)
              .commandBus(p.bus)
              .sagaType(SagaStepLockFixture.TYPE)
              .clock(p.clock)
              .stepLock(p.lock)
              .claimWait(CLAIM_WAIT)
              .batchSize(1_000)
              .build();
      var parked = parkTheStepBeforeItsSecondCommand(p);

      fourPolls(p, lockBound);

      assertThat(p.storedOrder(ORDER))
          .as("never claimed under the step")
          .containsExactly("Reserved");
      assertThat(p.row(ORDER).episodeVersion()).isNull();
      assertThat(lockBound.claimsDeferredPastTheWait()).isEqualTo(1);

      parked.letTheStepGo().countDown();
      assertThat(outcomeOf(parked.step())).isNull();
      lockBound.pollOnce();
      assertThat(p.storedOrder(ORDER))
          .containsExactly("Reserved", "Shipped", "Released", "Refunded");
    }
  }

  // ---------------------------------------------------------------------------------------------

  private record ParkedStep(CompletableFuture<Void> step, CountDownLatch letTheStepGo) {}

  /** Delivers the start event on another thread; its step stores Reserve and parks before Ship. */
  private static ParkedStep parkTheStepBeforeItsSecondCommand(SagaStepLockFixture p) {
    EventEnvelope opened = p.append(new Opened(ORDER));
    var stepAtItsCommand = new CountDownLatch(1);
    var letTheStepGo = new CountDownLatch(1);
    p.bus.runOnceBefore(
        Ship.class,
        () -> {
          stepAtItsCommand.countDown();
          park(letTheStepGo);
        });
    CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(opened));
    await(stepAtItsCommand);
    return new ParkedStep(step, letTheStepGo);
  }

  /** The deadline passes; three polls ask the step lock once, the fourth waits its bound. */
  private static void fourPolls(SagaStepLockFixture p, SagaTimeoutRunner<State> runner) {
    p.clock.advance(SagaStepLockFixture.TIMEOUT.plusSeconds(1));
    runner.pollOnce();
    runner.pollOnce();
    runner.pollOnce();
    runner.pollOnce();
  }

  /** The timeout runner's log lines written while {@code body} runs. */
  private static List<ILoggingEvent> timeoutRunnerLinesDuring(Runnable body) {
    var logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(SagaTimeoutRunner.class);
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    Level before = logger.getLevel();
    logger.setLevel(Level.INFO);
    logger.addAppender(appender);
    try {
      body.run();
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(before);
      appender.stop();
    }
    return List.copyOf(appender.list);
  }
}
