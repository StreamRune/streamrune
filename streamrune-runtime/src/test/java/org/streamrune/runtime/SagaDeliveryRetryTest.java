package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.runtime.SagaStepLockFixture.TYPE;
import static org.streamrune.runtime.SagaStepLockFixture.await;
import static org.streamrune.runtime.SagaStepLockFixture.onAnotherThread;
import static org.streamrune.runtime.SagaStepLockFixture.outcomeOf;
import static org.streamrune.runtime.SagaStepLockFixture.park;
import static org.streamrune.runtime.SagaStepLockFixture.sagaOf;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.runtime.SagaStepLockFixture.Advanced;
import org.streamrune.runtime.SagaStepLockFixture.Completed;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.Release;
import org.streamrune.runtime.SagaStepLockFixture.Reserve;
import org.streamrune.runtime.SagaStepLockFixture.Ship;
import org.streamrune.runtime.SagaStepLockFixture.State;

/**
 * A step whose write of the saga row loses the compare-and-swap is run again in place, up to three
 * attempts, holding the saga's step lock once: on the live delivery and on the replay feed. The
 * router decides what the run again does with the row that won. Each case sets the other writer in
 * at the point where the stepping node has run its commands and not yet written its row, with a
 * hook on that node's saga store, so the loss is a fact of the schedule and not of timing.
 */
class SagaDeliveryRetryTest {

  private static final String ORDER = "1";

  /** Collects what {@link SagaRunner} logs. */
  private static final class RunnerLog implements AutoCloseable {
    private final Logger logger = (Logger) LoggerFactory.getLogger(SagaRunner.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    RunnerLog() {
      appender.start();
      logger.addAppender(appender);
    }

    List<ILoggingEvent> warnings() {
      return appender.list.stream().filter(e -> e.getLevel() == Level.WARN).toList();
    }

    /** Forgets what was logged so far: the setup of a case logs warnings of its own. */
    void clear() {
      appender.list.clear();
    }

    @Override
    public void close() {
      logger.detachAppender(appender);
      appender.stop();
    }
  }

  // ---- the router decides on the row that won ----

  @Test
  void aTwinThatAppliedTheEventFirst_isAcknowledgedInPlace() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      f.a.deliver(f.append(new Opened(ORDER)));
      EventEnvelope advanced = f.append(new Advanced(ORDER));
      f.a.sagaStore.runOnceBeforeTheEventWrite(() -> f.b.deliver(advanced));

      f.a.deliver(advanced);

      assertThat(f.row(ORDER).version()).isEqualTo(3);
      assertThat(f.row(ORDER).lastAppliedOffset()).isEqualTo(advanced.globalOffset().value());
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value());
      assertThat(f.a.casConflictSamples).hasSize(1);
      assertThat(f.b.deliveryRetriedSamples).isEmpty();
      assertThat(f.b.casConflictSamples).isEmpty();
      assertThat(f.storedOrder(ORDER)).containsExactly("Reserved", "Shipped", "Shipped");
    }
  }

  @Test
  void aTwinThatAppliedTheStartEventFirst_isAcknowledgedInPlace() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      EventEnvelope opened = f.append(new Opened(ORDER));
      f.a.sagaStore.runOnceBeforeTheEventWrite(() -> f.b.deliver(opened));

      f.a.deliver(opened);

      assertThat(f.row(ORDER).genesisApplied()).isTrue();
      assertThat(f.row(ORDER).version()).isEqualTo(2);
      assertThat(f.storedOrder(ORDER)).containsExactly("Reserved", "Shipped");
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value());
    }
  }

  @Test
  void aDeadlineClaimThatWonTheRow_isResumedInPlace() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      f.a.deliver(f.append(new Opened(ORDER)));
      EventEnvelope advanced = f.append(new Advanced(ORDER));
      f.b.bus.runOnceBefore(
          Release.class,
          () -> {
            throw new SagaStepLockFixture.ProcessDeath();
          });
      f.a.sagaStore.runOnceBeforeTheEventWrite(
          () ->
              assertThatThrownBy(f.b::timeOut)
                  .isInstanceOf(SagaStepLockFixture.ProcessDeath.class));

      f.a.deliver(advanced);

      assertThat(f.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(f.storedOrder(ORDER))
          .containsExactly("Reserved", "Shipped", "Shipped", "Released", "Refunded");
      long episode = f.row(ORDER).episodeVersion();
      for (int index = 0; index < 2; index++) {
        assertThat(
                f.inbox.find(
                    SagaCommandDispatch.episodeCompensationKey(sagaOf(ORDER), episode, index)))
            .as("compensation command %d is in the inbox", index)
            .isPresent();
      }
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value());
    }
  }

  @Test
  void aSagaThatEndedUnderTheStep_dropsTheEventInPlace() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      f.a.deliver(f.append(new Opened(ORDER)));
      EventEnvelope advanced = f.append(new Advanced(ORDER));
      EventEnvelope completed = f.append(new Completed(ORDER));
      f.a.sagaStore.runOnceBeforeTheEventWrite(() -> f.b.deliver(completed));

      f.a.deliver(advanced);

      assertThat(f.row(ORDER).status()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(f.row(ORDER).version()).as("the run again writes nothing").isEqualTo(3);
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value());
    }
  }

  @Test
  void aSagaFaultedUnderTheStep_holdsTheEventInPlace() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      f.a.deliver(f.append(new Opened(ORDER)));
      EventEnvelope advanced = f.append(new Advanced(ORDER));
      f.a.sagaStore.runOnceBeforeTheEventWrite(() -> f.rows.markFaulted(sagaOf(ORDER), TYPE, 2));

      f.a.deliver(advanced);

      assertThat(f.deadLetters.findBySaga(sagaOf(ORDER)))
          .singleElement()
          .satisfies(entry -> assertThat(entry.eventOffset()).isEqualTo(advanced.globalOffset()));
      assertThat(f.a.skippedWhileFaultedSamples).hasSize(1);
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value());
    }
  }

  @Test
  void aRowDeletedUnderTheStep_dropsACorrelatedEventInPlace() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      f.a.deliver(f.append(new Opened(ORDER)));
      EventEnvelope advanced = f.append(new Advanced(ORDER));
      f.a.sagaStore.runOnceBeforeTheEventWrite(() -> f.rows.delete(sagaOf(ORDER), TYPE));

      f.a.deliver(advanced);

      assertThat(f.rows.load(sagaOf(ORDER), TYPE, State.class)).isEmpty();
      assertThat(f.deadLetters.findBySaga(sagaOf(ORDER))).isEmpty();
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value());
    }
  }

  @Test
  void aRowGoneUnderAFencedStep_dropsACorrelatedEventInPlace() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      f.a.deliver(f.append(new Opened(ORDER)));
      EventEnvelope advanced = f.append(new Advanced(ORDER));
      // the fenced commit finds no row: the store refuses the command and the step reports it
      f.a.bus.runOnceBefore(Ship.class, () -> f.rows.delete(sagaOf(ORDER), TYPE));

      f.a.deliver(advanced);

      assertThat(f.rows.load(sagaOf(ORDER), TYPE, State.class)).isEmpty();
      assertThat(f.deadLetters.findBySaga(sagaOf(ORDER))).isEmpty();
      assertThat(f.a.fencedSamples).containsExactly(TYPE.value());
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value());
      assertThat(f.a.casConflictSamples).hasSize(1);
    }
  }

  @Test
  void aRowDeletedUnderTheStartStep_isCreatedAgainInPlace() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      EventEnvelope opened = f.append(new Opened(ORDER));
      f.a.sagaStore.runOnceBeforeTheEventWrite(() -> f.rows.delete(sagaOf(ORDER), TYPE));

      f.a.deliver(opened);

      assertThat(f.row(ORDER).genesisApplied()).isTrue();
      assertThat(f.row(ORDER).version()).isEqualTo(2);
      assertThat(f.storedOrder(ORDER)).containsExactly("Reserved", "Shipped");
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value());
    }
  }

  @Test
  void aStartRowCreatedAndRemovedBeforeItIsReadAgain_isCreatedAgainInPlace() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      f.a.sagaStore.loseTheNextGenesisCreate();

      f.a.deliver(f.append(new Opened(ORDER)));

      assertThat(f.row(ORDER).genesisApplied()).isTrue();
      assertThat(f.row(ORDER).version()).isEqualTo(2);
      assertThat(f.storedOrder(ORDER)).containsExactly("Reserved", "Shipped");
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value());
      assertThat(f.a.casConflictSamples).hasSize(1);
    }
  }

  // ---- the bound ----

  @Test
  void aConflictThatRepeatsTwice_isResolvedOnTheThirdAttempt() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      f.a.sagaStore.conflictOnTheNextEventWrites(2);

      f.a.deliver(f.append(new Opened(ORDER)));

      assertThat(f.a.sagaStore.eventWriteAttempts()).isEqualTo(3);
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value(), TYPE.value());
      assertThat(f.row(ORDER).genesisApplied()).isTrue();
    }
  }

  @Test
  void aPersistentConflict_propagatesAfterThreeAttempts_withOneWarning() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true);
        var log = new RunnerLog()) {
      f.a.sagaStore.conflictOnTheNextEventWrites(Integer.MAX_VALUE);
      EventEnvelope opened = f.append(new Opened(ORDER));

      assertThatThrownBy(() -> f.a.deliver(opened)).isInstanceOf(OptimisticLockException.class);

      assertThat(f.a.sagaStore.eventWriteAttempts()).isEqualTo(SagaRunner.MAX_DELIVERY_ATTEMPTS);
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value(), TYPE.value());
      assertThat(log.warnings())
          .singleElement()
          .satisfies(
              warning ->
                  assertThat(warning.getFormattedMessage())
                      .contains("lost its compare-and-swap on 3 attempts in a row")
                      .contains("delivers the page again"));
    }
  }

  @Test
  void aPoisonOnTheReRun_isQuarantined() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      f.a.deliver(f.append(new Opened(ORDER)));
      EventEnvelope advanced = f.append(new Advanced(ORDER));
      f.a.sagaStore.runOnceBeforeTheEventWrite(
          () -> {
            f.rows.update(sagaOf(ORDER), TYPE, f.row(ORDER).state(), SagaStatus.RUNNING, 2);
            f.a.saga.evolveThrows = true;
          });

      f.a.deliver(advanced);

      assertThat(f.deadLetters.findBySaga(sagaOf(ORDER)))
          .singleElement()
          .satisfies(entry -> assertThat(entry.eventOffset()).isEqualTo(advanced.globalOffset()));
      assertThat(f.row(ORDER).status()).isEqualTo(SagaStatus.FAULTED);
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value());
    }
  }

  // ---- what is not run again ----

  @Test
  void aForwardCommandsOwnVersionConflict_isNotRunAgainInPlace() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      f.a.deliver(f.append(new Opened(ORDER)));
      EventEnvelope advanced = f.append(new Advanced(ORDER));
      f.a.bus.runOnceBefore(
          Ship.class,
          () -> {
            throw new OptimisticLockException("Optimistic lock conflict on stream 'shipment-1'");
          });

      assertThatThrownBy(() -> f.a.deliver(advanced))
          .isInstanceOf(OptimisticLockException.class)
          .isNotInstanceOf(SagaRowConflictException.class);

      assertThat(f.a.deliveryRetriedSamples).isEmpty();
      assertThat(f.row(ORDER).version()).isEqualTo(2);
    }
  }

  @Test
  void aStepThatCannotTakeTheSagasLock_isNotRunAgain() {
    try (var p =
        new SagaStepLockFixture(
            new SagaStepLock(Duration.ofMillis(100)), SagaTimeoutRunner.DEFAULT_CLAIM_WAIT)) {
      EventEnvelope opened = p.append(new Opened(ORDER));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(opened));
      await(stepAtItsCommand);

      assertThatThrownBy(() -> p.deliver(opened)).isInstanceOf(SagaStepLockException.class);

      assertThat(p.deliveryRetriedSamples).isEmpty();
      letTheStepGo.countDown();
      assertThat(outcomeOf(step)).isNull();
    }
  }

  // ---- the replay feed ----

  /** A saga with a held event: the feed of that event is what a test drives. */
  private static EventEnvelope aHeldEvent(SagaForwardFenceTwoNodeFixture f) {
    f.a.deliver(f.append(new Opened(ORDER)));
    f.rows.markFaulted(sagaOf(ORDER), TYPE, 2);
    EventEnvelope advanced = f.append(new Advanced(ORDER));
    f.a.deliver(advanced);
    assertThat(f.deadLetters.findBySaga(sagaOf(ORDER))).hasSize(1);
    return advanced;
  }

  @Test
  void aTwinReplayThatAppliedTheEventFirst_isDedupedInPlace() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      EventEnvelope advanced = aHeldEvent(f);
      f.a.sagaStore.runOnceBeforeTheEventWrite(
          () -> f.b.runner.feedReplay(sagaOf(ORDER), advanced, false, false));

      StepOutcome outcome = f.a.runner.feedReplay(sagaOf(ORDER), advanced, false, false);

      assertThat(outcome).isEqualTo(StepOutcome.SKIPPED_DEDUP);
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value());
      assertThat(f.storedOrder(ORDER)).containsExactly("Reserved", "Shipped", "Shipped");
    }
  }

  @Test
  void aReplayFeedThatKeepsLosingItsCompareAndSwap_reachesTheCaller() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true);
        var log = new RunnerLog()) {
      EventEnvelope advanced = aHeldEvent(f);
      log.clear();
      int writesBefore = f.a.sagaStore.eventWriteAttempts();
      f.a.sagaStore.conflictOnTheNextEventWrites(Integer.MAX_VALUE);

      assertThatThrownBy(() -> f.a.runner.feedReplay(sagaOf(ORDER), advanced, false, false))
          .isInstanceOf(OptimisticLockException.class);

      assertThat(f.a.sagaStore.eventWriteAttempts() - writesBefore)
          .isEqualTo(SagaRunner.MAX_DELIVERY_ATTEMPTS);
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value(), TYPE.value());
      assertThat(log.warnings())
          .singleElement()
          .satisfies(
              warning ->
                  assertThat(warning.getFormattedMessage())
                      .contains("replayed event")
                      .contains("the replay call fails"));
    }
  }

  @Test
  void aRowDeletedUnderTheFedStep_defersTheReplayedEntryAndKeepsIt() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      EventEnvelope advanced = aHeldEvent(f);
      f.a.sagaStore.runOnceBeforeTheEventWrite(() -> f.rows.delete(sagaOf(ORDER), TYPE));

      assertThat(f.a.replayer.replayAll(sagaOf(ORDER)))
          .containsExactly(SagaDeadLetterReplayer.ReplayOutcome.SAGA_ROW_PENDING);

      assertThat(f.rows.load(sagaOf(ORDER), TYPE, State.class)).isEmpty();
      assertThat(f.deadLetters.findBySaga(sagaOf(ORDER)))
          .singleElement()
          .satisfies(entry -> assertThat(entry.eventOffset()).isEqualTo(advanced.globalOffset()));
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value());
    }
  }

  @Test
  void aReplayFeedWhoseReRunIsPoison_isAnsweredFaulted() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      EventEnvelope advanced = aHeldEvent(f);
      f.a.sagaStore.runOnceBeforeTheEventWrite(
          () -> {
            f.rows.markFaulted(sagaOf(ORDER), TYPE, f.row(ORDER).version());
            f.a.saga.evolveThrows = true;
          });

      StepOutcome outcome = f.a.runner.feedReplay(sagaOf(ORDER), advanced, false, false);

      assertThat(outcome).isEqualTo(StepOutcome.FAULTED);
      assertThat(f.deadLetters.findBySaga(sagaOf(ORDER)))
          .singleElement()
          .satisfies(
              entry -> {
                assertThat(entry.eventOffset()).isEqualTo(advanced.globalOffset());
                assertThat(entry.errorMessage()).contains("evolve fails on purpose");
              });
      assertThat(f.a.deliveryRetriedSamples).containsExactly(TYPE.value());
    }
  }
}
