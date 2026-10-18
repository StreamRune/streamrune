package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.runtime.SagaStepLockFixture.TYPE;
import static org.streamrune.runtime.SagaStepLockFixture.sagaOf;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.runtime.SagaDeadLetterReplayer.CompensateOutcome;
import org.streamrune.runtime.SagaStepLockFixture.Advanced;
import org.streamrune.runtime.SagaStepLockFixture.Completed;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.Refund;
import org.streamrune.runtime.SagaStepLockFixture.Release;
import org.streamrune.runtime.SagaStepLockFixture.Reserve;
import org.streamrune.runtime.SagaStepLockFixture.Ship;
import org.streamrune.runtime.SagaStepLockFixture.State;

/**
 * No forward command of a saga is stored after its compensation episode opened, when the path that
 * opens the episode — or ends the saga — runs on another process than the step: a timeout claim, a
 * twin delivery whose own command was rejected, an operator's compensation of a faulted forward
 * step, a completing event. Each case runs twice over {@link SagaForwardFenceTwoNodeFixture}: with
 * the fence on, the step's late command is refused at its commit and the step goes on from the row;
 * with the fence off (both nodes {@code forwardFence(false)}), the same schedule stores the forward
 * command after the compensation, which proves that the schedule reaches the window the fence
 * closes.
 *
 * <p>Two more cases run in one process, where the step lock does not hold the other path back: a
 * delivery nested on the step's own thread, and a timeout runner given a lock of its own.
 *
 * <p>Every case checks the stored order of the order's events and, on the two-node fixture, the
 * rule of the invariant read from the shared command inbox.
 */
class SagaForwardFenceScenarioTest {

  private static final String ORDER = "1";
  private static final List<String> FORWARD = List.of("Reserved", "Shipped");
  private static final List<String> COMPENSATION = List.of("Released", "Refunded");

  /** A business rejection of a forward command, thrown by the target aggregate. */
  static final class Rejected extends DomainException {
    Rejected() {
      super("the target aggregate rejects the command");
    }
  }

  /** Throws {@code failure} the first time only. */
  private static Supplier<RuntimeException> once(Supplier<RuntimeException> failure) {
    AtomicBoolean thrown = new AtomicBoolean();
    return () -> {
      if (thrown.compareAndSet(false, true)) {
        return failure.get();
      }
      throw new AssertionError("asked for the failure twice");
    };
  }

  /** Whether a forward event is stored after the first compensation event of the order. */
  private static boolean aForwardEventAfterTheFirstCompensationEvent(List<String> stored) {
    int firstCompensation = -1;
    for (int i = 0; i < stored.size(); i++) {
      if (COMPENSATION.contains(stored.get(i))) {
        firstCompensation = i;
        break;
      }
    }
    if (firstCompensation < 0) {
      return false;
    }
    return stored.subList(firstCompensation, stored.size()).stream().anyMatch(FORWARD::contains);
  }

  private static void assertTheFenceHeld(SagaForwardFenceTwoNodeFixture f) {
    assertThat(aForwardEventAfterTheFirstCompensationEvent(f.storedOrder(ORDER))).isFalse();
    assertThat(f.forwardCommandsStoredAfterACompensation(ORDER)).isEmpty();
  }

  private static void assertAForwardCommandLandedAfterTheCompensation(
      SagaForwardFenceTwoNodeFixture f) {
    assertThat(aForwardEventAfterTheFirstCompensationEvent(f.storedOrder(ORDER))).isTrue();
    assertThat(f.forwardCommandsStoredAfterACompensation(ORDER)).isNotEmpty();
  }

  // ---- the other node's timeout claim ----

  @Test
  void theOtherNodesDeadlineClaimLandsBeforeTheFirstForwardCommand_fenceOn() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      EventEnvelope opened = f.append(new Opened(ORDER));
      f.a.bus.runOnceBefore(Reserve.class, f.b::timeOut);

      f.a.deliver(opened);

      assertThat(f.storedOrder(ORDER)).containsExactly("Released", "Refunded");
      assertThat(f.a.fencedSamples).containsExactly(TYPE.value());
      assertThat(f.a.dispatched)
          .as("the step stops at the refused command")
          .doesNotContain(Ship.class);
      assertThat(f.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
      assertTheFenceHeld(f);
    }
  }

  @Test
  void theOtherNodesDeadlineClaimLandsBeforeTheFirstForwardCommand_fenceOff() {
    try (var f = new SagaForwardFenceTwoNodeFixture(false)) {
      EventEnvelope opened = f.append(new Opened(ORDER));
      f.a.bus.runOnceBefore(Reserve.class, f.b::timeOut);

      f.a.deliver(opened);

      assertThat(f.storedOrder(ORDER))
          .containsExactly("Released", "Refunded", "Reserved", "Shipped");
      assertThat(f.a.fencedSamples).isEmpty();
      assertAForwardCommandLandedAfterTheCompensation(f);
    }
  }

  @Test
  void theOtherNodesClaimLandsBetweenTwoForwardCommands_fenceOn() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      EventEnvelope opened = f.append(new Opened(ORDER));
      f.a.bus.runOnceBefore(Ship.class, f.b::timeOut);

      f.a.deliver(opened);

      assertThat(f.storedOrder(ORDER)).containsExactly("Reserved", "Released", "Refunded");
      assertThat(f.a.fencedSamples).containsExactly(TYPE.value());
      assertThat(f.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
      assertTheFenceHeld(f);
    }
  }

  @Test
  void theOtherNodesClaimLandsBetweenTwoForwardCommands_fenceOff() {
    try (var f = new SagaForwardFenceTwoNodeFixture(false)) {
      EventEnvelope opened = f.append(new Opened(ORDER));
      f.a.bus.runOnceBefore(Ship.class, f.b::timeOut);

      f.a.deliver(opened);

      assertThat(f.storedOrder(ORDER))
          .containsExactly("Reserved", "Released", "Refunded", "Shipped");
      assertAForwardCommandLandedAfterTheCompensation(f);
    }
  }

  // ---- a twin delivery on the other node, rejected at the first command ----

  @Test
  void aTwinOnTheOtherNodeIsRejectedAtTheFirstCommandAndClaims_fenceOn() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      EventEnvelope opened = f.append(new Opened(ORDER));
      f.b.reserve.failure = once(Rejected::new);
      f.a.bus.runOnceBefore(Reserve.class, () -> f.b.deliver(opened));

      f.a.deliver(opened);

      assertThat(f.storedOrder(ORDER)).containsExactly("Released", "Refunded");
      assertThat(f.a.fencedSamples).containsExactly(TYPE.value());
      assertThat(f.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
      assertTheFenceHeld(f);
    }
  }

  @Test
  void aTwinOnTheOtherNodeIsRejectedAtTheFirstCommandAndClaims_fenceOff() {
    try (var f = new SagaForwardFenceTwoNodeFixture(false)) {
      EventEnvelope opened = f.append(new Opened(ORDER));
      f.b.reserve.failure = once(Rejected::new);
      f.a.bus.runOnceBefore(Reserve.class, () -> f.b.deliver(opened));

      f.a.deliver(opened);

      assertThat(f.storedOrder(ORDER))
          .containsExactly("Released", "Refunded", "Reserved", "Shipped");
      assertAForwardCommandLandedAfterTheCompensation(f);
    }
  }

  // ---- a replay feed and the operator's compensation on the other node ----

  @Test
  void aReplayFeedMeetsTheOtherNodesCompensateFaulted_fenceOn() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      // A saga a forward step faulted, with no dead-letter entry left.
      EventEnvelope opened = f.append(new Opened(ORDER));
      f.rows.createFaulted(sagaOf(ORDER), TYPE, new State(SagaStatus.STARTED, ORDER));
      f.a.bus.runOnceBefore(
          Reserve.class,
          () ->
              assertThat(f.b.replayer.compensateFaulted(sagaOf(ORDER)))
                  .isEqualTo(CompensateOutcome.TERMINAL));

      assertThat(f.a.runner.feedReplay(sagaOf(ORDER), opened, true, false))
          .isEqualTo(StepOutcome.SKIPPED_HALTED);

      assertThat(f.storedOrder(ORDER)).containsExactly("Released", "Refunded");
      assertThat(f.a.fencedSamples).containsExactly(TYPE.value());
      assertTheFenceHeld(f);
    }
  }

  @Test
  void aReplayFeedMeetsTheOtherNodesCompensateFaulted_fenceOff() {
    try (var f = new SagaForwardFenceTwoNodeFixture(false)) {
      EventEnvelope opened = f.append(new Opened(ORDER));
      f.rows.createFaulted(sagaOf(ORDER), TYPE, new State(SagaStatus.STARTED, ORDER));
      f.a.bus.runOnceBefore(
          Reserve.class,
          () ->
              assertThat(f.b.replayer.compensateFaulted(sagaOf(ORDER)))
                  .isEqualTo(CompensateOutcome.TERMINAL));

      assertThat(f.a.runner.feedReplay(sagaOf(ORDER), opened, true, false))
          .isEqualTo(StepOutcome.SKIPPED_HALTED);

      assertAForwardCommandLandedAfterTheCompensation(f);
    }
  }

  // ---- a late command followed by a retry-later failure ----

  @Test
  void aLateCommandFollowedByARetryLaterFailure_fenceOn() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      EventEnvelope opened = f.append(new Opened(ORDER));
      f.a.ship.failure = () -> new CircuitBreakerOpenException("the shipment service is down");
      f.a.bus.runOnceBefore(Reserve.class, f.b::timeOut);

      f.a.deliver(opened);

      assertThat(f.storedOrder(ORDER)).containsExactly("Released", "Refunded");
      assertThat(f.a.dispatched).as("Ship is never dispatched").doesNotContain(Ship.class);
      assertTheFenceHeld(f);
    }
  }

  @Test
  void aLateCommandFollowedByARetryLaterFailure_fenceOff() {
    try (var f = new SagaForwardFenceTwoNodeFixture(false)) {
      EventEnvelope opened = f.append(new Opened(ORDER));
      f.a.ship.failure = () -> new CircuitBreakerOpenException("the shipment service is down");
      f.a.bus.runOnceBefore(Reserve.class, f.b::timeOut);

      assertThatThrownBy(() -> f.a.deliver(opened)).isInstanceOf(CircuitBreakerOpenException.class);

      assertThat(f.storedOrder(ORDER)).containsExactly("Released", "Refunded", "Reserved");
      assertThat(f.a.underStepSamples).as("the step reaches no write: nothing reported").isEmpty();
      assertAForwardCommandLandedAfterTheCompensation(f);
    }
  }

  // ---- the saga completes on the other node under the step ----

  @Test
  void theSagaCompletesOnTheOtherNodeUnderTheStep_fenceOn() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      f.a.saga.startAlsoShips = false;
      f.b.saga.startAlsoShips = false;
      f.a.deliver(f.append(new Opened(ORDER)));
      EventEnvelope advanced = f.append(new Advanced(ORDER));
      EventEnvelope completed = f.append(new Completed(ORDER));
      f.a.bus.runOnceBefore(Ship.class, () -> f.b.deliver(completed));

      f.a.deliver(advanced);

      assertThat(f.storedOrder(ORDER)).containsExactly("Reserved");
      assertThat(f.row(ORDER).status()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(f.row(ORDER).version()).as("A writes nothing").isEqualTo(3L);
      assertThat(f.a.fencedSamples).containsExactly(TYPE.value());
    }
  }

  @Test
  void theSagaCompletesOnTheOtherNodeUnderTheStep_fenceOff() {
    try (var f = new SagaForwardFenceTwoNodeFixture(false)) {
      f.a.saga.startAlsoShips = false;
      f.b.saga.startAlsoShips = false;
      f.a.deliver(f.append(new Opened(ORDER)));
      EventEnvelope advanced = f.append(new Advanced(ORDER));
      EventEnvelope completed = f.append(new Completed(ORDER));
      f.a.bus.runOnceBefore(Ship.class, () -> f.b.deliver(completed));

      f.a.deliver(advanced);

      assertThat(f.storedOrder(ORDER))
          .as("shipped after completion")
          .containsExactly("Reserved", "Shipped");
      assertThat(f.row(ORDER).status()).isEqualTo(SagaStatus.COMPLETED);
    }
  }

  // ---- the other node's episode is still open when the command is refused ----

  @Test
  void theOtherNodesEpisodeIsStillOpenWhenTheCommandIsRefused_fenceOn() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      EventEnvelope opened = f.append(new Opened(ORDER));
      // The other node's refund fails on infrastructure once: its episode stays COMPENSATING.
      f.b.refund.failure = once(() -> new IllegalStateException("connection reset"));
      f.a.bus.runOnceBefore(Ship.class, f.b::timeOut);

      f.a.deliver(opened);

      assertThat(f.storedOrder(ORDER)).containsExactly("Reserved", "Released", "Refunded");
      assertThat(f.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(f.b.dispatched).containsExactly(Release.class, Refund.class);
      assertThat(f.a.dispatched)
          .as("the step resumes the episode: the release answers from the inbox")
          .containsExactly(Reserve.class, Ship.class, Release.class, Refund.class);
      assertThat(f.a.fencedSamples).containsExactly(TYPE.value());
      assertThat(f.a.underStepSamples).isEmpty();
      assertTheFenceHeld(f);
    }
  }

  // ---- the other node faults the saga inside its episode under the step ----

  /** What the other node writes: it claims the episode, and its compensation faults. */
  private static Runnable theOtherNodeClaimsAndFaults(SagaForwardFenceTwoNodeFixture f) {
    return () -> {
      var row = f.row(ORDER);
      f.rows.claimCompensating(sagaOf(ORDER), TYPE, row.state(), row.version(), null);
      f.rows.markFaulted(sagaOf(ORDER), TYPE, row.version() + 1);
    };
  }

  @Test
  void theSagaFaultsInsideTheOtherNodesEpisodeUnderTheStep_theEventIsHeld_fenceOn() {
    try (var f = new SagaForwardFenceTwoNodeFixture(true)) {
      f.a.saga.startAlsoShips = false;
      f.a.deliver(f.append(new Opened(ORDER)));
      EventEnvelope advanced = f.append(new Advanced(ORDER));
      f.a.bus.runOnceBefore(Ship.class, theOtherNodeClaimsAndFaults(f));

      f.a.deliver(advanced);

      assertThat(f.storedOrder(ORDER)).containsExactly("Reserved");
      assertThat(f.row(ORDER).status()).isEqualTo(SagaStatus.FAULTED);
      assertThat(f.row(ORDER).preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(f.deadLetters.findBySaga(sagaOf(ORDER)))
          .as("held, as the delivery after the fault holds it")
          .singleElement()
          .satisfies(entry -> assertThat(entry.eventOffset()).isEqualTo(advanced.globalOffset()));
      assertThat(f.a.skippedWhileFaultedSamples).containsExactly(TYPE.value());
      assertThat(f.a.fencedSamples).containsExactly(TYPE.value());
    }
  }

  @Test
  void theSagaFaultsInsideTheOtherNodesEpisodeUnderTheStep_theEventIsHeld_fenceOff() {
    try (var f = new SagaForwardFenceTwoNodeFixture(false)) {
      f.a.saga.startAlsoShips = false;
      f.a.deliver(f.append(new Opened(ORDER)));
      EventEnvelope advanced = f.append(new Advanced(ORDER));
      f.a.bus.runOnceBefore(Ship.class, theOtherNodeClaimsAndFaults(f));

      f.a.deliver(advanced);

      assertThat(f.storedOrder(ORDER))
          .as("shipped under the episode")
          .containsExactly("Reserved", "Shipped");
      assertThat(f.deadLetters.findBySaga(sagaOf(ORDER)))
          .singleElement()
          .satisfies(entry -> assertThat(entry.eventOffset()).isEqualTo(advanced.globalOffset()));
      assertThat(f.a.skippedWhileFaultedSamples).containsExactly(TYPE.value());
    }
  }

  // ---- in one process: paths the step lock does not hold back ----

  @Test
  void aDeliveryNestedOnTheStepsThread_cannotStoreAForwardCommandAfterItsClaim_fenceOn() {
    try (var p = new SagaStepLockFixture(true)) {
      p.deliver(p.append(new Opened(ORDER)));
      EventEnvelope first = p.append(new Advanced(ORDER));
      EventEnvelope second = p.append(new Advanced(ORDER));
      p.bus.runOnceBefore(Ship.class, () -> nestedRejectedDelivery(p, second));

      p.deliver(first);

      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved", "Released", "Refunded");
      assertThat(aForwardEventAfterTheFirstCompensationEvent(p.storedOrder(ORDER))).isFalse();
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }

  @Test
  void aDeliveryNestedOnTheStepsThread_cannotStoreAForwardCommandAfterItsClaim_fenceOff() {
    try (var p = new SagaStepLockFixture(false)) {
      p.deliver(p.append(new Opened(ORDER)));
      EventEnvelope first = p.append(new Advanced(ORDER));
      EventEnvelope second = p.append(new Advanced(ORDER));
      p.bus.runOnceBefore(Ship.class, () -> nestedRejectedDelivery(p, second));

      p.deliver(first);

      assertThat(p.storedOrder(ORDER))
          .containsExactly("Reserved", "Released", "Refunded", "Shipped");
    }
  }

  /** A delivery of the saga's next event on the step's own thread; its shipment is rejected. */
  private static void nestedRejectedDelivery(SagaStepLockFixture p, EventEnvelope event) {
    p.ship.failure = Rejected::new;
    try {
      p.deliver(event);
    } finally {
      p.ship.failure = null;
    }
  }

  @Test
  void aTimeoutRunnerWithALockOfItsOwn_cannotPrecedeAForwardCommand_fenceOn() {
    try (var p = new SagaStepLockFixture(true)) {
      SagaTimeoutRunner<State> ownLock = timeoutRunnerWithALockOfItsOwn(p);
      EventEnvelope opened = p.append(new Opened(ORDER));
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            p.clock.advance(SagaStepLockFixture.TIMEOUT.plusSeconds(1));
            ownLock.pollOnce();
          });

      p.deliver(opened);

      assertThat(p.storedOrder(ORDER)).containsExactly("Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }

  @Test
  void aTimeoutRunnerWithALockOfItsOwn_cannotPrecedeAForwardCommand_fenceOff() {
    try (var p = new SagaStepLockFixture(false)) {
      SagaTimeoutRunner<State> ownLock = timeoutRunnerWithALockOfItsOwn(p);
      EventEnvelope opened = p.append(new Opened(ORDER));
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            p.clock.advance(SagaStepLockFixture.TIMEOUT.plusSeconds(1));
            ownLock.pollOnce();
          });

      p.deliver(opened);

      assertThat(p.storedOrder(ORDER)).containsExactly("Released", "Refunded", "Reserved");
    }
  }

  private static SagaTimeoutRunner<State> timeoutRunnerWithALockOfItsOwn(SagaStepLockFixture p) {
    return SagaTimeoutRunner.<State>builder()
        .decider(p.saga)
        .sagaStore(p.sagaStore)
        .commandBus(p.bus)
        .sagaType(TYPE)
        .clock(p.clock)
        .stepLock(new SagaStepLock())
        .build();
  }
}
