package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.runtime.SagaDeadLetterReplayer.CompensateOutcome;
import org.streamrune.runtime.SagaRunnerTestAccess;

/**
 * Two processes on one PostgreSQL database: no forward command of a saga is stored after its
 * compensation episode opened — or after the saga ended — when the path that opens the episode or
 * ends the saga runs on the other process: a timeout claim, a twin delivery whose own command was
 * rejected, an operator's compensation of a faulted saga, a completing event.
 *
 * <p>Each case runs twice. With the fence on, the late forward command is refused at its commit and
 * the step goes on from the row. With the fence off (both nodes {@code forwardFence(false)}), the
 * same schedule stores the forward command after the compensation, which proves the schedule
 * reaches the window the fence closes. One case, an append frozen after its fence read, runs with
 * the fence on only: there the other node's claim waits on the saga's fence lock, observed on the
 * server.
 *
 * <p>Every case checks the stored order of the saga's events in {@code event_stream} and the
 * invariant query over {@code command_inbox} ({@link SagaForwardFenceInvariant}).
 */
@Timeout(120)
class SagaForwardFenceTwoNodesIT extends SagaForwardFenceNodes {

  private static final String ORDER = "1";

  /** A business rejection of a forward command, thrown by the target aggregate. */
  static final class Rejected extends DomainException {
    Rejected() {
      super("the target aggregate rejects the command");
    }
  }

  private void assertTheFenceHeld() throws Exception {
    assertThat(SagaForwardFenceInvariant.sagasWithAForwardEventAfterACompensationEvent(probe))
        .isEmpty();
    assertThat(SagaForwardFenceInvariant.lateForwardCommands(dataSource)).isEmpty();
    assertTheCompensationIsStoredOnce();
  }

  /** The forward commands with these keys are stored after the saga's first compensation. */
  private void assertStoredAfterTheCompensation(String... forwardKeys) throws Exception {
    assertThat(SagaForwardFenceInvariant.sagasWithAForwardEventAfterACompensationEvent(probe))
        .containsOnlyKeys(sagaOf(ORDER).value());
    assertThat(
            SagaForwardFenceInvariant.forwardKeysOf(
                SagaForwardFenceInvariant.lateForwardCommands(dataSource)))
        .containsExactlyInAnyOrder(forwardKeys);
    assertTheCompensationIsStoredOnce();
  }

  private void assertTheCompensationIsStoredOnce() throws Exception {
    assertThat(SagaForwardFenceInvariant.sagasWithACompensationStoredTwice(probe, dataSource))
        .isEmpty();
    List<String> keys =
        SagaForwardFenceInvariant.compensationKeysOf(dataSource, sagaOf(ORDER).value());
    assertThat(keys).hasSize(2);
    assertThat(keys.get(0)).endsWith(":comp:0");
    assertThat(keys.get(1)).endsWith(":comp:1");
  }

  // ---- the other node's claim commits before the first forward append ----

  @Test
  void theOtherNodesClaimCommitsBeforeTheFirstForwardAppend_fenceOn() throws Exception {
    Node a = node("node-a", true);
    Node b = node("node-b", true);
    EventEnvelope opened = append(new Opened(ORDER));
    AtomicLong versionAfterTheClaim = new AtomicLong();
    a.bus.runOnceBefore(
        Reserve.class,
        () -> {
          timeOut(b);
          versionAfterTheClaim.set(row(ORDER).version());
        });

    a.deliver(opened);

    assertThat(storedOrder(ORDER)).containsExactly("Released", "Refunded");
    assertThat(a.fencedSamples).containsExactly(TYPE.value());
    assertThat(a.dispatched).as("the step stops at the refused command").doesNotContain(Ship.class);
    assertThat(row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(row(ORDER).version()).as("A writes nothing").isEqualTo(versionAfterTheClaim.get());
    assertTheFenceHeld();
  }

  @Test
  void theOtherNodesClaimCommitsBeforeTheFirstForwardAppend_fenceOff() throws Exception {
    Node a = node("node-a", false);
    Node b = node("node-b", false);
    EventEnvelope opened = append(new Opened(ORDER));
    a.bus.runOnceBefore(Reserve.class, () -> timeOut(b));

    a.deliver(opened);

    assertThat(storedOrder(ORDER)).containsExactly("Released", "Refunded", "Reserved", "Shipped");
    assertThat(a.fencedSamples).isEmpty();
    assertStoredAfterTheCompensation(forwardKey(ORDER, opened, 0), forwardKey(ORDER, opened, 1));
  }

  // ---- the other node's claim arrives while the append is frozen holding the fence ----

  @Test
  void theOtherNodesClaimArrivesWhileTheAppendIsFrozenHoldingTheFence_fenceOn() throws Exception {
    Node a = node("node-a", true);
    Node b = node("node-b", true);
    EventEnvelope opened = append(new Opened(ORDER));
    Connection freezer = freezeAppends();
    int freezerPid = backendPid(freezer);

    CompletableFuture<Void> delivery = onAnotherThread(() -> a.deliver(opened));
    int appendPid = awaitBlockedBy(freezerPid, "node-a");
    CompletableFuture<Void> claim = onAnotherThread(() -> timeOut(b));
    int claimPid = awaitBlockedBy(appendPid, "node-b");

    Activity waiting = activity(claimPid);
    assertThat(waiting.waitEventType()).isEqualTo("Lock");
    assertThat(waiting.waitEvent()).as("the saga's fence lock").isEqualTo("advisory");
    assertThat(waiting.query())
        .as("the exclusive fence lock of the claim")
        .contains("pg_advisory_xact_lock(")
        .doesNotContain("pg_advisory_xact_lock_shared(");
    assertThat(claim).as("the claim waits for the frozen append").isNotDone();
    assertThat(row(ORDER).episodeVersion()).as("no episode opened yet").isNull();
    assertThat(storedOrder(ORDER)).isEmpty();

    freezer.commit();

    assertThat(outcomeOf(claim)).isNull();
    assertThat(outcomeOf(delivery)).as("the step goes on from the refused command").isNull();
    assertThat(storedOrder(ORDER)).containsExactly("Reserved", "Released", "Refunded");
    assertThat(a.fencedSamples).as("the second forward command").containsExactly(TYPE.value());
    assertThat(row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    assertTheFenceHeld();
  }

  // ---- the other node's claim lands between the two forward commands ----

  @Test
  void theOtherNodesClaimLandsBetweenTheTwoForwardCommands_fenceOn() throws Exception {
    Node a = node("node-a", true);
    Node b = node("node-b", true);
    EventEnvelope opened = append(new Opened(ORDER));
    a.bus.runOnceBefore(Ship.class, () -> timeOut(b));

    a.deliver(opened);

    assertThat(storedOrder(ORDER)).containsExactly("Reserved", "Released", "Refunded");
    assertThat(a.fencedSamples).containsExactly(TYPE.value());
    assertThat(row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    assertTheFenceHeld();
  }

  @Test
  void theOtherNodesClaimLandsBetweenTheTwoForwardCommands_fenceOff() throws Exception {
    Node a = node("node-a", false);
    Node b = node("node-b", false);
    EventEnvelope opened = append(new Opened(ORDER));
    a.bus.runOnceBefore(Ship.class, () -> timeOut(b));

    a.deliver(opened);

    assertThat(storedOrder(ORDER)).containsExactly("Reserved", "Released", "Refunded", "Shipped");
    assertStoredAfterTheCompensation(forwardKey(ORDER, opened, 1));
  }

  // ---- a twin delivery on the other node, rejected at the first command ----

  @Test
  void aTwinOnTheOtherNodeIsRejectedAtTheFirstCommandAndClaims_fenceOn() throws Exception {
    Node a = node("node-a", true);
    Node b = node("node-b", true);
    EventEnvelope opened = append(new Opened(ORDER));
    b.bus.runOnceBefore(
        Reserve.class,
        () -> {
          throw new Rejected();
        });
    a.bus.runOnceBefore(Reserve.class, () -> b.deliver(opened));

    a.deliver(opened);

    assertThat(storedOrder(ORDER)).containsExactly("Released", "Refunded");
    assertThat(a.fencedSamples).containsExactly(TYPE.value());
    assertThat(row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    assertTheFenceHeld();
  }

  @Test
  void aTwinOnTheOtherNodeIsRejectedAtTheFirstCommandAndClaims_fenceOff() throws Exception {
    Node a = node("node-a", false);
    Node b = node("node-b", false);
    EventEnvelope opened = append(new Opened(ORDER));
    b.bus.runOnceBefore(
        Reserve.class,
        () -> {
          throw new Rejected();
        });
    a.bus.runOnceBefore(Reserve.class, () -> b.deliver(opened));

    a.deliver(opened);

    assertThat(storedOrder(ORDER)).containsExactly("Released", "Refunded", "Reserved", "Shipped");
    assertStoredAfterTheCompensation(forwardKey(ORDER, opened, 0), forwardKey(ORDER, opened, 1));
  }

  // ---- a replay feed and the operator's compensation on the other node ----

  @Test
  void aReplayFeedMeetsTheOtherNodesCompensateFaulted_fenceOn() throws Exception {
    Node a = node("node-a", true);
    Node b = node("node-b", true);
    // A saga a forward step faulted, with no dead-letter entry left.
    EventEnvelope opened = append(new Opened(ORDER));
    probeRows.createFaulted(sagaOf(ORDER), TYPE, new State(SagaStatus.STARTED, ORDER));
    a.bus.runOnceBefore(
        Reserve.class,
        () ->
            assertThat(b.replayer.compensateFaulted(sagaOf(ORDER)))
                .isEqualTo(CompensateOutcome.TERMINAL));

    assertThat(SagaRunnerTestAccess.feedReplay(a.runner, sagaOf(ORDER), opened, true))
        .isEqualTo("SKIPPED_HALTED");

    assertThat(storedOrder(ORDER)).containsExactly("Released", "Refunded");
    assertThat(a.fencedSamples).containsExactly(TYPE.value());
    assertThat(row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    assertTheFenceHeld();
  }

  @Test
  void aReplayFeedMeetsTheOtherNodesCompensateFaulted_fenceOff() throws Exception {
    Node a = node("node-a", false);
    Node b = node("node-b", false);
    EventEnvelope opened = append(new Opened(ORDER));
    probeRows.createFaulted(sagaOf(ORDER), TYPE, new State(SagaStatus.STARTED, ORDER));
    a.bus.runOnceBefore(
        Reserve.class,
        () ->
            assertThat(b.replayer.compensateFaulted(sagaOf(ORDER)))
                .isEqualTo(CompensateOutcome.TERMINAL));

    assertThat(SagaRunnerTestAccess.feedReplay(a.runner, sagaOf(ORDER), opened, true))
        .as("the run again meets the terminal row")
        .isEqualTo("SKIPPED_HALTED");

    assertThat(storedOrder(ORDER)).containsExactly("Released", "Refunded", "Reserved", "Shipped");
    assertStoredAfterTheCompensation(forwardKey(ORDER, opened, 0), forwardKey(ORDER, opened, 1));
  }

  // ---- a late command, then a retry-later failure ----

  @Test
  void aLateCommandThenARetryLaterFailure_fenceOn() throws Exception {
    Node a = node("node-a", true);
    Node b = node("node-b", true);
    EventEnvelope opened = append(new Opened(ORDER));
    a.ship.failure = () -> new CircuitBreakerOpenException("the shipment service is down");
    AtomicLong versionAfterTheClaim = new AtomicLong();
    a.bus.runOnceBefore(
        Reserve.class,
        () -> {
          timeOut(b);
          versionAfterTheClaim.set(row(ORDER).version());
        });

    a.deliver(opened);

    assertThat(storedOrder(ORDER)).containsExactly("Released", "Refunded");
    assertThat(a.dispatched).as("Ship is never dispatched").doesNotContain(Ship.class);
    assertThat(a.fencedSamples).containsExactly(TYPE.value());
    assertThat(row(ORDER).version())
        .as("nothing depends on a write of A")
        .isEqualTo(versionAfterTheClaim.get());
    assertTheFenceHeld();
  }

  @Test
  void aLateCommandThenARetryLaterFailure_fenceOff() throws Exception {
    Node a = node("node-a", false);
    Node b = node("node-b", false);
    EventEnvelope opened = append(new Opened(ORDER));
    a.ship.failure = () -> new CircuitBreakerOpenException("the shipment service is down");
    a.bus.runOnceBefore(Reserve.class, () -> timeOut(b));

    assertThatThrownBy(() -> a.deliver(opened)).isInstanceOf(CircuitBreakerOpenException.class);

    assertThat(storedOrder(ORDER)).containsExactly("Released", "Refunded", "Reserved");
    assertThat(a.underStepSamples).as("the step reaches no write: nothing reported").isEmpty();
    assertStoredAfterTheCompensation(forwardKey(ORDER, opened, 0));
  }

  // ---- the saga completes on the other node under the step ----

  @Test
  void theSagaCompletesOnTheOtherNodeUnderTheStep_fenceOn() throws Exception {
    Node a = node("node-a", true);
    Node b = node("node-b", true);
    a.saga.startAlsoShips = false;
    b.saga.startAlsoShips = false;
    a.deliver(append(new Opened(ORDER)));
    EventEnvelope advanced = append(new Advanced(ORDER));
    EventEnvelope completed = append(new Completed(ORDER));
    a.bus.runOnceBefore(Ship.class, () -> b.deliver(completed));

    a.deliver(advanced);

    assertThat(storedOrder(ORDER)).containsExactly("Reserved");
    assertThat(row(ORDER).status()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(row(ORDER).version()).as("A writes nothing").isEqualTo(3L);
    assertThat(a.fencedSamples).containsExactly(TYPE.value());
    assertThat(SagaForwardFenceInvariant.lateForwardCommands(dataSource)).isEmpty();
  }

  @Test
  void theSagaCompletesOnTheOtherNodeUnderTheStep_fenceOff() throws Exception {
    Node a = node("node-a", false);
    Node b = node("node-b", false);
    a.saga.startAlsoShips = false;
    b.saga.startAlsoShips = false;
    a.deliver(append(new Opened(ORDER)));
    EventEnvelope advanced = append(new Advanced(ORDER));
    EventEnvelope completed = append(new Completed(ORDER));
    a.bus.runOnceBefore(Ship.class, () -> b.deliver(completed));

    a.deliver(advanced);

    assertThat(a.dispatched)
        .as("the step dispatches Ship after B completed the saga")
        .contains(Ship.class);
    assertThat(storedOrder(ORDER))
        .as("shipped after completion")
        .containsExactly("Reserved", "Shipped");
    assertThat(row(ORDER).status()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(row(ORDER).version()).isEqualTo(3L);
    assertThat(SagaForwardFenceInvariant.lateForwardCommands(dataSource))
        .as("the saga ended without a compensation: the query has no compensation to compare")
        .isEmpty();
  }
}
