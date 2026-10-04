package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

/**
 * Covers the saga/inbox/outbox operational hooks added to {@link
 * org.streamrune.core.StreamRuneMetrics}, plus the previously-missing {@link
 * MicrometerStreamRuneMetrics} overrides that used to silently no-op.
 */
class MicrometerStreamRuneMetricsSagaTest {

  // ==================== New saga/inbox/outbox hooks ====================

  @Test
  void recordSagaQuarantinedIncrementsTaggedCounter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaQuarantined("OrderFulfillmentSaga");

    assertEquals(
        1.0,
        registry
            .find("streamrune.saga.quarantined")
            .tag("saga.type", "OrderFulfillmentSaga")
            .counter()
            .count());
  }

  @Test
  void recordSagaFaultedIncrementsTaggedCounter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaFaulted("OrderFulfillmentSaga");

    assertEquals(
        1.0,
        registry
            .find("streamrune.saga.faulted")
            .tag("saga.type", "OrderFulfillmentSaga")
            .counter()
            .count());
  }

  @Test
  void recordSagaSkippedWhileFaultedIncrementsTaggedCounter() {
    // The skip needs a series of its own, or an already-known fault reads as a
    // stream of NEW orchestrator defects on streamrune.saga.quarantined.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaSkippedWhileFaulted("OrderFulfillmentSaga");

    assertEquals(
        1.0,
        registry
            .find("streamrune.saga.skipped_while_faulted")
            .tag("saga.type", "OrderFulfillmentSaga")
            .counter()
            .count());
  }

  @Test
  void recordSagaEventHeldIncrementsCounterTaggedBySagaTypeAndReason() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaEventHeld("OrderFulfillmentSaga", "GENESIS_PENDING");
    metrics.recordSagaEventHeld("OrderFulfillmentSaga", "GENESIS_PENDING");
    metrics.recordSagaEventHeld("OrderFulfillmentSaga", "BACKLOG_PENDING");

    assertEquals(
        2.0,
        registry
            .find("streamrune.saga.event_held")
            .tag("saga.type", "OrderFulfillmentSaga")
            .tag("hold.reason", "GENESIS_PENDING")
            .counter()
            .count());
    assertEquals(
        1.0,
        registry
            .find("streamrune.saga.event_held")
            .tag("hold.reason", "BACKLOG_PENDING")
            .counter()
            .count());
  }

  @Test
  void recordSagaReplayDeferredIncrementsTaggedCounter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaReplayDeferred("OrderFulfillmentSaga");

    assertEquals(
        1.0,
        registry
            .find("streamrune.saga.replay_deferred")
            .tag("saga.type", "OrderFulfillmentSaga")
            .counter()
            .count());
  }

  @Test
  void recordSagaForcedStaleResumeIncrementsTaggedCounter() {
    // A forced resume past the executor's key-age guard is an operator
    // override of a double-execution guard, so it gets a series of its own.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaForcedStaleResume("OrderFulfillmentSaga");

    assertEquals(
        1.0,
        registry
            .find("streamrune.saga.forced_stale_resume")
            .tag("saga.type", "OrderFulfillmentSaga")
            .counter()
            .count());
  }

  @Test
  void recordSagaResumeFaultedIncrementsTaggedCounter() {
    // Every SagaDeadLetterReplayer.resumeFaulted outcome, refusals
    // included, is one sample tagged by saga type and outcome.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaResumeFaulted("OrderFulfillmentSaga", "RESUMED");
    metrics.recordSagaResumeFaulted("OrderFulfillmentSaga", "STALE_COMPENSATION_BLOCKED");
    metrics.recordSagaResumeFaulted("OrderFulfillmentSaga", "RESUMED");

    assertEquals(
        2.0,
        registry
            .find("streamrune.saga.resume_faulted")
            .tag("saga.type", "OrderFulfillmentSaga")
            .tag("outcome", "RESUMED")
            .counter()
            .count());
    assertEquals(
        1.0,
        registry
            .find("streamrune.saga.resume_faulted")
            .tag("saga.type", "OrderFulfillmentSaga")
            .tag("outcome", "STALE_COMPENSATION_BLOCKED")
            .counter()
            .count());
  }

  @Test
  void recordSagaCompensateFaultedIncrementsTaggedCounter() {
    // Every SagaDeadLetterReplayer.compensateFaulted outcome, refusals included, is one sample
    // tagged by saga type and outcome.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaCompensateFaulted("OrderFulfillmentSaga", "TERMINAL");
    metrics.recordSagaCompensateFaulted("OrderFulfillmentSaga", "ENTRIES_PENDING");
    metrics.recordSagaCompensateFaulted("OrderFulfillmentSaga", "TERMINAL");

    assertEquals(
        2.0,
        registry
            .find("streamrune.saga.compensate_faulted")
            .tag("saga.type", "OrderFulfillmentSaga")
            .tag("outcome", "TERMINAL")
            .counter()
            .count());
    assertEquals(
        1.0,
        registry
            .find("streamrune.saga.compensate_faulted")
            .tag("saga.type", "OrderFulfillmentSaga")
            .tag("outcome", "ENTRIES_PENDING")
            .counter()
            .count());
  }

  @Test
  void recordSagaCompensationIncrementsTaggedCounter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaCompensation("OrderFulfillmentSaga", "SUCCEEDED");

    assertEquals(
        1.0,
        registry
            .find("streamrune.saga.compensation")
            .tag("saga.type", "OrderFulfillmentSaga")
            .tag("outcome", "SUCCEEDED")
            .counter()
            .count());
  }

  @Test
  void recordSagaCasConflictIncrementsTaggedCounter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaCasConflict("OrderFulfillmentSaga");

    assertEquals(
        1.0,
        registry
            .find("streamrune.saga.cas_conflicts")
            .tag("saga.type", "OrderFulfillmentSaga")
            .counter()
            .count());
  }

  @Test
  void recordSagaReplayedIncrementsTaggedCounter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaReplayed("OrderFulfillmentSaga", "IGNORED");

    assertEquals(
        1.0,
        registry
            .find("streamrune.saga.replayed")
            .tag("saga.type", "OrderFulfillmentSaga")
            .tag("outcome", "IGNORED")
            .counter()
            .count());
  }

  @Test
  void recordSagaCompensatingBacklogRegistersTaggedGauge() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaCompensatingBacklog("OrderFulfillmentSaga", 4);

    assertEquals(
        4.0,
        registry
            .find("streamrune.saga.compensating")
            .tag("saga.type", "OrderFulfillmentSaga")
            .gauge()
            .value());
  }

  @Test
  void recordSagaCompensatingBacklogGaugeUpdatesInPlace() {
    // Gauge semantics: last write wins (the sweeper re-samples each cycle).
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaCompensatingBacklog("OrderFulfillmentSaga", 4);
    metrics.recordSagaCompensatingBacklog("OrderFulfillmentSaga", 1);

    assertEquals(
        1.0,
        registry
            .find("streamrune.saga.compensating")
            .tag("saga.type", "OrderFulfillmentSaga")
            .gauge()
            .value());
  }

  @Test
  void recordSagaTimedOutBacklogRegistersTaggedGauge() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaTimedOutBacklog("OrderFulfillmentSaga", 3);

    assertEquals(
        3.0,
        registry
            .find("streamrune.saga.timed_out")
            .tag("saga.type", "OrderFulfillmentSaga")
            .gauge()
            .value());
  }

  @Test
  void recordInboxReplayHitIncrementsTaggedCounter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordInboxReplayHit("PlaceOrder");

    assertEquals(
        1.0,
        registry
            .find("streamrune.inbox.replay_hits")
            .tag("command.type", "PlaceOrder")
            .counter()
            .count());
  }

  @Test
  void recordInboxSweptIncrementsCounterByRowCount() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordInboxSwept(7);

    assertEquals(7.0, registry.counter("streamrune.inbox.swept_rows").count());
  }

  @Test
  void recordSagaDeadLetterSweptIncrementsCounterByRowCount() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaDeadLetterSwept(3);

    assertEquals(3.0, registry.counter("streamrune.saga.dead_letters_swept").count());
  }

  @Test
  void recordOutboxSweptIncrementsCounterByRowCount() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordOutboxSwept(5);

    assertEquals(5.0, registry.counter("streamrune.outbox.swept_rows").count());
  }

  @Test
  void recordOutboxDeliveryFailedIncrementsCounter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordOutboxDeliveryFailed();
    metrics.recordOutboxDeliveryFailed();

    assertEquals(2.0, registry.counter("streamrune.outbox.delivery_failed").count());
  }

  @Test
  void recordOutboxSkippedSweptIncrementsCounterByRowCount() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordOutboxSkippedSwept(6);

    assertEquals(6.0, registry.counter("streamrune.outbox.skipped_swept").count());
  }

  @Test
  void recordOutboxReplayedIncrementsCounterByRowCount() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordOutboxReplayed(3);

    assertEquals(3.0, registry.counter("streamrune.outbox.replayed").count());
  }

  @Test
  void recordDeadLetterSweptIncrementsCounterByRowCount() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordDeadLetterSwept(4);

    assertEquals(4.0, registry.counter("streamrune.dlq.swept_rows").count());
  }

  // ==================== Previously-missing overrides (silent no-ops) ====================

  @Test
  void recordDeadLetterPublishedIncrementsCounter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordDeadLetterPublished("PlaceOrder");

    // The family is command.type-tagged, so it is located with find(...).tag(...) — the
    // bare registry.counter(name) lookup this test used to make would AUTO-VIVIFY a second,
    // tag-less shape of the family and read 0.0 (a shape mistake).
    assertEquals(
        1.0,
        registry
            .find("streamrune.dlq.published")
            .tag(org.streamrune.core.metrics.MetricNames.TAG_COMMAND_TYPE, "PlaceOrder")
            .counter()
            .count());
  }

  @Test
  void recordCommandRetriedIncrementsCounter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordCommandRetried("PlaceOrder");

    // command.type-tagged family — see recordDeadLetterPublishedIncrementsCounter.
    assertEquals(
        1.0,
        registry
            .find("streamrune.commands.retried")
            .tag(org.streamrune.core.metrics.MetricNames.TAG_COMMAND_TYPE, "PlaceOrder")
            .counter()
            .count());
  }

  @Test
  void recordCommandShortCircuitedIncrementsCounter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordCommandShortCircuited("PlaceOrder");

    // command.type-tagged family — see recordDeadLetterPublishedIncrementsCounter.
    assertEquals(
        1.0,
        registry
            .find("streamrune.commands.short.circuited")
            .tag(org.streamrune.core.metrics.MetricNames.TAG_COMMAND_TYPE, "PlaceOrder")
            .counter()
            .count());
  }

  @Test
  void recordLockWaitRecordsTimer() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordLockWait(5);

    assertEquals(1L, registry.timer("streamrune.locks.wait").count());
  }

  @Test
  void recordSnapshotCreatedIncrementsCounter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSnapshotCreated();

    assertEquals(1.0, registry.counter("streamrune.snapshots.created").count());
  }

  @Test
  void recordSnapshotLoadedIncrementsCounter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSnapshotLoaded();

    assertEquals(1.0, registry.counter("streamrune.snapshots.loaded").count());
  }

  @Test
  void recordCommandDispatchedTaggedIncrementsOnlyTheTaggedCounter() {
    // This test used to be named "...IncrementsBothUntaggedAndTaggedCounters" and
    // pinned that double emission as correct. It was the bug: sum(streamrune_commands_dispatched)
    // counted every recordCommandDispatched(String) call twice. The sibling series the no-arg
    // method feeds must stay at 0 — only the command.type=PlaceOrder series records the call.
    // That sibling is command.type=unknown, not a tag-less series, so it is located by
    // tag rather than by the bare registry.counter(name) lookup (which would auto-vivify a second,
    // incompatible shape of the family — the very defect the tagged registration closed).
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordCommandDispatched("PlaceOrder");

    assertEquals(
        0.0,
        registry
            .find("streamrune.commands.dispatched")
            .tag("command.type", MicrometerStreamRuneMetrics.UNKNOWN_TAG_VALUE)
            .counter()
            .count(),
        "the no-arg sibling series must NOT be bumped by the tagged overload (no double count)");
    assertEquals(
        1.0,
        registry
            .find("streamrune.commands.dispatched")
            .tag("command.type", "PlaceOrder")
            .counter()
            .count());
  }

  @Test
  void recordEventAppendedTaggedIncrementsOnlyTheTaggedCounter() {
    // Companion case — recordEventAppended(String) had the identical double-count shape.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordEventAppended("OrderPlaced");

    assertEquals(
        0.0,
        registry
            .find("streamrune.events.appended")
            .tag("event.type", MicrometerStreamRuneMetrics.UNKNOWN_TAG_VALUE)
            .counter()
            .count(),
        "the no-arg sibling series must NOT be bumped by the tagged overload (no double count)");
    assertEquals(
        1.0,
        registry
            .find("streamrune.events.appended")
            .tag("event.type", "OrderPlaced")
            .counter()
            .count());
  }
}
