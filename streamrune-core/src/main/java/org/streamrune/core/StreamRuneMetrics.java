package org.streamrune.core;

import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;

/**
 * Metrics collector for the StreamRune framework. Implementations record counters and timers for
 * commands, events, and projections. Metric and tag names are centralized in {@link
 * org.streamrune.core.metrics.MetricNames}.
 *
 * <p>Tagged overloads (e.g. {@link #recordCommandSucceeded(String)}) carry a bounded dimension such
 * as the command's simple class name. Their defaults delegate to the untagged variant, so an
 * implementation that overrides only the untagged methods still records every sample;
 * implementations that want per-type breakdowns override the tagged variants. Tag values must be
 * bounded — never pass unbounded values such as aggregate ids.
 *
 * <p>The default no-op implementation does nothing. Use {@code
 * org.streamrune.integration.MicrometerStreamRuneMetrics} for Micrometer-based metrics.
 */
public interface StreamRuneMetrics {

  StreamRuneMetrics NOOP = new StreamRuneMetrics() {};

  /** Records a command dispatch. */
  default void recordCommandDispatched() {}

  /**
   * Records a command dispatch for the given command type.
   *
   * @param commandType simple class name of the command
   */
  default void recordCommandDispatched(String commandType) {
    recordCommandDispatched();
  }

  /** Records a successful command execution. */
  default void recordCommandSucceeded() {}

  /**
   * Records a successful command execution for the given command type.
   *
   * @param commandType simple class name of the command
   */
  default void recordCommandSucceeded(String commandType) {
    recordCommandSucceeded();
  }

  /** Records a failed command execution. */
  default void recordCommandFailed() {}

  /**
   * Records a failed command execution for the given command type.
   *
   * @param commandType simple class name of the command
   */
  default void recordCommandFailed(String commandType) {
    recordCommandFailed();
  }

  /**
   * Records command execution duration.
   *
   * @param durationNanos duration in nanoseconds
   */
  default void recordCommandDuration(long durationNanos) {}

  /**
   * Records command execution duration for the given command type.
   *
   * @param commandType simple class name of the command
   * @param durationNanos duration in nanoseconds
   */
  default void recordCommandDuration(String commandType, long durationNanos) {
    recordCommandDuration(durationNanos);
  }

  /**
   * Records a command retry attempt (optimistic-lock conflict or lock acquisition failure).
   *
   * @param commandType simple class name of the command
   */
  default void recordCommandRetried(String commandType) {}

  /**
   * Records a command short-circuited by an interceptor (e.g. authorization denial, open circuit
   * breaker). The command was not executed.
   *
   * @param commandType simple class name of the command
   */
  default void recordCommandShortCircuited(String commandType) {}

  /**
   * Records an async command submission refused admission because the bus's in-flight async budget
   * was exhausted — see {@link org.streamrune.core.metrics.MetricNames#COMMANDS_ASYNC_REJECTED}.
   * Refused before anything was attempted; the synchronous {@code execute} path never calls this.
   */
  default void recordCommandAsyncRejected() {}

  /**
   * Registers a live {@code streamrune.commands.in_flight} gauge whose value is computed by {@code
   * inFlightSupplier} on every metrics scrape — mirrors {@link
   * #registerSubscriptionLagGauge(SubscriptionName, java.util.function.LongSupplier)}'s rationale:
   * a live supplier recomputes on every scrape instead of only when something happens to push a
   * value, so the metrics endpoint always reads the current in-flight count. Called once per {@link
   * AsyncCommandBus} instance at construction. The default is a no-op, so a metrics implementation
   * that does not want the gauge need not override it.
   *
   * @param inFlightSupplier supplies the current in-flight command count (never negative) on demand
   */
  default void registerInFlightCommandsGauge(java.util.function.LongSupplier inFlightSupplier) {}

  /**
   * Records a command published to the dead-letter queue after retries were exhausted.
   *
   * @param commandType simple class name of the command
   */
  default void recordDeadLetterPublished(String commandType) {}

  /**
   * Records a command that exhausted every DLQ retry and was permanently dropped/abandoned — a lost
   * business operation whose domain events were never produced. Emitted once when a dead-letter
   * entry crosses {@code maxRetries}, regardless of the discard-on-exhaustion policy. This is the
   * terminal, alert-worthy signal for the command path (the analog of {@link
   * #recordOutboxDeliveryFailed()} for the outbox). See {@link
   * org.streamrune.core.metrics.MetricNames#DLQ_EXHAUSTED}.
   *
   * @param commandType simple class name of the command
   */
  default void recordDeadLetterExhausted(String commandType) {}

  /**
   * Records time spent waiting to acquire an aggregate lock.
   *
   * @param durationNanos wait duration in nanoseconds
   */
  default void recordLockWait(long durationNanos) {}

  /** Records an event appended to the store. */
  default void recordEventAppended() {}

  /**
   * Records an event appended to the store for the given event type.
   *
   * @param eventType simple class name of the event
   */
  default void recordEventAppended(String eventType) {
    recordEventAppended();
  }

  /**
   * Records one event folded into aggregate state during reconstruction — the events after the
   * snapshot, or the whole stream when no snapshot exists. Emitted per event by the command bus's
   * {@code reconstructState}, so {@code events.replayed / commands.dispatched} is the average
   * replay depth an operator uses to judge whether a snapshot interval is doing its job. See {@link
   * org.streamrune.core.metrics.MetricNames#EVENTS_REPLAYED}.
   */
  default void recordEventReplayed() {}

  /**
   * Records the duration of one event-store append that carried events — the round trip a p99
   * append-latency alert is written against. Emitted by the command bus around {@code
   * EventStore.append}/{@code appendWithKey}, on both the success and the failure path (a slow
   * failing append is exactly the degradation such an alert must catch). A keyed ZERO-event append
   * writes only the inbox claim and is not sampled. See {@link
   * org.streamrune.core.metrics.MetricNames#EVENTS_DURATION}.
   *
   * @param durationNanos duration in nanoseconds
   */
  default void recordEventAppendDuration(long durationNanos) {}

  /**
   * Records a snapshot written to the event store. Emitted by the command bus once {@code
   * SnapshotPolicy.EveryNEvents} fires and the store accepted the write — the series to watch when
   * verifying that snapshotting is actually happening. See {@link
   * org.streamrune.core.metrics.MetricNames#SNAPSHOTS_CREATED}.
   */
  default void recordSnapshotCreated() {}

  /**
   * Records a stored snapshot that was successfully used to rehydrate an aggregate (the load
   * returned a non-null snapshot state). Together with {@link #recordSnapshotCreated()} this is the
   * pair the production guide points operators at; a created-but-never-loaded snapshot means the
   * expected snapshot version and the stored one disagree. See {@link
   * org.streamrune.core.metrics.MetricNames#SNAPSHOTS_LOADED}.
   */
  default void recordSnapshotLoaded() {}

  /**
   * Records a stored snapshot that was DISCARDED on load, so the aggregate was rebuilt by replaying
   * its events (the source of truth) from the beginning instead of permanently failing to load. Two
   * causes, told apart by {@code reason}: {@code "deserialize_failure"} — the payload could not be
   * deserialized (a plain Jackson/IO failure, typically an aggregate-state field renamed or removed
   * without bumping the snapshot version) — and {@code "migration_failure"} — a registered {@code
   * SnapshotMigration} step threw. This is expected to be rare and near-zero; a nonzero rate flags
   * a snapshot-schema evolution mistake worth fixing (a missed {@code snapshotVersion} bump, or a
   * broken migration step). A crypto fail-closed decrypt failure is NOT counted here — it still
   * propagates. See {@link org.streamrune.core.metrics.MetricNames#SNAPSHOTS_DISCARDED}.
   *
   * @param reason a bounded, low-cardinality reason tag ({@code "deserialize_failure"} or {@code
   *     "migration_failure"})
   */
  default void recordSnapshotDiscarded(String reason) {}

  /**
   * Records a decrypt that fell back to the {@code [REDACTED]} tombstone because the subject's key
   * was absent (a legitimate crypto-shred OR a systemic key-store failure). Emitted once per
   * redacted field on the event-replay/projection decrypt path. A spike in this counter is the
   * alert-worthy signal that a key store is unavailable/empty — otherwise a systemic redaction is
   * silent and indistinguishable from lawful GDPR erasure. See {@link
   * org.streamrune.core.metrics.MetricNames#CRYPTO_SUBJECT_REDACTED}.
   */
  default void recordSubjectRedacted() {}

  /**
   * Records a detected <b>systemic</b> key-store failure: many distinct subjects redacted to {@code
   * [REDACTED]} with no successful decrypt in between — a wiped/misconfigured key store rather than
   * lawful per-subject erasure. Emitted once per outage episode. This is the crisp, page-worthy
   * "key store unavailable" alarm, distinct from the steady {@link #recordSubjectRedacted()}
   * trickle of legitimate forgets. See {@link
   * org.streamrune.core.metrics.MetricNames#CRYPTO_KEYSTORE_SYSTEMIC_FAILURE}.
   */
  default void recordSystemicKeyStoreFailure() {}

  /**
   * Records a GDPR right-to-erasure read-model purge that FAILED after the subject's key was
   * already crypto-shredded (a lock timeout, a renamed table, a revoked grant). The key deletion is
   * irreversible, so the forget cannot be rolled back — but the failed purge leaves unencrypted
   * derived PII behind in that read model, an Article-17 gap that needs remediation. This is the
   * terminal, alert-worthy signal for the erasure path: any nonzero value means a data subject was
   * told "erased" while plaintext PII may still be queryable. See {@link
   * org.streamrune.core.metrics.MetricNames#GDPR_PURGE_FAILED}.
   *
   * @param purgerName the failing purger's bounded, low-cardinality name
   */
  default void recordGdprPurgeFailed(String purgerName) {}

  /** Records a projection event processed. */
  default void recordProjectionProcessed() {}

  /**
   * Records a projection event processed for the given projection.
   *
   * @param projectionName the projection's registered name
   */
  default void recordProjectionProcessed(ProjectionName projectionName) {
    recordProjectionProcessed();
  }

  /** Records a projection failure. */
  default void recordProjectionFailed() {}

  /**
   * Records a projection failure for the given projection.
   *
   * @param projectionName the projection's registered name
   */
  default void recordProjectionFailed(ProjectionName projectionName) {
    recordProjectionFailed();
  }

  /** Records a projection batch dead-lettered by the DLQ error strategy. */
  default void recordProjectionDeadLettered() {}

  /**
   * Records a projection batch dead-lettered by the DLQ error strategy for the given projection —
   * the failed range is permanently skipped past in the read model. See {@link
   * org.streamrune.core.metrics.MetricNames#PROJECTIONS_DEAD_LETTERED}.
   *
   * @param projectionName the projection's registered name
   */
  default void recordProjectionDeadLettered(ProjectionName projectionName) {
    recordProjectionDeadLettered();
  }

  /**
   * Records projection processing duration.
   *
   * @param durationNanos duration in nanoseconds
   */
  default void recordProjectionDuration(long durationNanos) {}

  /**
   * Records projection processing duration for the given projection.
   *
   * @param projectionName the projection's registered name
   * @param durationNanos duration in nanoseconds
   */
  default void recordProjectionDuration(ProjectionName projectionName, long durationNanos) {
    recordProjectionDuration(durationNanos);
  }

  /**
   * Records the current projection dead-letter backlog — the total number of un-replayed
   * dead-letter entries across all projections. Reported as the latest sampled value (the
   * projection runners sample it once per cycle), so implementations should expose it as a gauge
   * rather than a counter. The projection analog of {@link #recordDlqBacklog(long)} and {@link
   * #recordSagaFaultedBacklog(long)}. See {@link
   * org.streamrune.core.metrics.MetricNames#PROJECTIONS_DEAD_LETTER_BACKLOG} for why this is the
   * alertable signal that a read model carries permanent holes awaiting an operator replay.
   *
   * @param count the number of dead-letter entries across all projections (never negative)
   */
  default void recordProjectionDeadLetterBacklog(long count) {}

  /**
   * Records the delivery mode a projection registration declared, once at start (INLINE included).
   * Implementations expose it as a gauge of value {@code 1} tagged with the projection name and the
   * mode. See {@link org.streamrune.core.metrics.MetricNames#PROJECTIONS_DELIVERY_MODE}.
   *
   * @param projectionName the projection's registered name
   * @param mode the declared delivery mode
   */
  default void recordProjectionDeliveryMode(
      ProjectionName projectionName, ProjectionDeliveryMode mode) {}

  /** Records a subscription event received. */
  default void recordSubscriptionEventReceived() {}

  /**
   * Records a subscription event received for the given subscription.
   *
   * @param subscriptionName the subscription's registered name
   */
  default void recordSubscriptionEventReceived(SubscriptionName subscriptionName) {
    recordSubscriptionEventReceived();
  }

  /**
   * Records subscription event delivery latency.
   *
   * @param durationNanos latency in nanoseconds
   */
  default void recordSubscriptionDeliveryLatency(long durationNanos) {}

  /**
   * Records subscription event delivery latency for the given subscription.
   *
   * @param subscriptionName the subscription's registered name
   * @param durationNanos latency in nanoseconds
   */
  default void recordSubscriptionDeliveryLatency(
      SubscriptionName subscriptionName, long durationNanos) {
    recordSubscriptionDeliveryLatency(durationNanos);
  }

  /**
   * Records the current processing lag of a subscription — the number of events between the global
   * stream head and the subscription's last committed offset. Reported as the latest sampled value,
   * so implementations should expose it as a gauge rather than a counter.
   *
   * <p><b>Prefer {@link #registerSubscriptionLagGauge(SubscriptionName,
   * java.util.function.LongSupplier)}.</b> This push variant only refreshes the gauge when a caller
   * happens to sample lag (historically only a health-endpoint scrape), so a metrics scraper
   * polling between samples sees a stale value. A live supplier gauge recomputes on every scrape
   * and is the primary path.
   *
   * @param subscriptionName the subscription's registered name
   * @param lag the number of unprocessed events (never negative)
   */
  default void recordSubscriptionLag(SubscriptionName subscriptionName, long lag) {}

  /**
   * Registers a live {@code streamrune.subscriptions.lag} gauge for {@code subscriptionName} whose
   * value is computed by {@code lagSupplier} on every metrics scrape — decoupling the lag meter
   * from health-endpoint scrapes so a Prometheus scrape of the metrics endpoint always reads
   * current lag. Registered once per name when the subscription is registered with the health
   * contributor; a second call for the same name is a no-op. The default is a no-op, so a metrics
   * implementation that only wants the push variant need not override it.
   *
   * @param subscriptionName the subscription's registered name
   * @param lagSupplier supplies the current lag (never negative) on demand
   */
  default void registerSubscriptionLagGauge(
      SubscriptionName subscriptionName, java.util.function.LongSupplier lagSupplier) {}

  /**
   * Records a LISTEN/NOTIFY listener reconnect. Incremented each time the push-notification
   * connection is lost and the listener re-establishes (or attempts to). See {@link
   * org.streamrune.core.metrics.MetricNames#SUBSCRIPTIONS_LISTENER_RECONNECTS}.
   *
   * @param channel the notification channel the listener reconnects on
   */
  default void recordListenerReconnect(String channel) {}

  /**
   * Records a query dispatched on the query bus.
   *
   * @param queryType simple class name of the query
   */
  default void recordQueryDispatched(String queryType) {}

  /**
   * Records query handling duration.
   *
   * @param queryType simple class name of the query
   * @param durationNanos duration in nanoseconds
   */
  default void recordQueryDuration(String queryType, long durationNanos) {}

  /**
   * Records a query result served from the query cache.
   *
   * @param queryType simple class name of the query
   */
  default void recordQueryCacheHit(String queryType) {}

  /**
   * Records a query cache miss (result was computed by the handler).
   *
   * @param queryType simple class name of the query
   */
  default void recordQueryCacheMiss(String queryType) {}

  /**
   * Records one saga dead-letter entry written by the saga runner (a poison quarantine or a hold).
   * See {@link org.streamrune.core.metrics.MetricNames#SAGA_QUARANTINED}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   */
  default void recordSagaQuarantined(String sagaType) {}

  /**
   * Records a saga's transition into {@code FAULTED}, from any path: step execution, the runner's
   * poison handler, the compensation-retry sweeper giving up, or the timeout runner. See {@link
   * org.streamrune.core.metrics.MetricNames#SAGA_FAULTED}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   */
  default void recordSagaFaulted(String sagaType) {}

  /**
   * Records a correlated event that was delivered to an already-{@code FAULTED} saga and therefore
   * NOT processed. The event is written to the saga dead-letter store so the operator's existing
   * replay workflow can re-deliver it; without that record a live delivery to a FAULTED saga is
   * lost forever, because the batch completes and the subscription checkpoint advances past it.
   *
   * <p>A strict subset of {@link #recordSagaQuarantined(String)} — the entry genuinely is a
   * quarantine, so both fire — as is {@link #recordSagaEventHeld(String, String)}, which is what
   * makes {@code quarantined - skipped_while_faulted - event_held} the count of genuine poison
   * quarantines (a NEW defect; apart from event-path {@code SagaStaleCompensationEpisodeException}
   * refusals — see {@link org.streamrune.core.metrics.MetricNames#SAGA_SKIPPED_WHILE_FAULTED})
   * while this series measures the blast radius of a fault that is already known. See {@link
   * org.streamrune.core.metrics.MetricNames#SAGA_SKIPPED_WHILE_FAULTED}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   */
  default void recordSagaSkippedWhileFaulted(String sagaType) {}

  /**
   * Records a correlated event that was delivered to a KNOWN saga that cannot consume it yet and
   * was HELD — recorded in the saga dead-letter store instead of processed. {@code reason} is one
   * of {@code GENESIS_PENDING} (the start event's dispatch loop has not committed), {@code
   * BACKLOG_PENDING} (the saga owns older quarantined entries; consuming a newer live event first
   * would reorder its history) or {@code BEFORE_START} (no row, but the saga owns a dead-letter
   * record — its start could not even produce an initial state). A strict subset of {@link
   * #recordSagaQuarantined(String)} and a blast-radius signal, not a new-defect signal; {@code
   * SagaDeadLetterReplayer.replayAll} feeds held events in order once the blocker is gone. See
   * {@link org.streamrune.core.metrics.MetricNames#SAGA_EVENT_HELD}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   * @param reason the hold reason name
   */
  default void recordSagaEventHeld(String sagaType, String reason) {}

  /**
   * Records a dead-letter entry {@code SagaDeadLetterReplayer} DEFERRED without feeding it (its
   * saga row is absent or its genesis has not been applied yet — a drain feeds it after the start
   * entry lands). Deferrals are not replay attempts, so they are counted here and never under
   * {@link #recordSagaReplayed}. See {@link
   * org.streamrune.core.metrics.MetricNames#SAGA_REPLAY_DEFERRED}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   */
  default void recordSagaReplayDeferred(String sagaType) {}

  /**
   * Records a saga compensation attempt. See {@link
   * org.streamrune.core.metrics.MetricNames#SAGA_COMPENSATION}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   * @param outcome the compensation outcome enum name
   */
  default void recordSagaCompensation(String sagaType, String outcome) {}

  /**
   * Records a compensation episode re-driven out of band by the {@code
   * SagaCompensationRetrySweeper} — the automatic, durable recovery path for a {@code COMPENSATING}
   * saga with no timeout runner and no redelivered event to re-drive it. Emitted once per sweeper
   * re-drive that was attempted — a re-drive whose every compensation command was refused admission
   * is not counted; the resulting compensation outcome is still recorded via {@link
   * #recordSagaCompensation(String, String)}. See {@link
   * org.streamrune.core.metrics.MetricNames#SAGA_COMPENSATION_RETRIES}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   */
  default void recordSagaCompensationRetry(String sagaType) {}

  /**
   * Records an optimistic-concurrency (CAS) conflict while persisting saga state. See {@link
   * org.streamrune.core.metrics.MetricNames#SAGA_CAS_CONFLICTS}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   */
  default void recordSagaCasConflict(String sagaType) {}

  /**
   * Records the current {@code COMPENSATING} backlog for a saga type — sagas whose compensation has
   * not yet completed. Reported as the latest sampled value (the {@code
   * SagaCompensationRetrySweeper} samples it each cycle), so implementations should expose it as a
   * gauge rather than a counter. See {@link
   * org.streamrune.core.metrics.MetricNames#SAGA_COMPENSATING}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   * @param count the number of {@code COMPENSATING} sagas of this type (never negative)
   */
  default void recordSagaCompensatingBacklog(String sagaType, long count) {}

  /**
   * Records the current timed-out backlog for a saga type — the sagas picked up in the most recent
   * {@code SagaTimeoutRunner} poll, forward timeouts plus {@code COMPENSATING} re-picks (bounded by
   * its batch size). Reported as the latest sampled value, so implementations should expose it as a
   * gauge rather than a counter. See {@link
   * org.streamrune.core.metrics.MetricNames#SAGA_TIMED_OUT}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   * @param count the number of sagas picked up this cycle (never negative)
   */
  default void recordSagaTimedOutBacklog(String sagaType, long count) {}

  /**
   * Records the current <b>stranded-saga backlog</b> — the dead-letter entries the retention guard
   * protects (the population is listed on {@link
   * org.streamrune.core.metrics.MetricNames#SAGA_FAULTED_BACKLOG}). Reported as the latest sampled
   * value (the {@code SagaDeadLetterRetentionSweeper} samples it each cycle), so implementations
   * should expose it as a gauge rather than a counter. Fleet-wide, not per saga type: the sweeper
   * prunes the dead-letter table as a whole. See {@link
   * org.streamrune.core.metrics.MetricNames#SAGA_FAULTED_BACKLOG} for why this is the alertable
   * signal that a business process is halted awaiting an operator replay.
   *
   * @param count the number of dead-letter entries the retention guard protects (never negative)
   */
  default void recordSagaFaultedBacklog(long count) {}

  /**
   * Records the current number of saga rows in {@code FAULTED} for a saga type. Reported as the
   * latest sampled value (the {@code SagaCompensationRetrySweeper} samples it each cycle), so
   * implementations should expose it as a gauge rather than a counter.
   *
   * <p>Distinct from {@link #recordSagaFaultedBacklog(long)}, which counts DEAD-LETTER ENTRIES (the
   * ones the retention guard protects) and therefore cannot see the framework's automatic stale-key
   * / give-up / compensate-poison quarantines — those write {@code FAULTED} with no entry. See
   * {@link org.streamrune.core.metrics.MetricNames#SAGA_FAULTED_ROWS}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   * @param count the number of {@code FAULTED} sagas of this type (never negative)
   */
  default void recordSagaFaultedRows(String sagaType, long count) {}

  /**
   * Records the outcome of one saga dead-letter replay attempt ({@code SagaDeadLetterReplayer}). A
   * deferral is not an attempt and is recorded through {@link #recordSagaReplayDeferred} instead.
   * See {@link org.streamrune.core.metrics.MetricNames#SAGA_REPLAYED}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   * @param outcome the replay outcome enum name
   */
  default void recordSagaReplayed(String sagaType, String outcome) {}

  /**
   * Records a stale compensation episode an operator FORCED to resume: the saga executor skipped
   * its key-age guard because the replay feed or the resume carried the operator's at-least-once
   * acknowledgement ({@code SagaDeadLetterReplayer.replay(..., true)} / {@code replayAll(sagaId,
   * true)} / {@code resumeFaulted(sagaId, true)}), so compensations whose inbox dedup keys were
   * already pruned may re-execute. Emitted once per forced resume that actually overrode the guard
   * — a forced feed or resume over a fresh episode is not counted. See {@link
   * org.streamrune.core.metrics.MetricNames#SAGA_FORCED_STALE_RESUME}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   */
  default void recordSagaForcedStaleResume(String sagaType) {}

  /**
   * Records the outcome of one operator {@code SagaDeadLetterReplayer.resumeFaulted} call — the
   * resume of a compensation episode the compensation-retry sweeper or the timeout runner gave up
   * on ({@code FAULTED} with {@code pre_fault_status = COMPENSATING} and no dead-letter entry). A
   * refusal is recorded too. See {@link
   * org.streamrune.core.metrics.MetricNames#SAGA_RESUME_FAULTED}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   * @param outcome the resume outcome enum name
   */
  default void recordSagaResumeFaulted(String sagaType, String outcome) {}

  /**
   * Records the outcome of one operator {@code SagaDeadLetterReplayer.compensateFaulted} call — the
   * compensation of a saga a forward step faulted once it has no dead-letter entry left to drain
   * (its poison entry was discarded). A refusal is recorded too. See {@link
   * org.streamrune.core.metrics.MetricNames#SAGA_COMPENSATE_FAULTED}.
   *
   * @param sagaType the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga
   *     state's fully-qualified class name
   * @param outcome the compensate outcome enum name
   */
  default void recordSagaCompensateFaulted(String sagaType, String outcome) {}

  /**
   * Records a command recognized as a replay via the inbox and not re-executed. See {@link
   * org.streamrune.core.metrics.MetricNames#INBOX_REPLAY_HITS}.
   *
   * @param commandType simple class name of the command
   */
  default void recordInboxReplayHit(String commandType) {}

  /**
   * Records inbox rows swept by the housekeeping job. See {@link
   * org.streamrune.core.metrics.MetricNames#INBOX_SWEPT_ROWS}.
   *
   * @param rows number of rows swept
   */
  default void recordInboxSwept(long rows) {}

  /**
   * Records saga dead-letter rows swept by the housekeeping job. See {@link
   * org.streamrune.core.metrics.MetricNames#SAGA_DEAD_LETTERS_SWEPT}.
   *
   * @param rows number of rows swept
   */
  default void recordSagaDeadLetterSwept(long rows) {}

  /**
   * Records outbox rows swept by the housekeeping job. See {@link
   * org.streamrune.core.metrics.MetricNames#OUTBOX_SWEPT_ROWS}.
   *
   * @param rows number of rows swept
   */
  default void recordOutboxSwept(long rows) {}

  /**
   * Records an outbox entry that reached terminal {@code FAILED} after exhausting retries. See
   * {@link org.streamrune.core.metrics.MetricNames#OUTBOX_DELIVERY_FAILED}.
   */
  default void recordOutboxDeliveryFailed() {}

  /**
   * Records the current outbox backlog — the number of {@code PENDING} entries awaiting delivery to
   * the broker. Reported as the latest sampled value (the {@code OutboxPoller} samples it each poll
   * cycle), so implementations should expose it as a gauge rather than a counter. See {@link
   * org.streamrune.core.metrics.MetricNames#OUTBOX_PENDING}.
   *
   * @param pending the number of PENDING outbox entries (never negative)
   */
  default void recordOutboxBacklog(long pending) {}

  /**
   * Records the current number of {@code IN_PROGRESS} outbox entries — claimed by a relay but not
   * yet resolved. Reported as the latest sampled value (the {@code OutboxPoller} samples it each
   * poll cycle alongside the {@code PENDING} backlog), so implementations should expose it as a
   * gauge rather than a counter.
   *
   * <p>This is the only backlog signal for a sustained post-hand-off delivery outage: an {@code
   * IN_FLIGHT} failure holds the claim rather than releasing the entry back to {@code PENDING}, so
   * the backlog parks here while {@link org.streamrune.core.metrics.MetricNames#OUTBOX_PENDING}
   * reads ~0. See {@link org.streamrune.core.metrics.MetricNames#OUTBOX_IN_FLIGHT}.
   *
   * @param inFlight the number of IN_PROGRESS outbox entries (never negative)
   */
  default void recordOutboxInFlight(long inFlight) {}

  /**
   * Records an {@code OutboxPublisher} contract violation caught at delivery time: {@code
   * classifyFailure} returned {@code IN_FLIGHT} while the publisher's {@code inFlightHorizon()}
   * still reports the {@code ZERO} default — so the {@code OutboxPoller.build()} lease guard never
   * validated the claim lease against the transport's real in-flight window. The poller fails safe
   * (the claim is held for the full lease), but any nonzero rate means the publisher must override
   * {@code inFlightHorizon()} with an honest bound. Emitted once per held entry, alongside a
   * one-time ERROR log per relay. See {@link
   * org.streamrune.core.metrics.MetricNames#OUTBOX_IN_FLIGHT_HORIZON_VIOLATION}.
   */
  default void recordOutboxInFlightHorizonViolation() {}

  /**
   * Records {@code FAILED} outbox entries reset to {@code PENDING} by the operator replayer. See
   * {@link org.streamrune.core.metrics.MetricNames#OUTBOX_REPLAYED}.
   *
   * @param rows number of entries reset
   */
  default void recordOutboxReplayed(long rows) {}

  /**
   * Records the number of distinct non-null aggregates with an unresolved {@code FAILED} outbox
   * entry (gauge semantics, latest sample wins). See {@link
   * org.streamrune.core.metrics.MetricNames#OUTBOX_BLOCKED_AGGREGATES}.
   */
  default void recordOutboxBlockedAggregates(long failedAggregates) {}

  /**
   * Records the age in seconds of the oldest unresolved {@code FAILED} outbox entry, {@code 0} when
   * none (gauge semantics). See {@link
   * org.streamrune.core.metrics.MetricNames#OUTBOX_BLOCKAGE_AGE_SECONDS}.
   */
  default void recordOutboxBlockageAge(long seconds) {}

  /**
   * Records one operator skip of a {@code FAILED} outbox entry. See {@link
   * org.streamrune.core.metrics.MetricNames#OUTBOX_SKIPPED}.
   */
  default void recordOutboxSkipped() {}

  /**
   * Records {@code SKIPPED} outbox rows swept by the retention job. See {@link
   * org.streamrune.core.metrics.MetricNames#OUTBOX_SKIPPED_SWEPT}.
   */
  default void recordOutboxSkippedSwept(long rows) {}

  /**
   * Records command dead-letter-queue rows swept by the housekeeping job. See {@link
   * org.streamrune.core.metrics.MetricNames#DLQ_SWEPT_ROWS}.
   *
   * @param rows number of rows swept
   */
  default void recordDeadLetterSwept(long rows) {}

  /**
   * Records the current command dead-letter-queue depth — entries queued for retry/inspection and
   * not yet discarded. Reported as the latest sampled value (the {@code DeadLetterRetryRunner}
   * samples it each poll cycle), so implementations should expose it as a gauge rather than a
   * counter. The command-path analog of {@link #recordOutboxBacklog(long)}. See {@link
   * org.streamrune.core.metrics.MetricNames#DLQ_PENDING}.
   *
   * @param pending the number of queued dead-letter entries (never negative)
   */
  default void recordDlqBacklog(long pending) {}

  /**
   * Records the outbox relay's current consecutive poll-cycle failure count — {@code 0} when the
   * last cycle succeeded, rising while the relay is degraded (in backoff-retry). Reported as a
   * gauge sampled each poll cycle so a stalled-but-alive relay is visible on the metrics endpoint,
   * not only in the logs. See {@link
   * org.streamrune.core.metrics.MetricNames#OUTBOX_RELAY_DEGRADED}.
   *
   * @param consecutiveFailures consecutive poll-cycle failures (never negative)
   */
  default void recordOutboxRelayDegradation(long consecutiveFailures) {}

  /**
   * Records the command DLQ retry runner's current consecutive poll-cycle failure count — {@code 0}
   * when healthy, rising while the runner is degraded (in backoff-retry). Reported as a gauge
   * sampled each poll cycle. See {@link
   * org.streamrune.core.metrics.MetricNames#DLQ_RETRY_DEGRADED}.
   *
   * @param consecutiveFailures consecutive poll-cycle failures (never negative)
   */
  default void recordDlqRetryDegradation(long consecutiveFailures) {}
}
