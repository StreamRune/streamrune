package org.streamrune.core.metrics;

/**
 * Centralized metric names for StreamRune framework. Following Micrometer naming conventions.
 *
 * <p>This class is the <b>operator-facing contract</b>: dashboards, PromQL alerts and SLOs are
 * written against these constants. The shipped Micrometer collector ({@code
 * MicrometerStreamRuneMetrics}) derives every meter name from a constant here rather than from a
 * string literal, and a reflective test asserts each {@code streamrune.*} constant below has a
 * matching registered meter — so a rename moves both sides at once and the contract cannot silently
 * drift from what is emitted.
 *
 * <p>Constants whose value does <em>not</em> start with {@code streamrune.} are tag keys, not meter
 * names (see the "Tag Keys" section at the bottom).
 */
public final class MetricNames {

  private MetricNames() {}

  // ==================== Command Metrics ====================

  /** Total commands dispatched */
  public static final String COMMANDS_DISPATCHED = "streamrune.commands.dispatched";

  /** Commands that succeeded */
  public static final String COMMANDS_SUCCEEDED = "streamrune.commands.succeeded";

  /** Commands that failed */
  public static final String COMMANDS_FAILED = "streamrune.commands.failed";

  /** Command execution duration */
  public static final String COMMANDS_DURATION = "streamrune.commands.duration";

  /**
   * Command retry attempts (optimistic-lock conflicts, lock acquisition failures). Tagged by {@link
   * #TAG_COMMAND_TYPE}.
   */
  public static final String COMMANDS_RETRIED = "streamrune.commands.retried";

  /**
   * Commands short-circuited by an interceptor (not executed). Tagged by {@link #TAG_COMMAND_TYPE}.
   */
  public static final String COMMANDS_SHORT_CIRCUITED = "streamrune.commands.short.circuited";

  /**
   * Commands published to the dead-letter queue. Tagged by {@link #TAG_COMMAND_TYPE} (the simple
   * class name) — the same key and value convention as {@link #DLQ_EXHAUSTED}, so the two join per
   * command type: {@code exhausted / published} for one type is that command's permanent-loss rate
   * through the DLQ. (The published side carries the type tag too, so that join is possible.)
   */
  public static final String DLQ_PUBLISHED = "streamrune.dlq.published";

  /**
   * Commands that exhausted every DLQ retry and were permanently dropped/abandoned — a lost
   * business operation whose domain events were never produced. Terminal counterpart of {@link
   * #DLQ_PUBLISHED}; alert on it (the analog of {@link #OUTBOX_DELIVERY_FAILED} for the command
   * path). Tagged by {@link #TAG_COMMAND_TYPE}.
   */
  public static final String DLQ_EXHAUSTED = "streamrune.dlq.exhausted";

  /** Command dead-letter-queue rows swept by the housekeeping job */
  public static final String DLQ_SWEPT_ROWS = "streamrune.dlq.swept_rows";

  /**
   * Current command dead-letter-queue depth — entries queued for retry/inspection and not yet
   * discarded. Reported as a gauge, sampled by the {@code DeadLetterRetryRunner} each poll cycle
   * (the command-path analog of {@link #OUTBOX_PENDING}). A sustained rise means commands are
   * failing faster than they are being retried/resolved; alert on it alongside {@link
   * #DLQ_EXHAUSTED}, which fires only once an entry exhausts its whole retry ladder.
   */
  public static final String DLQ_PENDING = "streamrune.dlq.pending";

  /**
   * Consecutive poll-cycle failures of the outbox relay ({@code OutboxPoller}) — a gauge, {@code 0}
   * when the last cycle succeeded, rising while the relay is in backoff-retry (store/broker
   * unreachable). A nonzero value means the relay is DEGRADED but still running; combine with the
   * relay-liveness health check (which catches a wholly dead relay thread) for full coverage.
   */
  public static final String OUTBOX_RELAY_DEGRADED = "streamrune.outbox.relay.consecutive_failures";

  /**
   * Consecutive poll-cycle failures of the command DLQ retry runner ({@code DeadLetterRetryRunner})
   * — a gauge, {@code 0} when healthy, rising while the runner is in backoff-retry (DLQ store
   * unreachable). A nonzero value means the runner is DEGRADED but still running.
   */
  public static final String DLQ_RETRY_DEGRADED = "streamrune.dlq.retry.consecutive_failures";

  /** Time spent waiting to acquire an aggregate lock */
  public static final String LOCKS_WAIT = "streamrune.locks.wait";

  /**
   * Async command submissions refused admission because {@code
   * VirtualThreadCommandBus.Builder#maxInFlightAsyncCommands(int)}'s budget was exhausted. Refused
   * BEFORE anything is attempted — the synchronous {@code execute} path is unaffected, bounded only
   * by the caller's own threads. A sustained nonzero rate means submissions are outpacing the
   * configured budget (or the shared, commit-ordered append path downstream of it): size the budget
   * to a measured capacity profile, add capacity, or apply backpressure upstream. Counter.
   */
  public static final String COMMANDS_ASYNC_REJECTED = "streamrune.commands.async.rejected";

  /**
   * Current in-flight command count — commands admitted (synchronous {@code execute} or async
   * {@code executeAsync}) but not yet completed. Reported as a live gauge (see {@code
   * StreamRuneMetrics#registerInFlightCommandsGauge}), sourced from the same counter {@code
   * close()} drains against. Read alongside {@link #COMMANDS_ASYNC_REJECTED}: a gauge pinned near
   * the configured async budget with a rising rejected counter means the budget is the active
   * bottleneck; a low gauge with rejections means bursts are short but sharp enough to still exceed
   * it momentarily.
   */
  public static final String COMMANDS_IN_FLIGHT = "streamrune.commands.in_flight";

  // ==================== Event Metrics ====================

  /** Total events appended */
  public static final String EVENTS_APPENDED = "streamrune.events.appended";

  /**
   * Events folded into aggregate state during reconstruction — the events after the snapshot, or
   * the whole stream when no snapshot exists. Divided by {@link #COMMANDS_DISPATCHED} this is the
   * average replay depth: a rising ratio means the snapshot interval is too coarse.
   */
  public static final String EVENTS_REPLAYED = "streamrune.events.replayed";

  /**
   * Event append duration. <b>Renamed in emission:</b> the Micrometer collector used to register
   * this timer as {@code streamrune.events.append.duration} while this constant documented {@code
   * streamrune.events.duration}, so the documented series never existed and an append-latency alert
   * written against it could never fire. The constant is the contract and it won.
   *
   * <p><b>Emission:</b> the timer was registered under this name but had no recording call, so the
   * series existed at {@code count=0} and the alert remained unfirable. The command bus now samples
   * every append that carries events, on both the success and the failure path; a keyed zero-event
   * inbox claim appends nothing and is not sampled.
   */
  public static final String EVENTS_DURATION = "streamrune.events.duration";

  // ==================== Projection Metrics ====================

  /** Total events processed by projections */
  public static final String PROJECTIONS_PROCESSED = "streamrune.projections.processed";

  /** Projection processing failures */
  public static final String PROJECTIONS_FAILED = "streamrune.projections.failed";

  /** Projection processing duration */
  public static final String PROJECTIONS_DURATION = "streamrune.projections.duration";

  /**
   * Projection batches dead-lettered by the DLQ error strategy. Unlike {@link #PROJECTIONS_FAILED}
   * (a retryable processing failure), a dead-letter permanently advances the read model PAST the
   * failed range — alert on this, it is the one projection path that skips data.
   */
  public static final String PROJECTIONS_DEAD_LETTERED = "streamrune.projections.dead_lettered";

  /**
   * Current projection dead-letter backlog — the total number of un-replayed dead-letter entries
   * across all projections. Reported as a gauge, sampled once per cycle by the projection runners
   * (the projection analog of {@link #DLQ_PENDING} / {@link #OUTBOX_PENDING} / {@link
   * #SAGA_FAULTED_BACKLOG}).
   *
   * <p><b>Why this exists.</b> {@link #PROJECTIONS_DEAD_LETTERED} is a counter fired once at
   * dead-letter time, not a standing depth. Because the runner advances the checkpoint PAST every
   * dead-lettered range (permanent read-model holes) and stays health-UP by design, once a failure
   * burst ends there is no signal at all that N ranges are un-replayed. This gauge is that signal.
   * <b>Alert on any sustained nonzero value:</b> every unit is a permanent hole in a read model
   * awaiting an operator replay.
   *
   * <p>Untagged fleet-wide depth (like {@link #DLQ_PENDING}): the runners share one dead-letter
   * store and sample its total. Sampling degrades gracefully — a {@code ProjectionDeadLetterStore}
   * that does not implement {@code countPending()} disables the gauge instead of reporting a false
   * 0.
   */
  public static final String PROJECTIONS_DEAD_LETTER_BACKLOG =
      "streamrune.projections.dead_letter_backlog";

  /**
   * One sample per started projection registration (INLINE included), value {@code 1}, tagged
   * {@link #TAG_PROJECTION_NAME} and {@link #TAG_DELIVERY_MODE}: what each projection promises.
   * Alert when a projection you consider authoritative reports a mode other than {@code
   * TRANSACTIONAL_LOCAL}.
   */
  public static final String PROJECTIONS_DELIVERY_MODE = "streamrune.projections.delivery_mode";

  // ==================== Subscription Metrics ====================

  /** Events received by subscriptions */
  public static final String SUBSCRIPTIONS_EVENTS_RECEIVED =
      "streamrune.subscriptions.events.received";

  /** Subscription delivery latency */
  public static final String SUBSCRIPTIONS_DELIVERY_LATENCY =
      "streamrune.subscriptions.delivery.latency";

  /** Subscription processing lag (events between the global head and the last committed offset) */
  public static final String SUBSCRIPTIONS_LAG = "streamrune.subscriptions.lag";

  /**
   * LISTEN/NOTIFY listener reconnect attempts (each fired when the push-notification connection is
   * lost and the listener reconnects). A rising count signals a flapping push path even while
   * polling keeps delivering; a listener that has permanently died stops incrementing it.
   */
  public static final String SUBSCRIPTIONS_LISTENER_RECONNECTS =
      "streamrune.subscriptions.listener.reconnects";

  // ==================== Snapshot Metrics ====================

  /** Snapshots created */
  public static final String SNAPSHOTS_CREATED = "streamrune.snapshots.created";

  /** Snapshots loaded */
  public static final String SNAPSHOTS_LOADED = "streamrune.snapshots.loaded";

  /**
   * Stored snapshots discarded on load. The aggregate is rebuilt by replaying all events from the
   * beginning instead of wedging. Tagged by {@link #TAG_REASON}: {@code deserialize_failure} (the
   * payload could not be deserialized — a plain Jackson/IO failure, e.g. an aggregate-state field
   * renamed/removed without a snapshot-version bump) or {@code migration_failure} (a registered
   * {@code SnapshotMigration} step threw). Expected near-zero; a nonzero rate points at a
   * snapshot-schema evolution mistake. A crypto fail-closed decrypt failure is never counted here
   * (it propagates).
   */
  public static final String SNAPSHOTS_DISCARDED = "streamrune.snapshots.discarded";

  // ==================== Crypto Metrics ====================

  /**
   * A decrypt fell back to the {@code [REDACTED]} tombstone because the subject's key was absent
   * ({@code KeyNotFoundException}). Expected at a low, steady rate for legitimately crypto-shredded
   * subjects. A <b>spike</b> is the alertable signal that a whole key store is unavailable/empty (a
   * restored DB without the key rows, a truncated key table, a wrong datasource) — every
   * {@code @Encrypted} field would then be silently redacted into read models, indistinguishable
   * from lawful erasure without this metric. Alert on the rate.
   */
  public static final String CRYPTO_SUBJECT_REDACTED = "streamrune.crypto.subject_redacted";

  /**
   * A <b>systemic</b> key-store failure was detected: many distinct subjects decrypted to {@code
   * [REDACTED]} with no successful decrypt in between — the signature of a wiped/misconfigured key
   * store (a restored DB without key rows, a truncated key table, a wrong datasource), NOT lawful
   * per-subject GDPR erasure (which leaves every other subject decryptable). Emitted once per
   * detected outage episode alongside an {@code ERROR} log. This is the crisp "key store
   * unavailable" alarm, cleanly separated from the steady {@link #CRYPTO_SUBJECT_REDACTED} trickle
   * of legitimate erasures — page on any nonzero value.
   */
  public static final String CRYPTO_KEYSTORE_SYSTEMIC_FAILURE =
      "streamrune.crypto.keystore_systemic_failure";

  /**
   * A GDPR right-to-erasure read-model purge FAILED after the subject's key was already
   * crypto-shredded — unencrypted derived PII may remain in that read model (an Article-17 gap the
   * caller must not report as fully erased). Terminal and alert-worthy: page on any nonzero value.
   * Tagged by {@link #TAG_PURGER}.
   */
  public static final String GDPR_PURGE_FAILED = "streamrune.gdpr.purge_failed";

  // ==================== Query Metrics ====================

  /** Queries dispatched on the query bus */
  public static final String QUERIES_DISPATCHED = "streamrune.queries.dispatched";

  /** Query handling duration */
  public static final String QUERIES_DURATION = "streamrune.queries.duration";

  /** Query results served from the query cache */
  public static final String QUERIES_CACHE_HITS = "streamrune.queries.cache.hits";

  /** Query cache misses (result computed by the handler) */
  public static final String QUERIES_CACHE_MISSES = "streamrune.queries.cache.misses";

  // ==================== Saga Metrics ====================

  /**
   * Saga dead-letter entries written by the saga runner — a poison event quarantined, or a
   * correlated event held behind a blocker. One sample per write, so a still-poison replay that
   * refreshes an existing entry counts again. {@link #SAGA_SKIPPED_WHILE_FAULTED} and {@link
   * #SAGA_EVENT_HELD} are strict subsets. Tagged by {@link #TAG_SAGA_TYPE}.
   */
  public static final String SAGA_QUARANTINED = "streamrune.saga.quarantined";

  /**
   * Sagas that transitioned into {@code FAULTED} — one sample per transition, from any path: step
   * execution, the saga runner's poison handler, the compensation-retry sweeper giving up, or the
   * timeout runner. Tagged by {@link #TAG_SAGA_TYPE}.
   */
  public static final String SAGA_FAULTED = "streamrune.saga.faulted";

  /**
   * Correlated events delivered to a saga that was already {@code FAULTED} and therefore NOT
   * processed. Each one is recorded in the saga dead-letter store so the operator's existing replay
   * workflow can re-deliver it — a live delivery to a FAULTED saga is otherwise lost forever,
   * because the batch completes and the subscription checkpoint advances past it.
   *
   * <p><b>A strict subset of {@link #SAGA_QUARANTINED}</b>, which counts every dead-letter entry
   * written regardless of cause. Subtract this series AND {@link #SAGA_EVENT_HELD} (the other
   * holds, equally not defects) from it to get the genuine poison quarantines — the ones that
   * indicate a NEW orchestrator defect — with one cause that is not a defect and that the entry's
   * error type identifies: an event-path refusal to resume a compensation episode older than the
   * command-inbox retention window ({@code
   * org.streamrune.runtime.SagaStaleCompensationEpisodeException}), which is recorded through the
   * same poison handler. This one instead measures the <em>blast radius</em> of a fault that is
   * already known: a rising rate means business events are piling up behind a halted saga, so it
   * tells you how urgent the pending replay is, not that there is a new bug. Tagged by {@link
   * #TAG_SAGA_TYPE}.
   *
   * <p>Replayed events are never counted: a replay feed goes straight to the saga executor and
   * never passes the live FAULTED hold — the dead-letter entry being replayed is already that
   * event's durable record.
   */
  public static final String SAGA_SKIPPED_WHILE_FAULTED = "streamrune.saga.skipped_while_faulted";

  /**
   * Correlated events held (recorded, not processed) because their KNOWN saga cannot consume them
   * yet — tagged by {@link #TAG_SAGA_TYPE} and {@link #TAG_HOLD_REASON} ({@code GENESIS_PENDING},
   * {@code BACKLOG_PENDING}, {@code BEFORE_START}). A strict subset of {@link #SAGA_QUARANTINED}.
   * The FAULTED hold keeps its own series, {@link #SAGA_SKIPPED_WHILE_FAULTED}.
   */
  public static final String SAGA_EVENT_HELD = "streamrune.saga.event_held";

  /**
   * Dead-letter entries {@code SagaDeadLetterReplayer} deferred ({@code SAGA_ROW_PENDING}: saga row
   * absent or genesis pending) without feeding them, in a single {@code replay} or a {@code
   * replayAll} drain. Never a {@link #SAGA_REPLAYED} sample. Tagged by {@link #TAG_SAGA_TYPE}.
   */
  public static final String SAGA_REPLAY_DEFERRED = "streamrune.saga.replay_deferred";

  /** Saga compensation attempts, tagged by outcome */
  public static final String SAGA_COMPENSATION = "streamrune.saga.compensation";

  /**
   * Compensation episodes re-driven out of band by the {@code SagaCompensationRetrySweeper}. A
   * no-timeout saga whose compensation fails transiently has no timeout runner and no redelivered
   * event to re-drive it; the leadership-gated sweeper periodically re-drives such stuck {@code
   * COMPENSATING} sagas under the shared episode-scoped idempotency key until the undo durably
   * completes (or the give-up horizon terminalizes it {@code FAULTED}). A sustained nonzero rate
   * means compensations are not completing on the first attempt — alert on it alongside {@link
   * #SAGA_FAULTED}. Counts only cycles that actually re-drove the episode: a re-drive refused
   * admission wholesale (an open circuit breaker, a closing bus, an interceptor veto) dispatched
   * nothing and is not counted, so a breaker-open window does not read as a climbing retry rate.
   * Tagged by {@link #TAG_SAGA_TYPE}.
   */
  public static final String SAGA_COMPENSATION_RETRIES = "streamrune.saga.compensation_retries";

  /**
   * Optimistic-concurrency (CAS) conflicts while persisting saga state. Tagged by {@link
   * #TAG_SAGA_TYPE}.
   */
  public static final String SAGA_CAS_CONFLICTS = "streamrune.saga.cas_conflicts";

  /**
   * Current {@code COMPENSATING} saga backlog — sagas whose compensation (refund / stock release)
   * has not yet completed. Reported as a gauge, sampled each cycle by the {@code
   * SagaCompensationRetrySweeper} (the saga analog of {@link #OUTBOX_PENDING}/{@link
   * #DLQ_PENDING}). A sustained rise means compensations are not completing — a stalled/dead saga
   * driver (also caught by the driver-liveness health check) or a genuinely stuck downstream.
   * Tagged by {@link #TAG_SAGA_TYPE}.
   *
   * <p>Sampled whenever sagas are enabled and a {@code SagaRunner} is wired: {@code
   * streamrune.saga.compensation-retry-enabled=false} switches the sweeper into sampling-only mode
   * — the re-drive stops, this sample does not. This is the sole emitter of the series.
   */
  public static final String SAGA_COMPENSATING = "streamrune.saga.compensating";

  /**
   * Current timed-out saga backlog — the number of sagas the {@code SagaTimeoutRunner} picked up in
   * its most recent poll cycle: forward timeouts plus {@code COMPENSATING} re-picks, bounded by its
   * batch size, each population getting at least half the batch when both are backlogged. Reported
   * as a gauge. A sustained nonzero value means the timeout runner is not draining them (a
   * stalled/dead runner — also caught by the driver-liveness health check — timeouts arriving
   * faster than compensation completes, or re-picked episodes whose compensation keeps failing;
   * compare {@link #SAGA_COMPENSATING}). Tagged by {@link #TAG_SAGA_TYPE}.
   */
  public static final String SAGA_TIMED_OUT = "streamrune.saga.timed_out";

  /**
   * Current <b>stranded-saga backlog</b> — the number of saga dead-letter entries the retention
   * guard protects: named entries whose saga row is {@code FAULTED} or carries the {@code
   * dead_letter_pending} shield, named entries whose saga has no row, and null-saga entries the
   * replayer has resolved to a target. Reported as a gauge, sampled once per cycle by the {@code
   * SagaDeadLetterRetentionSweeper} (the saga-fault analog of {@link #OUTBOX_PENDING}/{@link
   * #DLQ_PENDING}/{@link #SAGA_COMPENSATING}).
   *
   * <p>These are exactly the entries retention <em>refuses</em> to prune: each is the only record
   * of an event its saga has not consumed, so pruning it would lose that event for good. Protecting
   * them makes the population grow silently — {@link #SAGA_FAULTED} is a counter fired once at
   * fault time, not a standing depth — so this gauge is the signal that entries await an operator
   * replay or an explicit discard. <b>Alert on any sustained nonzero value:</b> every unit is a
   * business process (an order never fulfilled) waiting on a human.
   *
   * <p>Untagged: the sweeper prunes the dead-letter table as a whole and has no saga type in scope,
   * so this is a fleet-wide depth. Sampling is skipped entirely when retention is disabled ({@code
   * maxAge <= 0}) — the sweeper never runs — and disables itself against a store that does not
   * support {@code countFaultedBacklog()}.
   */
  public static final String SAGA_FAULTED_BACKLOG = "streamrune.saga.faulted_backlog";

  /**
   * Current count of saga rows sitting in {@code FAULTED}, per saga type. Reported as a gauge,
   * sampled by the {@code SagaCompensationRetrySweeper} each cycle — whenever sagas are enabled and
   * a {@code SagaRunner} is wired, independent of {@code
   * streamrune.saga.compensation-retry-enabled} (the knob only stops the re-drive). This is the
   * sole emitter of the series.
   *
   * <p><b>Why this exists alongside {@link #SAGA_FAULTED_BACKLOG}.</b> That gauge counts
   * DEAD-LETTER ENTRIES (the ones the retention guard protects), and its scoping is deliberately
   * pinned to the retention guard so the gauge and the pruning decision can never disagree. A whole
   * population of halted sagas therefore never reaches it: the framework's own AUTOMATIC quarantine
   * paths fault a saga with <b>no dead-letter entry at all</b> — {@code
   * SagaRunner.faultStuckCompensation} (the compensation sweeper's stale-key / give-up route),
   * {@code SagaTimeoutRunner.faultStaleEpisode} and {@code faultPastDwell}, and {@code
   * SagaStepExecutor.faultEpisode} (a {@code compensate()} that threw on a resume). Those sagas are
   * halted — every runner, sweeper and timeout poller skips {@code FAULTED} — often with money
   * half-moved, and they showed up on NO standing series: {@code faulted_backlog} reports 0 because
   * there is no row to count, {@link #SAGA_COMPENSATING} reports 0 because the status moved off
   * {@code COMPENSATING}, and {@link #SAGA_FAULTED} is a counter fired once at fault time, not a
   * depth. The only trace was a WARN line.
   *
   * <p><b>Alert on any sustained nonzero value.</b> Read it together with {@code faulted_backlog}:
   * this series is the full halted population, that one is the replayable subset.
   *
   * <p>Tagged by {@link #TAG_SAGA_TYPE}. Sampling degrades exactly like {@link #SAGA_COMPENSATING}
   * — a {@code SagaStore} that does not implement {@code countByStatus} disables it permanently
   * instead of reporting a false 0.
   */
  public static final String SAGA_FAULTED_ROWS = "streamrune.saga.faulted_rows";

  /**
   * Saga dead-letter replay outcomes: one sample per {@code SagaDeadLetterReplayer} outcome — each
   * single {@code replay} call and each entry a {@code replayAll} drain attempts — tagged by {@link
   * #TAG_SAGA_TYPE} and {@link #TAG_OUTCOME} (the {@code SagaDeadLetterReplayer.ReplayOutcome}
   * name). A deferral ({@code SAGA_ROW_PENDING}) is never a sample here — it is counted under
   * {@link #SAGA_REPLAY_DEFERRED} — so {@code outcome=SAGA_ROW_PENDING} does not appear on this
   * series.
   */
  public static final String SAGA_REPLAYED = "streamrune.saga.replayed";

  /**
   * Stale compensation episodes an operator FORCED to resume: a {@code
   * SagaDeadLetterReplayer.replay(..., true)} / {@code replayAll(sagaId, true)} feed, or a {@code
   * resumeFaulted(sagaId, true)}, reached the saga executor over a compensation episode older than
   * the command-inbox retention window, and the executor honoured the operator's at-least-once
   * acknowledgement instead of refusing the resume (the event-path key-age guard). Every sample is
   * an episode whose succeeded compensations MAY have re-executed because their dedup keys were
   * already pruned — each one should match a reconciliation an operator performed. Tagged by {@link
   * #TAG_SAGA_TYPE}.
   */
  public static final String SAGA_FORCED_STALE_RESUME = "streamrune.saga.forced_stale_resume";

  /**
   * Operator resumes of a give-up-FAULTED compensation episode: one sample per {@code
   * SagaDeadLetterReplayer.resumeFaulted} call, tagged by {@link #TAG_SAGA_TYPE} and {@link
   * #TAG_OUTCOME} (the {@code SagaDeadLetterReplayer.ResumeOutcome} name). The refusals ({@code
   * SAGA_NOT_FOUND}, {@code NOT_FAULTED}, {@code FORWARD_FAULT}, {@code ENTRIES_PENDING}, {@code
   * STALE_COMPENSATION_BLOCKED}) are samples too: they changed nothing, and each one names the
   * remedy. A forced resume that overrode the executor's key-age guard is additionally counted
   * under {@link #SAGA_FORCED_STALE_RESUME}.
   */
  public static final String SAGA_RESUME_FAULTED = "streamrune.saga.resume_faulted";

  /**
   * Operator compensations of a saga a forward step faulted, once it has no dead-letter entry left
   * to drain: one sample per {@code SagaDeadLetterReplayer.compensateFaulted} call, tagged by
   * {@link #TAG_SAGA_TYPE} and {@link #TAG_OUTCOME} (the {@code
   * SagaDeadLetterReplayer.CompensateOutcome} name). The refusals ({@code SAGA_NOT_FOUND}, {@code
   * NOT_FAULTED}, {@code COMPENSATION_FAULT}, {@code ENTRIES_PENDING}) and a lost claim ({@code
   * CLAIM_LOST}) are samples too: they dispatched nothing, and each one names the remedy. Every
   * other sample is an undo the operator decided on, so each one should match a decision someone
   * took.
   */
  public static final String SAGA_COMPENSATE_FAULTED = "streamrune.saga.compensate_faulted";

  /** Saga dead-letter rows swept by the housekeeping job */
  public static final String SAGA_DEAD_LETTERS_SWEPT = "streamrune.saga.dead_letters_swept";

  // ==================== Inbox Metrics ====================

  /** Commands recognized as replays via the inbox and not re-executed */
  public static final String INBOX_REPLAY_HITS = "streamrune.inbox.replay_hits";

  /** Inbox rows swept by the housekeeping job */
  public static final String INBOX_SWEPT_ROWS = "streamrune.inbox.swept_rows";

  // ==================== Outbox Metrics ====================

  /** Outbox rows swept by the housekeeping job */
  public static final String OUTBOX_SWEPT_ROWS = "streamrune.outbox.swept_rows";

  /**
   * Current PENDING outbox backlog — events written to the outbox but not yet delivered to the
   * broker. Reported as a gauge, sampled by the {@code OutboxPoller} each poll cycle. A sustained
   * rise means the relay is not keeping up (or is stalled); alert on it alongside {@link
   * #OUTBOX_DELIVERY_FAILED}, which only fires once an entry exhausts its whole retry ladder.
   */
  public static final String OUTBOX_PENDING = "streamrune.outbox.pending";

  /**
   * Current {@code IN_PROGRESS} outbox backlog — entries a relay has CLAIMED but not yet resolved.
   * Reported as a gauge, sampled by the {@code OutboxPoller} each poll cycle alongside {@link
   * #OUTBOX_PENDING}.
   *
   * <p>Why this is a SEPARATE series rather than being folded into {@link #OUTBOX_PENDING}: since
   * delivery failures are classified by hand-off phase, an {@code IN_FLIGHT} failure deliberately
   * <em>holds</em> the claim instead of releasing the entry back to {@code PENDING}, so a sustained
   * post-hand-off outage (an LB dropping the connection after the request was sent, or a dead
   * backend behind one) parks the whole backlog in {@code IN_PROGRESS} while {@code OUTBOX_PENDING}
   * reads ~0 and {@link #OUTBOX_DELIVERY_FAILED} never fires — an outage invisible to a dashboard
   * watching only the pending gauge. Counting {@code IN_PROGRESS} into {@code OUTBOX_PENDING} would
   * silently redefine what every existing alert on that series means; this gauge adds the missing
   * signal without touching that contract.
   *
   * <p>A healthy relay shows a small, constantly-churning value (at most one claimed batch per
   * relay). Alert on it staying high across several poll intervals — that is the signature of
   * entries being redelivered only by lease-reclaim.
   */
  public static final String OUTBOX_IN_FLIGHT = "streamrune.outbox.in_flight";

  /**
   * An {@code OutboxPublisher} contract violation caught at delivery time: {@code classifyFailure}
   * returned {@code IN_FLIGHT} — "the hand-off may still resolve at the transport" — while the
   * publisher's {@code inFlightHorizon()} still reports the {@code ZERO} default, i.e. "nothing is
   * ever left in flight". The two statements contradict each other, and the contradiction is
   * dangerous: the {@code OutboxPoller.build()} lease guard skips a zero horizon, so the claim
   * lease was never validated against the transport's REAL in-flight window and may be too short to
   * prevent a reclaiming relay from re-publishing a still-in-flight entry (duplicate +
   * same-aggregate reorder).
   *
   * <p>The poller fails safe — the claim is held for the full lease exactly as for a correctly
   * declared horizon — but any nonzero rate here means a custom publisher must override {@code
   * inFlightHorizon()} with an honest bound so the lease guard can actually protect the deployment.
   * Counter, incremented once per held entry.
   */
  public static final String OUTBOX_IN_FLIGHT_HORIZON_VIOLATION =
      "streamrune.outbox.in_flight_horizon_violation";

  /** Outbox entries that reached terminal FAILED after exhausting retries */
  public static final String OUTBOX_DELIVERY_FAILED = "streamrune.outbox.delivery_failed";

  /** FAILED outbox entries reset to PENDING by the operator replayer */
  public static final String OUTBOX_REPLAYED = "streamrune.outbox.replayed";

  /**
   * Distinct NON-NULL aggregates with an unresolved {@code FAILED} outbox entry, sampled by the
   * {@code OutboxPoller} each poll cycle (gauge). On a {@code STRICT_PER_AGGREGATE} channel each
   * one is BLOCKED: none of its later entries is delivered until the head is replayed or skipped.
   * On an {@code AVAILABILITY_FIRST} channel each one has a gap; the remedy is the same. Alert when
   * > 0 for longer than one poll interval.
   */
  public static final String OUTBOX_BLOCKED_AGGREGATES = "streamrune.outbox.blocked_aggregates";

  /**
   * Age in seconds of the oldest unresolved {@code FAILED} outbox entry of ANY aggregate
   * (null-aggregate rows included), {@code 0} when none; sampled each poll cycle (gauge). <b>The
   * primary alert series</b>: it rises monotonically until that entry is replayed or skipped. The
   * age is the relay clock against the store-stamped {@code processed_at}; a skew of seconds is
   * irrelevant to a threshold of minutes.
   */
  public static final String OUTBOX_BLOCKAGE_AGE_SECONDS = "streamrune.outbox.blockage_age_seconds";

  /**
   * Operator skips ({@code OutboxFailedReplayer.skip} / {@code skipFailed} → {@code SKIPPED}), one
   * increment per row. A rising rate means messages are being dropped routinely — review the mapper
   * or the consumer, not the operator.
   */
  public static final String OUTBOX_SKIPPED = "streamrune.outbox.skipped";

  /**
   * {@code SKIPPED} outbox rows pruned by the retention job. {@code FAILED} rows are never swept.
   */
  public static final String OUTBOX_SKIPPED_SWEPT = "streamrune.outbox.skipped_swept";

  // ==================== Tag Keys ====================

  // Tag values must be bounded (class names, configured names). Unbounded values such as
  // aggregate ids create one time series per value and will blow up the metrics backend.

  public static final String TAG_COMMAND_TYPE = "command.type";
  public static final String TAG_EVENT_TYPE = "event.type";

  /**
   * Stream id = aggregate id, which is unbounded cardinality. Never use this as a metric tag; it
   * exists for exemplar/log correlation only.
   */
  public static final String TAG_STREAM_ID = "stream.id";

  public static final String TAG_PROJECTION_NAME = "projection.name";

  /** The declared {@code ProjectionDeliveryMode} of a projection registration. */
  public static final String TAG_DELIVERY_MODE = "delivery.mode";

  public static final String TAG_SUBSCRIPTION_NAME = "subscription.name";
  public static final String TAG_QUERY_TYPE = "query.type";

  /**
   * Bounded: the saga type, {@link org.streamrune.core.types.SagaType#value()} — the saga state's
   * fully-qualified class name. Not the simple class name: two saga-state classes sharing one
   * across packages are two saga types, and a simple-name tag merged their series.
   */
  public static final String TAG_SAGA_TYPE = "saga.type";

  /**
   * Bounded: the hold reason ({@code GENESIS_PENDING}, {@code BACKLOG_PENDING}, {@code
   * BEFORE_START}).
   */
  public static final String TAG_HOLD_REASON = "hold.reason";

  /** Bounded: enum name. */
  public static final String TAG_OUTCOME = "outcome";

  /** Bounded: a short, low-cardinality reason code (e.g. {@code "deserialize_failure"}). */
  public static final String TAG_REASON = "reason";

  /** Bounded: a registered {@code SubjectDataPurger} name. */
  public static final String TAG_PURGER = "purger";

  /**
   * Bounded: the LISTEN/NOTIFY channel name a listener reconnected to (one per configured channel),
   * on {@link #SUBSCRIPTIONS_LISTENER_RECONNECTS}.
   */
  public static final String TAG_CHANNEL = "channel";
}
