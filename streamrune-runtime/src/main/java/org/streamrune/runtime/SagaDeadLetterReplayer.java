package org.streamrune.runtime;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.saga.SagaUnstampedCompensationEpisodeException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;

/**
 * Operator-triggered replay of quarantined saga events: a drain over RECORDED facts. Every decision
 * reads what the stores recorded — the entry's saga id, the saga row's {@code status} / {@code
 * pre_fault_status} / {@code genesis_applied} / {@code last_replayed_offset} / {@code
 * dead_letter_pending}, the entry's immutable first-replay anchor — never a live-row heuristic.
 * There is no replay marker, no un-fault write and no attribution of a live row to a prior attempt:
 * a FAULTED row is cleared only by the executor's own successful CAS ({@link SagaStore#applyEvent}
 * / {@link SagaStore#update}), and a still-poison feed re-records its fault through the runner's
 * single poison handler ({@link SagaRunner#recordPoison}).
 *
 * <p><b>Feed.</b> {@link SagaRunner#feedReplay} — the executor's live {@code ForwardStep} with
 * {@code replayRedrive = true}: a FAULTED row routes on {@link LoadedSaga#effectiveStatus()} (a
 * faulted COMPENSATING episode resumes, a faulted genesis-pending row re-evolves), an applied
 * genesis dedups a start feed, and a correlated feed at exactly the recorded {@code
 * last_replayed_offset} dedups — which is what makes every rerun converge.
 *
 * <p><b>Drain ({@link #replayAll}).</b> The saga's named entries ({@link
 * SagaDeadLetterStore#findBySaga}) and the null-saga entries durably RESOLVED to it ({@link
 * SagaDeadLetterStore#findNullSagaEntriesByResolvedTarget}) are drained in offset order; a null
 * entry at the same offset as a named one is discarded first — the named entry is the record. A
 * correlated entry ahead of the saga's genesis (row absent, or genesis not applied and not a
 * faulted compensation) is DEFERRED ({@link ReplayOutcome#SAGA_ROW_PENDING}) and fed once a start
 * entry lands in the same pass; any other non-{@code REPLAYED} outcome STOPS the drain — nothing is
 * ever fed ahead of a blocker. Entries still deferred when the drain ends — because no start entry
 * landed in it (the start entry is still poison or was discarded, the live start redelivery has not
 * completed, or the saga row was deleted), or because the drain stopped first — stay quarantined,
 * with a WARN naming their count; a later {@code replayAll} retries them and {@link #discard}
 * removes them. The shield ({@code dead_letter_pending}) is cleared only by the store's own
 * conditional clear ({@link SagaDeadLetterStore#clearShieldIfDrained}: no named and no resolved
 * entry left, serialized against the runner's hold); this replayer never observes-then-writes, and
 * a store that cannot answer never has its shield cleared.
 *
 * <p><b>Single replay ({@link #replay}).</b> The same drain step for one entry, refused with {@link
 * ReplayOutcome#OLDER_ENTRY_PENDING} when an older entry (named or resolved) of the saga is
 * pending. A null-saga entry resolves its target through the runner's routers: the resolution is
 * stamped ({@link SagaDeadLetterStore#setResolvedTarget}) and the target shielded BEFORE any
 * refusal, anchor or feed, so a refused or crash-interrupted entry keeps its retention shield; a
 * target that is FAULTED, genesis-pending or has an older entry pending answers {@link
 * ReplayOutcome#TARGET_PENDING} — drain the target with {@link #replayAll}, the entry folds in at
 * its causal position. A named entry is always fed to its RECORDED saga: the router only classifies
 * start vs correlated.
 *
 * <p><b>{@code force}</b> overrides only the two key-age refusals ({@link
 * ReplayOutcome#STALE_REDRIVE_BLOCKED}, {@link ReplayOutcome#STALE_COMPENSATION_BLOCKED}); it never
 * overrides ordering ({@code OLDER_ENTRY_PENDING}, {@code SAGA_ROW_PENDING}). It is the operator's
 * explicit, per-saga acknowledgement of at-least-once, and it is passed through the feed ({@link
 * SagaRunner#feedReplay}) to the saga executor, whose own event-path key-age guard honours it: a
 * forced resume of a stale compensation episode re-dispatches the episode's compensation commands
 * under their original keys, so any whose inbox dedup key was already pruned executes again — the
 * executor WARNs naming the saga and records {@link StreamRuneMetrics#recordSagaForcedStaleResume}.
 * The re-drive anchor ({@link SagaDeadLetterEntry#firstReplayStartedAt()}) is established
 * immediately before the first feed of an entry and never moves. A crash inside a forced resume,
 * after some compensation commands executed and before the terminal write, leaves the row FAULTED
 * over the same episode (the refusal re-arms for a plain rerun); a forced rerun re-dispatches under
 * the same keys, so the commands the crashed attempt committed dedup and only the rest run.
 *
 * <p><b>Stops.</b> {@code EVENT_NOT_FOUND}, {@code ENTRY_NOT_FOUND} and the {@code STALE_*}
 * refusals leave the row unchanged; the shield holds live correlated events while entries remain;
 * {@link #discard} is the remedy for an unreadable event (the {@code STALE_*} refusals take {@code
 * force} after reconciliation) and clears the shield once the backlog is empty. A {@code
 * STILL_POISON} feed leaves the row FAULTED by its own fresh fault; a resume that made no durable
 * progress — the row is still FAULTED after the feed — is classified the same way so the next drain
 * retries. A row that faulted mid-compensation WITHOUT an episode stamp — a saga-store contract
 * violation: every write that enters {@code COMPENSATING} stamps it — is refused by throwing {@link
 * SagaUnstampedCompensationEpisodeException} before anything is anchored or fed, whatever {@code
 * force} says (a null-saga entry keeps the resolved target and the shield it was given on the way
 * in, like every other refusal); a drain stops there. Every durable write here is a single
 * statement, and a crash between any two of them converges on the rerun.
 *
 * <p><b>Give-up faults ({@link #resumeFaulted}).</b> The compensation-retry sweeper and the timeout
 * runner fault a compensation episode they give up on WITHOUT a dead-letter entry (they have no
 * triggering event), so the drain has nothing to feed. {@link #resumeFaulted(SagaId, boolean)} is
 * the explicit operator operation for exactly that shape — {@code FAULTED}, {@code pre_fault_status
 * = COMPENSATING}, no pending entry — and refuses every other one with a specific outcome. It
 * resumes the episode through the executor's own compensation resume (no fabricated entry, no
 * un-fault write: the terminal CAS clears the fault), under the same staleness rule and the same
 * {@code force} pass-through as the drain.
 *
 * <p><b>Forward faults with nothing left to drain ({@link #compensateFaulted}).</b> A forward fault
 * owns the dead-letter entry of the event that faulted it, and the drain is its remedy — until an
 * operator discards that entry (its event can no longer be read, or the business process was
 * cancelled out-of-band). The row then stays {@code FAULTED} with a forward {@code
 * pre_fault_status}, and no automatic driver selects a {@code FAULTED} row. {@link #discard} names
 * the remedy when it leaves a row in that shape (or in the give-up shape), and {@link
 * #compensateFaulted} is it: the fresh claim to {@code COMPENSATING} the timeout runner would have
 * made, taken from the faulted row, then the compensation of the persisted state. Once the claim
 * commits the episode is an ordinary claimed one, re-driven by the automatic drivers.
 *
 * <p>Deferrals are recorded through {@link StreamRuneMetrics#recordSagaReplayDeferred} and never
 * under {@link StreamRuneMetrics#recordSagaReplayed}; {@code resumeFaulted} outcomes through {@link
 * StreamRuneMetrics#recordSagaResumeFaulted}; {@code compensateFaulted} outcomes through {@link
 * StreamRuneMetrics#recordSagaCompensateFaulted}.
 */
public final class SagaDeadLetterReplayer {
  private static final Logger LOG = LoggerFactory.getLogger(SagaDeadLetterReplayer.class);

  private static final String SAGA_ID_REQUIRED = "sagaId is required";

  /** Outcome of a single replay / drain step. */
  public enum ReplayOutcome {
    /** The event was fed (or was moot over a terminal row) and the entry discarded. */
    REPLAYED,
    /**
     * The feed re-recorded the fault (entry refreshed, row FAULTED), or the resume made no durable
     * progress (the row is still FAULTED after the feed); the entry stays for the next drain.
     */
    STILL_POISON,
    /** No such entry for this replayer's saga type. */
    ENTRY_NOT_FOUND,
    /**
     * The event store cannot read the entry's offset (pruned or archived); nothing was fed and the
     * row is unchanged — {@link #discard} is the remedy.
     */
    EVENT_NOT_FOUND,
    /**
     * The entry's first replay attempt is older than the command-inbox retention window, so a
     * re-drive could re-execute a committed forward command; reconcile, then {@code force}.
     */
    STALE_REDRIVE_BLOCKED,
    /**
     * The saga faulted mid-compensation and its episode claim is older than the inbox retention
     * window (a resume could double-execute a succeeded compensation); reconcile, then {@code
     * force} — the forced resume re-dispatches the episode at-least-once, past the executor's own
     * key-age guard too.
     */
    STALE_COMPENSATION_BLOCKED,
    /**
     * A null-saga entry resolved to a target that is FAULTED, genesis-pending or has an older entry
     * pending ahead of it: the target is stamped on the entry and shielded — drain the target with
     * {@link #replayAll}.
     */
    TARGET_PENDING,
    /**
     * Deferred: a correlated entry whose saga row is absent, or whose genesis is not applied while
     * the row is neither in nor faulted out of {@code COMPENSATING}. Nothing is fed and the entry
     * stays; a {@link #replayAll} drain feeds it once a start entry of the saga lands in the same
     * pass. Never counted as a replay attempt: deferrals are recorded under {@code
     * streamrune.saga.replay_deferred}, never under {@code streamrune.saga.replayed}.
     *
     * <p>A correlated entry whose saga row was deliberately deleted (by an operator, or by the
     * application through {@code SagaStore.delete}) answers this on every drain that lands no start
     * entry for the saga, and stays quarantined until it is removed with {@link #discard}. This is
     * conservative by design: an absent row reads exactly like a saga whose start has not landed
     * yet, so the entry is kept rather than dropped.
     */
    SAGA_ROW_PENDING,
    /**
     * Single replay refused: an older entry (named or resolved) of the saga is pending and must be
     * applied first — use {@link #replayAll}, or discard it explicitly.
     */
    OLDER_ENTRY_PENDING
  }

  /**
   * Outcome of {@link #resumeFaulted(SagaId, boolean)}: the operator resume of a compensation
   * episode the compensation-retry sweeper or the timeout runner gave up on. Only {@link #RESUMED}
   * changed the saga row's status; every other outcome leaves it {@code FAULTED} (or, for a
   * refusal, exactly as it was).
   */
  public enum ResumeOutcome {
    /**
     * The episode re-ran under its durable episode keys and its terminal write committed: the row
     * is {@code COMPENSATED} or {@code FAILED} (the episode's own classification) and no longer
     * {@code FAULTED}. Compensations an earlier attempt committed deduplicated in the command
     * inbox.
     */
    RESUMED,
    /**
     * {@code compensate()} threw again (the row was re-faulted, {@code pre_fault_status} kept), or
     * the terminal write cannot convert the saga state (nothing is written; the row is unchanged).
     * Fix the orchestrator and resume again.
     */
    STILL_POISON,
    /**
     * The resume wrote nothing durable: the episode ended non-terminal — a compensation failed
     * transiently or was refused admission (open circuit breaker, closing bus, interceptor veto) —
     * or its terminal write lost a race to a concurrent writer after the compensations were
     * dispatched. Only a successful CAS clears a fault, so the row stays {@code FAULTED}; resume
     * again once the cause is cleared (a rerun dedups every compensation that already executed).
     */
    NO_PROGRESS,
    /**
     * The episode's claim ({@code episode_claimed_at}) is older than the command-inbox retention
     * window, so a succeeded compensation's dedup key may already be pruned and a resume could
     * execute it again (double refund). Nothing was dispatched and the row is unchanged. Reconcile
     * what actually executed downstream, then {@code resumeFaulted(sagaId, true)}.
     */
    STALE_COMPENSATION_BLOCKED,
    /** No saga row of this replayer's type exists for the id; nothing to resume. */
    SAGA_NOT_FOUND,
    /**
     * The row is not {@code FAULTED}: a {@code COMPENSATING} row is the sweeper's (or the timeout
     * runner's) to re-drive, a live or terminal row has no episode to resume.
     */
    NOT_FAULTED,
    /**
     * The row is {@code FAULTED} by a forward step ({@code pre_fault_status} is not {@code
     * COMPENSATING}). A forward fault owns the dead-letter entry of the event that faulted it —
     * {@link #replayAll} is the remedy; once that entry was discarded and nothing is left to drain,
     * {@link SagaDeadLetterReplayer#compensateFaulted(SagaId)} is.
     */
    FORWARD_FAULT,
    /**
     * Dead-letter entries (named, or null-saga entries resolved to the saga) are pending for the
     * saga. Ordering belongs to the drain: {@link #replayAll} feeds them oldest-first, and the
     * first feed resumes the faulted episode itself.
     */
    ENTRIES_PENDING
  }

  /**
   * Outcome of {@link #compensateFaulted(SagaId)}: the operator compensation of a saga a forward
   * step faulted, once it has no dead-letter entry left to drain. {@link #TERMINAL}, {@link
   * #COMPENSATING} and a {@code compensate()} that threw ({@link #POISON}) claimed a fresh
   * compensation episode — the row left the forward fault; every other outcome dispatched nothing
   * and left the row exactly as it was.
   */
  public enum CompensateOutcome {
    /**
     * The claim committed and so did the episode's terminal write: the row is {@code COMPENSATED}
     * or {@code FAILED} (the episode's own classification) and no longer {@code FAULTED}.
     */
    TERMINAL,
    /**
     * The claim committed but the episode did not reach its terminal write: a compensation failed
     * transiently or was refused admission (open circuit breaker, closing bus, interceptor veto),
     * or the terminal write lost a race to a concurrent writer. The row is a stamped {@code
     * COMPENSATING} episode like any claimed one, re-driven under the same episode keys by the
     * compensation-retry sweeper, the timeout runner and the saga's correlated events.
     */
    COMPENSATING,
    /**
     * {@code compensate()} threw on the fresh episode, so the row was faulted inside it ({@code
     * pre_fault_status = COMPENSATING}, the new stamp kept, no dead-letter entry): fix the
     * orchestrator, then {@link SagaDeadLetterReplayer#resumeFaulted(SagaId)}. Or the claim could
     * not convert the saga state: nothing was written; fix the conversion and call again.
     */
    POISON,
    /**
     * A concurrent writer moved the row between the load and the claim's compare-and-swap; nothing
     * was dispatched. Read the row again before deciding again.
     */
    CLAIM_LOST,
    /** No saga row of this replayer's type exists for the id; nothing to compensate. */
    SAGA_NOT_FOUND,
    /**
     * The row is not {@code FAULTED}: a live saga is bounded by its own timeout, a {@code
     * COMPENSATING} one is re-driven by its automatic drivers, a terminal one is done.
     */
    NOT_FAULTED,
    /**
     * The row faulted inside a compensation episode ({@code pre_fault_status = COMPENSATING}). A
     * fresh claim would key a new episode and re-execute every compensation the faulted one already
     * committed; {@link SagaDeadLetterReplayer#resumeFaulted(SagaId, boolean)} resumes it under its
     * own keys.
     */
    COMPENSATION_FAULT,
    /**
     * Dead-letter entries (named, or null-saga entries resolved to the saga) are pending: events
     * the saga never consumed. Drain them with {@link SagaDeadLetterReplayer#replayAll(SagaId)} — a
     * fed event moves the saga forward again — or discard each one deliberately, then compensate.
     */
    ENTRIES_PENDING
  }

  private final SagaDeadLetterStore sagaDeadLetterStore;
  private final SagaStore sagaStore;
  private final EventStore eventStore;
  private final SagaRunner<?> sagaRunner;
  private final StreamRuneMetrics metrics;
  private final Duration inboxRetentionMaxAge;
  private final Clock clock;

  private SagaDeadLetterReplayer(Builder builder) {
    this.sagaDeadLetterStore = builder.sagaDeadLetterStore;
    this.sagaStore = builder.sagaStore;
    this.eventStore = builder.eventStore;
    this.sagaRunner = builder.sagaRunner;
    this.metrics = builder.metrics;
    // zero/negative means the inbox retention sweeper is DISABLED (dedup keys are
    // never pruned — the InboxRetentionSweeper/SagaRetentionValidator convention), so no key can
    // ever go stale and the staleness guards have nothing to protect against. Normalize to null
    // (guard inert), matching SagaCompensationRetrySweeper and SagaTimeoutRunner.
    // Pre-fix, a raw ZERO made the guard compare every entry's positive key age against a
    // zero-width window and refuse EVERY mid-compensation replay as STALE_COMPENSATION_BLOCKED —
    // training operators to routinely force, the exact bypass that matters when pruning IS on.
    this.inboxRetentionMaxAge =
        builder.inboxRetentionMaxAge == null
                || builder.inboxRetentionMaxAge.isZero()
                || builder.inboxRetentionMaxAge.isNegative()
            ? null
            : builder.inboxRetentionMaxAge;
    this.clock = builder.clock;
  }

  /**
   * Replays a single quarantined event.
   *
   * @param sagaId the saga the entry is quarantined under, or {@code null} for a null-saga entry
   *     (the saga could not be identified when the event was quarantined)
   * @param eventOffset the quarantined event's global offset (the dead-letter idempotency key,
   *     together with {@code sagaId})
   * @return the outcome; see {@link ReplayOutcome}
   */
  public ReplayOutcome replay(SagaId sagaId, GlobalOffset eventOffset) {
    return replay(sagaId, eventOffset, false);
  }

  /**
   * Replays a single quarantined event, optionally bypassing the two key-age refusals.
   *
   * @param sagaId the saga the entry is quarantined under, or {@code null} for a null-saga entry
   * @param eventOffset the quarantined event's global offset
   * @throws SagaUnstampedCompensationEpisodeException when the entry's saga faulted
   *     mid-compensation and its row carries no episode stamp (a saga-store contract violation):
   *     nothing was anchored or fed, and the row and entry are otherwise unchanged (a null-saga
   *     entry keeps the resolved target and the shield it was given on the way in, like every other
   *     refusal), whatever {@code force} says
   * @param force when {@code true}, bypass {@link ReplayOutcome#STALE_REDRIVE_BLOCKED} and {@link
   *     ReplayOutcome#STALE_COMPENSATION_BLOCKED} after the operator has reconciled; ordering
   *     refusals are never bypassed. It acknowledges at-least-once for the saga: it is passed to
   *     the saga executor, whose event-path key-age guard then resumes a stale compensation episode
   *     instead of refusing it, so a compensation whose inbox dedup key was already pruned executes
   *     again (logged at WARNING, metered as {@code streamrune.saga.forced_stale_resume})
   * @return the outcome; see {@link ReplayOutcome}
   */
  public ReplayOutcome replay(SagaId sagaId, GlobalOffset eventOffset, boolean force) {
    Objects.requireNonNull(eventOffset, "eventOffset is required");
    Optional<SagaDeadLetterEntry> entry = findEntry(sagaId, eventOffset);
    if (entry.isEmpty()) {
      return record(ReplayOutcome.ENTRY_NOT_FOUND);
    }
    if (sagaId != null) {
      Optional<Long> oldest = oldestPendingOffset(sagaId);
      if (oldest.isPresent() && oldest.get() < eventOffset.value()) {
        LOG.warn(
            "Refusing to replay the entry at offset {} for saga {}: an older entry (offset {})"
                + " is still pending and must be applied first. Use replayAll(sagaId), or discard"
                + " the older entry explicitly if it must be skipped.",
            eventOffset.value(),
            LogSanitizer.sanitizeForLog(sagaId.value()),
            oldest.get());
        return record(ReplayOutcome.OLDER_ENTRY_PENDING);
      }
    }
    Drained drained = drainOne(sagaId, entry.get(), force, null);
    if (drained.target() != null) {
      clearShieldIfDrained(drained.target());
    }
    return drained.outcome();
  }

  /**
   * Drains every entry of {@code sagaId} in offset order — see the class javadoc.
   *
   * @param sagaId the saga to drain (required)
   * @return one outcome per drained entry, in the order they were attempted
   */
  public List<ReplayOutcome> replayAll(SagaId sagaId) {
    return replayAll(sagaId, false);
  }

  /**
   * Drains every entry of {@code sagaId} in offset order, optionally bypassing the two key-age
   * refusals — see the class javadoc.
   *
   * @param sagaId the saga to drain (required)
   * @param force see {@link #replay(SagaId, GlobalOffset, boolean)}
   * @return one outcome per drained entry, in the order they were attempted
   * @throws SagaUnstampedCompensationEpisodeException when the saga faulted mid-compensation and
   *     its row carries no episode stamp: the entries drained before it are done, the one it
   *     stopped at and every later one stay quarantined, and nothing was fed for them (a null-saga
   *     entry it stopped at keeps the resolved target and shield it was given on the way in)
   */
  public List<ReplayOutcome> replayAll(SagaId sagaId, boolean force) {
    Objects.requireNonNull(sagaId, SAGA_ID_REQUIRED);
    List<DrainItem> items = planDrain(sagaId);
    List<ReplayOutcome> outcomes = new ArrayList<>(items.size());
    List<DrainItem> deferred = new ArrayList<>();
    boolean genesisLanded = false;
    boolean stopped = false;
    for (int i = 0; i < items.size(); i++) {
      DrainItem item = items.get(i);
      Drained drained = drainItem(sagaId, item, force);
      outcomes.add(drained.outcome());
      if (drained.outcome() == ReplayOutcome.SAGA_ROW_PENDING) {
        deferred.add(item);
        continue;
      }
      if (drained.outcome() != ReplayOutcome.REPLAYED) {
        logDrainStopped(sagaId, item, drained.outcome(), items.size() - (i + 1) + deferred.size());
        stopped = true;
        break;
      }
      if (drained.target() != null && !sagaId.equals(drained.target())) {
        // A resolved null-saga item
        // whose router now points at a DIFFERENT saga fed THAT saga's start — it lands no genesis
        // for THIS drain (so the deferred entries are not retried for nothing), and the other
        // saga's shield, set by this item's resolution, is released now rather than at its own
        // next drain.
        clearShieldIfDrained(drained.target());
        continue;
      }
      genesisLanded |= drained.startPath();
    }
    if (!stopped && genesisLanded && !deferred.isEmpty()) {
      List<DrainItem> retry = List.copyOf(deferred);
      deferred.clear();
      for (int j = 0; j < retry.size(); j++) {
        DrainItem item = retry.get(j);
        Drained drained = drainItem(sagaId, item, force);
        outcomes.add(drained.outcome());
        if (drained.outcome() == ReplayOutcome.REPLAYED) {
          continue;
        }
        if (drained.outcome() == ReplayOutcome.SAGA_ROW_PENDING) {
          deferred.add(item);
          continue;
        }
        deferred.addAll(retry.subList(j + 1, retry.size()));
        logDrainStopped(sagaId, item, drained.outcome(), deferred.size());
        break;
      }
    }
    if (!deferred.isEmpty()) {
      LOG.warn(
          "Saga dead-letter drain for saga {} left {} correlated entr{} deferred: either the"
              + " genesis of the saga is still not applied (the start event is still poison, was"
              + " discarded by hand, or its live redelivery has not completed, or the saga row"
              + " was deleted) or the drain stopped at a blocker before retrying them. They stay"
              + " quarantined; re-run replayAll once the genesis lands or the blocker is cleared,"
              + " or discard them (a saga left FAULTED with no entry is then recovered with"
              + " compensateFaulted(sagaId)).",
          LogSanitizer.sanitizeForLog(sagaId.value()),
          deferred.size(),
          deferred.size() == 1 ? "y" : "ies");
    }
    clearShieldIfDrained(sagaId);
    return List.copyOf(outcomes);
  }

  /**
   * Discards one entry; clears the owner's (or, for a null-saga entry, the resolved target's)
   * shield when its backlog is empty afterwards. A saga the discard leaves {@code FAULTED} with no
   * entry is advanced by nothing, so a WARNING then names its remedy: {@link #compensateFaulted}
   * for a forward fault, {@link #resumeFaulted} for a fault inside a compensation episode.
   *
   * @param sagaId the entry's saga id, or {@code null} for a null-saga entry
   * @param eventOffset the entry's event offset (required)
   * @return {@code true} if an entry was removed
   */
  public boolean discard(SagaId sagaId, GlobalOffset eventOffset) {
    Objects.requireNonNull(eventOffset, "eventOffset is required");
    SagaId shieldOwner = sagaId;
    if (sagaId == null) {
      shieldOwner =
          sagaDeadLetterStore
              .findNullSagaEntry(sagaType(), eventOffset)
              .map(SagaDeadLetterEntry::targetSagaId)
              .orElse(null);
    }
    boolean removed = sagaDeadLetterStore.discard(sagaId, sagaType(), eventOffset);
    if (shieldOwner != null) {
      clearShieldIfDrained(shieldOwner);
      if (removed) {
        reportFaultLeftWithoutEntries(shieldOwner);
      }
    }
    return removed;
  }

  /**
   * After a discard: a {@code FAULTED} row left with no dead-letter entry is advanced by nothing —
   * no drain has anything to feed it, its later correlated events are held behind the fault, and
   * neither the timeout runner nor the compensation-retry sweeper selects a {@code FAULTED} row —
   * so the discard names the operator remedy for its shape: {@link #compensateFaulted} for a
   * forward fault, {@link #resumeFaulted} for a fault inside a compensation episode. Read-only and
   * best-effort: the entry is already gone, so a read that fails is logged and swallowed rather
   * than hiding a discard that succeeded, and a crash before it loses one log line — the row itself
   * still shows its shape to {@code SagaStore.findByStatus} and the {@code
   * streamrune.saga.faulted_rows} gauge.
   */
  private void reportFaultLeftWithoutEntries(SagaId sagaId) {
    String id = LogSanitizer.sanitizeForLog(sagaId.value());
    try {
      Optional<? extends LoadedSaga<?>> row = sagaStore.load(sagaId, sagaType(), stateType());
      if (row.isEmpty() || row.get().status() != SagaStatus.FAULTED) {
        return;
      }
      if (pendingEntryCount(sagaId) > 0) {
        return;
      }
      LoadedSaga<?> faulted = row.get();
      if (faulted.preFaultStatus() == SagaStatus.COMPENSATING) {
        LOG.warn(
            "discard: saga {} is FAULTED inside a compensation episode and has no dead-letter"
                + " entry left; nothing re-drives a FAULTED row. Resume the episode with"
                + " resumeFaulted(sagaId).",
            id);
      } else if (!faulted.genesisApplied()) {
        LOG.warn(
            "discard: saga {} faulted before its genesis was applied and has no dead-letter entry"
                + " left: no start event is left to land the genesis, its later correlated events"
                + " are held behind the fault, and neither the timeout runner nor the"
                + " compensation-retry sweeper selects a FAULTED row. Compensate it from its"
                + " recorded state with compensateFaulted(sagaId).",
            id);
      } else {
        LOG.warn(
            "discard: saga {} is FAULTED by a forward step (pre_fault_status {}) and has no"
                + " dead-letter entry left: nothing will advance it — its later correlated events"
                + " are held behind the fault, and neither the timeout runner nor the"
                + " compensation-retry sweeper selects a FAULTED row. Either compensate it with"
                + " compensateFaulted(sagaId), or, once a later correlated event is held behind the"
                + " fault, drain it with replayAll(sagaId) to move the saga forward without the"
                + " discarded event.",
            id,
            faulted.preFaultStatus() == null ? "NULL" : faulted.preFaultStatus());
      }
    } catch (RuntimeException checkFailed) {
      LOG.warn(
          "discard: the entry was removed, but whether saga "
              + id
              + " is left FAULTED with no dead-letter entry could not be checked. If"
              + " SagaStore.findByStatus lists it FAULTED with no entry, compensateFaulted(sagaId)"
              + " recovers a forward fault and resumeFaulted(sagaId) a compensation fault.",
          checkFailed);
    }
  }

  /**
   * Resumes the compensation episode of a saga the compensation-retry sweeper or the timeout runner
   * gave up on, without overriding the key-age refusal — {@code resumeFaulted(sagaId, false)}.
   *
   * @param sagaId the faulted saga (required)
   * @return the outcome; see {@link ResumeOutcome}
   */
  public ResumeOutcome resumeFaulted(SagaId sagaId) {
    return resumeFaulted(sagaId, false);
  }

  /**
   * Resumes the compensation episode of a saga the compensation-retry sweeper or the timeout runner
   * gave up on . Their give-up writes the row {@code FAULTED} with {@code pre_fault_status =
   * COMPENSATING} and — having no triggering event — no dead-letter entry, so {@link #replayAll}
   * has nothing to feed. No entry is fabricated: the episode resumes through the saga executor's
   * own compensation resume, at the row's current version and under the durable episode keys, so
   * every compensation an earlier attempt committed dedups in the command inbox and only the rest
   * run. That reads the row's stamped episode ({@code episodeVersion}, {@code episodeClaimedAt}),
   * which every write that enters {@code COMPENSATING} carries — a claim, or the forward path
   * persisting an evolved {@code SagaState.status()} of {@code COMPENSATING} (the episode stamp
   * rule, {@link SagaStore#claimCompensating}); a row in an episode without it is a store contract
   * violation and is refused with {@link SagaUnstampedCompensationEpisodeException} before anything
   * is dispatched or written — never resumed at a guessed version, which would re-execute a
   * committed compensation. Only the resume's own terminal CAS ({@code COMPENSATED} or {@code
   * FAILED}) clears the fault — this replayer never un-faults. A dead-letter entry that lands while
   * the resume runs (a live correlated event held behind the still-FAULTED row) is moot over the
   * terminal row but keeps the saga's shield: the backlog is re-read after a {@link
   * ResumeOutcome#RESUMED} terminal write and a WARNING names {@link #replayAll}, which discards
   * it.
   *
   * <p>Only that shape is resumed. Every other one is refused with a specific outcome and nothing
   * is written: no row ({@link ResumeOutcome#SAGA_NOT_FOUND}); not {@code FAULTED} ({@link
   * ResumeOutcome#NOT_FAULTED} — a {@code COMPENSATING} row is the sweeper's); a forward fault
   * ({@link ResumeOutcome#FORWARD_FAULT} — it owns its poison entry, use {@link #replayAll}; once
   * that entry was discarded, {@link #compensateFaulted}); dead-letter entries pending for the
   * saga, named or resolved ({@link ResumeOutcome#ENTRIES_PENDING} — ordering belongs to {@link
   * #replayAll}, whose first feed resumes the same episode).
   *
   * <p><b>Staleness.</b> The drain's rule: an episode whose claim ({@code episode_claimed_at}) is
   * older than the command-inbox retention window is refused ({@link
   * ResumeOutcome#STALE_COMPENSATION_BLOCKED}), because a succeeded compensation's dedup key may
   * already be pruned. {@code force} lifts that refusal and reaches the executor's key-age guard as
   * the explicit at-least-once acknowledgement: the executor WARNs naming the saga and records
   * {@code streamrune.saga.forced_stale_resume}, and a compensation whose key was pruned executes
   * again. The executor's guard is also the second line: a replayer built without {@link
   * Builder#inboxRetentionMaxAge} is still refused by the runner's window, with nothing dispatched
   * and nothing written.
   *
   * <p><b>Crash.</b> The resume writes nothing durable before its terminal CAS except the inbox
   * keys of the compensations it executes. A crash after some of them committed and before the
   * terminal write leaves the row {@code FAULTED} over the same episode (same version, same claim
   * instant — a stale episode stays refused without force, the crash forges no license); a rerun
   * re-dispatches under the same keys, the committed compensations dedup, the rest run and the
   * terminal write lands. Only a compensation whose key was pruned runs again, and only under
   * {@code force}.
   *
   * <p>Every outcome is logged (ids sanitized) and recorded under {@code
   * streamrune.saga.resume_faulted{saga.type, outcome}}.
   *
   * @param sagaId the faulted saga (required)
   * @param force when {@code true}, lift {@link ResumeOutcome#STALE_COMPENSATION_BLOCKED} after the
   *     operator has reconciled what executed downstream: the explicit, per-saga acknowledgement of
   *     at-least-once, passed to the saga executor's key-age guard. It overrides nothing else
   * @return the outcome; see {@link ResumeOutcome}
   * @throws SagaUnstampedCompensationEpisodeException when the faulted episode's row carries no
   *     stamp: nothing dispatched, nothing written, and — not being an outcome of the operation —
   *     not recorded under {@code streamrune.saga.resume_faulted}
   */
  public ResumeOutcome resumeFaulted(SagaId sagaId, boolean force) {
    Objects.requireNonNull(sagaId, SAGA_ID_REQUIRED);
    Optional<? extends LoadedSaga<? extends SagaState>> row =
        sagaStore.load(sagaId, sagaType(), stateType()); // INFRA load → propagates
    Optional<ResumeOutcome> refused = refuseResume(sagaId, row);
    if (refused.isPresent()) {
      return recordResume(refused.get());
    }
    LoadedSaga<? extends SagaState> faulted = row.orElseThrow();
    Instant claim = requireStampedClaim(sagaId, faulted);
    if (!force && inboxRetentionMaxAge != null && claimOutlivedTheInboxWindow(claim)) {
      LOG.warn(
          "Refusing resumeFaulted for saga {}: its faulted compensation episode's claim ({}) is"
              + " older than the command-inbox retention window ({}), so a succeeded"
              + " compensation's dedup key may already be pruned and a resume could re-execute it"
              + " (double refund). Nothing was dispatched and the row is unchanged. Reconcile what"
              + " executed downstream, then resumeFaulted(sagaId, true) — a forced resume"
              + " re-dispatches the episode at-least-once.",
          LogSanitizer.sanitizeForLog(sagaId.value()),
          claim,
          inboxRetentionMaxAge);
      return recordResume(ResumeOutcome.STALE_COMPENSATION_BLOCKED);
    }
    StepOutcome resumed;
    try {
      resumed = sagaRunner.resumeFaultedEpisode(sagaId, force);
    } catch (SagaStaleCompensationEpisodeException _) {
      // The runner's window refused it (this replayer has none, or a narrower one); the executor
      // has already WARNed naming the remedy. Nothing was dispatched or written.
      return recordResume(ResumeOutcome.STALE_COMPENSATION_BLOCKED);
    } catch (SagaPoisonException unwritable) {
      LOG.warn(
          "resumeFaulted for saga "
              + LogSanitizer.sanitizeForLog(sagaId.value())
              + ": the resumed episode's terminal write cannot convert the saga state; the row"
              + " stays FAULTED. Fix the state conversion, then resume again.",
          unwritable.getCause());
      return recordResume(ResumeOutcome.STILL_POISON);
    }
    return recordResume(classifyResume(sagaId, faulted, resumed));
  }

  /**
   * Compensates a saga a forward step faulted, once it has no dead-letter entry left to drain — the
   * operator path for a forward fault whose poison entry was discarded (its event can no longer be
   * read, or the business process was cancelled out-of-band). Such a row is {@code FAULTED} with
   * {@code pre_fault_status} {@code NULL} (a fault before the genesis was applied), {@code STARTED}
   * or {@code RUNNING}: {@link #replayAll} has nothing to feed, {@link #resumeFaulted} refuses a
   * forward fault, the live path holds the saga's later correlated events behind the fault, and
   * neither the timeout runner nor the compensation-retry sweeper ever selects a {@code FAULTED}
   * row — so without this operation the saga stays halted with its forward side effects in place.
   *
   * <p>It does what the saga's timeout would have done, from the faulted row: a compare-and-swap
   * claim to {@code COMPENSATING} at the row's version — which stamps a fresh episode ({@code
   * episode_version} = that version + 1, {@code episode_claimed_at} = now) and clears {@code
   * pre_fault_status} — then {@code compensate()} over the persisted state and the dispatch of its
   * commands under the new episode's keys. A forward fault belongs to no episode, so those keys are
   * new: no forward command is re-dispatched, nothing dedups against an earlier attempt, and there
   * is no key-age refusal and no {@code force}. The persisted state is the state before the event
   * that faulted it; a forward command that step dispatched before its fault is covered by the
   * cancel/void discipline of {@code compensate()}, as on the timeout path. The orchestrator
   * receives an internal marker as {@code failure} and {@code null} as {@code failedCommand}.
   *
   * <p>Only that shape is compensated. Every other one is refused with a specific outcome, nothing
   * dispatched and nothing written: no row ({@link CompensateOutcome#SAGA_NOT_FOUND}); not {@code
   * FAULTED} ({@link CompensateOutcome#NOT_FAULTED}); faulted inside a compensation episode ({@link
   * CompensateOutcome#COMPENSATION_FAULT} — {@link #resumeFaulted} resumes it under its own keys);
   * dead-letter entries pending, named or resolved ({@link CompensateOutcome#ENTRIES_PENDING} —
   * drain them with {@link #replayAll} or discard them first).
   *
   * <p><b>Crash points.</b> Before the claim commits nothing is written: rerun the call. Once it
   * commits, the row is a stamped {@code COMPENSATING} episode and no longer this operation's — a
   * rerun answers {@code NOT_FAULTED} and dispatches nothing — and its automatic re-drivers (the
   * compensation-retry sweeper, the timeout runner, a correlated event) resume it at that version
   * under the same episode keys: a compensation the crashed call committed dedups in the command
   * inbox, the rest run, and the terminal write lands. A {@code compensate()} that throws faults
   * the row inside the new episode, which {@link #resumeFaulted} then owns. Two concurrent calls
   * race on the claim's compare-and-swap: one wins, the other dispatches nothing ({@link
   * CompensateOutcome#CLAIM_LOST}).
   *
   * <p>Every outcome is logged (ids sanitized) and recorded under {@code
   * streamrune.saga.compensate_faulted{saga.type, outcome}}.
   *
   * @param sagaId the faulted saga (required)
   * @return the outcome; see {@link CompensateOutcome}
   */
  public CompensateOutcome compensateFaulted(SagaId sagaId) {
    Objects.requireNonNull(sagaId, SAGA_ID_REQUIRED);
    Optional<? extends LoadedSaga<? extends SagaState>> row =
        sagaStore.load(sagaId, sagaType(), stateType()); // INFRA load → propagates
    Optional<CompensateOutcome> refused = refuseCompensate(sagaId, row);
    if (refused.isPresent()) {
      return recordCompensate(refused.get());
    }
    LoadedSaga<? extends SagaState> faulted = row.orElseThrow();
    StepOutcome compensated;
    try {
      compensated = sagaRunner.compensateForwardFault(sagaId);
    } catch (SagaPoisonException unwritable) {
      LOG.warn(
          "compensateFaulted for saga "
              + LogSanitizer.sanitizeForLog(sagaId.value())
              + ": the saga state cannot be converted for the compensation claim, so nothing was"
              + " written and the row stays FAULTED. Fix the state conversion, then call again.",
          unwritable.getCause());
      return recordCompensate(CompensateOutcome.POISON);
    }
    return recordCompensate(classifyCompensate(sagaId, faulted, compensated));
  }

  /** The shape checks of {@link #compensateFaulted(SagaId)}, in the documented order. */
  private Optional<CompensateOutcome> refuseCompensate(
      SagaId sagaId, Optional<? extends LoadedSaga<? extends SagaState>> row) {
    String id = LogSanitizer.sanitizeForLog(sagaId.value());
    if (row.isEmpty()) {
      LOG.warn(
          "compensateFaulted: no saga row of type {} exists for saga {}; nothing to compensate.",
          sagaType().value(),
          id);
      return Optional.of(CompensateOutcome.SAGA_NOT_FOUND);
    }
    LoadedSaga<? extends SagaState> loaded = row.get();
    if (loaded.status() != SagaStatus.FAULTED) {
      LOG.warn(
          "compensateFaulted: saga {} is {}, not FAULTED; nothing to compensate (a live saga is"
              + " bounded by its own timeout, a COMPENSATING episode is re-driven by the"
              + " compensation-retry sweeper, the timeout runner and its correlated events).",
          id,
          loaded.status());
      return Optional.of(CompensateOutcome.NOT_FAULTED);
    }
    if (loaded.preFaultStatus() == SagaStatus.COMPENSATING) {
      LOG.warn(
          "compensateFaulted: saga {} faulted inside its compensation episode (pre_fault_status"
              + " COMPENSATING). A fresh claim would key a new episode and re-execute the"
              + " compensations this one already committed; resume it under its own keys with"
              + " resumeFaulted(sagaId).",
          id);
      return Optional.of(CompensateOutcome.COMPENSATION_FAULT);
    }
    int pending = pendingEntryCount(sagaId);
    if (pending > 0) {
      LOG.warn(
          "compensateFaulted: saga {} has {} pending dead-letter entr{}, events it never"
              + " consumed. Drain them with replayAll(sagaId) — a fed event moves the saga forward"
              + " again — or discard each one deliberately with discard(sagaId, eventOffset), then"
              + " compensate it.",
          id,
          pending,
          pending == 1 ? "y" : "ies");
      return Optional.of(CompensateOutcome.ENTRIES_PENDING);
    }
    return Optional.empty();
  }

  /**
   * Classifies the executor's answer; a committed claim is confirmed on the ROW, as the drain and
   * the resume confirm theirs: only a committed terminal write makes it {@link
   * CompensateOutcome#TERMINAL}.
   */
  private CompensateOutcome classifyCompensate(
      SagaId sagaId, LoadedSaga<? extends SagaState> faulted, StepOutcome compensated) {
    String id = LogSanitizer.sanitizeForLog(sagaId.value());
    switch (compensated) {
      case CLAIM_LOST -> {
        LOG.warn(
            "compensateFaulted: saga {} was written concurrently between the load and the claim;"
                + " the claim lost its compare-and-swap and nothing was dispatched. Read the row"
                + " again before deciding again.",
            id);
        return CompensateOutcome.CLAIM_LOST;
      }
      case NOT_COMPENSATING -> {
        // The row changed between this replayer's load and the runner's: classify what is there.
        return refuseCompensate(sagaId, sagaStore.load(sagaId, sagaType(), stateType()))
            .orElse(CompensateOutcome.NOT_FAULTED);
      }
      case FAULTED -> {
        releaseResidualShield(sagaId, faulted);
        LOG.warn(
            "compensateFaulted: compensate() threw for saga {}; the row was faulted inside the new"
                + " compensation episode (pre_fault_status COMPENSATING, no dead-letter entry). Fix"
                + " the orchestrator, then resumeFaulted(sagaId).",
            id);
        return CompensateOutcome.POISON;
      }
      default -> {
        // COMPENSATED / FAILED / COMPENSATING_LEFT / COMPENSATING_REFUSED: the claim committed.
      }
    }
    releaseResidualShield(sagaId, faulted);
    Optional<? extends LoadedSaga<?>> after = sagaStore.load(sagaId, sagaType(), stateType());
    String now = after.map(r -> r.status().name()).orElse("deleted");
    boolean terminal = after.map(r -> r.status().isTerminal()).orElse(false);
    if ((compensated == StepOutcome.COMPENSATED || compensated == StepOutcome.FAILED) && terminal) {
      LOG.info("compensateFaulted: saga {} was compensated; the row is now {}.", id, now);
      reportEntriesThatArrivedDuring(
          sagaId, "compensateFaulted", "was compensated", "compensation");
      return CompensateOutcome.TERMINAL;
    }
    LOG.warn(
        "compensateFaulted: saga {} was claimed for compensation, but the episode did not reach its"
            + " terminal write (outcome {}); the row is now {}. A COMPENSATING episode is"
            + " re-driven under the same keys by the compensation-retry sweeper, the timeout runner"
            + " and its correlated events.",
        id,
        compensated,
        now);
    reportEntriesThatArrivedDuring(
        sagaId, "compensateFaulted", "was claimed for compensation", "compensation");
    return CompensateOutcome.COMPENSATING;
  }

  /**
   * A shield with no entry is a legal residue (a crash between a discard and its conditional
   * clear). The claim has taken the row out of the forward fault, so the store's conditional clear
   * releases it now; with an entry present it clears nothing.
   */
  private void releaseResidualShield(SagaId sagaId, LoadedSaga<?> faulted) {
    if (faulted.deadLetterPending()) {
      clearShieldIfDrained(sagaId);
    }
  }

  /** The shape checks of {@link #resumeFaulted(SagaId, boolean)}, in the documented order. */
  private Optional<ResumeOutcome> refuseResume(
      SagaId sagaId, Optional<? extends LoadedSaga<? extends SagaState>> row) {
    String id = LogSanitizer.sanitizeForLog(sagaId.value());
    if (row.isEmpty()) {
      LOG.warn(
          "resumeFaulted: no saga row of type {} exists for saga {}; nothing to resume.",
          sagaType().value(),
          id);
      return Optional.of(ResumeOutcome.SAGA_NOT_FOUND);
    }
    LoadedSaga<? extends SagaState> loaded = row.get();
    if (loaded.status() != SagaStatus.FAULTED) {
      LOG.warn(
          "resumeFaulted: saga {} is {}, not FAULTED; nothing to resume (a COMPENSATING episode"
              + " is re-driven by the compensation-retry sweeper and the timeout runner).",
          id,
          loaded.status());
      return Optional.of(ResumeOutcome.NOT_FAULTED);
    }
    if (loaded.preFaultStatus() != SagaStatus.COMPENSATING) {
      LOG.warn(
          "resumeFaulted: saga {} faulted in a forward step (pre_fault_status {}), not in a"
              + " compensation episode; the event that faulted it is a dead-letter entry. Drain it"
              + " with replayAll(sagaId) — or, if that entry was discarded and nothing is left to"
              + " drain, compensate the saga with compensateFaulted(sagaId).",
          id,
          loaded.preFaultStatus() == null ? "NULL" : loaded.preFaultStatus());
      return Optional.of(ResumeOutcome.FORWARD_FAULT);
    }
    int pending = pendingEntryCount(sagaId);
    if (pending > 0) {
      LOG.warn(
          "resumeFaulted: saga {} has {} pending dead-letter entr{}; ordering belongs to the"
              + " drain. Use replayAll(sagaId): it feeds them oldest-first, and the first feed"
              + " resumes the faulted compensation episode itself.",
          id,
          pending,
          pending == 1 ? "y" : "ies");
      return Optional.of(ResumeOutcome.ENTRIES_PENDING);
    }
    return Optional.empty();
  }

  /**
   * Classifies the executor's answer on the ROW, as the drain does: only a committed terminal CAS
   * clears a fault, so an episode that ended non-terminal — or whose terminal write lost a race —
   * leaves the row FAULTED and reports it.
   */
  private ResumeOutcome classifyResume(
      SagaId sagaId, LoadedSaga<? extends SagaState> faulted, StepOutcome resumed) {
    String id = LogSanitizer.sanitizeForLog(sagaId.value());
    switch (resumed) {
      case COMPENSATED, FAILED -> {
        Optional<? extends LoadedSaga<?>> after = sagaStore.load(sagaId, sagaType(), stateType());
        if (after.isPresent() && after.get().status() == SagaStatus.FAULTED) {
          LOG.warn(
              "resumeFaulted: saga {}: the compensations were dispatched but the terminal write"
                  + " (outcome {}) lost a race to a concurrent writer, so the resume made no"
                  + " durable progress and the row stays FAULTED. Resume again; a rerun dedups every"
                  + " compensation that already executed.",
              id,
              resumed);
          return ResumeOutcome.NO_PROGRESS;
        }
        if (faulted.deadLetterPending()) {
          // A shield with no entry (the shield errs toward recording — a crash residue): the
          // store's
          // conditional clear releases it now that the saga is terminal.
          clearShieldIfDrained(sagaId);
        }
        LOG.info(
            "resumeFaulted: saga {} resumed its faulted compensation episode; the row is now {}.",
            id,
            after.map(r -> r.status().name()).orElse("deleted"));
        reportEntriesThatArrivedDuring(sagaId, "resumeFaulted", "resumed", "resume");
        return ResumeOutcome.RESUMED;
      }
      case FAULTED -> {
        LOG.warn(
            "resumeFaulted: compensate() still throws for saga {}; the row was re-faulted in"
                + " place (pre_fault_status kept, no dead-letter entry). Fix the orchestrator,"
                + " then resume again.",
            id);
        return ResumeOutcome.STILL_POISON;
      }
      case NOT_COMPENSATING -> {
        // The row changed between this replayer's load and the runner's (a concurrent drain or
        // resume): classify what is there now.
        return refuseResume(sagaId, sagaStore.load(sagaId, sagaType(), stateType()))
            .orElse(ResumeOutcome.NOT_FAULTED);
      }
      default -> {
        // COMPENSATING_LEFT / COMPENSATING_REFUSED: a transient or refused compensation, no write.
      }
    }
    LOG.warn(
        "resumeFaulted: saga {}: the resume made no durable progress (outcome {}); the row stays"
            + " FAULTED. Resume again once the cause is cleared.",
        id,
        resumed);
    return ResumeOutcome.NO_PROGRESS;
  }

  /**
   * The backlog was empty when {@link #resumeFaulted} or {@link #compensateFaulted} checked it, but
   * an entry can land while the operation runs — a live correlated event that loaded the row while
   * it was still FAULTED is held behind it (entry + shield; the hold bumps no version, so the next
   * write still commits). Such an entry keeps the saga's shield until a drain takes it (over a
   * terminal row it is moot and discarded), so the backlog is re-read afterwards and a non-empty
   * one names {@link #replayAll} as the next step. Read-only and best-effort: a hold that publishes
   * after this read is not reported here (its own WARN already names {@code replayAll}), and
   * nothing durable depends on it — a crash before this read loses one log line, never an entry or
   * its shield. It runs after the operation's write has committed, so a read that fails is logged
   * and swallowed: the operation DID succeed, and an exception here would hide that from the caller
   * and the metric (a rerun would then answer {@code NOT_FAULTED}).
   *
   * @param operation the operator call, named in the log
   * @param done what the call did to the saga, as a past-tense phrase
   * @param run what was running while the entry arrived
   */
  private void reportEntriesThatArrivedDuring(
      SagaId sagaId, String operation, String done, String run) {
    int pending;
    try {
      pending = pendingEntryCount(sagaId);
    } catch (RuntimeException rereadFailed) {
      LOG.warn(
          "{}: saga {} {}, but its dead-letter backlog could not be re-read afterwards. Entries"
              + " held while the {} ran keep the saga's shield: run replayAll(sagaId) if any"
              + " exist.",
          operation,
          LogSanitizer.sanitizeForLog(sagaId.value()),
          done,
          run,
          rereadFailed);
      return;
    }
    if (pending == 0) {
      return;
    }
    LOG.warn(
        "{}: saga {} {}, but {} dead-letter entr{} arrived while the {} ran (held behind the"
            + " still-FAULTED row). They keep the saga's shield until drained: run"
            + " replayAll(sagaId) next — over a terminal row it discards them as moot and releases"
            + " the shield.",
        operation,
        LogSanitizer.sanitizeForLog(sagaId.value()),
        done,
        pending,
        pending == 1 ? "y" : "ies",
        run);
  }

  /** The saga's pending entries of this type: named ones plus null-saga entries resolved to it. */
  private int pendingEntryCount(SagaId sagaId) {
    return (int)
            sagaDeadLetterStore.findBySaga(sagaId).stream().filter(this::ownedByThisType).count()
        + resolvedNullSagaEntriesFor(sagaId).map(List::size).orElse(0);
  }

  private record DrainItem(SagaDeadLetterEntry entry, boolean nullSaga, boolean shadowsNullTwin) {}

  private record Drained(ReplayOutcome outcome, boolean startPath, SagaId target) {}

  private List<DrainItem> planDrain(SagaId sagaId) {
    List<SagaDeadLetterEntry> named = new ArrayList<>();
    Set<Long> namedOffsets = new HashSet<>();
    for (SagaDeadLetterEntry entry : sagaDeadLetterStore.findBySaga(sagaId)) {
      if (ownedByThisType(entry)) {
        named.add(entry);
        namedOffsets.add(entry.eventOffset().value());
      }
    }
    Set<Long> shadowed = new HashSet<>();
    List<DrainItem> items = new ArrayList<>();
    for (SagaDeadLetterEntry resolved : resolvedNullSagaEntriesFor(sagaId).orElse(List.of())) {
      if (namedOffsets.contains(resolved.eventOffset().value())) {
        shadowed.add(resolved.eventOffset().value()); // the named entry is the record
        continue;
      }
      items.add(new DrainItem(resolved, true, false));
    }
    for (SagaDeadLetterEntry entry : named) {
      items.add(new DrainItem(entry, false, shadowed.contains(entry.eventOffset().value())));
    }
    items.sort(Comparator.comparingLong(item -> item.entry().eventOffset().value()));
    return items;
  }

  private Drained drainItem(SagaId sagaId, DrainItem item, boolean force) {
    if (item.shadowsNullTwin()) {
      sagaDeadLetterStore.discard(null, sagaType(), item.entry().eventOffset());
      LOG.info(
          "Saga dead-letter drain for saga {} discarded the null-saga twin of the entry at"
              + " offset {} before feeding the named entry (one event, one feed).",
          LogSanitizer.sanitizeForLog(sagaId.value()),
          item.entry().eventOffset().value());
    }
    return drainOne(item.nullSaga() ? null : sagaId, item.entry(), force, sagaId);
  }

  /**
   * One entry, by lookup: read the event, route it, load the target row, apply the drain rules in
   * order, establish the anchor, feed, discard.
   *
   * @param entrySagaId the entry's own saga id ({@code null} for a null-saga entry)
   * @param drainOf the saga whose {@code replayAll} is running, or {@code null} for a single replay
   *     — a null-saga entry resolved to {@code drainOf} is fed as part of that drain
   */
  private Drained drainOne(
      SagaId entrySagaId, SagaDeadLetterEntry entry, boolean force, SagaId drainOf) {
    GlobalOffset offset = entry.eventOffset();
    Optional<EventEnvelope> event = readEvent(offset);
    if (event.isEmpty()) {
      LOG.warn(
          "Saga dead-letter entry at offset {} (saga={}) refers to an event the event store"
              + " cannot read (pruned or archived); nothing was fed and the saga row is unchanged."
              + " discard(sagaId, offset) is the remedy; until then the saga's shield holds live"
              + " correlated events.",
          offset.value(),
          entrySagaId == null ? "<null>" : LogSanitizer.sanitizeForLog(entrySagaId.value()));
      return new Drained(record(ReplayOutcome.EVENT_NOT_FOUND), false, null);
    }
    Optional<SagaRunner.ReplayTarget> routed;
    try {
      routed = sagaRunner.resolveTarget(event.get());
    } catch (SagaPoisonException routerPoison) {
      sagaRunner.recordPoison(entrySagaId, event.get(), routerPoison.getCause());
      LOG.warn(
          "Saga dead-letter replay at offset "
              + offset.value()
              + ": the orchestrator's router still throws; the entry was refreshed",
          routerPoison.getCause());
      return new Drained(record(ReplayOutcome.STILL_POISON), false, null);
    }
    // The entry's saga id is a RECORDED fact and stays the authority for a named entry;
    // the router only classifies start vs correlated. A null-saga entry has no recorded target, so
    // for it the router's answer IS the target.
    boolean startPath = routed.map(SagaRunner.ReplayTarget::startPath).orElse(false);
    SagaId sagaId =
        entrySagaId != null
            ? entrySagaId
            : routed.map(SagaRunner.ReplayTarget::sagaId).orElse(null);
    if (sagaId == null) {
      sagaDeadLetterStore.discard(null, sagaType(), offset);
      LOG.info(
          "Null-saga dead-letter entry at offset {} routes to no saga any more; discarded.",
          offset.value());
      return new Drained(record(ReplayOutcome.REPLAYED), false, null);
    }
    if (entrySagaId != null && (routed.isEmpty() || !entrySagaId.equals(routed.get().sagaId()))) {
      LOG.warn(
          "Saga dead-letter entry at offset {} was recorded for saga {} but the orchestrator"
              + " now routes the event {}; feeding the RECORDED saga — the entry is the"
              + " authority, and the saga was faulted by this very event.",
          offset.value(),
          LogSanitizer.sanitizeForLog(entrySagaId.value()),
          routed
              .map(t -> "to saga " + LogSanitizer.sanitizeForLog(t.sagaId().value()))
              .orElse("nowhere"));
    }
    if (entrySagaId == null) {
      // Record the resolution and shield the target BEFORE any refusal, anchor or feed, so a
      // refused or crash-interrupted null-saga entry keeps its retention shield.
      sagaDeadLetterStore.setResolvedTarget(sagaType(), offset, sagaId);
      sagaStore.setDeadLetterPending(sagaId, sagaType(), true);
    }
    Optional<? extends LoadedSaga<? extends SagaState>> row =
        sagaStore.load(sagaId, sagaType(), stateType()); // INFRA load → propagates
    if (entrySagaId == null && !sagaId.equals(drainOf)) {
      // A null-saga entry outside its target's drain may be fed only if the target can consume it
      // NOW and in order: not FAULTED, genesis applied (or a start), and no OLDER pending
      // entry — named or resolved — ahead of it. (The shield flag itself is not consulted here:
      // this call set it a moment ago; ordering is what the shield protects.)
      Optional<Long> older = oldestPendingOffset(sagaId); // includes this entry; equal is fine
      boolean targetBusy =
          row.map(
                      r ->
                          r.status() == SagaStatus.FAULTED
                              || (!startPath
                                  && !r.genesisApplied()
                                  && r.effectiveStatus() != SagaStatus.COMPENSATING))
                  .orElse(!startPath && targetOwnsOwnTypeDeadLetterRecord(sagaId))
              || (older.isPresent() && older.get() < offset.value());
      if (targetBusy) {
        LOG.warn(
            "Null-saga dead-letter entry at offset {} now routes to saga {}, which is FAULTED,"
                + " genesis-pending or has an older entry pending ahead of this one; the resolved"
                + " target is recorded on the entry and the target is shielded. Drain that saga"
                + " with replayAll(sagaId) — the entry folds in at its causal position.",
            offset.value(),
            LogSanitizer.sanitizeForLog(sagaId.value()));
        return new Drained(record(ReplayOutcome.TARGET_PENDING), startPath, sagaId);
      }
    }
    if (row.isPresent() && row.get().status().isTerminal()) {
      sagaDeadLetterStore.discard(entrySagaId, sagaType(), offset);
      LOG.info(
          "Saga {} is already terminal; the dead-letter entry at offset {} is moot and was"
              + " discarded.",
          LogSanitizer.sanitizeForLog(sagaId.value()),
          offset.value());
      return new Drained(record(ReplayOutcome.REPLAYED), startPath, sagaId);
    }
    if (!startPath
        && (row.isEmpty()
            || (!row.get().genesisApplied()
                && row.get().effectiveStatus() != SagaStatus.COMPENSATING))) {
      recordDeferred();
      return new Drained(ReplayOutcome.SAGA_ROW_PENDING, false, sagaId);
    }
    if (row.isPresent()
        && row.get().status() == SagaStatus.FAULTED
        && row.get().preFaultStatus() == SagaStatus.COMPENSATING) {
      // The faulted episode's stamp is required before any anchor, feed or write — and
      // whatever force says: force acknowledges at-least-once over KNOWN keys, never a guessed key.
      Instant claim = requireStampedClaim(sagaId, row.get());
      if (!force && inboxRetentionMaxAge != null && claimOutlivedTheInboxWindow(claim)) {
        LOG.warn(
            "Refusing saga dead-letter replay for saga {} at offset {}: the saga faulted"
                + " mid-compensation and its episode claim ({}) is older than the command-inbox"
                + " retention window ({}); a resume could re-execute an already-succeeded"
                + " compensation (double refund). Reconcile the partial compensation manually,"
                + " then replay(..., force=true) if it is safe — a forced resume re-dispatches the"
                + " episode at-least-once, and any compensation whose dedup key was pruned executes"
                + " again.",
            LogSanitizer.sanitizeForLog(sagaId.value()),
            offset.value(),
            claim,
            inboxRetentionMaxAge);
        return new Drained(record(ReplayOutcome.STALE_COMPENSATION_BLOCKED), startPath, sagaId);
      }
    }
    Instant anchor = entry.firstReplayStartedAt();
    if (anchor == null) {
      anchor = clock.instant();
      sagaDeadLetterStore.establishFirstReplayAnchor(entrySagaId, sagaType(), offset, anchor);
    }
    if (!force
        && inboxRetentionMaxAge != null
        && Duration.between(anchor, clock.instant()).compareTo(inboxRetentionMaxAge) > 0) {
      LOG.warn(
          "Refusing saga dead-letter re-drive for saga {} at offset {}: its first replay attempt"
              + " ({}) is older than the command-inbox retention window ({}), so a prior"
              + " attempt's forward command keys may already be pruned and a re-drive could"
              + " re-execute a committed command. Verify which commands executed, reconcile, then"
              + " replay(..., force=true) if it is safe.",
          LogSanitizer.sanitizeForLog(sagaId.value()),
          offset.value(),
          anchor,
          inboxRetentionMaxAge);
      return new Drained(record(ReplayOutcome.STALE_REDRIVE_BLOCKED), startPath, sagaId);
    }
    if (!startPath
        && row.isPresent()
        && row.get().lastAppliedOffset() != null
        && offset.value() < row.get().lastAppliedOffset()) {
      // Nothing held a later event behind this entry (a null-saga entry, or one deferred until the
      // genesis landed): evolve receives it after an event it already applied. Fed, but not
      // silently. Equality is not a later event: it is this entry's own event, applied by a drain
      // that crashed before the discard, and the feed below dedups it.
      LOG.warn(
          "Saga dead-letter entry at offset {} for saga {} is fed out of order: the saga already"
              + " applied a later event (last_applied_offset {}), so evolve receives this one after"
              + " it. Keep evolve tolerant of a late event (saga guide, Out-of-order feeds).",
          offset.value(),
          LogSanitizer.sanitizeForLog(sagaId.value()),
          row.get().lastAppliedOffset());
    }
    StepOutcome fed = sagaRunner.feedReplay(sagaId, event.get(), startPath, force);
    if (fed == StepOutcome.FAULTED) {
      return new Drained(record(ReplayOutcome.STILL_POISON), startPath, sagaId);
    }
    if (fed == StepOutcome.SKIPPED_DEDUP || fed == StepOutcome.SKIPPED_HALTED) {
      // A moot feed (the genesis is already applied, the exact replay offset
      // already committed, or the row went terminal underneath) can never make progress, whatever
      // the row's FAULTED status says — it was faulted by a DIFFERENT event, and classifying this
      // entry STILL_POISON would wedge the drain on an entry no drain can ever consume (the
      // rule for resumes that made no durable progress does not cover moot feeds). The
      // entry is discarded and reported REPLAYED; the drain proceeds to the entry that matters.
      sagaDeadLetterStore.discard(entrySagaId, sagaType(), offset);
      LOG.info(
          "Saga dead-letter entry at offset {} for saga {} is moot ({}: already applied or the"
              + " saga is halted for another reason); discarded.",
          offset.value(),
          LogSanitizer.sanitizeForLog(sagaId.value()),
          fed);
      return new Drained(record(ReplayOutcome.REPLAYED), startPath, sagaId);
    }
    if (rowStillFaulted(sagaId, fed)) {
      return new Drained(record(ReplayOutcome.STILL_POISON), startPath, sagaId);
    }
    sagaDeadLetterStore.discard(entrySagaId, sagaType(), offset);
    return new Drained(record(ReplayOutcome.REPLAYED), startPath, sagaId);
  }

  /**
   * A replay resume that ended non-terminal WITHOUT a durable write (retry-later before the first
   * compensation CAS, a refused claim) leaves the row FAULTED — only a successful CAS clears a
   * fault and the replayer never un-faults. The entry stays so the next drain retries; the sweeper
   * sees only COMPENSATING rows and the timeout runner excludes halted rows, so neither is a second
   * actor on the episode.
   */
  private boolean rowStillFaulted(SagaId sagaId, StepOutcome fed) {
    Optional<? extends LoadedSaga<?>> after = sagaStore.load(sagaId, sagaType(), stateType());
    if (after.isEmpty() || after.get().status() != SagaStatus.FAULTED) {
      return false;
    }
    LOG.warn(
        "saga {}: replay resume made no durable progress (outcome {}); entry kept, the next"
            + " drain retries",
        LogSanitizer.sanitizeForLog(sagaId.value()),
        fed);
    return true;
  }

  /**
   * The key-age anchor of a faulted compensation episode: the durable, fault-cycle-immutable {@code
   * episodeClaimedAt} — the same anchor as the executor, the sweeper and the timeout runner, and
   * nothing else. The ONE helper both replayer paths use (the drain's stale-compensation refusal
   * and {@link #resumeFaulted}), so they cannot anchor an episode differently.
   *
   * <p><b>No fallback .</b> Every write that enters {@code COMPENSATING} stamps the episode — the
   * claim writes and, for an evolved {@code SagaState.status()} of {@code COMPENSATING}, the
   * forward path's own {@code applyEvent} / {@code createGenesisPending} (the episode stamp rule,
   * {@link SagaStore#claimCompensating}) — and a fault keeps the stamp, so a faulted compensation
   * episode without {@code episodeVersion} and {@code episodeClaimedAt} is a store contract
   * violation. The old {@code updatedAt} fallback measured such an episode from its last write —
   * the fault — and the executor keyed it by the row version, so a committed compensation
   * re-executed after a fault→replay cycle. It is refused instead: thrown here, before any anchor,
   * feed or write, and whatever {@code force} says.
   */
  private static Instant requireStampedClaim(SagaId sagaId, LoadedSaga<?> row) {
    if (row.episodeVersion() == null || row.episodeClaimedAt() == null) {
      var unstamped = SagaUnstampedCompensationEpisodeException.ofRow(sagaId, row);
      LOG.warn(
          "Refusing to resume the faulted compensation episode of saga {}: {}",
          LogSanitizer.sanitizeForLog(sagaId.value()),
          unstamped.getMessage());
      throw unstamped;
    }
    return row.episodeClaimedAt();
  }

  /**
   * Stale once older than the window; the claim is never null here ({@link #requireStampedClaim}).
   */
  private boolean claimOutlivedTheInboxWindow(Instant claim) {
    return Duration.between(claim, clock.instant()).compareTo(inboxRetentionMaxAge) > 0;
  }

  /**
   * The shield clear is the STORE's conditional clear ({@link
   * SagaDeadLetterStore#clearShieldIfDrained}): the store clears only where no named entry of this
   * type and no null-saga entry resolved to the saga exist AT THE STORE, serialized against the
   * runner's hold (Postgres: a row lock, then a fresh-snapshot conditional UPDATE; in-memory: the
   * entries monitor). The replayer never observes-then-writes and never writes {@code false}
   * itself. A store that cannot answer never clears: the SPI default throws, and this WARNs and
   * leaves the shield.
   */
  private void clearShieldIfDrained(SagaId sagaId) {
    try {
      sagaDeadLetterStore.clearShieldIfDrained(sagaId, sagaType());
    } catch (UnsupportedOperationException storeCannot) {
      LOG.warn(
          "Saga dead-letter drain for saga "
              + LogSanitizer.sanitizeForLog(sagaId.value())
              + ": this SagaDeadLetterStore does not implement clearShieldIfDrained, so the saga's"
              + " dead_letter_pending shield is never cleared by this replayer (a store that"
              + " cannot answer never clears) and its live correlated events stay held. Implement"
              + " it (see PostgresSagaDeadLetterStore / InMemorySagaDeadLetterStore).",
          storeCannot);
    }
  }

  private Optional<Long> oldestPendingOffset(SagaId sagaId) {
    Optional<Long> named =
        sagaDeadLetterStore.findBySaga(sagaId).stream()
            .filter(this::ownedByThisType)
            .map(e -> e.eventOffset().value())
            .min(Long::compare);
    Optional<Long> resolved =
        resolvedNullSagaEntriesFor(sagaId).orElse(List.of()).stream()
            .map(e -> e.eventOffset().value())
            .min(Long::compare);
    return Stream.of(named, resolved).flatMap(Optional::stream).min(Long::compare);
  }

  private Optional<EventEnvelope> readEvent(GlobalOffset offset) {
    List<EventEnvelope> read = eventStore.readGlobalStream(GlobalOffset.of(offset.value() - 1), 1);
    if (read.isEmpty() || !read.get(0).globalOffset().equals(offset)) {
      return Optional.empty();
    }
    return Optional.of(read.get(0));
  }

  private void logDrainStopped(SagaId sagaId, DrainItem item, ReplayOutcome outcome, int left) {
    LOG.warn(
        "Saga dead-letter drain for saga {} stopped at the entry for offset {} with outcome {}:"
            + " its event was NOT consumed and {} entr{} stay quarantined behind it. They are"
            + " deliberately not applied ahead of a blocker (feeding a later event to a saga that"
            + " never consumed an earlier one corrupts its state). The saga's shield keeps holding"
            + " live correlated events. Clear the blocker (fix the orchestrator, discard an"
            + " unreadable entry, or force a stale re-drive after reconciling) and re-run"
            + " replayAll.",
        LogSanitizer.sanitizeForLog(sagaId.value()),
        item.entry().eventOffset().value(),
        outcome,
        left,
        left == 1 ? "y" : "ies");
  }

  private Optional<SagaDeadLetterEntry> findEntry(SagaId sagaId, GlobalOffset eventOffset) {
    if (sagaId != null) {
      return sagaDeadLetterStore.findBySaga(sagaId).stream()
          .filter(e -> e.eventOffset().equals(eventOffset))
          .filter(this::ownedByThisType)
          .findFirst();
    }
    return sagaDeadLetterStore.findNullSagaEntry(sagaType(), eventOffset);
  }

  private boolean ownedByThisType(SagaDeadLetterEntry entry) {
    return sagaType().equals(entry.sagaType());
  }

  private boolean targetOwnsOwnTypeDeadLetterRecord(SagaId target) {
    return sagaDeadLetterStore.findBySaga(target).stream().anyMatch(this::ownedByThisType);
  }

  /**
   * Empty Optional = this store cannot list resolved entries (fold impossible, shield never
   * cleared).
   */
  private Optional<List<SagaDeadLetterEntry>> resolvedNullSagaEntriesFor(SagaId sagaId) {
    try {
      return Optional.of(
          sagaDeadLetterStore.findNullSagaEntriesByResolvedTarget(sagaType(), sagaId));
    } catch (UnsupportedOperationException storeCannot) {
      LOG.warn(
          "Saga dead-letter drain for saga "
              + LogSanitizer.sanitizeForLog(sagaId.value())
              + ": this SagaDeadLetterStore does not implement"
              + " findNullSagaEntriesByResolvedTarget, so null-saga entries resolved to this saga"
              + " cannot be folded in at their causal position and the saga's"
              + " shield is never cleared by this replayer. Implement it (see"
              + " PostgresSagaDeadLetterStore / InMemorySagaDeadLetterStore).",
          storeCannot);
      return Optional.empty();
    }
  }

  private Class<? extends SagaState> stateType() {
    return sagaRunner.stateType();
  }

  private SagaType sagaType() {
    return SagaType.fromClass(stateType());
  }

  private ReplayOutcome record(ReplayOutcome outcome) {
    try {
      metrics.recordSagaReplayed(sagaType().value(), outcome.name());
    } catch (RuntimeException e) {
      LOG.warn("Metrics recording failed for recordSagaReplayed", e);
    }
    return outcome;
  }

  private ResumeOutcome recordResume(ResumeOutcome outcome) {
    try {
      metrics.recordSagaResumeFaulted(sagaType().value(), outcome.name());
    } catch (RuntimeException e) {
      LOG.warn("Metrics recording failed for recordSagaResumeFaulted", e);
    }
    return outcome;
  }

  private CompensateOutcome recordCompensate(CompensateOutcome outcome) {
    try {
      metrics.recordSagaCompensateFaulted(sagaType().value(), outcome.name());
    } catch (RuntimeException e) {
      LOG.warn("Metrics recording failed for recordSagaCompensateFaulted", e);
    }
    return outcome;
  }

  private void recordDeferred() {
    try {
      metrics.recordSagaReplayDeferred(sagaType().value());
    } catch (RuntimeException e) {
      LOG.warn("Metrics recording failed for recordSagaReplayDeferred", e);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link SagaDeadLetterReplayer}. */
  public static final class Builder {
    private SagaDeadLetterStore sagaDeadLetterStore;
    private SagaStore sagaStore;
    private EventStore eventStore;
    private SagaRunner<?> sagaRunner;
    private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;
    private Duration inboxRetentionMaxAge;
    private Clock clock = Clock.systemUTC();

    public Builder sagaDeadLetterStore(SagaDeadLetterStore sagaDeadLetterStore) {
      this.sagaDeadLetterStore = sagaDeadLetterStore;
      return this;
    }

    public Builder sagaStore(SagaStore sagaStore) {
      this.sagaStore = sagaStore;
      return this;
    }

    public Builder eventStore(EventStore eventStore) {
      this.eventStore = eventStore;
      return this;
    }

    public Builder sagaRunner(SagaRunner<?> sagaRunner) {
      this.sagaRunner = sagaRunner;
      return this;
    }

    /** Sets the metrics collector. Optional; defaults to {@link StreamRuneMetrics#NOOP}. */
    public Builder metrics(StreamRuneMetrics metrics) {
      this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
      return this;
    }

    /**
     * Sets the command-inbox retention window (i.e. {@code streamrune.inbox.retention-max-age}),
     * which arms the two key-age refusals of a replay and {@link #resumeFaulted}'s {@link
     * ResumeOutcome#STALE_COMPENSATION_BLOCKED}. {@link ReplayOutcome#STALE_COMPENSATION_BLOCKED}:
     * an entry whose saga row is {@code FAULTED} with {@code preFaultStatus == COMPENSATING} is
     * refused rather than silently resumed unless the episode's inbox dedup keys are provably
     * younger than this window — measured from the row's episode claim instant ({@code
     * episodeClaimedAt}, the earliest possible key write; every write that enters {@code
     * COMPENSATING} stamps it, and a row in an episode without it is refused with {@link
     * SagaUnstampedCompensationEpisodeException} rather than measured from {@code updatedAt}). A
     * succeeded compensation's inbox dedup key may already have been pruned past that age, so a
     * resume could re-execute it (double refund). {@link ReplayOutcome#STALE_REDRIVE_BLOCKED}: a
     * re-drive is refused once the entry's first replay attempt ({@link
     * SagaDeadLetterEntry#firstReplayStartedAt()}) is older than this window. Operators SHOULD
     * configure this to match their deployment's inbox retention so the guards are active; when
     * {@code null} (the default) the replayer cannot reason about key staleness and both guards are
     * inert.
     *
     * <p><b>Zero/negative normalizes to inert</b>: that value means inbox pruning itself is
     * disabled — dedup keys are never swept, so they can never go stale and there is nothing for
     * the staleness guards to protect against. One convention across every consumer of this knob
     * ({@code SagaCompensationRetrySweeper}, {@code SagaTimeoutRunner}, {@code
     * InboxRetentionSweeper}, {@code SagaRetentionValidator}): pass your deployment's raw {@code
     * streamrune.inbox.retention-max-age} value verbatim, disabled-pruning included.
     */
    public Builder inboxRetentionMaxAge(Duration inboxRetentionMaxAge) {
      this.inboxRetentionMaxAge = inboxRetentionMaxAge;
      return this;
    }

    /**
     * Clock for the key-age refusals (a replay's two and {@link #resumeFaulted}'s {@link
     * ResumeOutcome#STALE_COMPENSATION_BLOCKED}) and for stamping an entry's first-replay anchor.
     * Optional; defaults to {@link Clock#systemUTC}.
     */
    public Builder clock(Clock clock) {
      this.clock = clock != null ? clock : Clock.systemUTC();
      return this;
    }

    public SagaDeadLetterReplayer build() {
      Objects.requireNonNull(sagaDeadLetterStore, "sagaDeadLetterStore is required");
      Objects.requireNonNull(sagaStore, "sagaStore is required");
      Objects.requireNonNull(eventStore, "eventStore is required");
      Objects.requireNonNull(sagaRunner, "sagaRunner is required");
      return new SagaDeadLetterReplayer(this);
    }
  }
}
