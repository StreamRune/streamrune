package org.streamrune.core.saga;

import java.time.Instant;

/**
 * A saga loaded from the {@link SagaStore}: the deserialized domain {@code state}, the
 * framework-owned, authoritative lifecycle {@code status} from the store row, the {@code version}
 * CAS token for {@link SagaStore#update}, and the {@code updatedAt} instant the row was last
 * written. The engine uses {@code status} (not {@code state.status()}) for terminal / skip
 * decisions, because the framework may persist a terminal status the domain state does not reflect
 * (e.g. {@code COMPENSATED} after a compensation the orchestrator's own evolve never saw).
 *
 * <p>{@code updatedAt} is the row's last-write instant. For a {@code COMPENSATING} episode it
 * usually coincides with the claim instant (a transient-failure re-drive leaves the row untouched)
 * — but NOT across a fault→replay cycle: {@link SagaStore#markFaulted} and the replayed step's CAS
 * write that clears the fault ({@link SagaStore#applyEvent} / {@link SagaStore#update}) both
 * refresh it. {@code SagaTimeoutRunner}'s max-COMPENSATING dwell bound therefore measures from
 * {@code episodeClaimedAt} below and never from {@code updatedAt}; {@code updatedAt} still drives
 * the {@code COMPENSATING} re-pick spacing in {@code SagaStore#findTimedOut}.
 *
 * <p>{@code episodeVersion} is the durable compensation-episode identity: the row version at which
 * the saga entered its {@code COMPENSATING} episode, stamped by the write that entered it — {@link
 * SagaStore#claimCompensating}, or {@link SagaStore#applyEvent} / {@link
 * SagaStore#createGenesisPending} / {@link SagaStore#create} / {@link SagaStore#update} landing an
 * evolved {@code COMPENSATING} (the episode stamp rule, see {@link SagaStore#claimCompensating}) —
 * and deliberately left untouched by every other write — {@code markFaulted} and the CAS writes,
 * terminal ones included. The engine derives the compensation command idempotency key from this
 * value, with no fallback, so a replayed resume — whose row {@code version} was bumped by an
 * intervening fault→replay cycle — still re-derives the original episode key and an
 * already-executed compensation dedups in the command inbox instead of double-executing (e.g. a
 * double refund). {@code null} only for a row that has never entered a compensation episode: a
 * {@code COMPENSATING} row, or a {@code FAULTED} row whose {@code preFaultStatus} is {@code
 * COMPENSATING}, without it violates the {@link SagaStore} contract, and the runtime refuses the
 * episode with {@link SagaUnstampedCompensationEpisodeException} rather than guess the key from
 * {@code version}: a missing stamp is refused, never guessed.
 *
 * <p>{@code episodeClaimedAt} is the durable claim <em>instant</em> of the current compensation
 * episode — the earliest instant any of the episode's command-inbox dedup keys can have been
 * written (the first compensation command dispatches right after the claim). Stamped by the same
 * claim writes that stamp {@code episodeVersion} and, exactly like it, deliberately left untouched
 * by every other write, so it never resets across a fault→replay cycle. {@code
 * SagaDeadLetterReplayer}'s stale-compensation guard anchors on it: once {@code now -
 * episodeClaimedAt} exceeds the command-inbox retention window, the dedup keys of the episode's
 * succeeded compensations may already be pruned and a resume could re-execute them (a double
 * refund). {@code null} only for a row that has never entered a compensation episode — exactly as
 * {@code episodeVersion}, and refused the same way when it is missing on one that has: the guards
 * have no {@code updatedAt} fallback, because a fault refreshes {@code updatedAt} and would make a
 * stale episode look fresh.
 *
 * <p>{@code genesisApplied} records whether the start event's forward step (evolve → handle →
 * dispatch loop) has committed: {@code false} for a row inserted by {@link
 * SagaStore#createGenesisPending} (the executor's actual create-first write) until {@link
 * SagaStore#applyEvent} is called with a start-path {@link SagaStore.AppliedEvent} ({@code
 * startPath = true}: {@link SagaStore.AppliedEvent#liveStart} or {@link
 * SagaStore.AppliedEvent#replayedStart}), which sets it {@code true} and never clears it again;
 * {@link SagaStore#create} — the tooling/test insert — produces a complete row with it already
 * {@code true}. Three things read this flag instead of inferring it from row version or presence:
 * the executor's start-path dedup (a replayed START entry over an applied genesis is {@code
 * SKIPPED_DEDUP}), {@code SagaRunner}'s live hold ({@code !genesisApplied} holds a correlated event
 * as {@code GENESIS_PENDING}), and {@code SagaDeadLetterReplayer}'s {@code SAGA_ROW_PENDING}
 * deferral. ({@code SagaTimeoutRunner}'s {@code findTimedOut} does NOT read this flag — its
 * visibility into an in-flight genesis comes from row presence and status alone, since {@link
 * SagaStore#createGenesisPending} inserts the row already {@code STARTED}/{@code RUNNING}.)
 *
 * <p>{@code preFaultStatus} is the status the saga was in immediately before the fault that
 * produced the current {@code FAULTED} row — set by {@link SagaStore#markFaulted} to the status
 * being replaced (kept, not overwritten, when the row was already FAULTED — a still-poison
 * refresh), and cleared to {@code null} by every other CAS write. It is the recorded, authoritative
 * answer to what a replay resumes from, read via {@link #effectiveStatus()}: {@code null} only for
 * a genesis-pending FORWARD fault, where there is nothing to resume and a replayed start entry
 * re-evolves from {@code initialState} — a claimed {@code COMPENSATING} episode is recorded
 * whatever {@link #genesisApplied()} says (it resumes, never re-drives forward).
 *
 * <p>{@code deadLetterPending} is the durable dead-letter shield: {@code true} whenever this saga
 * owns a dead-letter entry (set with every entry recorded for a named saga — {@link
 * SagaDeadLetterStore#publishShielded} writes the entry and the shield as one durable write), and
 * cleared only by the dead-letter store's conditional clear, {@link
 * SagaDeadLetterStore#clearShieldIfDrained}, which clears it only where no named entry of the
 * saga's type and no null-saga entry resolved to the saga remain; the replayer invokes it and never
 * observes-then-writes the flag. While {@code true}, live correlated events are held (quarantined)
 * rather than applied, so a saga's backlog drains oldest-first ahead of its own live traffic. Two
 * entries have nothing held behind them — a null-saga entry (its saga was unknown when it was
 * quarantined) and a correlated entry deferred until the genesis lands — so one may be fed after a
 * later event was applied; the replayer logs a WARNING when it does (see the saga guide's
 * out-of-order feeds).
 *
 * <p>{@code lastAppliedOffset} is the highest {@link org.streamrune.core.types.GlobalOffset} value
 * ever applied to this saga — maintained by every {@link SagaStore#applyEvent} call, live or
 * replayed, as {@code GREATEST(existing, eventOffset)} and never lowered (a replay drain that later
 * feeds an older deferred entry must not lower it). Its use, not its write, is live-path-only: it
 * is the live-path redelivery dedup — a live correlated event at or below this offset is a
 * redelivery.
 *
 * <p>{@code lastReplayedOffset} is the offset of the last event applied by a dead-letter replay
 * feed — set exactly (not a maximum) by {@link SagaStore#applyEvent} when the {@link
 * SagaStore.AppliedEvent}'s {@code replayRedrive} is {@code true}, and left unchanged otherwise. It
 * is the replay drain's feed→discard crash dedup: a crash between a replay's committed CAS and its
 * entry discard re-feeds the same event, which this offset lets the executor recognize and skip
 * rather than re-apply.
 *
 * @param <S> the saga state type
 */
public record LoadedSaga<S extends SagaState>(
    S state,
    SagaStatus status,
    long version,
    Instant updatedAt,
    Long episodeVersion,
    Instant episodeClaimedAt,
    boolean genesisApplied,
    SagaStatus preFaultStatus,
    boolean deadLetterPending,
    Long lastAppliedOffset,
    Long lastReplayedOffset) {

  /**
   * The status a step resumes from: the recorded {@link #preFaultStatus()} while FAULTED, the
   * status itself otherwise. {@code null} only for a genesis-pending FORWARD fault (nothing to
   * resume — a replayed start entry re-evolves from {@code initialState}); a claimed {@code
   * COMPENSATING} episode is recorded whatever {@link #genesisApplied()} says.
   */
  public SagaStatus effectiveStatus() {
    return status == SagaStatus.FAULTED ? preFaultStatus : status;
  }
}
