package org.streamrune.core.saga;

/**
 * Lifecycle states of a saga instance.
 *
 * <ul>
 *   <li>{@link #STARTED} — initial state after the start event is processed.
 *   <li>{@link #RUNNING} — one or more steps are in progress.
 *   <li>{@link #COMPLETED} — all steps succeeded; terminal state.
 *   <li>{@link #COMPENSATING} — claimed by {@code SagaTimeoutRunner} (timeout path) or by {@code
 *       SagaRunner}'s claim-first event-path compensation, to claim exclusive ownership of a
 *       compensation episode <em>before</em> any compensation command is dispatched (a CAS write
 *       from the pre-compensation status). This is not merely descriptive: it is the ownership
 *       handoff that prevents the event path from concurrently re-running forward {@code
 *       evolve}/{@code handle} on a saga whose compensation is in flight — a correlated event
 *       redelivered while a saga is {@code COMPENSATING} instead RESUMES the existing compensation
 *       episode (re-driving it at the existing version, under the shared episode-scoped key; see
 *       {@code SagaRunner}'s explicit status check and {@code resumeCompensation}) rather than
 *       being skipped — and the claim-first ordering eliminates orphaned compensation (a lost
 *       terminal-write race can no longer follow an already-dispatched side effect, because the
 *       claim happens first). {@code COMPENSATING} is intentionally still selected by {@code
 *       SagaStore.findTimedOut} (i.e. it is <em>not</em> {@linkplain #isHalted() halted}) and is
 *       not terminal-guarded by {@code SagaStore.update}: a crashed compensation episode is
 *       re-picked by {@code SagaTimeoutRunner} once another timeout interval elapses, OR re-driven
 *       immediately by the event path on the next redelivered correlated event (the only recovery
 *       path for a saga with no timeout configured) — both resume with the same episode-scoped
 *       idempotency keys (effectively-once redispatch via the command inbox) rather than wedging
 *       forever.
 *   <li>{@link #COMPENSATED} — all compensation steps completed; terminal state.
 *   <li>{@link #FAILED} — unrecoverable failure; terminal state.
 *   <li>{@link #FAULTED} — the saga has been quarantined: by a poison event (e.g. repeated
 *       deserialization or handler failures), which leaves a dead-letter entry, or by the
 *       compensation-retry sweeper or the timeout runner giving up on a compensation episode, which
 *       leaves none. It is halted: no further events are processed and it is excluded from timeout
 *       sweeps. Unlike terminal statuses it is <em>not</em> protected by the {@code
 *       SagaStore.update} terminal guard: {@code SagaDeadLetterReplayer.replayAll} drains a poison
 *       fault, {@code SagaDeadLetterReplayer.resumeFaulted} resumes a give-up fault, and {@code
 *       SagaDeadLetterReplayer.compensateFaulted} compensates a forward fault whose dead-letter
 *       entry was discarded.
 * </ul>
 */
public enum SagaStatus {
  STARTED,
  RUNNING,
  COMPLETED,
  COMPENSATING,
  COMPENSATED,
  FAILED,
  FAULTED;

  /** Terminal statuses: the saga is finished and must not be processed further. */
  public boolean isTerminal() {
    return this == COMPLETED || this == COMPENSATED || this == FAILED;
  }

  /**
   * Halted statuses: the saga must not process further events or appear in timeout sweeps. This
   * includes all {@linkplain #isTerminal() terminal} statuses plus {@link #FAULTED} (quarantined).
   * Unlike terminal statuses, {@code FAULTED} is not protected by the {@code SagaStore.update}
   * terminal guard (which rejects writes to {@code COMPLETED}/{@code COMPENSATED}/{@code FAILED}
   * rows) — a replay or manual intervention may transition a faulted saga.
   *
   * <p>{@link #COMPENSATING} is deliberately <em>not</em> halted: it must still be returned by
   * {@code SagaStore.findTimedOut} so a crashed compensation episode is re-picked once another
   * timeout interval elapses (crash-recovery — see {@link #COMPENSATING}'s class-doc entry). The
   * event path separately handles a {@code COMPENSATING} saga via an explicit status check in
   * {@code SagaRunner} (not via {@code isHalted}, since that check must not also affect
   * timeout-sweep eligibility) — it does NOT refuse/skip the saga on redelivery, it RESUMES the
   * in-flight compensation episode (re-driving it at the existing version under the shared
   * episode-scoped key), which is what lets a saga with no {@code SagaTimeoutRunner} configured
   * still reach a terminal status after its compensation owner crashes.
   */
  public boolean isHalted() {
    return isTerminal() || this == FAULTED;
  }
}
