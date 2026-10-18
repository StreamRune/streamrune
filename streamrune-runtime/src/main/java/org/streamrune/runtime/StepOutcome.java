package org.streamrune.runtime;

/** The result of executing a single saga step. */
enum StepOutcome {
  FORWARD_PROGRESSED,
  COMPLETED,
  COMPENSATING_LEFT,
  /**
   * The episode was left {@code COMPENSATING} because every failed compensation command was REFUSED
   * ADMISSION (open breaker, closing bus, interceptor veto) — nothing was attempted. Identical to
   * {@link #COMPENSATING_LEFT} for every consumer except the compensation-retry sweeper's give-up
   * bookkeeping, which must not count it as a re-drive attempt.
   */
  COMPENSATING_REFUSED,
  COMPENSATED,
  FAILED,
  FAULTED,
  CLAIM_LOST,
  /**
   * A claim with no event of its own — a timeout claim, an operator's compensation of a faulted
   * forward step — was not made because a step of the saga is in flight in this process ({@link
   * SagaStepLock}). Nothing was written and nothing was dispatched; the caller comes back.
   */
  CLAIM_DEFERRED,
  SKIPPED_HALTED,
  SKIPPED_DEDUP,
  /**
   * A correlated {@code ForwardStep} on an absent row — the executor never creates rows for
   * correlated events; the runner holds or drops such events before they reach it, and the replayer
   * defers the entry of a replay feed that meets it.
   */
  SKIPPED_NO_ROW,
  /**
   * A correlated replay feed over a row whose start step is not applied yet and which is not in a
   * compensation episode. The live path holds such an event ({@code GENESIS_PENDING}) and never
   * applies it before the genesis; the feed is refused the same way, in the executor, because the
   * row the executor reads can be a different row from the one the drain's pre-check saw (a row
   * deleted and inserted again for the saga id, between the drain's read and the feed's, or between
   * the attempts of a step run again). Nothing is dispatched and nothing is written; the replayer
   * defers the entry ({@code SAGA_ROW_PENDING}) and keeps it.
   */
  SKIPPED_GENESIS_PENDING,
  NOT_COMPENSATING
}
