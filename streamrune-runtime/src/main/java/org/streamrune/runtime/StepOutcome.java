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
  SKIPPED_HALTED,
  SKIPPED_DEDUP,
  /**
   * A correlated {@code ForwardStep} on an absent row — the executor never creates rows for
   * correlated events; the runner holds or drops such events before they reach it.
   */
  SKIPPED_NO_ROW,
  NOT_COMPENSATING
}
