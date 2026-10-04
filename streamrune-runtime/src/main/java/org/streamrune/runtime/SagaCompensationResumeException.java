package org.streamrune.runtime;

/**
 * Internal marker failure passed to {@link org.streamrune.core.saga.SagaOrchestrator#compensate}
 * (or {@link org.streamrune.core.saga.SagaDecider#compensate}) when the correlated-event path
 * <em>resumes</em> a compensation episode that a prior owner left in {@link
 * org.streamrune.core.saga.SagaStatus#COMPENSATING} after crashing between the claim and the
 * terminal write.
 *
 * <p>The original triggering failure did not survive the crash, so it cannot be replayed. This
 * marker is deliberately <em>not</em> a {@link org.streamrune.core.saga.SagaTimeoutException}: a
 * resume of an event-path (forward-command) compensation episode must land in the same non-timeout
 * branch the original attempt took, and compensation must be recomputed from the persisted saga
 * state — exactly as the {@code SagaTimeoutRunner} resume already relies on. Compensation logic
 * that is a deterministic function of state (the documented contract) recomputes the identical
 * command list, so re-dispatch under the shared episode-scoped idempotency key is effectively-once.
 *
 * <p><b>{@code compensate()} must ignore this argument entirely.</b> The failure/marker object
 * passed to {@code compensate} is diagnostic only (safe to log) and must never drive a branch in
 * compensation logic — {@code compensate} must be a pure function of saga state. For the very same
 * {@code COMPENSATING} episode, the object landing in that parameter can differ depending on which
 * path resumes it: a re-picked {@code SagaTimeoutRunner} passes a fresh {@link
 * org.streamrune.core.saga.SagaTimeoutException}, while a redelivered correlated event on the event
 * path passes this marker instead — the two are not interchangeable signals, they are simply
 * whatever each resuming path happens to construct.
 *
 * <p>Package-private on purpose: orchestrators receive it only as an opaque {@link Throwable}.
 * There is nothing actionable to branch on — the correct behaviour is to compensate from state.
 */
final class SagaCompensationResumeException extends RuntimeException {

  SagaCompensationResumeException() {
    super(
        "Resuming a compensation episode left COMPENSATING by a crashed owner; the original"
            + " triggering failure is not recoverable — compensate from the persisted saga state.");
  }
}
