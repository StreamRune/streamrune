package org.streamrune.runtime;

/**
 * Internal marker failure passed to {@link org.streamrune.core.saga.SagaOrchestrator#compensate}
 * (or {@link org.streamrune.core.saga.SagaDecider#compensate}) when an operator compensates a saga
 * a forward step faulted ({@code SagaDeadLetterReplayer.compensateFaulted}): the event that faulted
 * it was never consumed and its dead-letter entry was discarded, so there is no failed command and
 * no triggering failure to pass on.
 *
 * <p><b>{@code compensate()} must ignore this argument entirely</b>, exactly as it ignores {@link
 * SagaCompensationResumeException} and a {@link org.streamrune.core.saga.SagaTimeoutException}: it
 * is diagnostic only (safe to log), and the episode it starts is re-driven by whichever path picks
 * it up next — the compensation-retry sweeper, the timeout runner or a correlated event — each
 * passing its own object here. Compensation is a pure function of the persisted saga state.
 *
 * <p>Package-private on purpose: orchestrators receive it only as an opaque {@link Throwable}.
 */
final class SagaForwardFaultCompensationException extends RuntimeException {

  SagaForwardFaultCompensationException() {
    super(
        "An operator compensated this saga after a forward step faulted it and its dead-letter"
            + " entry was discarded; there is no failed command — compensate from the persisted"
            + " saga state.");
  }
}
