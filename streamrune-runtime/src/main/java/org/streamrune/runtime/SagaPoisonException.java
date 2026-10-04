package org.streamrune.runtime;

import org.streamrune.core.saga.SagaId;

/**
 * Signals that an orchestrator pure-logic step threw an exception for a specific event, making that
 * event a "poison" event for this saga. The {@link SagaRunner} catches this signal per-event,
 * quarantines the event to the {@link org.streamrune.core.saga.SagaDeadLetterStore}, marks the saga
 * {@link org.streamrune.core.saga.SagaStatus#FAULTED}, and continues processing the next event in
 * the batch — eliminating head-of-line blocking.
 *
 * <p>Exceptions from orchestrator pure-logic methods ({@code isStartEvent}, {@code correlate},
 * {@code extractSagaId}, {@code evolve}, {@code compensate}, {@code handle}) are wrapped in this
 * exception. Infrastructure failures ({@code SagaStore} {@code load}/{@code create}/{@code update})
 * are NOT wrapped — they propagate out of {@code onEvents} so the subscription can retry.
 *
 * <p>The one non-orchestrator source is a <em>deterministic</em> {@link
 * org.streamrune.core.saga.SagaStateSerializationException}, wrapped by {@link
 * SagaStateConversion}: the store could not convert the saga state to or from its stored form, and
 * the cause chain proves a retry can never succeed. It shares this signal because it needs exactly
 * the same remedy — quarantine the one event, FAULT the one saga, keep the batch moving — and the
 * alternative (propagating it as infrastructure) retries an impossible conversion forever and stops
 * every saga on the subscription. A conversion failure that could be transient is never wrapped.
 *
 * <p>Package-private: only {@link SagaRunner} creates and consumes instances.
 */
final class SagaPoisonException extends RuntimeException {

  private final SagaId sagaId;

  /**
   * @param sagaId the saga that faulted, or {@code null} when the saga could not be identified
   *     before the exception occurred (e.g. {@code correlate} or {@code extractSagaId} threw)
   * @param cause the underlying orchestrator exception (required)
   */
  SagaPoisonException(SagaId sagaId, Throwable cause) {
    super(cause);
    this.sagaId = sagaId;
  }

  /** The faulted saga id, or {@code null} when the saga could not be identified. */
  SagaId sagaId() {
    return sagaId;
  }
}
