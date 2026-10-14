package org.streamrune.core.saga;

import java.util.Objects;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;

/**
 * A saga's forward command refused at its commit: the saga left its forward phase.
 *
 * <p>An event store that fences a saga's forward command ({@code EventStore.appendWithKey} with a
 * {@link SagaOrigin}) reads the saga's row inside the append transaction, under a lock that every
 * write which opens a compensation episode or ends the saga conflicts with, and throws this when
 * the row carries an episode stamp, is terminal, or is gone. The transaction is rolled back: none
 * of the command's events, its idempotency key, outbox or audit rows are stored, so a later call
 * under the same key runs again.
 *
 * <p>The refusal is final for that saga row: an episode stamp is never cleared and a terminal saga
 * never resumes. {@code CommandFailureClassification} therefore treats it as a permanent rejection
 * (no dead letter, no circuit-breaker count, no retry). It is not a {@code DomainException}: it is
 * a fact about the saga, not the target aggregate's answer to the command.
 *
 * <p>The saga id in the message goes through {@link LogSanitizer#sanitizeForLog(String)}; {@link
 * #sagaId()} keeps the raw value.
 */
public final class SagaForwardFencedException extends RuntimeException {

  private final transient SagaType sagaType;
  private final transient SagaId sagaId;

  /**
   * @param forwardOf the saga whose forward command is refused
   * @throws NullPointerException if {@code forwardOf} is {@code null}
   */
  public SagaForwardFencedException(SagaOrigin forwardOf) {
    super(message(Objects.requireNonNull(forwardOf, "forwardOf is required")));
    this.sagaType = forwardOf.sagaType();
    this.sagaId = forwardOf.sagaId();
  }

  private static String message(SagaOrigin forwardOf) {
    return "Forward command of saga "
        + forwardOf.sagaType().value()
        + " "
        + LogSanitizer.sanitizeForLog(forwardOf.sagaId().value())
        + " refused: the saga has left its forward phase (a compensation episode is open, the saga"
        + " is terminal, or its row is gone)";
  }

  /** The type of the saga whose forward command was refused. */
  public SagaType sagaType() {
    return sagaType;
  }

  /** The saga whose forward command was refused (raw, unsanitized). */
  public SagaId sagaId() {
    return sagaId;
  }
}
