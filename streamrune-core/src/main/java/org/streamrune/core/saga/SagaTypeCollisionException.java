package org.streamrune.core.saga;

import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;

/**
 * Thrown when two <em>distinct</em> saga types attempt to use the same {@link SagaId}.
 *
 * <p>The saga store is keyed by {@code saga_id} alone. If an application defines two saga types
 * that both derive their id from the same business key (e.g. {@code SagaId.of(orderId)} for both an
 * {@code OrderFulfillmentSaga} and an {@code OrderRefundSaga}), the second type's start would
 * otherwise either silently read and CAS-overwrite the first type's persisted state (cross-type
 * corruption, if the JSON shapes happen to be compatible) or fail to deserialize it and wedge the
 * subscription forever with only a generic "Failed to load saga state". Neither is acceptable for a
 * money/GDPR-critical subsystem.
 *
 * <p>To make the misconfiguration loud and actionable, the framework's saga stores detect the
 * collision at {@code create} time and throw this exception — naming BOTH the type that already
 * owns the id and the type that tried to claim it — instead of the benign {@link
 * org.streamrune.core.OptimisticLockException} used for a same-type concurrent-create dedup.
 * Because it is <em>not</em> an {@code OptimisticLockException}, {@code SagaRunner} cannot
 * mis-swallow a genuine type collision as a spurious dedup; it propagates as an
 * infrastructure-class failure so an operator sees exactly which two types collided and can fix the
 * id-derivation scheme.
 *
 * <p>It extends {@link IllegalStateException} because a cross-type id collision is a programming /
 * configuration error, not a recoverable optimistic-lock conflict.
 *
 * <p>The message renders the saga id through {@link LogSanitizer#sanitizeForLog(String)}: the
 * subscription logs this failure, and the id is usually derived from correlated event data. {@link
 * #sagaId()} keeps the raw value.
 */
public final class SagaTypeCollisionException extends IllegalStateException {

  private final transient SagaId sagaId;
  private final transient SagaType existingType;
  private final transient SagaType attemptedType;

  /**
   * @param sagaId the colliding saga id
   * @param existingType the saga type that already owns {@code sagaId}
   * @param attemptedType the saga type that tried to claim the same {@code sagaId}
   */
  public SagaTypeCollisionException(SagaId sagaId, SagaType existingType, SagaType attemptedType) {
    super(
        "Saga id collision: '"
            + LogSanitizer.sanitizeForLog(sagaId.value())
            + "' is already owned by saga type '"
            + existingType.value()
            + "', but saga type '"
            + attemptedType.value()
            + "' tried to start/claim the same id. Two distinct saga types must not derive the same"
            + " SagaId — give each type a distinct id prefix (e.g. \"fulfillment-\" + orderId vs"
            + " \"refund-\" + orderId).");
    this.sagaId = sagaId;
    this.existingType = existingType;
    this.attemptedType = attemptedType;
  }

  /** The colliding saga id. */
  public SagaId sagaId() {
    return sagaId;
  }

  /** The saga type that already owns {@link #sagaId()}. */
  public SagaType existingType() {
    return existingType;
  }

  /** The saga type that attempted to claim the same {@link #sagaId()}. */
  public SagaType attemptedType() {
    return attemptedType;
  }
}
