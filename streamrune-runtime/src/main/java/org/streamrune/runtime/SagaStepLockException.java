package org.streamrune.runtime;

/**
 * A saga step did not get its saga's {@link SagaStepLock}: another step of the same saga stayed in
 * flight in this process for longer than the step bound, or the waiting thread was interrupted (its
 * interrupt status is left set). A claim that is interrupted while it waits throws it too.
 *
 * <p>Nothing was read and nothing was dispatched when it is thrown. It leaves the listener like an
 * infrastructure failure, so the subscription's checkpoint does not advance and the event is
 * delivered again; {@code SagaCommandDispatch.isRetryLater} counts it as retry-later evidence, so
 * it never starts a compensation should it surface from a forward command's dispatch.
 */
public class SagaStepLockException extends RuntimeException {

  public SagaStepLockException(String message) {
    super(message);
  }

  public SagaStepLockException(String message, Throwable cause) {
    super(message, cause);
  }
}
