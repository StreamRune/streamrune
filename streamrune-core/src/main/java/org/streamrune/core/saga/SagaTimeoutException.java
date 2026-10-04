package org.streamrune.core.saga;

import java.time.Duration;

/**
 * Thrown when a saga exceeds its configured {@link SagaDecider#timeout() timeout} duration. Passed
 * to {@link SagaDecider#compensate} as the {@code failure} argument on the timeout path.
 *
 * <p>This is diagnostic only — see {@link SagaDecider#compensate}'s contract: implementations must
 * ignore the {@code failure} argument entirely and compute compensation purely from {@code state},
 * not by branching on whether this exception (vs. a real command failure, vs. an internal
 * crash-resume marker) was passed in.
 */
public final class SagaTimeoutException extends RuntimeException {

  private final SagaId sagaId;
  private final Duration timeout;

  public SagaTimeoutException(SagaId sagaId, Duration timeout) {
    super("Saga '%s' timed out after %s".formatted(sagaId.value(), timeout));
    this.sagaId = sagaId;
    this.timeout = timeout;
  }

  public SagaId sagaId() {
    return sagaId;
  }

  public Duration timeout() {
    return timeout;
  }
}
