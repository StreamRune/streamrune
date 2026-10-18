package org.streamrune.runtime;

import org.streamrune.core.OptimisticLockException;

/**
 * A saga step lost the compare-and-swap of its saga row to another writer: the row it read was
 * written, or removed, before its own write. {@link SagaRunner} runs such a step again on the row
 * that won; any other {@link OptimisticLockException} — a forward command's own version conflict —
 * leaves the step as it is.
 */
final class SagaRowConflictException extends OptimisticLockException {

  SagaRowConflictException(OptimisticLockException lost) {
    super(lost.getMessage(), lost);
  }

  SagaRowConflictException(String message) {
    super(message);
  }
}
