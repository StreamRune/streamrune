package org.streamrune.runtime;

/**
 * What the span {@code saga/{SagaType}/compensate} records for a compensation command its domain
 * rejected: the failure of the compensation, with the {@link org.streamrune.core.DomainException}
 * as its cause. It has no message, and the span names its cause by type and frames alone, so the
 * rejection's message, which can quote what the command carried, is not exported. Never thrown.
 */
final class CompensationRejectedException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  CompensationRejectedException(RuntimeException rejection) {
    super(null, rejection, false, false);
  }
}
