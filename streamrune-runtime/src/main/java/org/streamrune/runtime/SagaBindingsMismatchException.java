package org.streamrune.runtime;

import java.io.Serial;

/**
 * The command bus refused a saga's command because the bindings it read under the saga's dispatch
 * name another saga than the one the dispatch was made for.
 *
 * <p>Saga dispatch binds the saga's correlation id, its origin and its markers as scoped values
 * around the command, and hands the saga to {@link VirtualThreadCommandBus} beside them, in a field
 * of the dispatching thread the bus reads once. The bus compares the two before it looks the
 * command up, authorizes, executes, stores or dead-letters anything, and refuses the command when
 * they differ: what it read is not what the dispatch bound. Nothing about the business was decided
 * and nothing was attempted, so the saga runner never compensates on it — like a {@code
 * ReservedCorrelationIdException} under a saga's own dispatch, a forward command refused this way
 * leaves the saga as it was and its event is delivered again, and a compensation command refused
 * this way keeps its episode open for the next re-drive. Both are logged at {@code ERROR}.
 *
 * <p>The message names neither saga; the bus's {@code WARN} line names what it read.
 */
public final class SagaBindingsMismatchException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  /** Creates the refusal with its one fixed message. */
  public SagaBindingsMismatchException() {
    super(
        "the bindings the command bus read under a saga's dispatch are not the ones the dispatch"
            + " made");
  }
}
