package org.streamrune.core;

import org.streamrune.core.types.IdConstraints;

/**
 * A command or a query was dispatched under a correlation id in the reserved form ({@link
 * IdConstraints#RESERVED_PREFIX}) that is not the bound saga origin's.
 *
 * <p>A reserved correlation id is the framework's own. Saga dispatch binds it together with the
 * saga's origin, and {@link StreamRuneContext#continuing(EventEnvelope)} binds both for a
 * saga-caused event; nothing else binds one. Bound without that origin it was put there by
 * something else: a request context built with the plain constructor from a client's value, a
 * listener that copied a saga-caused event's correlation id into a context of its own instead of
 * continuing the event, a task that carried the context to another thread without the cause, a
 * binding of another saga's id beside this saga's origin. The shipped command and query buses
 * refuse such a dispatch before anything of it is looked up, authorized, audited, stored or
 * dead-lettered ({@link StreamRuneContext#requireNoForeignReservedCorrelationId()}).
 *
 * <p><b>Not an argument error and not a domain rejection.</b> This type extends neither {@link
 * IllegalArgumentException} nor {@link DomainException}, on purpose. A listener that treats a
 * rejection by the domain as its command's answer catches those two and moves on; this refusal is a
 * wiring error of the code that bound the context, the same on every attempt, and must not be taken
 * for an answer. Left uncaught it stops the caller: a request fails, a subscription delivers the
 * event again and reports the failure each time.
 *
 * <p>It is a permanent rejection for the dead letter queue and the circuit breaker ({@link
 * CommandFailureClassification#isPermanentRejection}): a retry or a replay under the same bindings
 * is refused again.
 *
 * <p>The message names the prefix and the two binders, and never the bound value.
 */
public final class ReservedCorrelationIdException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** Creates the refusal with its one fixed message. */
  public ReservedCorrelationIdException() {
    super(
        "the bound correlation id uses the reserved '"
            + IdConstraints.RESERVED_PREFIX
            + "' prefix and is not the bound saga origin's; only a saga dispatch or"
            + " StreamRuneContext.continuing binds it");
  }
}
