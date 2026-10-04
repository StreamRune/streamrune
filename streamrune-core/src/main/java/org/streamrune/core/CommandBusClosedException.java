package org.streamrune.core;

/**
 * Thrown when a command reaches a {@link CommandBus} whose {@code close()} has begun: the command
 * was <b>refused admission</b> — nothing was attempted, nothing was decided, no infrastructure was
 * touched. A graceful shutdown drains the in-flight commands it already admitted; this exception is
 * only ever the answer to work arriving <em>after</em> that point.
 *
 * <p><b>Why the dedicated type is load-bearing.</b> The closed-bus rejection used to be a bare
 * {@code IllegalStateException}, and the two saga dispatch paths read that opposite ways: the
 * forward classifier ({@code SagaCommandDispatch.isRetryLater}) could not prove it retriable, so a
 * node shutting down mid-flow <em>claimed a compensation episode and issued the undo</em> for a
 * perfectly healthy saga — while the compensation classifier binned the very same exception as
 * transient/RETRY. A shutdown is a pure "later" signal, exactly like {@link
 * CircuitBreakerOpenException} (the framework's own admission gate) — the node is going away and
 * another node (or this one, restarted) will redeliver the event and converge via the deterministic
 * idempotency keys. With this type, both saga paths classify it retry-later: the forward path
 * propagates for redelivery (nothing durable written), and the compensation path keeps the episode
 * {@code COMPENSATING} for the resume paths.
 *
 * <p>Extends {@link IllegalStateException} deliberately: every {@code catch
 * (IllegalStateException)} and every test that treats "bus is closed" as an illegal-state condition
 * also covers this subtype; only consumers that need the shutdown/misuse split test for this
 * subtype. Note the OTHER {@code IllegalStateException} the bus throws — "execute(command, key)
 * requires a CommandInbox" — is genuine misconfiguration, stays the plain type, and is deliberately
 * NOT retry-later.
 */
public class CommandBusClosedException extends IllegalStateException {

  public CommandBusClosedException(String message) {
    super(message);
  }
}
