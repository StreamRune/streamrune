package org.streamrune.core;

/**
 * A stored event or snapshot payload that <b>deterministically</b> cannot be turned into its typed
 * form: the JSON cannot be parsed or bound to the registered class, an upcaster threw or produced
 * output the target class cannot bind, or the crypto layer refused the stored shape with a
 * deterministic mapping error. Retrying the read re-fails identically, forever — the store itself
 * is healthy; the defect is in the stored bytes (or in the code that must interpret them).
 *
 * <p><b>Why this type exists:</b> before it existed, the upcast read branch let Jackson's unchecked
 * {@code IllegalArgumentException} (from {@code convertValue} on a malformed upcaster output) and
 * any user-upcaster throw escape raw. A top-level {@code IllegalArgumentException} is classified as
 * a caller-input permanent rejection ({@link CommandFailureClassification#isPermanentRejection}),
 * so a stored-data defect was silently excluded from the dead letter queue and the circuit breaker,
 * and the caller saw an exception indistinguishable from their own bad input. Wrapping in this type
 * keeps the cause chain (the {@code IllegalArgumentException} is still there for diagnostics) while
 * giving every classifier a frame that says what actually happened.
 *
 * <p><b>Contract:</b> a throw site must only use this type for failures it has established are
 * deterministic. In particular, a bare {@link org.streamrune.core.crypto.CryptoOperationException}
 * from a Vault/KMS engine (possibly wrapped by Jackson into a {@code JsonMappingException}) must be
 * wrapped as a plain {@link EventStoreException} instead — it reports a condition of the key store
 * or the key (an outage, a disabled or inaccessible key), not of the stored event, and must keep
 * classifying as an infrastructure failure.
 *
 * <p>Consumers:
 *
 * <ul>
 *   <li>{@code ReadPoisonClassifier} (runtime) treats this frame as deterministic read poison, so a
 *       projection stops after its bounded poison streak instead of retrying forever;
 *   <li>{@link CommandFailureClassification#isDeterministicPerAggregate} keys on it to keep a
 *       single wedged aggregate from opening the bus-global circuit breaker, while the failure
 *       stays DLQ-eligible (the DLQ entry's {@code errorType} records this class name, so replay
 *       tooling can see the entry re-fails until a code/data fix).
 * </ul>
 */
public class EventDeserializationException extends EventStoreException {

  public EventDeserializationException(String message, Throwable cause) {
    super(message, cause);
  }
}
