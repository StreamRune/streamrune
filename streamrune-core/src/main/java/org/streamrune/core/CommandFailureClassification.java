package org.streamrune.core;

import java.util.function.Predicate;
import org.streamrune.core.crypto.SubjectForgottenException;

/**
 * The single answer to "what does this command failure say about the system?" — split along two
 * independent axes that its two consumers ask about:
 *
 * <ul>
 *   <li>the <b>dead letter queue</b> asks "could a replay ever succeed, and is recording the entry
 *       safe?" — a permanent rejection must not be dead-lettered, because a replay can only
 *       re-reject it, forever;
 *   <li>the <b>circuit breaker</b> asks "is the shared infrastructure unhealthy?" — a failure that
 *       says nothing about infrastructure health must not count toward the threshold, because
 *       opening the circuit would reject healthy traffic for every unrelated aggregate on the bus.
 * </ul>
 *
 * <p><b>The classification matrix.</b> The two questions used to be treated as the same question,
 * which is right for two of the three failure families and wrong for the third:
 *
 * <table border="1">
 *   <caption>failure family &rarr; consumer verdicts</caption>
 *   <tr><th>family</th><th>DLQ-eligible</th><th>breaker-eligible</th><th>caller sees</th></tr>
 *   <tr><td>permanent rejection ({@link #isPermanentRejection}: {@link DomainException},
 *       top-level {@link IllegalArgumentException}, {@link SubjectForgottenException} anywhere)
 *       </td><td>no</td><td>no</td><td>the typed rejection</td></tr>
 *   <tr><td>deterministic per-aggregate stored-data failure ({@link #isDeterministicPerAggregate}:
 *       {@link EventDeserializationException} / {@link UnknownEventTypeException} anywhere)</td>
 *       <td><b>YES</b></td><td><b>NO</b></td><td>a typed {@link EventStoreException}</td></tr>
 *   <tr><td>infrastructure (everything else)</td><td>yes</td><td>yes</td><td>the failure</td></tr>
 * </table>
 *
 * <p>Why the middle row diverges: a corrupt stored payload or a missing event-type registration is
 * a final fact about ONE aggregate's stored data — the store, pool and network answered perfectly,
 * so it carries <em>zero</em> infrastructure-health evidence, and one wedged aggregate hammered by
 * a client retry loop or a DLQ replayer must never open the bus-global circuit (breaker: NO). But
 * it is also not a business rejection: nothing about the COMMAND is wrong, and excluding it from
 * the DLQ as well would make the defect vanish from every operational surface — precisely the
 * silent-exclusion failure the dedicated exception type exists to prevent. So it IS dead-lettered
 * (DLQ: YES); the entry's {@code errorType} column records the typed exception's class name, which
 * is how replay tooling can tell that replaying this entry re-fails until a code/data/registration
 * fix ships. The caller still gets a loud, typed failure either way.
 *
 * <p>The predicates used to be separate inline lambdas in the two consumers, and they drifted: the
 * DLQ predicate excluded a crypto-shredded ({@link SubjectForgottenException}) subject while the
 * breaker's did not — even though a comment claimed they mirrored each other "exactly". The
 * asymmetry was reachable: a batch replay or client retry loop over commands for GDPR-erased
 * subjects produced consecutive failures that the DLQ correctly refused to record but the breaker
 * happily counted, opening the global circuit against infrastructure that was never unhealthy. Both
 * consumers still derive from this one class — {@link #DLQ_ELIGIBLE} and {@link #BREAKER_ELIGIBLE}
 * share {@link #isPermanentRejection}, and their only difference is the deliberate, documented
 * middle row above — so a clause cannot be added to one and forgotten in the other.
 *
 * <p>What counts as a permanent rejection:
 *
 * <ul>
 *   <li>{@link DomainException} (including {@code AuthorizationException}) — the domain gave a
 *       correct, final answer;
 *   <li>top-level {@link IllegalArgumentException} (including {@code ValidationException}, {@code
 *       NoDeciderException}, and the idempotency-key-collision guard) — the caller's input is wrong
 *       and will stay wrong. Top-level only: an IAE buried under an {@link
 *       EventDeserializationException} was thrown by Jackson against STORED data, not by validation
 *       against the caller's input;
 *   <li>{@link SubjectForgottenException} anywhere in the cause chain — the subject's key was
 *       crypto-shredded by a GDPR erasure and the engine refuses to re-encrypt. No retry can ever
 *       succeed, and dead-lettering it would persist the subject's identifier in {@code
 *       dead_letter_queue.error_message}, so the erasure flow would re-materialize the data it
 *       erased.
 * </ul>
 */
public final class CommandFailureClassification {

  /** Maximum cause-chain depth walked, so a self-referential chain terminates. */
  private static final int MAX_CAUSE_DEPTH = 50;

  /** {@link #isPermanentRejection} as a predicate. */
  public static final Predicate<Throwable> PERMANENT_REJECTION =
      CommandFailureClassification::isPermanentRejection;

  /**
   * The DLQ's question, answered: everything except a permanent rejection is worth recording for
   * replay/inspection — including a {@link #isDeterministicPerAggregate deterministic per-aggregate
   * stored-data failure}, whose DLQ entry (marked by its {@code errorType}) is the durable operator
   * signal that one aggregate's stored data needs a fix. The default for {@code
   * VirtualThreadCommandBus.DEFAULT_DLQ_ELIGIBLE}.
   */
  public static final Predicate<Throwable> DLQ_ELIGIBLE = PERMANENT_REJECTION.negate();

  /**
   * The breaker's question, answered: a failure counts toward the threshold only when it carries
   * actual infrastructure-health evidence — so neither a permanent rejection nor a {@link
   * #isDeterministicPerAggregate deterministic per-aggregate stored-data failure} counts (the
   * latter is a final fact about one aggregate's stored bytes, produced by a perfectly healthy
   * store). The default for {@code CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES}. See
   * the class javadoc's matrix for why this deliberately differs from {@link #DLQ_ELIGIBLE}.
   */
  public static final Predicate<Throwable> BREAKER_ELIGIBLE =
      DLQ_ELIGIBLE.and(t -> !isDeterministicPerAggregate(t));

  private CommandFailureClassification() {}

  /**
   * True when {@code error} is a permanent rejection — a correct, final "no" that no retry, replay
   * or cooldown can turn into a success. See the class javadoc for the exact set.
   */
  public static boolean isPermanentRejection(Throwable error) {
    return error instanceof DomainException
        || error instanceof IllegalArgumentException
        || hasSubjectForgottenCause(error);
  }

  /**
   * True when {@code error}'s cause chain (bounded) carries the typed deterministic stored-data
   * family: {@link EventDeserializationException} (a stored payload that deterministically cannot
   * be bound — corrupt bytes, a malformed or throwing upcaster) or {@link
   * UnknownEventTypeException} (a persisted type name with no registered class — strictly
   * per-event-type rather than per-aggregate, but equally a fact about stored data and a registry,
   * not about infrastructure). Such a failure re-fails identically on every retry <em>of that
   * aggregate</em> while saying nothing about the health of anything shared — see the class
   * javadoc's matrix for how the two consumers split on it.
   */
  public static boolean isDeterministicPerAggregate(Throwable error) {
    Throwable c = error;
    for (int depth = 0; c != null && depth < MAX_CAUSE_DEPTH; depth++, c = c.getCause()) {
      if (c instanceof EventDeserializationException || c instanceof UnknownEventTypeException) {
        return true;
      }
    }
    return false;
  }

  /**
   * True when {@code t} or any exception in its cause chain is a {@link SubjectForgottenException}.
   * The rejection surfaces WRAPPED on the append path (Jackson {@code JsonMappingException} →
   * {@code EventStoreException}) rather than as a top-level {@code SubjectForgottenException}, so
   * the chain is walked. Bounded to guard against a self-referential/cyclic cause chain.
   */
  public static boolean hasSubjectForgottenCause(Throwable t) {
    Throwable c = t;
    for (int depth = 0; c != null && depth < MAX_CAUSE_DEPTH; depth++, c = c.getCause()) {
      if (c instanceof SubjectForgottenException) {
        return true;
      }
    }
    return false;
  }
}
