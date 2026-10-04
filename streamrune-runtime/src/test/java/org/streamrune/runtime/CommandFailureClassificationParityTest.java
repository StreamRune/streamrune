package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonMappingException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.LockException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.ValidationException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;

/**
 * The dead letter queue and the circuit breaker must agree on what a permanent rejection is. They
 * used to be two inline lambdas with a comment claiming they mirrored each other "exactly", and
 * they had already drifted on the crypto-shredded-subject clause: a post-erasure command was
 * correctly kept OUT of the DLQ (replaying it would re-materialize erased PII and could never
 * succeed) yet still counted toward the breaker's threshold, so a batch replay or client retry loop
 * over erased subjects could open the global circuit against healthy infrastructure — and every
 * probe that happened to pick another erased-subject command re-opened it.
 */
class CommandFailureClassificationParityTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  record TestCommand() implements Command {}

  private static final CommandContext CTX =
      new CommandContext(
          new TestCommand(),
          "TestCommand",
          CommandId.of("cmd-1"),
          TYPE,
          AggregateId.of("agg-1"),
          null,
          Instant.now());

  /** The exact shape a GDPR-erased subject produces on the append path. */
  private static EventStoreException forgottenSubjectFailure() {
    return new EventStoreException(
        "Failed to serialize event for stream: orders-1",
        new JsonMappingException(
            (java.io.Closeable) null,
            "encrypt",
            new SubjectForgottenException("subject erased under GDPR")));
  }

  @Test
  void forgottenSubjectFailure_doesNotCountTowardTheBreakerThreshold() {
    var cb = new CircuitBreakerCommandInterceptor(2, Duration.ofMinutes(5));
    cb.onError(CTX, forgottenSubjectFailure());
    cb.onError(CTX, forgottenSubjectFailure());
    cb.onError(CTX, forgottenSubjectFailure());
    assertEquals(
        "CLOSED",
        cb.circuitState(),
        "a permanent GDPR rejection says nothing about infrastructure health — it must not open the"
            + " circuit for every unrelated aggregate on the bus");
  }

  @Test
  void forgottenSubjectFailure_isExcludedFromTheDeadLetterQueue() {
    assertFalse(VirtualThreadCommandBus.DEFAULT_DLQ_ELIGIBLE.test(forgottenSubjectFailure()));
  }

  /** The exact shape a corrupt/unbindable stored event produces on the read path. */
  private static EventDeserializationException deterministicReadCorruption() {
    return new EventDeserializationException(
        "Failed to deserialize event data for stream: orders-1",
        new IllegalArgumentException("malformed upcaster output"));
  }

  @Test
  void deterministicPerAggregateCorruption_doesNotOpenTheGlobalCircuit() {
    // A deterministic per-aggregate read corruption (a corrupt stored payload, a missing
    // event-type registration) says NOTHING about infrastructure health — the store answered
    // perfectly. Counting it toward the bus-global breaker lets ONE wedged aggregate (retried by a
    // client loop or DLQ replayer) open the circuit for every unrelated aggregate on the bus.
    var cb = new CircuitBreakerCommandInterceptor(2, Duration.ofMinutes(5));
    cb.onError(CTX, deterministicReadCorruption());
    cb.onError(CTX, deterministicReadCorruption());
    cb.onError(
        CTX,
        new UnknownEventTypeException("event type", "GoneEvent", java.util.List.of("KnownEvent")));
    assertEquals(
        "CLOSED",
        cb.circuitState(),
        "one aggregate's deterministic stored-data defect must not reject healthy traffic for the"
            + " whole bus");
  }

  @Test
  void deterministicPerAggregateCorruption_staysDlqEligible() {
    // The other half of the matrix: excluding the family from the DLQ as well would
    // restore the silent exclusion — the defect would vanish from every operational
    // surface. The DLQ entry is the durable operator signal; its errorType column records the
    // typed exception's class name, so replay tooling can see the entry re-fails until a
    // code/data fix.
    assertTrue(VirtualThreadCommandBus.DEFAULT_DLQ_ELIGIBLE.test(deterministicReadCorruption()));
    assertTrue(
        VirtualThreadCommandBus.DEFAULT_DLQ_ELIGIBLE.test(
            new UnknownEventTypeException("event type", "GoneEvent", java.util.List.of("Known"))));
  }

  @Test
  void theTwoDefaultsDeriveFromTheOneClassification_notTwoCopies() {
    // The two defaults are no longer the SAME predicate — they deliberately diverge on
    // exactly the deterministic per-aggregate stored-data family (DLQ yes, breaker no; see the
    // CommandFailureClassification matrix). What must still hold is that each is the one
    // definition from CommandFailureClassification, not a local copy that can drift.
    assertSame(
        VirtualThreadCommandBus.DEFAULT_DLQ_ELIGIBLE,
        org.streamrune.core.CommandFailureClassification.DLQ_ELIGIBLE,
        "the bus default must BE the shared DLQ definition");
    assertSame(
        CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES,
        org.streamrune.core.CommandFailureClassification.BREAKER_ELIGIBLE,
        "the breaker default must BE the shared breaker definition");
  }

  @Test
  void bothPredicatesAgreeOnTheWholeClassificationTable() {
    record Case(Throwable error, boolean eligible) {}
    List<Case> table =
        List.of(
            new Case(new DomainException("invariant violated"), false),
            new Case(new ValidationException(List.of()), false),
            new Case(new AuthorizationException("denied"), false),
            new Case(new IllegalArgumentException("bad input"), false),
            new Case(forgottenSubjectFailure(), false),
            new Case(new SubjectForgottenException("erased"), false),
            new Case(new EventStoreException("connection refused"), true),
            new Case(new OptimisticLockException("conflict"), true),
            new Case(new LockException("lock timeout"), true),
            new Case(new CircuitBreakerOpenException("open"), true),
            new Case(new RuntimeException("unexpected"), true));
    for (Case c : table) {
      String name = c.error().getClass().getSimpleName();
      assertEquals(
          c.eligible(),
          VirtualThreadCommandBus.DEFAULT_DLQ_ELIGIBLE.test(c.error()),
          "DLQ eligibility for " + name);
      assertEquals(
          c.eligible(),
          CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES.test(c.error()),
          "breaker eligibility for " + name);
    }
  }
}
