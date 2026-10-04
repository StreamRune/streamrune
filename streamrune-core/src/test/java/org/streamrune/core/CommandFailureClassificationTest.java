package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.SubjectForgottenException;

/**
 * The one classifier both the dead letter queue and the circuit breaker derive from. Its whole
 * purpose is that there is no second copy to drift from, so these cases pin the exact membership of
 * "permanent rejection".
 */
class CommandFailureClassificationTest {

  @Test
  void domainAndClientRejectionsArePermanent() {
    assertTrue(
        CommandFailureClassification.isPermanentRejection(
            new DomainException("invariant violated")));
    assertTrue(CommandFailureClassification.isPermanentRejection(new AuthorizationException("no")));
    assertTrue(
        CommandFailureClassification.isPermanentRejection(new ValidationException(List.of())));
    assertTrue(
        CommandFailureClassification.isPermanentRejection(new IllegalArgumentException("bad")));
    assertTrue(
        CommandFailureClassification.isPermanentRejection(
            new NoDeciderException(CommandFailureClassificationTest.class, List.of())));
  }

  @Test
  void aForgottenSubjectIsPermanentEvenWhenWrappedDeepInTheCauseChain() {
    // The real shape on the append path: the crypto engine refuses to re-encrypt, Jackson wraps it,
    // the store wraps that. Nothing in the chain is a DomainException or IllegalArgumentException.
    Throwable wrapped =
        new EventStoreException(
            "append failed",
            new RuntimeException(
                "encrypt", new SubjectForgottenException("subject erased under GDPR")));
    assertTrue(CommandFailureClassification.isPermanentRejection(wrapped));
    assertTrue(CommandFailureClassification.hasSubjectForgottenCause(wrapped));
  }

  @Test
  void infrastructureFailuresAreNotPermanent() {
    assertFalse(CommandFailureClassification.isPermanentRejection(new EventStoreException("down")));
    assertFalse(
        CommandFailureClassification.isPermanentRejection(new OptimisticLockException("conflict")));
    assertFalse(CommandFailureClassification.isPermanentRejection(new LockException("busy")));
    assertFalse(CommandFailureClassification.isPermanentRejection(new RuntimeException("boom")));
  }

  @Test
  void nullIsNotAPermanentRejection() {
    assertFalse(CommandFailureClassification.isPermanentRejection(null));
    assertFalse(CommandFailureClassification.hasSubjectForgottenCause(null));
  }

  @Test
  void aSelfReferentialCauseChainTerminates() {
    // A cyclic chain must not spin forever — the walk is depth-bounded.
    RuntimeException a = new RuntimeException("a");
    RuntimeException b = new RuntimeException("b", a);
    a.initCause(b); // initCause on a no-cause exception built with the (msg) ctor
    assertFalse(CommandFailureClassification.hasSubjectForgottenCause(b));
  }

  @Test
  void dlqEligibilityIsTheExactNegationOfPermanentRejection() {
    List<Throwable> table =
        List.of(
            new DomainException("d"),
            new AuthorizationException("a"),
            new ValidationException(List.of()),
            new IllegalArgumentException("i"),
            new EventStoreException(
                "e", new SubjectForgottenException("erased")), // wrapped forgotten subject
            new EventStoreException("e"),
            new OptimisticLockException("o"),
            new LockException("l"),
            new CircuitBreakerOpenException("c"),
            new RuntimeException("r"),
            new EventDeserializationException("corrupt", new IllegalArgumentException("bad map")),
            new UnknownEventTypeException("event type", "Gone", List.of("Known")));
    for (Throwable t : table) {
      assertNotEquals(
          CommandFailureClassification.PERMANENT_REJECTION.test(t),
          CommandFailureClassification.DLQ_ELIGIBLE.test(t),
          "DLQ eligibility must stay the exact negation of permanent rejection for "
              + t.getClass().getSimpleName());
    }
  }

  // ---- The deterministic per-aggregate stored-data family and the breaker/DLQ split ----

  @Test
  void deterministicPerAggregateFamilyMembership() {
    assertTrue(
        CommandFailureClassification.isDeterministicPerAggregate(
            new EventDeserializationException(
                "corrupt payload", new IllegalArgumentException("malformed upcaster output"))));
    assertTrue(
        CommandFailureClassification.isDeterministicPerAggregate(
            new UnknownEventTypeException("event type", "Gone", List.of("Known"))));
    // Wrapped deeper (e.g. a retry decorator re-wrapping the store failure) still counts.
    assertTrue(
        CommandFailureClassification.isDeterministicPerAggregate(
            new RuntimeException(
                "outer",
                new EventDeserializationException("corrupt", new NullPointerException("bug")))));
    // A plain store failure (connectivity, SQL, unknown cause) is NOT in the family.
    assertFalse(
        CommandFailureClassification.isDeterministicPerAggregate(new EventStoreException("down")));
    assertFalse(
        CommandFailureClassification.isDeterministicPerAggregate(new RuntimeException("boom")));
    assertFalse(CommandFailureClassification.isDeterministicPerAggregate(null));
  }

  @Test
  void deterministicPerAggregateWalkTerminatesOnACyclicChain() {
    RuntimeException a = new RuntimeException("a");
    RuntimeException b = new RuntimeException("b", a);
    a.initCause(b);
    assertFalse(CommandFailureClassification.isDeterministicPerAggregate(a));
  }

  @Test
  void theMatrix_breakerDivergesFromDlqOnExactlyTheDeterministicPerAggregateFamily() {
    record Row(Throwable error, boolean dlq, boolean breaker) {}
    List<Row> matrix =
        List.of(
            // permanent rejections: excluded from both.
            new Row(new DomainException("d"), false, false),
            new Row(new IllegalArgumentException("i"), false, false),
            new Row(new EventStoreException("e", new SubjectForgottenException("x")), false, false),
            // deterministic per-aggregate stored-data failures: DLQ yes, breaker NO.
            new Row(
                new EventDeserializationException("corrupt", new IllegalArgumentException("m")),
                true,
                false),
            new Row(new UnknownEventTypeException("event type", "Gone", List.of("K")), true, false),
            // infrastructure: counted by both.
            new Row(new EventStoreException("conn refused"), true, true),
            new Row(new OptimisticLockException("conflict"), true, true),
            new Row(new RuntimeException("unexpected"), true, true));
    for (Row row : matrix) {
      String name = row.error().getClass().getSimpleName();
      assertEquals(
          row.dlq(),
          CommandFailureClassification.DLQ_ELIGIBLE.test(row.error()),
          "DLQ eligibility for " + name);
      assertEquals(
          row.breaker(),
          CommandFailureClassification.BREAKER_ELIGIBLE.test(row.error()),
          "breaker eligibility for " + name);
    }
  }
}
