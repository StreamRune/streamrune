package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.SagaType;

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
  void aReservedCorrelationIdRefusalIsPermanent() {
    // The dispatch ran under a reserved correlation id that is not its saga origin's: a wiring
    // error of the caller, the same on every retry and every replay.
    var refusal = new ReservedCorrelationIdException();

    assertTrue(CommandFailureClassification.isPermanentRejection(refusal));
    assertFalse(CommandFailureClassification.DLQ_ELIGIBLE.test(refusal));
    assertFalse(CommandFailureClassification.BREAKER_ELIGIBLE.test(refusal));
  }

  @Test
  void aFencedForwardCommandIsAPermanentRejection() {
    // The saga row left its forward phase: the episode stamp and a terminal status are absorbing,
    // so the same command is refused on every retry and every replay.
    var refusal =
        new SagaForwardFencedException(
            SagaOrigin.dispatchedBy(SagaType.of("orders.Fulfillment"), SagaId.of("saga-1")));

    assertTrue(CommandFailureClassification.PERMANENT_REJECTION.test(refusal));
    assertFalse(CommandFailureClassification.DLQ_ELIGIBLE.test(refusal));
    assertFalse(CommandFailureClassification.BREAKER_ELIGIBLE.test(refusal));
    assertFalse(
        CommandFailureClassification.isPermanentRejection(new EventStoreException("x", refusal)),
        "top level only, like the other rejections a store or the bus raises itself");
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
            // a reader behind the stored schema: DLQ yes (a replay on an upgraded replica
            // succeeds), breaker NO (the store answered; this replica's code is behind).
            new Row(schemaAhead(), true, false),
            new Row(new RuntimeException("outer", schemaAhead()), true, false),
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

  // ---- A reader behind the stored schema (rolling deployment of a schema bump) ----

  @Test
  void aReaderBehindTheStoredSchemaIsRetryLater_notPermanent_notDeterministic() {
    var refusal = schemaAhead();

    assertTrue(CommandFailureClassification.isReaderBehindStoredSchema(refusal));
    assertTrue(
        CommandFailureClassification.isReaderBehindStoredSchema(
            new RuntimeException("a retry decorator re-wrapped it", refusal)));
    assertFalse(CommandFailureClassification.isPermanentRejection(refusal));
    assertFalse(CommandFailureClassification.isDeterministicPerAggregate(refusal));
    assertTrue(CommandFailureClassification.DLQ_ELIGIBLE.test(refusal));
    assertFalse(CommandFailureClassification.BREAKER_ELIGIBLE.test(refusal));
  }

  @Test
  void onlyTheSchemaAheadRefusalIsInTheReaderBehindFamily() {
    assertFalse(
        CommandFailureClassification.isReaderBehindStoredSchema(new EventStoreException("down")));
    assertFalse(
        CommandFailureClassification.isReaderBehindStoredSchema(
            new EventDeserializationException("corrupt", new IllegalArgumentException("m"))));
    assertFalse(CommandFailureClassification.isReaderBehindStoredSchema(null));
    RuntimeException a = new RuntimeException("a");
    RuntimeException b = new RuntimeException("b", a);
    a.initCause(b);
    assertFalse(CommandFailureClassification.isReaderBehindStoredSchema(a));
  }

  @Test
  void theSchemaAheadRefusalCarriesTheTypeAndBothVersions() {
    var refusal = schemaAhead();

    assertEquals(new EventType("OrderPlaced"), refusal.eventType());
    assertEquals(2, refusal.storedVersion());
    assertEquals(1, refusal.readerVersion());
    assertTrue(EventStoreException.class.isAssignableFrom(EventSchemaAheadException.class));
    assertFalse(
        EventDeserializationException.class.isAssignableFrom(EventSchemaAheadException.class),
        "a deserialization subtype would make the read path quarantine the newer events");
  }

  @Test
  void aSchemaAheadRefusalNeedsAStoredVersionAboveTheReaders() {
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new EventSchemaAheadException("m", new EventType("OrderPlaced"), 2, 2));
    org.junit.jupiter.api.Assertions.assertThrows(
        NullPointerException.class, () -> new EventSchemaAheadException("m", null, 2, 1));
  }

  private static EventSchemaAheadException schemaAhead() {
    return new EventSchemaAheadException(
        "OrderPlaced is stored at schema_version 2", new EventType("OrderPlaced"), 2, 1);
  }
}
