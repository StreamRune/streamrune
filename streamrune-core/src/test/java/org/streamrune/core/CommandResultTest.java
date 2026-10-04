package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandBus.CommandResult;
import org.streamrune.core.CommandBus.ShortCircuitReason;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class CommandResultTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  @Test
  void executedResultIsNotShortCircuited() {
    var result =
        new CommandResult(
            List.of(), StreamId.of(TYPE, AggregateId.of("s-1")), Version.initial(), List.of());

    assertFalse(result.shortCircuited());
    assertNull(result.shortCircuitedBy());
    assertEquals(
        ShortCircuitReason.NONE, result.reason(), "4-arg constructor defaults reason=NONE");
    assertFalse(result.vetoed());
    assertFalse(result.idempotentReplay());
    assertEquals(List.of(), result.envelopes(), "4-arg constructor defaults envelopes to empty");
  }

  @Test
  void executedResultWithEnvelopesIsNotShortCircuited() {
    var result =
        new CommandResult(
            List.of(),
            StreamId.of(TYPE, AggregateId.of("s-1")),
            Version.initial(),
            List.of(),
            List.of());

    assertFalse(result.shortCircuited());
    assertNull(result.shortCircuitedBy());
    assertEquals(
        ShortCircuitReason.NONE, result.reason(), "5-arg constructor defaults reason=NONE");
    assertFalse(result.vetoed());
    assertFalse(result.idempotentReplay());
  }

  @Test
  void vetoedResultNamesTheInterceptor() {
    var result =
        new CommandResult(
            List.of(),
            StreamId.of(TYPE, AggregateId.of("s-1")),
            Version.initial(),
            List.of(),
            List.of(),
            "RateLimitInterceptor",
            ShortCircuitReason.VETOED);

    assertTrue(result.shortCircuited());
    assertTrue(result.vetoed(), "a VETOED result is a rejection");
    assertFalse(result.idempotentReplay());
    assertEquals(ShortCircuitReason.VETOED, result.reason());
    assertEquals("RateLimitInterceptor", result.shortCircuitedBy());
    assertTrue(result.events().isEmpty(), "vetoed commands never produce events");
  }

  @Test
  void idempotentReplayResultIsASuccessNotARejection() {
    // A replay carries the ORIGINAL outcome: the events were persisted by the prior execution.
    // shortCircuited() is true, but this is a SUCCESS — idempotentReplay() must report it as such,
    // and vetoed() must NOT, so callers do not turn an at-least-once redelivery into a 403/409.
    var result =
        new CommandResult(
            List.of(),
            StreamId.of(TYPE, AggregateId.of("s-1")),
            new Version(3),
            List.of(),
            List.of(),
            "order-create-o-1",
            ShortCircuitReason.IDEMPOTENT_REPLAY);

    assertTrue(result.shortCircuited());
    assertTrue(
        result.idempotentReplay(), "an IDEMPOTENT_REPLAY is a success (events already persisted)");
    assertFalse(result.vetoed(), "an idempotent replay is NOT a rejection");
    assertEquals(ShortCircuitReason.IDEMPOTENT_REPLAY, result.reason());
    assertEquals(
        "order-create-o-1", result.shortCircuitedBy(), "carries the idempotency-key value");
  }

  @Test
  void canonicalConstructorRejectsNullReason() {
    assertThrows(
        NullPointerException.class,
        () ->
            new CommandResult(
                List.of(),
                StreamId.of(TYPE, AggregateId.of("s-1")),
                Version.initial(),
                List.of(),
                List.of(),
                null,
                null));
  }

  @Test
  void reasonNoneWithNonNullNameIsPermittedAndReportsNotShortCircuited() {
    // shortCircuited() is driven by reason, not by the string: NONE means the command ran.
    var result =
        new CommandResult(
            List.of(),
            StreamId.of(TYPE, AggregateId.of("s-1")),
            Version.initial(),
            List.of(),
            List.of(),
            null,
            ShortCircuitReason.NONE);

    assertFalse(result.shortCircuited());
    assertFalse(result.vetoed());
    assertFalse(result.idempotentReplay());
  }
}
