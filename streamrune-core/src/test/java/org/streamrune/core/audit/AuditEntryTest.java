package org.streamrune.core.audit;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

class AuditEntryTest {

  private static final AggregateType TYPE = AggregateType.of("order");
  private static final CommandId CMD_ID = CommandId.of("cmd-1");
  private static final CorrelationId CORR_ID = CorrelationId.of("corr-1");
  private static final Instant NOW = Instant.now();

  @Test
  void entry_storesAllFields() {
    var entry =
        new AuditEntry(
            CMD_ID,
            "PlaceOrder",
            TYPE,
            AggregateId.of("order-1"),
            UserId.of("user-42"),
            NOW,
            AuditOutcome.SUCCESS,
            null,
            3,
            CORR_ID);

    assertEquals(CMD_ID, entry.commandId());
    assertEquals("PlaceOrder", entry.commandType());
    assertEquals(TYPE, entry.aggregateType());
    assertEquals(AggregateId.of("order-1"), entry.aggregateId());
    assertEquals(UserId.of("user-42"), entry.userId());
    assertEquals(NOW, entry.occurredAt());
    assertEquals(AuditOutcome.SUCCESS, entry.outcome());
    assertNull(entry.errorMessage());
    assertEquals(3, entry.eventCount());
    assertEquals(CORR_ID, entry.correlationId());
  }

  @Test
  void entry_withNullCorrelationId_defaultsToNull() {
    var entry =
        new AuditEntry(
            CMD_ID,
            "PlaceOrder",
            TYPE,
            AggregateId.of("order-1"),
            UserId.of("user-42"),
            NOW,
            AuditOutcome.SUCCESS,
            null,
            3,
            null);

    assertNull(entry.correlationId());
    assertEquals(CMD_ID, entry.commandId());
  }

  @Test
  void entry_allowsNullCommandId_forQueryOrigin() {
    var entry =
        new AuditEntry(
            null,
            "FindUserById",
            null,
            null,
            UserId.of("user-1"),
            NOW,
            AuditOutcome.SUCCESS,
            null,
            0,
            null);
    assertNull(entry.commandId());
  }

  @Test
  void entry_allowsNullCorrelationId() {
    var entry =
        new AuditEntry(
            CMD_ID,
            "Cmd",
            TYPE,
            AggregateId.of("agg-1"),
            UserId.of("user-1"),
            NOW,
            AuditOutcome.SUCCESS,
            null,
            0,
            null);
    assertNull(entry.correlationId());
  }

  @Test
  void entry_allowsNullUserId() {
    assertDoesNotThrow(
        () ->
            new AuditEntry(
                CMD_ID,
                "Cmd",
                TYPE,
                AggregateId.of("agg-1"),
                null,
                NOW,
                AuditOutcome.SUCCESS,
                null,
                0,
                null));
  }

  @Test
  void entry_allowsNullErrorMessage() {
    assertDoesNotThrow(
        () ->
            new AuditEntry(
                CMD_ID,
                "Cmd",
                TYPE,
                AggregateId.of("agg-1"),
                UserId.of("user-1"),
                NOW,
                AuditOutcome.SUCCESS,
                null,
                0,
                null));
  }

  @Test
  void entry_allowsErrorMessageForFailure() {
    var entry =
        new AuditEntry(
            CMD_ID,
            "Cmd",
            TYPE,
            AggregateId.of("agg-1"),
            UserId.of("user-1"),
            NOW,
            AuditOutcome.FAILURE,
            "access denied",
            0,
            null);
    assertEquals("access denied", entry.errorMessage());
    assertEquals(AuditOutcome.FAILURE, entry.outcome());
  }

  @Test
  void entry_rejectsNullCommandType() {
    assertThrows(
        NullPointerException.class,
        () ->
            new AuditEntry(
                CMD_ID,
                null,
                TYPE,
                AggregateId.of("agg-1"),
                UserId.of("user-1"),
                NOW,
                AuditOutcome.SUCCESS,
                null,
                0,
                null));
  }

  @Test
  void entry_allowsNullAggregateId_forQueryOriginEntries() {
    // Query-origin entries carry no aggregate — aggregateId is documented as nullable.
    assertDoesNotThrow(
        () ->
            new AuditEntry(
                CMD_ID,
                "Cmd",
                null,
                null,
                UserId.of("user-1"),
                NOW,
                AuditOutcome.SUCCESS,
                null,
                0,
                null));
  }

  @Test
  void entry_rejectsNullOccurredAt() {
    assertThrows(
        NullPointerException.class,
        () ->
            new AuditEntry(
                CMD_ID,
                "Cmd",
                TYPE,
                AggregateId.of("agg-1"),
                UserId.of("user-1"),
                null,
                AuditOutcome.SUCCESS,
                null,
                0,
                null));
  }

  @Test
  void entry_rejectsNullOutcome() {
    assertThrows(
        NullPointerException.class,
        () ->
            new AuditEntry(
                CMD_ID,
                "Cmd",
                TYPE,
                AggregateId.of("agg-1"),
                UserId.of("user-1"),
                NOW,
                null,
                null,
                0,
                null));
  }

  @Test
  void entry_rejectsNegativeEventCount() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new AuditEntry(
                CMD_ID,
                "Cmd",
                TYPE,
                AggregateId.of("agg-1"),
                UserId.of("user-1"),
                NOW,
                AuditOutcome.SUCCESS,
                null,
                -1,
                null));
  }

  @Test
  void entry_rejectsNonZeroEventCountForFailure() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new AuditEntry(
                CMD_ID,
                "Cmd",
                TYPE,
                AggregateId.of("agg-1"),
                UserId.of("user-1"),
                NOW,
                AuditOutcome.FAILURE,
                "err",
                1,
                null));
  }

  @Test
  void auditOutcome_hasThreeValues() {
    assertEquals(3, AuditOutcome.values().length);
    assertNotNull(AuditOutcome.valueOf("SUCCESS"));
    assertNotNull(AuditOutcome.valueOf("FAILURE"));
    assertNotNull(AuditOutcome.valueOf("VETOED"));
  }

  @Test
  void vetoedOutcome_requiresZeroEventCount() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new AuditEntry(
                CommandId.of("cmd-veto"),
                "PlaceOrder",
                TYPE,
                AggregateId.of("order-1"),
                null,
                Instant.parse("2026-01-01T00:00:00Z"),
                AuditOutcome.VETOED,
                "vetoed by X",
                1,
                null));
  }

  @Test
  void aggregateType_isNullable_forQueryOriginAndSubjectEntries() {
    var query =
        new AuditEntry(
            null, "GetOrder", null, null, null, Instant.EPOCH, AuditOutcome.SUCCESS, null, 0, null);
    assertNull(query.aggregateType());
    var command =
        new AuditEntry(
            CommandId.of("c-1"),
            "PlaceOrder",
            AggregateType.of("order"),
            AggregateId.of("o-1"),
            null,
            Instant.EPOCH,
            AuditOutcome.SUCCESS,
            null,
            1,
            null);
    assertEquals(AggregateType.of("order"), command.aggregateType());
  }
}
