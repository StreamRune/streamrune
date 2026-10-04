package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;

class AuditCommandInterceptorTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  record PlaceOrderCommand() implements Command {}

  private static final CommandId CMD_ID = CommandId.of("cmd-1");
  private static final Instant SUBMITTED_AT = Instant.parse("2026-01-01T10:00:00Z");

  // CommandContext with no result (as seen in before/onError)
  private static final CommandContext CTX_NO_RESULT =
      new CommandContext(
          new PlaceOrderCommand(),
          "PlaceOrder",
          CMD_ID,
          TYPE,
          AggregateId.of("order-1"),
          null,
          SUBMITTED_AT);

  // CommandContext with 2 events (as seen in after)
  private static final CommandContext CTX_WITH_RESULT =
      new CommandContext(
          new PlaceOrderCommand(),
          "PlaceOrder",
          CMD_ID,
          TYPE,
          AggregateId.of("order-1"),
          new CommandBus.CommandResult(
              List.of(mock(DomainEvent.class), mock(DomainEvent.class)), null, null, null),
          SUBMITTED_AT);

  @Test
  void before_returnsTrue_andCapturesNullUserId_whenScopeNotBound() {
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);
    assertTrue(interceptor.before(CTX_NO_RESULT));
    interceptor.after(CTX_WITH_RESULT);
    verify(store).save(argThat(entry -> entry.userId() == null));
  }

  @Test
  void after_savesSuccessEntry_withEventCount() {
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);

    interceptor.before(CTX_NO_RESULT);
    interceptor.after(CTX_WITH_RESULT);

    verify(store)
        .save(
            argThat(
                entry ->
                    entry.commandId().equals(CMD_ID)
                        && entry.commandType().equals("PlaceOrder")
                        && TYPE.equals(entry.aggregateType())
                        && entry.aggregateId().equals(AggregateId.of("order-1"))
                        && entry.userId() == null
                        && entry.occurredAt().equals(SUBMITTED_AT)
                        && entry.outcome() == AuditOutcome.SUCCESS
                        && entry.errorMessage() == null
                        && entry.eventCount() == 2));
  }

  @Test
  void after_savesSuccessEntry_withUserIdFromScopedContext() {
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);
    UserId userId = UserId.of("user-42");

    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, userId, CorrelationId.of("corr-1"), Instant.now(), null))
        .run(
            () -> {
              interceptor.before(CTX_NO_RESULT);
              interceptor.after(CTX_WITH_RESULT);
            });

    verify(store).save(argThat(entry -> UserId.of("user-42").equals(entry.userId())));
  }

  @Test
  void after_savesNullUserId_whenScopedValueNotBound() {
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);

    interceptor.before(CTX_NO_RESULT); // no ScopedValue bound
    interceptor.after(CTX_WITH_RESULT);

    verify(store).save(argThat(entry -> entry.userId() == null));
  }

  @Test
  void after_savesCorrelationId_capturedFromContext() {
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);
    CorrelationId correlationId = CorrelationId.of("corr-flow-77");

    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, UserId.of("user-1"), correlationId, Instant.now(), null))
        .run(
            () -> {
              interceptor.before(CTX_NO_RESULT);
              interceptor.after(CTX_WITH_RESULT);
            });

    verify(store).save(argThat(entry -> correlationId.equals(entry.correlationId())));
  }

  @Test
  void after_savesNullCorrelationId_whenScopedValueNotBound() {
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);

    interceptor.before(CTX_NO_RESULT); // no ScopedValue bound
    interceptor.after(CTX_WITH_RESULT);

    verify(store).save(argThat(entry -> entry.correlationId() == null));
  }

  @Test
  void onError_savesCorrelationId_capturedInBefore() {
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);
    CorrelationId correlationId = CorrelationId.of("corr-flow-88");

    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, UserId.of("user-2"), correlationId, Instant.now(), null))
        .run(() -> interceptor.before(CTX_NO_RESULT));

    // ScopedValue no longer bound — correlationId was captured in ThreadLocal in before().
    interceptor.onError(CTX_NO_RESULT, new RuntimeException("fail"));

    verify(store).save(argThat(entry -> correlationId.equals(entry.correlationId())));
  }

  @Test
  void after_savesNullUserId_whenUserNotSetInContext() {
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);

    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, null, CorrelationId.of("corr-1"), Instant.now(), null))
        .run(
            () -> {
              interceptor.before(CTX_NO_RESULT);
              interceptor.after(CTX_WITH_RESULT);
            });

    verify(store).save(argThat(entry -> entry.userId() == null));
  }

  @Test
  void onError_savesFailureEntry_withErrorMessage() {
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);
    RuntimeException error = new RuntimeException("DB unavailable");

    interceptor.before(CTX_NO_RESULT);
    interceptor.onError(CTX_NO_RESULT, error);

    verify(store)
        .save(
            argThat(
                entry ->
                    entry.outcome() == AuditOutcome.FAILURE
                        && TYPE.equals(entry.aggregateType())
                        && AggregateId.of("order-1").equals(entry.aggregateId())
                        && "DB unavailable".equals(entry.errorMessage())
                        && entry.eventCount() == 0));
  }

  @Test
  void onError_savesUserIdCapturedInBefore() {
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);
    UserId userId = UserId.of("user-99");

    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, userId, CorrelationId.of("corr-1"), Instant.now(), null))
        .run(() -> interceptor.before(CTX_NO_RESULT));

    // ScopedValue is no longer bound here — userId was captured in ThreadLocal
    interceptor.onError(CTX_NO_RESULT, new RuntimeException("fail"));

    verify(store).save(argThat(entry -> UserId.of("user-99").equals(entry.userId())));
  }

  // CommandContext for a VETOED short-circuit (a later interceptor's before() returned false).
  private static final CommandContext CTX_VETOED =
      new CommandContext(
          new PlaceOrderCommand(),
          "PlaceOrder",
          CMD_ID,
          TYPE,
          AggregateId.of("order-1"),
          new CommandBus.CommandResult(
              List.of(),
              null,
              null,
              null,
              List.of(),
              "MaintenanceModeInterceptor",
              CommandBus.ShortCircuitReason.VETOED),
          SUBMITTED_AT);

  @Test
  void after_vetoedCommand_isNotAuditedAsSuccess() {
    // A veto is a REJECTION — the command never executed and produced zero events.
    // Auditing it as SUCCESS is a false record: a compliance/reconciliation reader would conclude
    // the operation was performed. after() must record a distinct VETOED outcome that names the
    // vetoing interceptor, never SUCCESS.
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);

    interceptor.before(CTX_NO_RESULT);
    interceptor.after(CTX_VETOED);

    verify(store)
        .save(
            argThat(
                entry ->
                    entry.outcome() == AuditOutcome.VETOED
                        && TYPE.equals(entry.aggregateType())
                        && AggregateId.of("order-1").equals(entry.aggregateId())
                        && entry.eventCount() == 0
                        && entry.errorMessage() != null
                        && entry.errorMessage().contains("MaintenanceModeInterceptor")));
    verify(store, never()).save(argThat(entry -> entry.outcome() == AuditOutcome.SUCCESS));
  }

  @Test
  void constructor_rejectsNullStore() {
    assertThrows(IllegalArgumentException.class, () -> new AuditCommandInterceptor(null));
  }

  // === The audit row is a persisted-text sink ===

  /**
   * An exception message is free text that can carry caller-supplied input; written verbatim, a
   * CR/LF in it forges a line for every reader of {@code audit_log} and a NUL makes the PostgreSQL
   * INSERT fail, so the FAILURE row is lost. The persisted message is the sanitized one.
   */
  @Test
  void onError_persistsTheErrorMessageSanitized() {
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);
    String raw = "rejected\r\n2026-10-02 10:00:00 INFO audit - approved by admin\u0000";

    interceptor.before(CTX_NO_RESULT);
    interceptor.onError(CTX_NO_RESULT, new IllegalStateException(raw));

    var captor = org.mockito.ArgumentCaptor.forClass(org.streamrune.core.audit.AuditEntry.class);
    verify(store).save(captor.capture());
    String persisted = captor.getValue().errorMessage();
    assertEquals(LogSanitizer.sanitizeFreeText(raw), persisted);
    assertFalse(persisted.contains("\n") || persisted.contains("\r"), persisted);
    assertFalse(persisted.contains("\u0000"), persisted);
  }

  /**
   * The finding's own chain, end to end: a key reused across commands is echoed by the inbox's
   * collision guard, and that exception reaches this sink. A key rebuilt through the canonical
   * constructor (the decode door keeps no charset rule) carries its control characters this far; it
   * must not carry them into the row.
   */
  @Test
  void onError_aReusedKeysCollisionMessage_reachesTheRowWithoutItsControlCharacters() {
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);
    var recorded =
        new org.streamrune.core.CommandInbox.InboxResult(
            new IdempotencyKey("client-key\n2026-10-02 WARN forged\u0000"),
            "com.example.CreateOrder",
            StreamId.of(TYPE, AggregateId.of("order-1")),
            new Version(1),
            List.of(),
            SUBMITTED_AT);
    var collision =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                recorded.requireBoundTo(
                    "com.example.RefundPayment", StreamId.of(TYPE, AggregateId.of("order-1"))));

    interceptor.before(CTX_NO_RESULT);
    interceptor.onError(CTX_NO_RESULT, collision);

    var captor = org.mockito.ArgumentCaptor.forClass(org.streamrune.core.audit.AuditEntry.class);
    verify(store).save(captor.capture());
    String persisted = captor.getValue().errorMessage();
    assertTrue(persisted.contains("client-key"), persisted);
    assertTrue(persisted.chars().noneMatch(Character::isISOControl), persisted);
  }

  @Test
  void onError_nullMessage_staysNull() {
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);

    interceptor.before(CTX_NO_RESULT);
    interceptor.onError(CTX_NO_RESULT, new RuntimeException((String) null));

    verify(store).save(argThat(entry -> entry.errorMessage() == null));
  }

  /** Every message this sink persists goes through the sanitizer, the veto text included. */
  @Test
  void after_vetoMessage_isPersistedSanitized() {
    AuditStore store = mock(AuditStore.class);
    var interceptor = new AuditCommandInterceptor(store);
    var vetoed =
        new CommandContext(
            new PlaceOrderCommand(),
            "PlaceOrder",
            CMD_ID,
            TYPE,
            AggregateId.of("order-1"),
            new CommandBus.CommandResult(
                List.of(),
                null,
                null,
                null,
                List.of(),
                "Maintenance\nInterceptor",
                CommandBus.ShortCircuitReason.VETOED),
            SUBMITTED_AT);

    interceptor.before(CTX_NO_RESULT);
    interceptor.after(vetoed);

    verify(store)
        .save(
            argThat(
                entry ->
                    entry.outcome() == AuditOutcome.VETOED
                        && "vetoed by Maintenance Interceptor".equals(entry.errorMessage())));
  }
}
