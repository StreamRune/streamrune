package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

/**
 * What every {@link AuditStore} must accept, taken from the interface's Javadoc. The store is a
 * write-only sink, so this suite pins what {@code save} takes without error: an entry of every
 * outcome, a query-origin entry, an anonymous one, a subject entry, a repeated {@code commandId} (a
 * retry adds an entry), and that a {@code null} entry fails with {@link NullPointerException}. What
 * {@code save} stored, and how it reads back, is pinned by {@link CommandAuditQueryContract}. There
 * is no in-memory command audit store: run by {@code PostgresAuditContractsTest}.
 */
public abstract class AuditStoreContract {

  private static final Instant T0 = Instant.parse("2026-01-01T10:00:00.123Z");

  /** A fresh, empty store. */
  protected abstract AuditStore newStore();

  private AuditStore store;

  @BeforeEach
  void freshStore() {
    store = newStore();
  }

  private static AuditEntry command(String id, AuditOutcome outcome, int events, String error) {
    return new AuditEntry(
        CommandId.of(id),
        "PlaceOrder",
        AggregateType.of("order"),
        AggregateId.of("order-1"),
        UserId.of("user-1"),
        T0,
        outcome,
        error,
        events,
        CorrelationId.of("corr-1"));
  }

  @Test
  void acceptsASuccessEntryWithItsEventCount() {
    assertDoesNotThrow(() -> store.save(command("c-1", AuditOutcome.SUCCESS, 3, null)));
  }

  @Test
  void acceptsAFailureEntryWithItsErrorMessage() {
    assertDoesNotThrow(() -> store.save(command("c-1", AuditOutcome.FAILURE, 0, "boom")));
  }

  @Test
  void acceptsAVetoedEntryNamingTheRejectingInterceptor() {
    assertDoesNotThrow(
        () -> store.save(command("c-1", AuditOutcome.VETOED, 0, "QuotaInterceptor")));
  }

  @Test
  void acceptsAnAnonymousEntryWithoutCorrelation() {
    assertDoesNotThrow(
        () ->
            store.save(
                new AuditEntry(
                    CommandId.of("c-1"),
                    "PlaceOrder",
                    AggregateType.of("order"),
                    AggregateId.of("order-1"),
                    null,
                    T0,
                    AuditOutcome.SUCCESS,
                    null,
                    1,
                    null)));
  }

  /** A query is not a command: no command id, no aggregate. */
  @Test
  void acceptsAQueryOriginEntry() {
    assertDoesNotThrow(
        () ->
            store.save(
                new AuditEntry(
                    null,
                    "GetOrder",
                    null,
                    null,
                    UserId.of("user-1"),
                    T0,
                    AuditOutcome.SUCCESS,
                    null,
                    0,
                    CorrelationId.of("corr-1"))));
  }

  /** A GDPR entry names a subject (an id) and no aggregate type. */
  @Test
  void acceptsASubjectEntryWithAnIdAndNoAggregateType() {
    assertDoesNotThrow(
        () ->
            store.save(
                new AuditEntry(
                    CommandId.of("c-1"),
                    "ForgetSubject",
                    null,
                    AggregateId.of("subject-hash"),
                    UserId.of("dpo"),
                    T0,
                    AuditOutcome.SUCCESS,
                    null,
                    0,
                    null)));
  }

  /** The trail is append-only and a retry runs the same command again: a repeat is not an error. */
  @Test
  void acceptsTheSameCommandIdTwice() {
    store.save(command("c-1", AuditOutcome.FAILURE, 0, "first attempt"));

    assertDoesNotThrow(() -> store.save(command("c-1", AuditOutcome.SUCCESS, 1, null)));
  }

  @Test
  void aNullEntryIsRefusedWithANullPointerException() {
    assertThrows(NullPointerException.class, () -> store.save(null));
  }
}
