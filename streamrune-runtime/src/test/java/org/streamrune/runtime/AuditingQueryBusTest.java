package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.streamrune.core.Query;
import org.streamrune.core.QueryHandler;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.audit.Auditable;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.UserId;

class AuditingQueryBusTest {

  private SimpleQueryBus delegate;
  private AuditStore auditStore;
  private AuditingQueryBus bus;

  @Auditable
  record FindUserById(String id) implements Query<FindUserByIdResult> {}

  record FindUserByIdResult(String name) {}

  record NonAuditableQuery(String id) implements Query<NonAuditableResult> {}

  record NonAuditableResult(String data) {}

  @BeforeEach
  void setUp() {
    delegate = new SimpleQueryBus();
    auditStore = mock(AuditStore.class);
    bus = new AuditingQueryBus(delegate, auditStore);
  }

  @Test
  void auditableQuerySavesAuditEntry() {
    delegate.register(FindUserById.class, q -> new FindUserByIdResult("Alice"));

    CorrelationId correlationId = CorrelationId.of("corr-flow-1");
    FindUserByIdResult[] holder = new FindUserByIdResult[1];
    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, UserId.of("caller-1"), correlationId, Instant.now(), java.util.Map.of()))
        .run(() -> holder[0] = bus.dispatch(new FindUserById("u1")));

    assertThat(holder[0].name()).isEqualTo("Alice");

    ArgumentCaptor<AuditEntry> captor = ArgumentCaptor.forClass(AuditEntry.class);
    verify(auditStore).save(captor.capture());
    AuditEntry entry = captor.getValue();
    assertThat(entry.commandType()).isEqualTo("FindUserById");
    assertThat(entry.aggregateId()).isNull();
    assertThat(entry.userId()).isEqualTo(UserId.of("caller-1"));
    assertThat(entry.outcome()).isEqualTo(AuditOutcome.SUCCESS);
    assertThat(entry.eventCount()).isZero();
    // Queries are not commands: no fabricated CommandId, just the business-flow correlation token.
    assertThat(entry.commandId()).isNull();
    assertThat(entry.correlationId()).isEqualTo(correlationId);
  }

  @Test
  void queryOriginEntryHasNullCommandIdAndCorrelationWhenUnbound() {
    delegate.register(FindUserById.class, q -> new FindUserByIdResult("Carol"));

    bus.dispatch(new FindUserById("u9"));

    ArgumentCaptor<AuditEntry> captor = ArgumentCaptor.forClass(AuditEntry.class);
    verify(auditStore).save(captor.capture());
    AuditEntry entry = captor.getValue();
    assertThat(entry.commandId()).isNull();
    assertThat(entry.correlationId()).isNull();
  }

  @Test
  void nonAuditableQueryDoesNotSaveEntry() {
    delegate.register(NonAuditableQuery.class, q -> new NonAuditableResult("data"));

    NonAuditableResult result = bus.dispatch(new NonAuditableQuery("x"));

    assertThat(result.data()).isEqualTo("data");
    verifyNoInteractions(auditStore);
  }

  @Test
  void failedQuerySavesFailureEntry() {
    delegate.register(
        FindUserById.class,
        q -> {
          throw new RuntimeException("DB down");
        });

    assertThatThrownBy(() -> bus.dispatch(new FindUserById("u1")))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("DB down");

    ArgumentCaptor<AuditEntry> captor = ArgumentCaptor.forClass(AuditEntry.class);
    verify(auditStore).save(captor.capture());
    AuditEntry entry = captor.getValue();
    assertThat(entry.outcome()).isEqualTo(AuditOutcome.FAILURE);
    assertThat(entry.errorMessage()).isEqualTo("DB down");
    assertThat(entry.eventCount()).isZero();
  }

  /**
   * The query FAILURE row is the same {@code audit_log} sink as the command one — the persisted
   * message is the sanitized one, while the caller still gets the original exception.
   */
  @Test
  void failedQuery_persistsTheErrorMessageSanitized_andRethrowsTheOriginal() {
    String raw = "no user 'u1\n2026-10-02 WARN forged'\u0000";
    delegate.register(
        FindUserById.class,
        q -> {
          throw new RuntimeException(raw);
        });

    assertThatThrownBy(() -> bus.dispatch(new FindUserById("u1"))).hasMessage(raw);

    ArgumentCaptor<AuditEntry> captor = ArgumentCaptor.forClass(AuditEntry.class);
    verify(auditStore).save(captor.capture());
    assertThat(captor.getValue().errorMessage())
        .isEqualTo(LogSanitizer.sanitizeFreeText(raw))
        .doesNotContain("\n")
        .doesNotContain("\u0000");
  }

  @Test
  void
      handlerThrows_andAuditFailureSaveThrows_originalExceptionPropagatesWithSuppressedAuditError() {
    // The audit_log DB is out at the same moment the business query fails. The FAILURE
    // audit save also throws — the caller and logs must still see the REAL business failure, not
    // the audit-store error (which merely masks it). The save error is attached as suppressed.
    delegate.register(
        FindUserById.class,
        q -> {
          throw new IllegalStateException("DB down");
        });
    doThrow(new RuntimeException("audit store unavailable")).when(auditStore).save(any());

    assertThatThrownBy(() -> bus.dispatch(new FindUserById("u1")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("DB down")
        .satisfies(
            t ->
                assertThat(t.getSuppressed())
                    .anySatisfy(
                        s -> assertThat(s).hasMessageContaining("audit store unavailable")));
  }

  @Test
  void handlerSucceeds_andAuditSuccessSaveThrows_resultReturnedFailOpenNoFailureRow() {
    // A successful query whose SUCCESS audit save throws must NOT be misrouted through the
    // failure branch — fail-open (matching the command path, VirtualThreadCommandBus.notifyAfter):
    // return the result, log, and NEVER write a false FAILURE audit row.
    delegate.register(FindUserById.class, q -> new FindUserByIdResult("Alice"));
    doThrow(new RuntimeException("audit store unavailable")).when(auditStore).save(any());

    FindUserByIdResult result = bus.dispatch(new FindUserById("u1"));

    assertThat(result.name()).isEqualTo("Alice");
    ArgumentCaptor<AuditEntry> captor = ArgumentCaptor.forClass(AuditEntry.class);
    verify(auditStore, times(1)).save(captor.capture());
    // The single save attempt was the SUCCESS entry; no FAILURE row was ever written.
    assertThat(captor.getValue().outcome()).isEqualTo(AuditOutcome.SUCCESS);
  }

  @Test
  void nullUserIdForAnonymousRequest() {
    delegate.register(FindUserById.class, q -> new FindUserByIdResult("Bob"));

    bus.dispatch(new FindUserById("u2"));

    ArgumentCaptor<AuditEntry> captor = ArgumentCaptor.forClass(AuditEntry.class);
    verify(auditStore).save(captor.capture());
    assertThat(captor.getValue().userId()).isNull();
  }

  @Test
  void registerDelegatesToWrappedBus() {
    QueryHandler<FindUserById, FindUserByIdResult> handler = q -> new FindUserByIdResult("test");
    bus.register(FindUserById.class, handler);

    FindUserByIdResult result = bus.dispatch(new FindUserById("x"));
    assertThat(result.name()).isEqualTo("test");
  }

  @Test
  void annotationLookupIsMemoized() {
    delegate.register(FindUserById.class, q -> new FindUserByIdResult("A"));

    bus.dispatch(new FindUserById("1"));
    bus.dispatch(new FindUserById("2"));
    bus.dispatch(new FindUserById("3"));

    // All three should be audited (annotation found each time from cache)
    verify(auditStore, times(3)).save(any());
  }
}
