package org.streamrune.runtime.gdpr;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;

class ForgetSubjectServiceTest {

  @Test
  void forget_callsDeleteKeyAndWritesAuditSuccess() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    service.forget(SubjectId.of("user-1"), UserId.of("admin-42"));

    verify(crypto).deleteKey(SubjectId.of("user-1"));
    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit).save(captor.capture());
    AuditEntry entry = captor.getValue();
    assertEquals("GDPR_FORGET", entry.commandType());
    // The audit row stores the HASHED subject id, never the raw value (which may be
    // PII) — otherwise the Article-17 erasure itself mints a fresh cleartext copy of the
    // identifier.
    assertEquals(AggregateId.of(SubjectId.of("user-1").redacted()), entry.aggregateId());
    assertNotEquals(
        AggregateId.of("user-1"),
        entry.aggregateId(),
        "the raw subject id must never be persisted in the GDPR audit row");
    assertEquals(UserId.of("admin-42"), entry.userId());
    assertEquals(AuditOutcome.SUCCESS, entry.outcome());
    assertNull(entry.errorMessage());
    assertEquals(0, entry.eventCount());
    assertTrue(entry.commandId().value().startsWith("gdpr-"));
  }

  @Test
  void forget_nullSubjectThrowsBeforeAudit() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    assertThrows(NullPointerException.class, () -> service.forget(null, UserId.of("admin-1")));
    verifyNoInteractions(crypto, audit);
  }

  @Test
  void forget_nullAuditStore_callsDeleteKeyOnly() {
    var crypto = mock(CryptoEngine.class);
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).build();

    service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

    verify(crypto).deleteKey(SubjectId.of("user-1"));
  }

  @Test
  void forget_cryptoThrows_writesAuditFailureAndRethrows() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var boom = new RuntimeException("KMS down");
    doThrow(boom).when(crypto).deleteKey(SubjectId.of("user-1"));
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    var thrown =
        assertThrows(
            RuntimeException.class,
            () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));
    assertSame(boom, thrown);

    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit).save(captor.capture());
    assertEquals(AuditOutcome.FAILURE, captor.getValue().outcome());
    assertEquals("KMS down", captor.getValue().errorMessage());
  }

  /**
   * Every GDPR row goes through {@code GdprAuditWriter}, the GDPR side of the {@code audit_log}
   * sink — a backend's message is free text and is persisted sanitized.
   */
  @Test
  void forget_cryptoThrows_auditFailureMessageIsPersistedSanitized() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    String raw = "KMS down\r\n2026-10-02 INFO gdpr - erasure completed\u0000";
    doThrow(new RuntimeException(raw)).when(crypto).deleteKey(SubjectId.of("user-1"));
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    assertThrows(
        RuntimeException.class, () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));

    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit).save(captor.capture());
    assertEquals(LogSanitizer.sanitizeFreeText(raw), captor.getValue().errorMessage());
    assertFalse(captor.getValue().errorMessage().chars().anyMatch(Character::isISOControl));
  }

  @Test
  void forget_cryptoThrowsAnError_writesAuditFailureAndRethrowsTheSameError() {
    // An Error from deleteKey (OutOfMemoryError,
    // LinkageError) is a failed forget too. The catch took only RuntimeException, so such a forget
    // left no audit row at all.
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var purger = mock(SubjectDataPurger.class);
    var boom = new LinkageError("crypto backend class failed to link");
    doThrow(boom).when(crypto).deleteKey(SubjectId.of("user-1"));
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .purgers(List.of(purger))
            .build();

    var thrown =
        assertThrows(
            LinkageError.class, () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));
    assertSame(boom, thrown);

    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit).save(captor.capture());
    assertEquals(AuditOutcome.FAILURE, captor.getValue().outcome());
    assertEquals("crypto backend class failed to link", captor.getValue().errorMessage());
    verify(purger, never()).purge(any());
  }

  @Test
  void forget_cryptoThrowsAnError_andTheFailureAuditThrows_rethrowsTheOriginalError() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var boom = new LinkageError("crypto backend class failed to link");
    var auditEx = new RuntimeException("audit also down");
    doThrow(boom).when(crypto).deleteKey(SubjectId.of("user-1"));
    doThrow(auditEx).when(audit).save(any());
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    var thrown =
        assertThrows(
            LinkageError.class, () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));
    assertSame(boom, thrown);
    assertEquals(List.of(auditEx), List.of(thrown.getSuppressed()));
  }

  @Test
  void forget_auditAfterSuccessThrows_doesNotPropagate() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    doThrow(new RuntimeException("audit DB down")).when(audit).save(any());
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    // Crypto-shred is irreversible — audit failure must NOT propagate.
    assertDoesNotThrow(() -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));
    verify(crypto).deleteKey(SubjectId.of("user-1"));
  }

  @Test
  void forget_auditOnFailureThrows_doesNotMaskOriginal() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var original = new RuntimeException("original crypto failure");
    var auditEx = new RuntimeException("audit also down");
    doThrow(original).when(crypto).deleteKey(SubjectId.of("user-1"));
    doThrow(auditEx).when(audit).save(any());
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    var thrown =
        assertThrows(
            RuntimeException.class,
            () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));
    assertSame(original, thrown);
    assertEquals(
        List.of(auditEx),
        List.of(thrown.getSuppressed()),
        "the audit failure is not lost: it rides along on the original as a suppressed exception");
  }

  @Test
  void forget_idempotent_secondCallSucceeds() {
    var crypto = mock(CryptoEngine.class);
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).build();

    service.forget(SubjectId.of("user-1"), null);
    service.forget(SubjectId.of("user-1"), null);

    verify(crypto, times(2)).deleteKey(SubjectId.of("user-1"));
  }

  @Test
  void forget_nullRequesterUserId_allowed() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    service.forget(SubjectId.of("user-1"), null);

    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit).save(captor.capture());
    assertNull(captor.getValue().userId());
  }

  @Test
  void builder_rejectsNullCryptoEngine() {
    assertThrows(NullPointerException.class, () -> ForgetSubjectService.builder().build());
  }

  // === Read-model purging (GDPR forget completeness) ===

  @Test
  void forget_invokesAllPurgersAfterKeyDeletion() {
    var crypto = mock(CryptoEngine.class);
    var p1 = mock(SubjectDataPurger.class);
    var p2 = mock(SubjectDataPurger.class);
    when(p1.name()).thenReturn("orders-read-model");
    when(p2.name()).thenReturn("customer-profile");
    var service =
        ForgetSubjectService.builder().cryptoEngine(crypto).purgers(List.of(p1, p2)).build();

    service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

    // Purgers run AFTER the key is deleted (crypto-shred first, then remove ciphertext rows).
    InOrder order = inOrder(crypto, p1, p2);
    order.verify(crypto).deleteKey(SubjectId.of("user-1"));
    order.verify(p1).purge(SubjectId.of("user-1"));
    order.verify(p2).purge(SubjectId.of("user-1"));
  }

  @Test
  void forget_auditsEachPurgeSuccess() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var p1 = mock(SubjectDataPurger.class);
    when(p1.name()).thenReturn("orders-read-model");
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .purgers(List.of(p1))
            .build();

    service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    // One FORGET success entry for the key-deletion, one per purger.
    verify(audit, times(2)).save(captor.capture());
    boolean purgeAudited =
        captor.getAllValues().stream()
            .anyMatch(
                e ->
                    e.outcome() == AuditOutcome.SUCCESS
                        && e.errorMessage() != null
                        && e.errorMessage().contains("orders-read-model"));
    assertTrue(purgeAudited, "each purge outcome is audited with the purger name");
  }

  @Test
  void forget_purgerFailure_isAuditedAndDoesNotAbortRemainingPurgers() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var metrics = new CapturingMetrics();
    var failing = mock(SubjectDataPurger.class);
    var ok = mock(SubjectDataPurger.class);
    when(failing.name()).thenReturn("broken-read-model");
    when(ok.name()).thenReturn("good-read-model");
    doThrow(new RuntimeException("DB down")).when(failing).purge(SubjectId.of("user-1"));
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .metrics(metrics)
            .purgers(List.of(failing, ok))
            .build();

    // A purger failure must not propagate (key already shredded) and must not skip later purgers,
    // but it must be reported PROGRAMMATICALLY so the caller does not tell the subject "erased".
    ForgetResult result = service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));
    verify(ok).purge(SubjectId.of("user-1"));

    assertTrue(result.keyDeleted(), "the key was crypto-shredded");
    assertFalse(result.fullyErased(), "a failed purge means erasure is NOT complete");
    assertEquals(List.of("broken-read-model"), result.failedPurgers());
    assertEquals(1, metrics.purgeFailed.get(), "the failed purge increments the alert metric");
    assertEquals("broken-read-model", metrics.lastPurger);

    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit, atLeast(1)).save(captor.capture());
    boolean failureAudited =
        captor.getAllValues().stream()
            .anyMatch(
                e ->
                    e.outcome() == AuditOutcome.FAILURE
                        && e.errorMessage() != null
                        && e.errorMessage().contains("broken-read-model"));
    assertTrue(failureAudited, "a purge failure is recorded as a FAILURE audit entry");
  }

  @Test
  void forget_allPurgersSucceed_reportsFullyErased() {
    var crypto = mock(CryptoEngine.class);
    var metrics = new CapturingMetrics();
    var p1 = mock(SubjectDataPurger.class);
    var p2 = mock(SubjectDataPurger.class);
    when(p1.name()).thenReturn("orders-read-model");
    when(p2.name()).thenReturn("customer-profile");
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .metrics(metrics)
            .purgers(List.of(p1, p2))
            .build();

    ForgetResult result = service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

    assertTrue(result.fullyErased(), "key deleted + all purgers succeeded");
    assertTrue(result.failedPurgers().isEmpty());
    assertEquals(2, result.purgeOutcomes().size());
    assertEquals(0, metrics.purgeFailed.get(), "no purge-failure metric when all succeed");
  }

  /**
   * {@code recordGdprPurgeFailed} is not a cheap increment — the Micrometer collector does live
   * registry work ({@code Counter.builder(...).register(registry)}), which genuinely throws in
   * production (a registry rejecting a colliding meter id, a user MeterFilter, a registry
   * mid-shutdown). Unguarded, that exception escaped the purge loop AFTER the key was already
   * irreversibly crypto-shredded: the remaining purgers never ran, no FAILURE audit row was written
   * for them, and the caller saw a throw it could not distinguish from "the shred itself failed".
   */
  @Test
  void forget_purgeFailedMetricThrowing_stillRunsRemainingPurgersAndReturnsOutcomes() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    org.streamrune.core.StreamRuneMetrics throwingMetrics =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void recordGdprPurgeFailed(String purgerName) {
            throw new IllegalStateException("meter registry rejected the id");
          }
        };
    var failing = mock(SubjectDataPurger.class);
    var ok = mock(SubjectDataPurger.class);
    when(failing.name()).thenReturn("broken-read-model");
    when(ok.name()).thenReturn("good-read-model");
    doThrow(new RuntimeException("DB down")).when(failing).purge(SubjectId.of("user-1"));
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .metrics(throwingMetrics)
            .purgers(List.of(failing, ok))
            .build();

    ForgetResult result =
        assertDoesNotThrow(
            () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")),
            "the key is already shredded — a metrics failure must never propagate out of forget()");

    verify(ok).purge(SubjectId.of("user-1"));
    assertEquals(2, result.purgeOutcomes().size(), "one outcome per registered purger");
    assertFalse(result.fullyErased(), "a failed purge means erasure is NOT complete");
    assertEquals(List.of("broken-read-model"), result.failedPurgers());

    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit, atLeast(1)).save(captor.capture());
    boolean failureAudited =
        captor.getAllValues().stream()
            .anyMatch(
                e ->
                    e.outcome() == AuditOutcome.FAILURE
                        && e.errorMessage() != null
                        && e.errorMessage().contains("broken-read-model"));
    assertTrue(
        failureAudited,
        "the FAILURE audit row two lines below the meter must still be written — it is the durable"
            + " record of the Article-17 gap");
  }

  /** Captures {@link org.streamrune.core.StreamRuneMetrics#recordGdprPurgeFailed(String)} calls. */
  static final class CapturingMetrics implements org.streamrune.core.StreamRuneMetrics {
    final java.util.concurrent.atomic.AtomicInteger purgeFailed =
        new java.util.concurrent.atomic.AtomicInteger();
    volatile String lastPurger;

    @Override
    public void recordGdprPurgeFailed(String purgerName) {
      purgeFailed.incrementAndGet();
      lastPurger = purgerName;
    }
  }

  @Test
  void forget_emptyPurgerList_isSafeNoOpAndBackwardCompatible() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .purgers(List.of())
            .build();

    service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

    verify(crypto).deleteKey(SubjectId.of("user-1"));
    // Only the key-deletion FORGET success entry — no purger entries.
    verify(audit, times(1)).save(any());
  }

  @Test
  void forget_noPurgersConfigured_behavesAsBefore() {
    var crypto = mock(CryptoEngine.class);
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).build();

    service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

    verify(crypto).deleteKey(SubjectId.of("user-1"));
  }

  @Test
  void forget_keyDeletionFailure_skipsPurgers() {
    var crypto = mock(CryptoEngine.class);
    var purger = mock(SubjectDataPurger.class);
    when(purger.name()).thenReturn("orders-read-model");
    doThrow(new RuntimeException("KMS down")).when(crypto).deleteKey(SubjectId.of("user-1"));
    var service =
        ForgetSubjectService.builder().cryptoEngine(crypto).purgers(List.of(purger)).build();

    assertThrows(
        RuntimeException.class, () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));
    // The key was never deleted, so purging read-model rows would be premature.
    verify(purger, never()).purge(any());
  }

  // ── The two audit catches inside forget, for a Throwable that is not a RuntimeException ──────
  //
  // FAILURE path: deleteKey failed, so nothing was erased and forget must say so with the ORIGINAL
  // failure. An Error from the FAILURE audit write (an AssertionError from an audit store, a
  // LinkageError) used to escape that catch and replace it, so the caller saw an audit-store Error
  // and lost the reason the erasure failed.
  //
  // SUCCESS path: deleteKey succeeded, so the key is gone for good. An Error after that propagates
  // (an Error is not swallowed anywhere in forget), but it must say that the irreversible part is
  // done: without that, an erasure that happened reads as one that failed. The last section of
  // this class pins that path (crash points 1 to 4).
  //
  // Durable flow (GDPR erasure), crash point on this path: deleteKey throws, the FAILURE audit
  // throws: no key change, the original failure propagates, a rerun calls deleteKey again.

  @Test
  void forget_cryptoThrows_andTheFailureAuditThrowsAnError_rethrowsTheOriginalWithTheAuditError() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var purger = mock(SubjectDataPurger.class);
    var original = new RuntimeException("KMS down");
    var auditError = new AssertionError("audit store broke an invariant");
    doThrow(original).when(crypto).deleteKey(SubjectId.of("user-1"));
    doThrow(auditError).when(audit).save(any());
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .purgers(List.of(purger))
            .build();

    var thrown =
        assertThrows(
            RuntimeException.class,
            () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));

    assertSame(original, thrown, "the audit write's Error must not replace the original failure");
    assertSame(
        auditError,
        thrown.getSuppressed().length == 1 ? thrown.getSuppressed()[0] : null,
        "the audit write's Error is not lost either: it rides along as a suppressed exception");
    verify(purger, never()).purge(any());
  }

  @Test
  void forget_cryptoThrowsAnError_andTheFailureAuditThrowsAnError_rethrowsTheOriginalError() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var original = new LinkageError("crypto backend class failed to link");
    var auditError = new LinkageError("audit store class failed to link");
    doThrow(original).when(crypto).deleteKey(SubjectId.of("user-1"));
    doThrow(auditError).when(audit).save(any());
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    var thrown =
        assertThrows(
            LinkageError.class, () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));

    assertSame(original, thrown);
    assertEquals(List.of(auditError), List.of(thrown.getSuppressed()));
  }

  @Test
  void forget_cryptoThrows_andTheFailureAuditThrowsTheSameInstance_stillRethrowsIt() {
    // A store that rethrows whatever it is handed must not turn the rethrow into an
    // IllegalArgumentException ("Self-suppression not permitted").
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var shared = new IllegalStateException("one shared failure");
    doThrow(shared).when(crypto).deleteKey(SubjectId.of("user-1"));
    doThrow(shared).when(audit).save(any());
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    var thrown =
        assertThrows(
            IllegalStateException.class,
            () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));

    assertSame(shared, thrown);
    assertEquals(0, thrown.getSuppressed().length, "an exception never suppresses itself");
  }

  // === An Error after the irreversible shred never stops the erasure ===
  //
  // Once deleteKey has returned the key is gone for good, so every remaining step is attempted:
  // the SUCCESS audit row, each purger, its audit row and the purge_failed metric. An Error from
  // any of them is not swallowed, but it waits: the first one propagates once every purger has run,
  // carrying a note that the shred completed and what is left, and later ones ride along on it.
  // The crash points below are numbered 1 to 4; each test's second half re-runs the forget and
  // shows it converges (deleteKey is a no-op on a deleted key, every purger is idempotent).

  /**
   * Crash point 1. A persistently broken audit store throws at the SUCCESS row, the first write
   * after the shred. If that Error skipped the purgers (the earlier behaviour), no read model would
   * ever be purged until the store was fixed, so derived PII would outlive a shred that already
   * happened.
   */
  @Test
  void forget_shredDone_successAuditThrowsAnError_stillRunsThePurgers_thenPropagatesIt() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var purger = mock(SubjectDataPurger.class);
    when(purger.name()).thenReturn("orders-read-model");
    var auditError = new LinkageError("audit store class failed to link");
    doThrow(auditError).when(audit).save(any());
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .purgers(List.of(purger))
            .build();

    var thrown =
        assertThrows(
            LinkageError.class, () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));

    assertSame(auditError, thrown, "an Error is not swallowed after the shred either");
    verify(crypto).deleteKey(SubjectId.of("user-1"));
    verify(purger).purge(SubjectId.of("user-1"));
    // The per-purge row threw the very same instance: it is neither suppressed on itself (that
    // would throw IllegalArgumentException and replace it) nor carried twice.
    assertEquals(1, thrown.getSuppressed().length, "the note is the only suppressed exception");
    String note = shredCompletedNote(thrown);
    assertTrue(note.contains("SUCCESS audit row was not written"), note);
    assertTrue(note.contains("[orders-read-model]"), "the note names the purger that ran: " + note);

    // Rerun convergence once the audit store is healthy again: the SUCCESS row is written and the
    // purger runs again (a no-op on a purged read model).
    reset(audit);
    ForgetResult result = service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

    verify(crypto, times(2)).deleteKey(SubjectId.of("user-1"));
    verify(purger, times(2)).purge(SubjectId.of("user-1"));
    assertTrue(result.fullyErased());
    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit, times(2)).save(captor.capture());
    assertEquals(AuditOutcome.SUCCESS, captor.getAllValues().getFirst().outcome());
  }

  @Test
  void forget_shredDone_successAuditThrowsAnError_withNoPurgerRegistered_saysSo() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var auditError = new LinkageError("audit store class failed to link");
    doThrow(auditError).when(audit).save(any());
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    var thrown =
        assertThrows(
            LinkageError.class, () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));

    assertSame(auditError, thrown);
    String note = shredCompletedNote(thrown);
    assertTrue(note.contains("No read-model purger is registered"), note);
  }

  /**
   * Crash point 2. A purger whose class cannot link (a missing optional dependency) fails the same
   * way on every run. Stopping at it would block every purger after it, on every re-run, until the
   * classpath is fixed.
   */
  @Test
  void forget_purgerThrowsAnError_stillRunsTheRemainingPurgers_thenPropagatesIt() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var metrics = new CapturingMetrics();
    var broken = mock(SubjectDataPurger.class);
    var good = mock(SubjectDataPurger.class);
    when(broken.name()).thenReturn("broken-read-model");
    when(good.name()).thenReturn("good-read-model");
    var purgeError = new NoClassDefFoundError("com/acme/SearchIndexClient");
    doThrow(purgeError).when(broken).purge(SubjectId.of("user-1"));
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .metrics(metrics)
            .purgers(List.of(broken, good))
            .build();

    var thrown =
        assertThrows(
            NoClassDefFoundError.class,
            () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));

    assertSame(purgeError, thrown, "an Error from a purger is not swallowed");
    InOrder order = inOrder(crypto, broken, good);
    order.verify(crypto).deleteKey(SubjectId.of("user-1"));
    order.verify(broken).purge(SubjectId.of("user-1"));
    order.verify(good).purge(SubjectId.of("user-1"));
    String note = shredCompletedNote(thrown);
    assertTrue(note.contains("purged [good-read-model]"), note);
    assertTrue(note.contains("failed [broken-read-model]"), note);
    assertFalse(
        note.contains("SUCCESS audit row was not written"), "that row was written: " + note);
    // The failed purge is recorded like any other: the alert metric and a FAILURE row naming it.
    assertEquals(1, metrics.purgeFailed.get());
    assertEquals("broken-read-model", metrics.lastPurger);
    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit, times(3)).save(captor.capture());
    assertTrue(
        captor.getAllValues().stream()
            .anyMatch(
                e ->
                    e.outcome() == AuditOutcome.FAILURE
                        && e.errorMessage().contains("broken-read-model")),
        "the Error'd purge leaves a FAILURE row");

    // Rerun convergence once the purger links again.
    doNothing().when(broken).purge(SubjectId.of("user-1"));
    ForgetResult result = service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

    assertTrue(result.fullyErased());
    verify(broken, times(2)).purge(SubjectId.of("user-1"));
    verify(good, times(2)).purge(SubjectId.of("user-1"));
  }

  /** Crash point 3. The audit store breaks after the forget's SUCCESS row, at a per-purge row. */
  @Test
  void forget_purgeAuditRowThrowsAnError_stillRunsTheRemainingPurgers_thenPropagatesIt() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var p1 = mock(SubjectDataPurger.class);
    var p2 = mock(SubjectDataPurger.class);
    when(p1.name()).thenReturn("orders-read-model");
    when(p2.name()).thenReturn("customer-profile");
    var auditError = new LinkageError("audit store class failed to link");
    // The forget's SUCCESS row goes through; every per-purge row after it throws the same Error.
    doNothing().doThrow(auditError).when(audit).save(any());
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .purgers(List.of(p1, p2))
            .build();

    var thrown =
        assertThrows(
            LinkageError.class, () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));

    assertSame(auditError, thrown);
    verify(p1).purge(SubjectId.of("user-1"));
    verify(p2).purge(SubjectId.of("user-1"));
    verify(audit, times(3)).save(any());
    assertEquals(1, thrown.getSuppressed().length, "the repeated Error is carried once, as itself");
    String note = shredCompletedNote(thrown);
    assertTrue(note.contains("[orders-read-model, customer-profile]"), note);
    assertFalse(
        note.contains("SUCCESS audit row was not written"), "that row was written: " + note);

    // Rerun convergence: both purges are no-ops now, and their rows are written this time.
    reset(audit);
    ForgetResult result = service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

    assertTrue(result.fullyErased());
    verify(audit, times(3)).save(any());
  }

  /** Crash point 4. The metrics backend throws an Error while a purge failure is being counted. */
  @Test
  void forget_purgeFailedMetricThrowsAnError_stillAuditsTheFailureAndRunsTheRemainingPurgers() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var metricError = new AssertionError("meter registry invariant broken");
    org.streamrune.core.StreamRuneMetrics throwingMetrics =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void recordGdprPurgeFailed(String purgerName) {
            throw metricError;
          }
        };
    var failing = mock(SubjectDataPurger.class);
    var ok = mock(SubjectDataPurger.class);
    when(failing.name()).thenReturn("broken-read-model");
    when(ok.name()).thenReturn("good-read-model");
    doThrow(new RuntimeException("DB down")).when(failing).purge(SubjectId.of("user-1"));
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .metrics(throwingMetrics)
            .purgers(List.of(failing, ok))
            .build();

    var thrown =
        assertThrows(
            AssertionError.class,
            () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));

    assertSame(metricError, thrown);
    verify(ok).purge(SubjectId.of("user-1"));
    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit, times(3)).save(captor.capture());
    assertTrue(
        captor.getAllValues().stream()
            .anyMatch(
                e ->
                    e.outcome() == AuditOutcome.FAILURE
                        && e.errorMessage().contains("broken-read-model")),
        "the FAILURE row after the meter is still written: it is the durable record of the gap");
    String note = shredCompletedNote(thrown);
    assertTrue(note.contains("failed [broken-read-model]"), note);
  }

  @Test
  void forget_twoPostShredErrors_theFirstPropagatesAndCarriesTheSecond() {
    var crypto = mock(CryptoEngine.class);
    var p1 = mock(SubjectDataPurger.class);
    var p2 = mock(SubjectDataPurger.class);
    var p3 = mock(SubjectDataPurger.class);
    when(p1.name()).thenReturn("orders-read-model");
    when(p2.name()).thenReturn("customer-profile");
    when(p3.name()).thenReturn("search-index");
    var first = new StackOverflowError();
    var second = new NoClassDefFoundError("com/acme/ProfileStore");
    doThrow(first).when(p1).purge(SubjectId.of("user-1"));
    doThrow(second).when(p2).purge(SubjectId.of("user-1"));
    var service =
        ForgetSubjectService.builder().cryptoEngine(crypto).purgers(List.of(p1, p2, p3)).build();

    var thrown =
        assertThrows(
            StackOverflowError.class,
            () -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));

    assertSame(first, thrown, "the first Error propagates");
    verify(p3).purge(SubjectId.of("user-1"));
    assertTrue(
        List.of(thrown.getSuppressed()).contains(second), "the later Error rides along on it");
    String note = shredCompletedNote(thrown);
    assertTrue(note.contains("purged [search-index]"), note);
    assertTrue(note.contains("failed [orders-read-model, customer-profile]"), note);
  }

  /**
   * A purger that throws a checked exception without declaring it (a sneaky {@code SQLException})
   * failed exactly like one throwing a RuntimeException: it is a failed purge, reported in the
   * result, not a reason to abort the erasure. It used to escape the purge loop.
   */
  @Test
  void forget_purgerThrowsACheckedException_isAnOrdinaryPurgeFailure() {
    var crypto = mock(CryptoEngine.class);
    var metrics = new CapturingMetrics();
    var sneaky = mock(SubjectDataPurger.class);
    var ok = mock(SubjectDataPurger.class);
    when(sneaky.name()).thenReturn("legacy-jdbc-model");
    when(ok.name()).thenReturn("good-read-model");
    doAnswer(
            inv -> {
              throw new java.sql.SQLException("connection refused");
            })
        .when(sneaky)
        .purge(SubjectId.of("user-1"));
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .metrics(metrics)
            .purgers(List.of(sneaky, ok))
            .build();

    ForgetResult result =
        assertDoesNotThrow(() -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));

    verify(ok).purge(SubjectId.of("user-1"));
    assertEquals(List.of("legacy-jdbc-model"), result.failedPurgers());
    assertEquals("connection refused", result.purgeOutcomes().getFirst().detail());
    assertEquals(1, metrics.purgeFailed.get());
  }

  /**
   * An undeclared InterruptedException from a purger is a failed purge like any other exception, so
   * it is not rethrown, but the interrupt it carried must not be lost with it. The flag is restored
   * once, on the way out — not as soon as it is caught: on a virtual thread, blocking socket I/O
   * with the flag set closes the socket, so every later JDBC-backed purger and audit write would
   * fail too, cutting the erasure short after all.
   */
  @Test
  void forget_purgerThrowsAnInterruptedException_failsThatPurgeAndKeepsTheInterruptFlag() {
    var crypto = mock(CryptoEngine.class);
    var interrupted = mock(SubjectDataPurger.class);
    var ok = mock(SubjectDataPurger.class);
    when(interrupted.name()).thenReturn("slow-read-model");
    when(ok.name()).thenReturn("good-read-model");
    doAnswer(
            inv -> {
              throw new InterruptedException("purge interrupted");
            })
        .when(interrupted)
        .purge(SubjectId.of("user-1"));
    var laterPurgerRanInterrupted = new java.util.concurrent.atomic.AtomicBoolean(true);
    doAnswer(
            inv -> {
              laterPurgerRanInterrupted.set(Thread.currentThread().isInterrupted());
              return null;
            })
        .when(ok)
        .purge(SubjectId.of("user-1"));
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .purgers(List.of(interrupted, ok))
            .build();

    try {
      ForgetResult result = service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

      assertTrue(Thread.currentThread().isInterrupted(), "the interrupt is restored, not eaten");
      assertEquals(List.of("slow-read-model"), result.failedPurgers());
      verify(ok).purge(SubjectId.of("user-1"));
      assertFalse(
          laterPurgerRanInterrupted.get(),
          "the purger after the interrupted one runs on a thread without the flag set");
    } finally {
      Thread.interrupted(); // never leak the flag into the next test on this thread
    }
  }

  /**
   * The class contract: after the shred, an audit failure is not propagated, an Error being the one
   * exception. A checked exception thrown without being declared is an audit failure too.
   */
  @Test
  void forget_shredDone_successAuditThrowsACheckedException_isSwallowedAndThePurgersRun() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var purger = mock(SubjectDataPurger.class);
    when(purger.name()).thenReturn("orders-read-model");
    doAnswer(
            inv -> {
              throw new java.sql.SQLException("audit DB down");
            })
        .when(audit)
        .save(any());
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .purgers(List.of(purger))
            .build();

    ForgetResult result =
        assertDoesNotThrow(() -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));

    verify(purger).purge(SubjectId.of("user-1"));
    assertTrue(result.fullyErased(), "the purge itself succeeded; only its audit row is missing");
  }

  // ---- A purger's name() is read once, when the service is built. ----------------------------
  //
  // name() is the purger's own code. The builder reads it once, before any key can be deleted, and
  // forget() never calls it, so it is not a step after the shred. A name() that throws, or returns
  // null or blank, does not skip the purger: it is purged, and its outcome is recorded under its
  // class name, which says WHICH purger lacks a usable name.

  /** name() throws an exception: the purge still runs and the outcome is recorded. */
  @Test
  void forget_purgerNameThrows_stillPurgesUnderItsClassName_andAuditsIt() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var nameless = mock(SubjectDataPurger.class);
    var ok = mock(SubjectDataPurger.class);
    when(nameless.name()).thenThrow(new IllegalStateException("name lookup failed"));
    when(ok.name()).thenReturn("good-read-model");
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .purgers(List.of(nameless, ok))
            .build();

    ForgetResult result =
        assertDoesNotThrow(() -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));

    String placeholder = nameless.getClass().getSimpleName();
    verify(nameless).purge(SubjectId.of("user-1"));
    verify(ok).purge(SubjectId.of("user-1"));
    assertEquals(
        List.of(placeholder, "good-read-model"),
        result.purgeOutcomes().stream().map(ForgetResult.PurgeOutcome::purgerName).toList(),
        "the purger whose name() threw is reported under its class name");
    assertTrue(result.purgeOutcomes().getFirst().succeeded(), "the purge itself succeeded");
    assertTrue(result.fullyErased(), "the erasure is complete; only the name was unusable");
    assertTrue(
        auditSummaries(audit).contains("purged read model '" + placeholder + "'"),
        "the purge row is attributable to the purger: " + auditSummaries(audit));
  }

  /** A null or blank name is unusable too: PurgeOutcome used to NPE on the null. */
  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", "   ", "\t\n"})
  void forget_purgerNameNullOrBlank_stillPurgesUnderItsClassName(String unusableName) {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var nameless = mock(SubjectDataPurger.class);
    var ok = mock(SubjectDataPurger.class);
    when(nameless.name()).thenReturn(unusableName);
    when(ok.name()).thenReturn("good-read-model");
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .purgers(List.of(nameless, ok))
            .build();

    ForgetResult result =
        assertDoesNotThrow(() -> service.forget(SubjectId.of("user-1"), UserId.of("admin-1")));

    String placeholder = nameless.getClass().getSimpleName();
    verify(nameless).purge(SubjectId.of("user-1"));
    verify(ok).purge(SubjectId.of("user-1"));
    assertEquals(placeholder, result.purgeOutcomes().getFirst().purgerName());
    assertEquals("good-read-model", result.purgeOutcomes().get(1).purgerName());
    assertTrue(result.fullyErased());
    assertTrue(
        auditSummaries(audit).contains("purged read model '" + placeholder + "'"),
        auditSummaries(audit).toString());
  }

  /**
   * An anonymous class has no simple name, so its name without the package stands in. The package
   * is dropped because the sink caps a name at 64 characters from the left and a fully qualified
   * name would lose the distinguishing end of it.
   */
  @Test
  void forget_anonymousPurgerWithoutAName_isNamedByItsBinaryNameWithoutThePackage() {
    var crypto = mock(CryptoEngine.class);
    var purged = new java.util.concurrent.atomic.AtomicBoolean();
    SubjectDataPurger anonymous =
        new SubjectDataPurger() {
          @Override
          public String name() {
            return null;
          }

          @Override
          public void purge(SubjectId subjectId) {
            purged.set(true);
          }
        };
    var service =
        ForgetSubjectService.builder().cryptoEngine(crypto).purgers(List.of(anonymous)).build();

    ForgetResult result = service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

    assertTrue(purged.get());
    String name = result.purgeOutcomes().getFirst().purgerName();
    assertEquals("", anonymous.getClass().getSimpleName(), "premise: an anonymous class");
    assertEquals(
        anonymous
            .getClass()
            .getName()
            .substring(anonymous.getClass().getName().lastIndexOf('.') + 1),
        name);
    assertTrue(name.startsWith("ForgetSubjectServiceTest$"), name);
    assertFalse(name.contains("."), "no package: " + name);
  }

  /**
   * A purger that has no usable name AND fails to purge: the failure is counted, audited and
   * reported under the class name, never under null.
   */
  @Test
  void forget_purgerNameThrows_andThePurgeFails_isReportedUnderTheClassName() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var metrics = new CapturingMetrics();
    var nameless = mock(SubjectDataPurger.class);
    when(nameless.name()).thenThrow(new IllegalStateException("name lookup failed"));
    doThrow(new RuntimeException("DB down")).when(nameless).purge(SubjectId.of("user-1"));
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .metrics(metrics)
            .purgers(List.of(nameless))
            .build();

    ForgetResult result = service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

    String placeholder = nameless.getClass().getSimpleName();
    assertFalse(result.fullyErased());
    assertEquals(List.of(placeholder), result.failedPurgers());
    assertEquals(1, metrics.purgeFailed.get());
    assertEquals(placeholder, metrics.lastPurger, "the alert metric is never tagged with null");
    assertTrue(
        auditSummaries(audit).stream()
            .anyMatch(m -> m.contains("'" + placeholder + "'") && m.contains("failed: DB down")),
        auditSummaries(audit).toString());
  }

  /**
   * name() throws an Error (a purger class whose name() needs a class that does not link). It
   * propagates from build() unchanged, so the service is never created and no key can be deleted
   * with a purger the erasure could not name; the purgers after it are not asked for a name.
   */
  @Test
  void builder_purgerNameThrowsAnError_propagatesBeforeAnyKeyIsDeleted() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var broken = mock(SubjectDataPurger.class);
    var good = mock(SubjectDataPurger.class);
    var nameError = new NoClassDefFoundError("com/acme/NamingScheme");
    when(broken.name()).thenThrow(nameError);
    var builder =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .purgers(List.of(broken, good));

    var thrown = assertThrows(NoClassDefFoundError.class, builder::build);

    assertSame(nameError, thrown, "an Error from name() is not wrapped or swallowed");
    verify(broken, never()).purge(any());
    verifyNoInteractions(crypto, audit, good);
  }

  /**
   * The name is read once, when the service is built: forget() never asks for it again, so nothing
   * a name() does can happen after a key is deleted, and the outcome names are fixed. Here the
   * second purger would answer "orders", the first purger's name, from then on.
   */
  @Test
  void builder_readsEachPurgerNameOnce_andForgetNeverAsksAgain() {
    var crypto = mock(CryptoEngine.class);
    var orders = mock(SubjectDataPurger.class);
    var drifting = mock(SubjectDataPurger.class);
    when(orders.name()).thenReturn("orders");
    when(drifting.name()).thenReturn("customers", "orders");
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .purgers(List.of(orders, drifting))
            .build();
    verify(orders, times(1)).name();
    verify(drifting, times(1)).name();

    ForgetResult first = service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));
    ForgetResult second = service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

    verify(orders, times(1)).name();
    verify(drifting, times(1)).name();
    for (ForgetResult result : List.of(first, second)) {
      assertEquals(
          List.of("orders", "customers"),
          result.purgeOutcomes().stream().map(ForgetResult.PurgeOutcome::purgerName).toList());
    }
  }

  /**
   * A purger name is an identifier the purger chose, and a log line or an audit summary is a sink:
   * The name passes through {@link LogSanitizer#sanitizeForLog} in both, whichever of the purge
   * outcomes it describes. The returned outcome keeps the name as the purger gave it, so a caller
   * can still match it to the purger it registered.
   */
  @Test
  void forget_purgerNameWithControlCharacters_isSanitizedInLogLinesAndAuditSummaries() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var metrics = new CapturingMetrics();
    var forging = mock(SubjectDataPurger.class);
    var failing = mock(SubjectDataPurger.class);
    String forgedName = "orders\nERROR forged line\u202e";
    String failingName = "x".repeat(200) + "\r\nFORGED";
    when(forging.name()).thenReturn(forgedName);
    when(failing.name()).thenReturn(failingName);
    doThrow(new RuntimeException("DB down")).when(failing).purge(SubjectId.of("user-1"));
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .auditStore(audit)
            .metrics(metrics)
            .purgers(List.of(forging, failing))
            .build();
    var appender = attachGdprLogAppender();
    ForgetResult result;
    try {
      result = service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

      String cleanForged = LogSanitizer.sanitizeForLog(forgedName);
      String cleanFailing = LogSanitizer.sanitizeForLog(failingName);
      assertEquals("ordersERROR forged line", cleanForged);
      assertTrue(cleanFailing.endsWith("..."), "an over-long name is capped: " + cleanFailing);
      List<String> logged =
          appender.list.stream()
              .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
              .toList();
      assertTrue(
          logged.stream().anyMatch(m -> m.contains("'" + cleanFailing + "'")),
          "the failed-purge WARN names the purger sanitized: " + logged);
      assertTrue(
          logged.stream().noneMatch(m -> m.contains("\n") || m.contains("\r")),
          "no purger name forges a log line: " + logged);
      assertTrue(logged.stream().noneMatch(m -> m.contains("FORGED")), logged.toString());
      List<String> summaries = auditSummaries(audit);
      assertTrue(
          summaries.contains("purged read model '" + cleanForged + "'"),
          "the SUCCESS row carries the sanitized name: " + summaries);
      assertTrue(
          summaries.stream()
              .anyMatch(
                  m ->
                      m.startsWith("purge of read model '" + cleanFailing + "' failed: ")
                          && !m.contains("FORGED")),
          "the FAILURE row carries the sanitized name: " + summaries);
    } finally {
      detachGdprLogAppender(appender);
    }

    assertEquals(
        List.of(forgedName, failingName),
        result.purgeOutcomes().stream().map(ForgetResult.PurgeOutcome::purgerName).toList(),
        "the returned outcome keeps each name as the purger gave it");
  }

  /**
   * The WARN that says which purger lacks a usable name, so the operator can fix it. It is logged
   * once, when the service is built, and not again on every forget.
   */
  @Test
  void builder_purgerWithoutAUsableName_warnsWhichClassIsAtFault() {
    var crypto = mock(CryptoEngine.class);
    var throwing = mock(SubjectDataPurger.class);
    when(throwing.name()).thenThrow(new IllegalStateException("name lookup failed"));
    SubjectDataPurger nullNamed = purgerNamed(null);
    var appender = attachGdprLogAppender();
    try {
      var service =
          ForgetSubjectService.builder()
              .cryptoEngine(crypto)
              .purgers(List.of(throwing, nullNamed))
              .build();

      List<String> warnings = warnings(appender);
      assertTrue(
          warnings.stream()
              .anyMatch(
                  m ->
                      m.contains(throwing.getClass().getSimpleName())
                          && m.contains("name lookup failed")
                          && m.contains("name()")),
          "the WARN names the purger class and the cause: " + warnings);
      assertTrue(
          warnings.stream()
              .anyMatch(
                  m -> m.contains(GdprNames.classNameOf(nullNamed)) && m.contains("null or blank")),
          "the WARN names the purger class that returned no name: " + warnings);

      appender.list.clear();
      service.forget(SubjectId.of("user-1"), UserId.of("admin-1"));

      assertEquals(List.of(), warnings(appender), "a forget does not repeat the WARN");
    } finally {
      detachGdprLogAppender(appender);
    }
  }

  // ---- Two purgers under one name are refused when the service is built. ---------------------

  /**
   * Two purgers under one name: their outcomes, audit rows and purge_failed tags could not be told
   * apart, so a failed purge could not be traced to its read model. build() refuses it, before any
   * key can be deleted, and names the shared name and the classes sharing it.
   */
  @Test
  void builder_refusesTwoPurgersUnderOneName_beforeAnyKeyIsDeleted() {
    var crypto = mock(CryptoEngine.class);
    var first = purgerNamed("customers");
    var second = mock(SubjectDataPurger.class);
    when(second.name()).thenReturn("customers");
    var builder =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .purgers(List.of(purgerNamed("orders"), first, second));

    var ex = assertThrows(IllegalArgumentException.class, builder::build);

    assertTrue(ex.getMessage().contains("'customers'"), ex.getMessage());
    assertTrue(ex.getMessage().contains(GdprNames.classNameOf(first)), ex.getMessage());
    assertTrue(ex.getMessage().contains(GdprNames.classNameOf(second)), ex.getMessage());
    assertFalse(ex.getMessage().contains("'orders'"), "only a shared name: " + ex.getMessage());
    verifyNoInteractions(crypto);
    verify(second, never()).purge(any());
  }

  /**
   * A purger without a usable name is named by its class, so two such instances of one class would
   * share a name as well. They are refused the same way, and so is a purger whose own name is the
   * class name standing in for another one.
   */
  @Test
  void builder_refusesTwoNamelessPurgersOfOneClass() {
    var crypto = mock(CryptoEngine.class);
    var nullNamed = purgerNamed(null);
    var blankNamed = purgerNamed("\t");
    String placeholder = GdprNames.classNameOf(nullNamed);
    assertEquals(placeholder, GdprNames.classNameOf(blankNamed), "premise: one class");

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ForgetSubjectService.builder()
                    .cryptoEngine(crypto)
                    .purgers(List.of(nullNamed, blankNamed))
                    .build());
    assertTrue(ex.getMessage().contains("'" + placeholder + "'"), ex.getMessage());

    var namedLikeTheClass = mock(SubjectDataPurger.class);
    when(namedLikeTheClass.name()).thenReturn(placeholder);
    var clash =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ForgetSubjectService.builder()
                    .cryptoEngine(crypto)
                    .purgers(List.of(nullNamed, namedLikeTheClass))
                    .build());
    assertTrue(clash.getMessage().contains("'" + placeholder + "'"), clash.getMessage());
    verifyNoInteractions(crypto);
  }

  /** The refusal is an exception message, so the shared name goes through the log sanitizer. */
  @Test
  void builder_refusal_sanitizesTheSharedName() {
    String forged = "orders\nERROR forged line\u202e";
    var builder =
        ForgetSubjectService.builder()
            .cryptoEngine(mock(CryptoEngine.class))
            .purgers(List.of(purgerNamed(forged), purgerNamed(forged)));

    var ex = assertThrows(IllegalArgumentException.class, builder::build);

    assertTrue(
        ex.getMessage().contains("'" + LogSanitizer.sanitizeForLog(forged) + "'"), ex.getMessage());
    assertTrue(ex.getMessage().chars().noneMatch(c -> c == '\n' || c == '\u202e'), ex.getMessage());
  }

  /** A purger of one shared anonymous class that answers {@code name} and purges nothing. */
  private static SubjectDataPurger purgerNamed(String name) {
    return new SubjectDataPurger() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public void purge(SubjectId subjectId) {
        // nothing to purge
      }
    };
  }

  /** The WARN-level messages the GDPR logger received. */
  private static List<String> warnings(
      ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender) {
    return appender.list.stream()
        .filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
        .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
        .toList();
  }

  /** The {@code errorMessage} (summary) of every audit row the store was asked to save. */
  private static List<String> auditSummaries(AuditStore audit) {
    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit, atLeast(1)).save(captor.capture());
    return captor.getAllValues().stream()
        .map(AuditEntry::errorMessage)
        .filter(java.util.Objects::nonNull)
        .toList();
  }

  private static ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>
      attachGdprLogAppender() {
    var logger = (ch.qos.logback.classic.Logger) GdprAuditWriter.logger();
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    return appender;
  }

  private static void detachGdprLogAppender(
      ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender) {
    ((ch.qos.logback.classic.Logger) GdprAuditWriter.logger()).detachAppender(appender);
  }

  /** Returns the shred-completed note attached to {@code thrown}, after checking what it says. */
  private static String shredCompletedNote(Throwable thrown) {
    String note =
        java.util.Arrays.stream(thrown.getSuppressed())
            .map(Throwable::getMessage)
            .filter(java.util.Objects::nonNull)
            .filter(m -> m.contains("crypto-shred") && m.contains("completed"))
            .findFirst()
            .orElse(null);
    assertNotNull(
        note,
        "the propagating Error must say the irreversible crypto-shred completed, so the caller"
            + " does not read a done erasure as a failed one");
    assertTrue(note.contains("re-run"), "and how to finish it: forget is idempotent");
    assertFalse(note.contains("user-1"), "the raw subject id never lands in the message");
    return note;
  }
}
