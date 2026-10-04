package org.streamrune.runtime.gdpr;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;
import org.streamrune.test.InMemoryCryptoEngine;

/**
 * {@link ForgetSubjectService#reinstate(SubjectId, UserId)} — the audited way to lift a forgotten
 * subject's terminal-erasure tombstone. A reinstate reverses part of an erasure (on AWS KMS all of
 * it), so it must leave a {@code GDPR_REINSTATE} row in the audit log next to the {@code
 * GDPR_FORGET} rows it reverses.
 */
class ForgetSubjectServiceReinstateTest {

  private static final SubjectId SUBJECT = SubjectId.of("user-1");
  private static final UserId DPO = UserId.of("dpo-7");
  private static final byte[] PII = "alice@example.com".getBytes(StandardCharsets.UTF_8);

  @Test
  void reinstate_liftsTheTombstoneThroughTheEngine_thenRecordsASuccessRow() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    service.reinstate(SUBJECT, DPO);

    InOrder order = inOrder(crypto, audit);
    order.verify(crypto).reinstate(SUBJECT);
    var captor = ArgumentCaptor.forClass(AuditEntry.class);
    order.verify(audit).save(captor.capture());
    verifyNoMoreInteractions(crypto, audit);
    AuditEntry row = captor.getValue();
    assertEquals("GDPR_REINSTATE", row.commandType());
    assertEquals(
        AggregateId.of(SUBJECT.redacted()),
        row.aggregateId(),
        "the row stores the hashed subject id, the same one the GDPR_FORGET rows carry");
    assertEquals(DPO, row.userId(), "the row names who decided to reinstate");
    assertEquals(AuditOutcome.SUCCESS, row.outcome());
    assertNull(row.errorMessage());
    assertEquals(0, row.eventCount());
    assertTrue(row.commandId().value().startsWith("gdpr-"));
  }

  @Test
  void reinstate_nullSubject_throwsBeforeTouchingTheEngineOrTheAuditLog() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    assertThrows(NullPointerException.class, () -> service.reinstate(null, DPO));
    verifyNoInteractions(crypto, audit);
  }

  @Test
  void reinstate_withoutAnAuditStore_stillReinstates() {
    var crypto = mock(CryptoEngine.class);
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).build();

    service.reinstate(SUBJECT, null);

    verify(crypto).reinstate(SUBJECT);
  }

  @Test
  void reinstate_engineRefuses_recordsAFailureRowWithTheReason_andRethrowsTheSameException() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var refusal =
        new CryptoOperationException(
            "Refusing to reinstate subject: the erasure COMPLETED (the transit key was destroyed)");
    doThrow(refusal).when(crypto).reinstate(SUBJECT);
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    var thrown =
        assertThrows(CryptoOperationException.class, () -> service.reinstate(SUBJECT, DPO));

    assertSame(refusal, thrown);
    assertEquals(0, thrown.getSuppressed().length);
    var captor = ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit).save(captor.capture());
    AuditEntry row = captor.getValue();
    assertEquals("GDPR_REINSTATE", row.commandType());
    assertEquals(AuditOutcome.FAILURE, row.outcome());
    assertEquals(refusal.getMessage(), row.errorMessage());
    assertEquals(DPO, row.userId());
    assertEquals(AggregateId.of(SUBJECT.redacted()), row.aggregateId());
  }

  @Test
  void reinstate_engineThrowsAnError_recordsAFailureRow_andRethrowsTheSameError() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var error = new LinkageError("engine class failed to link");
    doThrow(error).when(crypto).reinstate(SUBJECT);
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    var thrown = assertThrows(LinkageError.class, () -> service.reinstate(SUBJECT, DPO));

    assertSame(error, thrown);
    var captor = ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit).save(captor.capture());
    assertEquals("GDPR_REINSTATE", captor.getValue().commandType());
    assertEquals(AuditOutcome.FAILURE, captor.getValue().outcome());
  }

  @Test
  void reinstate_engineFails_andTheFailureRowFailsToo_rethrowsTheEngineFailureCarryingTheOther() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var refusal = new CryptoOperationException("tombstone store unavailable");
    var auditDown = new IllegalStateException("audit_log unavailable");
    doThrow(refusal).when(crypto).reinstate(SUBJECT);
    doThrow(auditDown).when(audit).save(any());
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    var thrown =
        assertThrows(CryptoOperationException.class, () -> service.reinstate(SUBJECT, DPO));

    assertSame(refusal, thrown, "the reason the reinstate failed is never replaced");
    assertEquals(List.of(auditDown), List.of(thrown.getSuppressed()));
  }

  @Test
  void reinstate_engineFails_andTheAuditStoreRethrowsTheSameInstance_stillRethrowsIt() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var shared = new IllegalStateException("one shared failure");
    doThrow(shared).when(crypto).reinstate(SUBJECT);
    doThrow(shared).when(audit).save(any());
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    var thrown = assertThrows(IllegalStateException.class, () -> service.reinstate(SUBJECT, DPO));

    assertSame(shared, thrown);
    assertEquals(0, thrown.getSuppressed().length, "an exception never suppresses itself");
  }

  @Test
  void reinstate_successRowFails_propagatesTheAuditFailure_sayingTheTombstoneIsAlreadyLifted() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var auditDown = new IllegalStateException("audit_log unavailable");
    doThrow(auditDown).when(audit).save(any());
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();
    var appender = attachGdprLogAppender();
    try {
      var thrown = assertThrows(IllegalStateException.class, () -> service.reinstate(SUBJECT, DPO));

      assertSame(auditDown, thrown);
      verify(crypto).reinstate(SUBJECT);
      reinstateCompletedNote(thrown);
      List<String> warnings =
          appender.list.stream()
              .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
              .toList();
      assertTrue(
          warnings.stream()
              .anyMatch(
                  m ->
                      m.contains("reinstate")
                          && m.contains(SUBJECT.redacted())
                          && m.contains("audit_log unavailable")),
          "a WARN records the unaudited reinstate by subject hash: " + warnings);
      assertTrue(
          warnings.stream().noneMatch(m -> m.contains("user-1")),
          "the raw subject id never lands in a log line: " + warnings);
    } finally {
      detachGdprLogAppender(appender);
    }
  }

  @Test
  void reinstate_successRowThrowsAnError_propagatesTheErrorWithTheSameNote() {
    var crypto = mock(CryptoEngine.class);
    var audit = mock(AuditStore.class);
    var auditError = new AssertionError("audit store invariant broken");
    doThrow(auditError).when(audit).save(any());
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();

    var thrown = assertThrows(AssertionError.class, () -> service.reinstate(SUBJECT, DPO));

    assertSame(auditError, thrown);
    verify(crypto).reinstate(SUBJECT);
    reinstateCompletedNote(thrown);
  }

  /**
   * The DPO's view: one forget and one reinstate of the same subject read back as two decisions in
   * the audit log, by the same hashed subject id, each naming its requester.
   */
  @Test
  void forgetThenReinstate_leavesBothDecisionsInTheAuditTrail() {
    var crypto = new InMemoryCryptoEngine();
    List<AuditEntry> auditLog = new ArrayList<>();
    var service =
        ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(auditLog::add).build();
    crypto.encrypt(SUBJECT, PII);

    service.forget(SUBJECT, UserId.of("dpo-1"));
    assertThrows(SubjectForgottenException.class, () -> crypto.encrypt(SUBJECT, PII));
    service.reinstate(SUBJECT, DPO);

    assertDoesNotThrow(() -> crypto.encrypt(SUBJECT, PII), "the subject can re-register");
    assertEquals(
        List.of("GDPR_FORGET", "GDPR_REINSTATE"),
        auditLog.stream().map(AuditEntry::commandType).toList());
    assertTrue(auditLog.stream().allMatch(e -> e.outcome() == AuditOutcome.SUCCESS));
    assertTrue(
        auditLog.stream()
            .allMatch(e -> e.aggregateId().equals(AggregateId.of(SUBJECT.redacted()))));
    assertEquals(UserId.of("dpo-1"), auditLog.get(0).userId());
    assertEquals(DPO, auditLog.get(1).userId());
  }

  /**
   * Crash point: the engine lifted the tombstone and the SUCCESS row was not written (the audit
   * store failed, or the JVM died between the two). A re-run converges: the engine's reinstate is a
   * no-op on a subject that is no longer forgotten, and the re-run writes the missing SUCCESS row.
   */
  @Test
  void reinstate_rerunAfterTheSuccessRowWasLost_recordsTheReinstate() {
    var crypto = new InMemoryCryptoEngine();
    List<AuditEntry> auditLog = new ArrayList<>();
    var failNextWrite = new AtomicBoolean();
    AuditStore audit =
        entry -> {
          if (failNextWrite.getAndSet(false)) {
            throw new IllegalStateException("audit_log unavailable");
          }
          auditLog.add(entry);
        };
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).auditStore(audit).build();
    service.forget(SUBJECT, DPO);

    failNextWrite.set(true);
    assertThrows(IllegalStateException.class, () -> service.reinstate(SUBJECT, DPO));
    assertDoesNotThrow(
        () -> crypto.encrypt(SUBJECT, PII), "the reinstate took effect before the audit failed");
    assertEquals(List.of("GDPR_FORGET"), commandTypes(auditLog), "and is not on record yet");

    service.reinstate(SUBJECT, DPO);

    assertEquals(List.of("GDPR_FORGET", "GDPR_REINSTATE"), commandTypes(auditLog));
    assertEquals(AuditOutcome.SUCCESS, auditLog.get(1).outcome());
    assertDoesNotThrow(() -> crypto.encrypt(SUBJECT, PII));
  }

  private static List<String> commandTypes(List<AuditEntry> auditLog) {
    return auditLog.stream().map(AuditEntry::commandType).toList();
  }

  /** Checks the note saying the reinstate took effect although its SUCCESS row is missing. */
  private static void reinstateCompletedNote(Throwable thrown) {
    String note =
        Arrays.stream(thrown.getSuppressed())
            .map(Throwable::getMessage)
            .filter(Objects::nonNull)
            .filter(m -> m.contains("reinstate"))
            .findFirst()
            .orElse(null);
    assertNotNull(
        note,
        "the propagating failure must say the tombstone was already lifted, so the caller does not"
            + " read a reinstate that took effect as one that did not");
    assertTrue(note.contains("lifted"), note);
    assertTrue(note.contains("SUCCESS audit row"), note);
    assertTrue(note.contains("re-run"), "and how to put it on record: " + note);
    assertTrue(note.contains(SUBJECT.redacted()), note);
    assertFalse(note.contains("user-1"), "the raw subject id never lands in the message");
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
}
