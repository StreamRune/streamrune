package org.streamrune.runtime.gdpr;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.classfile.ClassFile;
import java.lang.constant.ClassDesc;
import java.lang.constant.ConstantDescs;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.gdpr.SubjectDataCollector;
import org.streamrune.core.gdpr.SubjectExport;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;

class ExportSubjectDataServiceTest {

  private final ObjectMapper mapper = new ObjectMapper();

  private SubjectDataCollector collector(String name, JsonNode value) {
    return new SubjectDataCollector() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public JsonNode collect(SubjectId subjectId) {
        return value;
      }
    };
  }

  private SubjectDataCollector failingCollector(String name, RuntimeException ex) {
    return new SubjectDataCollector() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public JsonNode collect(SubjectId subjectId) {
        throw ex;
      }
    };
  }

  private SubjectDataCollector throwingCollector(String name, Throwable t) {
    return new SubjectDataCollector() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public JsonNode collect(SubjectId subjectId) {
        if (t instanceof RuntimeException re) throw re;
        if (t instanceof Error err) throw err;
        throw new RuntimeException(t);
      }
    };
  }

  @Test
  void export_mergesAllCollectorsInOrder() {
    var profile = mapper.createObjectNode().put("email", "x@y.z");
    var orders = mapper.createArrayNode().add("ord-1");
    var service =
        ExportSubjectDataService.builder()
            .collectors(List.of(collector("profile", profile), collector("orders", orders)))
            .build();

    var export = service.export(SubjectId.of("user-1"), UserId.of("admin"));

    assertEquals(SubjectId.of("user-1"), export.subjectId());
    assertEquals(2, export.sections().size());
    var keys = new ArrayList<>(export.sections().keySet());
    assertEquals("profile", keys.get(0));
    assertEquals("orders", keys.get(1));
  }

  @Test
  void export_skipsCollectorsReturningNull() {
    var profile = mapper.createObjectNode().put("email", "x@y.z");
    var service =
        ExportSubjectDataService.builder()
            .collectors(List.of(collector("profile", profile), collector("orders", null)))
            .build();

    var export = service.export(SubjectId.of("user-1"), UserId.of("admin"));

    assertEquals(1, export.sections().size());
    assertTrue(export.sections().containsKey("profile"));
    assertFalse(export.sections().containsKey("orders"));
  }

  @Test
  void export_collectorThrows_carriesPartialSections() {
    var profile = mapper.createObjectNode().put("email", "x@y.z");
    var boom = new RuntimeException("CRM down");
    var service =
        ExportSubjectDataService.builder()
            .collectors(List.of(collector("profile", profile), failingCollector("crm", boom)))
            .build();

    var ex =
        assertThrows(
            GdprExportException.class,
            () -> service.export(SubjectId.of("user-1"), UserId.of("admin")));

    assertEquals("user-1", ex.subjectId());
    assertEquals("crm", ex.failedCollectorName());
    assertEquals(1, ex.partialSections().size());
    assertTrue(ex.partialSections().containsKey("profile"));
    assertSame(boom, ex.getCause());
  }

  @Test
  void export_collectorThrows_exceptionMessageRedactsSubjectId() {
    // The subject id is PII (an email). The GdprExportException message must carry
    // only its hash — a raw id here lands in application logs / HTTP error handlers verbatim.
    var pii = "alice@example.com";
    var service =
        ExportSubjectDataService.builder()
            .collectors(List.of(failingCollector("crm", new RuntimeException("boom"))))
            .build();

    var ex =
        assertThrows(
            GdprExportException.class, () -> service.export(SubjectId.of(pii), UserId.of("admin")));

    assertFalse(
        ex.getMessage().contains(pii),
        "exception message must not leak the raw subject id: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains(SubjectId.of(pii).redacted()),
        "exception message must carry the redacted subject hash: " + ex.getMessage());
    // The raw id stays available via the accessor for callers that legitimately need it.
    assertEquals(pii, ex.subjectId());
  }

  @Test
  void export_collectorThrows_writesAuditFailure() {
    var audit = mock(AuditStore.class);
    var boom = new RuntimeException("CRM down");
    var service =
        ExportSubjectDataService.builder()
            .collectors(List.of(failingCollector("crm", boom)))
            .auditStore(audit)
            .build();

    assertThrows(
        GdprExportException.class,
        () -> service.export(SubjectId.of("user-1"), UserId.of("admin")));

    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit).save(captor.capture());
    assertEquals("GDPR_EXPORT", captor.getValue().commandType());
    assertEquals(AuditOutcome.FAILURE, captor.getValue().outcome());
    assertTrue(captor.getValue().errorMessage().contains("crm"));
    assertTrue(captor.getValue().errorMessage().contains("CRM down"));
  }

  @Test
  void export_emptyCollectors_throwsAtCallTime() {
    var service = ExportSubjectDataService.builder().collectors(List.of()).build();

    assertThrows(
        IllegalStateException.class,
        () -> service.export(SubjectId.of("user-1"), UserId.of("admin")));
  }

  @Test
  void export_nullAuditStore_returnsExportWithoutAudit() {
    var service =
        ExportSubjectDataService.builder()
            .collectors(List.of(collector("profile", mapper.createObjectNode())))
            .build();

    var export = service.export(SubjectId.of("user-1"), null);

    assertEquals(1, export.sections().size());
  }

  @Test
  void export_writesAuditSuccessWithSectionNames() {
    var audit = mock(AuditStore.class);
    var service =
        ExportSubjectDataService.builder()
            .collectors(
                List.of(
                    collector("profile", mapper.createObjectNode()),
                    collector("orders", mapper.createObjectNode())))
            .auditStore(audit)
            .build();

    service.export(SubjectId.of("user-1"), UserId.of("admin"));

    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit).save(captor.capture());
    assertEquals(AuditOutcome.SUCCESS, captor.getValue().outcome());
    assertEquals("sections=profile,orders", captor.getValue().errorMessage());
  }

  @Test
  void export_auditSuccessThrows_propagates() {
    var audit = mock(AuditStore.class);
    doThrow(new RuntimeException("audit DB down")).when(audit).save(any());
    var service =
        ExportSubjectDataService.builder()
            .collectors(List.of(collector("profile", mapper.createObjectNode())))
            .auditStore(audit)
            .build();

    // Unlike forget, export audit failure propagates — caller can retry safely.
    assertThrows(
        RuntimeException.class, () -> service.export(SubjectId.of("user-1"), UserId.of("admin")));
  }

  @Test
  void builder_acceptsEmptyList() {
    // Construction allowed (Quarkus path); export() validates lazily.
    assertDoesNotThrow(() -> ExportSubjectDataService.builder().collectors(List.of()).build());
  }

  @Test
  void builder_rejectsNullCollectors() {
    assertThrows(NullPointerException.class, () -> ExportSubjectDataService.builder().build());
  }

  @Test
  void export_nullSubjectThrowsNPE() {
    @SuppressWarnings("unchecked")
    SubjectDataCollector spy = mock(SubjectDataCollector.class);
    var service = ExportSubjectDataService.builder().collectors(List.of(spy)).build();

    assertThrows(NullPointerException.class, () -> service.export(null, UserId.of("admin")));
    verify(spy, never()).collect(any());
  }

  /**
   * Rule S1181: an {@link Error} from a collector is audited as FAILURE like any other failure,
   * then rethrown unchanged. Wrapped in a {@link GdprExportException}, an {@code OutOfMemoryError}
   * read as a business failure carrying a partial export, which application code may handle and
   * carry on from.
   */
  @Test
  void export_collectorThrowsError_auditsFailureAndRethrowsTheErrorUnwrapped() {
    var audit = mock(AuditStore.class);
    var oom = new Error("simulated OOM");
    var service =
        ExportSubjectDataService.builder()
            .collectors(List.of(throwingCollector("crm", oom)))
            .auditStore(audit)
            .build();

    var thrown =
        assertThrows(Error.class, () -> service.export(SubjectId.of("user-1"), UserId.of("admin")));

    assertSame(oom, thrown);
    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit).save(captor.capture());
    assertEquals(AuditOutcome.FAILURE, captor.getValue().outcome());
    assertTrue(captor.getValue().errorMessage().contains("crm"));
    assertTrue(captor.getValue().errorMessage().contains("simulated OOM"));
  }

  // ---- A collector's name() is read once, when the service is built. --------------------------
  //
  // The builder reads every name() once and keys each collector's section by it for as long as the
  // service lives; export() never calls name(). A name() that throws, or returns null or blank,
  // does not stop anything: the collector is named by a placeholder, its class name, and a WARN at
  // build time says which class lacks a usable name.

  /** name() throws an exception: the collection still runs and the other collectors run too. */
  @Test
  void export_collectorNameThrows_isStillCollectedUnderItsClassName_andTheOthersRun() {
    var audit = mock(AuditStore.class);
    var profile = mapper.createObjectNode().put("email", "x@y.z");
    var orders = mapper.createArrayNode().add("ord-1");
    var nameless = mock(SubjectDataCollector.class);
    var ok = mock(SubjectDataCollector.class);
    when(nameless.name()).thenThrow(new IllegalStateException("name lookup failed"));
    when(nameless.collect(SubjectId.of("user-1"))).thenReturn(profile);
    when(ok.name()).thenReturn("orders");
    when(ok.collect(SubjectId.of("user-1"))).thenReturn(orders);
    var service =
        ExportSubjectDataService.builder()
            .collectors(List.of(nameless, ok))
            .auditStore(audit)
            .build();

    SubjectExport export =
        assertDoesNotThrow(() -> service.export(SubjectId.of("user-1"), UserId.of("admin")));

    String placeholder = nameless.getClass().getSimpleName();
    assertEquals(List.of(placeholder, "orders"), List.copyOf(export.sections().keySet()));
    assertSame(profile, export.sections().get(placeholder));
    assertSame(orders, export.sections().get("orders"));
    verify(nameless).collect(SubjectId.of("user-1"));
    verify(ok).collect(SubjectId.of("user-1"));
    assertEquals(
        "sections=" + placeholder + ",orders",
        auditSummaries(audit).getFirst(),
        "the SUCCESS row names the section by the placeholder");
  }

  /** A null or blank name is unusable too: it used to be a section keyed by null or by blank. */
  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", "   ", "\t\n"})
  void export_collectorNameNullOrBlank_isStillCollectedUnderItsClassName(String unusableName) {
    var audit = mock(AuditStore.class);
    var profile = mapper.createObjectNode().put("email", "x@y.z");
    var nameless = mock(SubjectDataCollector.class);
    var ok = mock(SubjectDataCollector.class);
    when(nameless.name()).thenReturn(unusableName);
    when(nameless.collect(SubjectId.of("user-1"))).thenReturn(profile);
    when(ok.name()).thenReturn("orders");
    when(ok.collect(SubjectId.of("user-1"))).thenReturn(mapper.createArrayNode());
    var service =
        ExportSubjectDataService.builder()
            .collectors(List.of(nameless, ok))
            .auditStore(audit)
            .build();

    SubjectExport export =
        assertDoesNotThrow(() -> service.export(SubjectId.of("user-1"), UserId.of("admin")));

    String placeholder = nameless.getClass().getSimpleName();
    assertEquals(List.of(placeholder, "orders"), List.copyOf(export.sections().keySet()));
    assertSame(profile, export.sections().get(placeholder));
    verify(ok).collect(SubjectId.of("user-1"));
    assertEquals("sections=" + placeholder + ",orders", auditSummaries(audit).getFirst());
  }

  /** The name is read once, when the service is built: an export never asks for it again. */
  @Test
  void builder_readsEachCollectorNameOnce_andAnExportNeverAsksAgain() {
    var collector = mock(SubjectDataCollector.class);
    when(collector.name()).thenReturn("profile");
    when(collector.collect(SubjectId.of("user-1"))).thenReturn(mapper.createObjectNode());
    var service = ExportSubjectDataService.builder().collectors(List.of(collector)).build();
    verify(collector, times(1)).name();

    service.export(SubjectId.of("user-1"), UserId.of("admin"));
    service.export(SubjectId.of("user-1"), UserId.of("admin"));

    verify(collector, times(1)).name();
  }

  /** A collector without a usable name that returns nothing contributes no section, as usual. */
  @Test
  void export_namelessCollectorReturningNull_contributesNoSection() {
    var nameless = mock(SubjectDataCollector.class);
    var ok = mock(SubjectDataCollector.class);
    when(nameless.name()).thenReturn(null);
    when(nameless.collect(SubjectId.of("user-1"))).thenReturn(null);
    when(ok.name()).thenReturn("orders");
    when(ok.collect(SubjectId.of("user-1"))).thenReturn(mapper.createArrayNode());
    var service = ExportSubjectDataService.builder().collectors(List.of(nameless, ok)).build();

    SubjectExport export = service.export(SubjectId.of("user-1"), UserId.of("admin"));

    assertEquals(List.of("orders"), List.copyOf(export.sections().keySet()));
  }

  /**
   * An anonymous class has no simple name, so its name without the package stands in. The package
   * is dropped because the sink caps a name at 64 characters from the left, and a fully qualified
   * name would lose the distinguishing end of it.
   */
  @Test
  void export_anonymousCollectorWithoutAName_isNamedByItsBinaryNameWithoutThePackage() {
    SubjectDataCollector anonymous =
        new SubjectDataCollector() {
          @Override
          public String name() {
            return null;
          }

          @Override
          public JsonNode collect(SubjectId subjectId) {
            return mapper.createObjectNode();
          }
        };
    var service = ExportSubjectDataService.builder().collectors(List.of(anonymous)).build();

    SubjectExport export = service.export(SubjectId.of("user-1"), UserId.of("admin"));

    String name = export.sections().keySet().iterator().next();
    String binary = anonymous.getClass().getName();
    assertEquals("", anonymous.getClass().getSimpleName(), "premise: an anonymous class");
    assertEquals(binary.substring(binary.lastIndexOf('.') + 1), name);
    assertTrue(name.startsWith("ExportSubjectDataServiceTest$"), name);
    assertFalse(name.contains("."), "no package: " + name);
  }

  /**
   * A collector that has no usable name AND fails: the failure is reported under the class name,
   * with the sections collected before it, never under null and never as the failure of name().
   */
  @Test
  void export_collectorNameThrows_andCollectFails_isReportedUnderTheClassName() {
    var audit = mock(AuditStore.class);
    var profile = mapper.createObjectNode().put("email", "x@y.z");
    var boom = new RuntimeException("CRM down");
    var nameless = mock(SubjectDataCollector.class);
    when(nameless.name()).thenThrow(new IllegalStateException("name lookup failed"));
    when(nameless.collect(SubjectId.of("user-1"))).thenThrow(boom);
    var service =
        ExportSubjectDataService.builder()
            .collectors(List.of(collector("profile", profile), nameless))
            .auditStore(audit)
            .build();

    var ex =
        assertThrows(
            GdprExportException.class,
            () -> service.export(SubjectId.of("user-1"), UserId.of("admin")));

    String placeholder = nameless.getClass().getSimpleName();
    assertEquals(placeholder, ex.failedCollectorName());
    assertTrue(ex.getMessage().contains("collector=" + placeholder), ex.getMessage());
    assertEquals(List.of("profile"), List.copyOf(ex.partialSections().keySet()));
    assertSame(boom, ex.getCause(), "the cause is the collection failure, not the name failure");
    var rows = auditRows(audit);
    assertEquals(1, rows.size());
    assertEquals(AuditOutcome.FAILURE, rows.getFirst().outcome());
    assertEquals("collector " + placeholder + " failed: CRM down", rows.getFirst().errorMessage());
  }

  /**
   * name() throws an Error (a collector class whose name() needs a class that does not link). It
   * propagates from build() unchanged: the service is never created, so nothing is collected and
   * nothing is audited, and the collectors after it are not asked for a name.
   */
  @Test
  void builder_collectorNameThrowsAnError_propagatesUnchanged() {
    var audit = mock(AuditStore.class);
    var nameError = new NoClassDefFoundError("com/acme/NamingScheme");
    var broken = mock(SubjectDataCollector.class);
    var after = mock(SubjectDataCollector.class);
    when(broken.name()).thenThrow(nameError);
    var builder =
        ExportSubjectDataService.builder().collectors(List.of(broken, after)).auditStore(audit);

    var thrown = assertThrows(NoClassDefFoundError.class, builder::build);

    assertSame(nameError, thrown, "an Error from name() is not wrapped or swallowed");
    verify(broken, never()).collect(any());
    verifyNoInteractions(after, audit);
  }

  /**
   * The section keys are fixed when the service is built. A name() that answers differently later
   * changes nothing for this service, so it cannot move a section under another collector's key:
   * here the second collector would answer "orders", the first collector's name, from then on.
   */
  @Test
  void export_sectionKeysAreFixedWhenTheServiceIsBuilt() {
    var orders = mapper.createArrayNode().add("ord-1");
    var profile = mapper.createObjectNode().put("email", "x@y.z");
    var drifting = mock(SubjectDataCollector.class);
    when(drifting.name()).thenReturn("profile", "orders");
    when(drifting.collect(SubjectId.of("user-1"))).thenReturn(profile);
    var service =
        ExportSubjectDataService.builder()
            .collectors(List.of(collector("orders", orders), drifting))
            .build();

    SubjectExport first = service.export(SubjectId.of("user-1"), UserId.of("admin"));
    SubjectExport second = service.export(SubjectId.of("user-1"), UserId.of("admin"));

    for (SubjectExport export : List.of(first, second)) {
      assertEquals(List.of("orders", "profile"), List.copyOf(export.sections().keySet()));
      assertSame(orders, export.sections().get("orders"));
      assertSame(profile, export.sections().get("profile"));
    }
  }

  /** A name() that failed when the service was built stays the class name until it is rebuilt. */
  @Test
  void export_aNameThatFailedAtBuild_staysTheClassNameUntilTheServiceIsRebuilt() {
    var flaky = mock(SubjectDataCollector.class);
    when(flaky.name()).thenThrow(new IllegalStateException("not yet")).thenReturn("profile");
    when(flaky.collect(SubjectId.of("user-1"))).thenReturn(mapper.createObjectNode());
    var service = ExportSubjectDataService.builder().collectors(List.of(flaky)).build();
    var rebuilt = ExportSubjectDataService.builder().collectors(List.of(flaky)).build();

    String placeholder = flaky.getClass().getSimpleName();
    for (int i = 0; i < 2; i++) {
      var export = service.export(SubjectId.of("user-1"), UserId.of("admin"));
      assertEquals(List.of(placeholder), List.copyOf(export.sections().keySet()));
    }
    var export = rebuilt.export(SubjectId.of("user-1"), UserId.of("admin"));
    assertEquals(List.of("profile"), List.copyOf(export.sections().keySet()));
  }

  /**
   * The WARN that says which collector lacks a usable name, so the operator can fix it. It is
   * logged once, when the service is built, and not again on every export.
   */
  @Test
  void builder_collectorWithoutAUsableName_warnsWhichClassIsAtFault() {
    var throwing = mock(SubjectDataCollector.class);
    when(throwing.name()).thenThrow(new IllegalStateException("name lookup failed"));
    SubjectDataCollector nullNamed = collector(null, mapper.createObjectNode());
    var appender = attachGdprLogAppender();
    try {
      var service =
          ExportSubjectDataService.builder().collectors(List.of(throwing, nullNamed)).build();

      List<String> warnings = warnings(appender);
      assertTrue(
          warnings.stream()
              .anyMatch(
                  m ->
                      m.contains(throwing.getClass().getSimpleName())
                          && m.contains("name lookup failed")
                          && m.contains("name()")),
          "the WARN names the collector class and the cause: " + warnings);
      assertTrue(
          warnings.stream()
              .anyMatch(
                  m -> m.contains(GdprNames.classNameOf(nullNamed)) && m.contains("null or blank")),
          "the WARN names the collector class that returned no name: " + warnings);

      appender.list.clear();
      service.export(SubjectId.of("user-1"), UserId.of("admin"));

      assertEquals(List.of(), warnings(appender), "an export does not repeat the WARN");
    } finally {
      detachGdprLogAppender(appender);
    }
  }

  /**
   * The placeholder is a name the service made up from a class name, and a class name can carry any
   * character the JVM allows in one (a generated or obfuscated class). It is rendered through
   * {@link LogSanitizer#sanitizeForLog} wherever it lands: the section key, the WARN, the audit
   * summary. A class that has a line break in its name is generated for the test, as no source file
   * can declare one.
   */
  @Test
  void export_placeholderOfAClassNamedWithControlCharacters_isSanitizedEverywhere()
      throws Exception {
    String rawName = "Evil\nERROR forged line\u202EName";
    SubjectDataCollector evil = collectorClassNamed(rawName);
    String placeholder = LogSanitizer.sanitizeForLog(rawName);
    assertEquals("EvilERROR forged lineName", placeholder, "premise: the sanitized form differs");
    assertEquals(rawName, evil.getClass().getSimpleName(), "premise: the class carries the name");
    var audit = mock(AuditStore.class);
    var appender = attachGdprLogAppender();
    try {
      var service =
          ExportSubjectDataService.builder().collectors(List.of(evil)).auditStore(audit).build();
      service.export(SubjectId.of("user-1"), UserId.of("admin"));

      List<String> logged = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
      assertTrue(
          logged.stream().anyMatch(m -> m.contains(placeholder)),
          "the WARN names the sanitized class name: " + logged);
      assertTrue(
          logged.stream().noneMatch(m -> m.contains("\n") || m.contains("\u202E")),
          "no class name forges a log line: " + logged);
    } finally {
      detachGdprLogAppender(appender);
    }
    assertEquals("sections=" + placeholder, auditSummaries(audit).getFirst());
  }

  /**
   * A name a collector chose is an identifier, and a log line, an audit summary or an exception
   * message is a sink: it passes through {@link LogSanitizer#sanitizeForLog} there. The export
   * itself keeps the name as the collector gave it, as it is the key a consumer parses.
   */
  @Test
  void export_collectorNameWithControlCharacters_isSanitizedInAuditSummariesAndExceptions() {
    var audit = mock(AuditStore.class);
    String forgedName = "crm\nERROR forged line\u202E";
    String failingName = "x".repeat(200) + "\r\nFORGED";
    String cleanForged = LogSanitizer.sanitizeForLog(forgedName);
    String cleanFailing = LogSanitizer.sanitizeForLog(failingName);
    assertEquals("crmERROR forged line", cleanForged);
    assertTrue(cleanFailing.endsWith("..."), "an over-long name is capped: " + cleanFailing);

    var succeeding =
        ExportSubjectDataService.builder()
            .collectors(List.of(collector(forgedName, mapper.createObjectNode())))
            .auditStore(audit)
            .build();
    SubjectExport export = succeeding.export(SubjectId.of("user-1"), UserId.of("admin"));

    assertEquals(
        List.of(forgedName),
        List.copyOf(export.sections().keySet()),
        "the export keeps the name as the collector gave it");
    assertEquals("sections=" + cleanForged, auditSummaries(audit).getFirst());

    var failing =
        ExportSubjectDataService.builder()
            .collectors(List.of(failingCollector(failingName, new RuntimeException("boom"))))
            .auditStore(audit)
            .build();
    var ex =
        assertThrows(
            GdprExportException.class,
            () -> failing.export(SubjectId.of("user-1"), UserId.of("admin")));

    assertEquals(failingName, ex.failedCollectorName(), "the accessor keeps the name as given");
    assertTrue(ex.getMessage().contains("collector=" + cleanFailing), ex.getMessage());
    assertFalse(ex.getMessage().contains("FORGED"), ex.getMessage());
    assertTrue(
        ex.getMessage().chars().noneMatch(c -> c == '\n' || c == '\r'),
        "no collector name forges a line: " + ex.getMessage());
    var rows = auditRows(audit);
    assertEquals("collector " + cleanFailing + " failed: boom", rows.getLast().errorMessage());
  }

  // ---- Two collectors under one section name are refused when the service is built. ----------

  /**
   * Two collectors under one name: the second one's section would replace the first one's, and the
   * SUCCESS row would still list the name, so the data subject would get an incomplete export that
   * looks complete. build() refuses it and names the shared name and the classes sharing it.
   */
  @Test
  void builder_refusesTwoCollectorsUnderOneName() {
    var first = collector("orders", mapper.createArrayNode());
    var second = mock(SubjectDataCollector.class);
    when(second.name()).thenReturn("orders");
    var builder =
        ExportSubjectDataService.builder()
            .collectors(List.of(collector("profile", mapper.createObjectNode()), first, second));

    var ex = assertThrows(IllegalArgumentException.class, builder::build);

    assertTrue(ex.getMessage().contains("'orders'"), ex.getMessage());
    assertTrue(ex.getMessage().contains(GdprNames.classNameOf(first)), ex.getMessage());
    assertTrue(ex.getMessage().contains(GdprNames.classNameOf(second)), ex.getMessage());
    assertFalse(ex.getMessage().contains("'profile'"), "only a shared name: " + ex.getMessage());
    verify(second, never()).collect(any());
  }

  /**
   * A collector without a usable name is named by its class, so two such instances of one class
   * would share a section key as well. They are refused the same way, and so is a collector whose
   * own name is the class name standing in for another one.
   */
  @Test
  void builder_refusesTwoNamelessCollectorsOfOneClass() {
    var nullNamed = collector(null, mapper.createObjectNode());
    var blankNamed = collector("  ", mapper.createObjectNode());
    String placeholder = GdprNames.classNameOf(nullNamed);
    assertEquals(placeholder, GdprNames.classNameOf(blankNamed), "premise: one class");

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ExportSubjectDataService.builder()
                    .collectors(List.of(nullNamed, blankNamed))
                    .build());
    assertTrue(ex.getMessage().contains("'" + placeholder + "'"), ex.getMessage());

    var namedLikeTheClass = mock(SubjectDataCollector.class);
    when(namedLikeTheClass.name()).thenReturn(placeholder);
    var clash =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ExportSubjectDataService.builder()
                    .collectors(List.of(nullNamed, namedLikeTheClass))
                    .build());
    assertTrue(clash.getMessage().contains("'" + placeholder + "'"), clash.getMessage());
  }

  /** The refusal is an exception message, so the shared name goes through the log sanitizer. */
  @Test
  void builder_refusal_sanitizesTheSharedName() {
    String forged = "orders\nERROR forged line\u202e";
    var builder =
        ExportSubjectDataService.builder()
            .collectors(
                List.of(
                    collector(forged, mapper.createObjectNode()),
                    collector(forged, mapper.createObjectNode())));

    var ex = assertThrows(IllegalArgumentException.class, builder::build);

    assertTrue(
        ex.getMessage().contains("'" + LogSanitizer.sanitizeForLog(forged) + "'"), ex.getMessage());
    assertTrue(ex.getMessage().chars().noneMatch(c -> c == '\n' || c == '\u202e'), ex.getMessage());
  }

  /** The WARN-level messages the GDPR logger received. */
  private static List<String> warnings(ListAppender<ILoggingEvent> appender) {
    return appender.list.stream()
        .filter(e -> e.getLevel() == Level.WARN)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }

  /** Every audit row the store was asked to save, in order. */
  private static List<AuditEntry> auditRows(AuditStore audit) {
    var captor = ArgumentCaptor.forClass(AuditEntry.class);
    verify(audit, atLeast(1)).save(captor.capture());
    return captor.getAllValues();
  }

  /** The {@code errorMessage} (summary) of every audit row the store was asked to save. */
  private static List<String> auditSummaries(AuditStore audit) {
    return auditRows(audit).stream()
        .map(AuditEntry::errorMessage)
        .filter(Objects::nonNull)
        .toList();
  }

  private static ListAppender<ILoggingEvent> attachGdprLogAppender() {
    var logger = (ch.qos.logback.classic.Logger) GdprAuditWriter.logger();
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    return appender;
  }

  private static void detachGdprLogAppender(ListAppender<ILoggingEvent> appender) {
    ((ch.qos.logback.classic.Logger) GdprAuditWriter.logger()).detachAppender(appender);
  }

  /**
   * An instance of a generated {@link SubjectDataCollector} class whose simple name is exactly
   * {@code simpleName}, which may hold characters a source file cannot spell. Its {@code name()}
   * returns {@code null} and its {@code collect} returns a text node, so the export keys its
   * section by the class name.
   */
  private static SubjectDataCollector collectorClassNamed(String simpleName) throws Exception {
    var lookup = MethodHandles.lookup();
    ClassDesc self = ClassDesc.of(lookup.lookupClass().getPackageName(), simpleName);
    ClassDesc json = JsonNode.class.describeConstable().orElseThrow();
    ClassDesc subject = SubjectId.class.describeConstable().orElseThrow();
    ClassDesc iface = SubjectDataCollector.class.describeConstable().orElseThrow();
    byte[] bytes =
        ClassFile.of()
            .build(
                self,
                cb ->
                    cb.withFlags(ClassFile.ACC_PUBLIC | ClassFile.ACC_FINAL)
                        .withInterfaceSymbols(iface)
                        .withMethodBody(
                            ConstantDescs.INIT_NAME,
                            ConstantDescs.MTD_void,
                            ClassFile.ACC_PUBLIC,
                            code ->
                                code.aload(0)
                                    .invokespecial(
                                        ConstantDescs.CD_Object,
                                        ConstantDescs.INIT_NAME,
                                        ConstantDescs.MTD_void)
                                    .return_())
                        .withMethodBody(
                            "name",
                            MethodTypeDesc.of(ConstantDescs.CD_String),
                            ClassFile.ACC_PUBLIC,
                            code -> code.aconst_null().areturn())
                        .withMethodBody(
                            "collect",
                            MethodTypeDesc.of(json, subject),
                            ClassFile.ACC_PUBLIC,
                            code ->
                                code.new_(
                                        ClassDesc.of(
                                            "com.fasterxml.jackson.databind.node.TextNode"))
                                    .dup()
                                    .ldc("data")
                                    .invokespecial(
                                        ClassDesc.of(
                                            "com.fasterxml.jackson.databind.node.TextNode"),
                                        ConstantDescs.INIT_NAME,
                                        MethodTypeDesc.of(
                                            ConstantDescs.CD_void, ConstantDescs.CD_String))
                                    .areturn()));
    return (SubjectDataCollector) lookup.defineClass(bytes).getDeclaredConstructor().newInstance();
  }
}
