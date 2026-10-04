package org.streamrune.runtime.gdpr;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.gdpr.GdprAction;
import org.streamrune.core.gdpr.SubjectDataCollector;
import org.streamrune.core.gdpr.SubjectExport;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;

/**
 * Orchestrates GDPR Article 20 (right to data portability) by invoking all registered {@link
 * SubjectDataCollector} beans sequentially in registration order and merging their output into a
 * single {@link SubjectExport}.
 *
 * <p>Collectors returning {@code null} have their section omitted. If any collector throws, a
 * {@link GdprExportException} is raised carrying the partial sections collected before the failure.
 * An {@link Error} from a collector is audited as FAILURE the same way, then rethrown unchanged.
 *
 * <p><b>Section names are fixed when the service is built.</b> {@link Builder#build()} reads every
 * collector's {@link SubjectDataCollector#name()} once, and every export keys that collector's
 * section by it; {@code export} never calls {@code name()}. The name must be non-blank, but it is
 * the collector's own code: a {@code name()} that throws an exception, or returns {@code null} or a
 * blank string, does not stop the build. The collector is named by its class instead (the simple
 * name, or for an anonymous class its binary name without the package, control characters dropped
 * and at most 64 characters), and a WARN names the class so the collector can be fixed. An {@link
 * Error} from {@code name()} propagates from {@code build()}.
 *
 * <p><b>Section names are unique.</b> {@code build()} throws {@link IllegalArgumentException}
 * naming every name two or more collectors share, including a class name standing in for a missing
 * one (two instances of one class without a usable name share it). A second section under one key
 * would replace the first one's, and the export would be incomplete without anything failing.
 *
 * <p>Every collector name the service renders into a log line, an audit summary or a {@link
 * GdprExportException} message goes through {@link LogSanitizer#sanitizeForLog}; the {@link
 * SubjectExport} keeps a name as the collector gave it, as it is the key a consumer parses.
 *
 * <p>Stateless and thread-safe. Construct via {@link #builder()} and register as a singleton bean.
 *
 * <p><b>Audit failure semantics:</b> Unlike {@link ForgetSubjectService}, audit failures here
 * propagate. The operation has no destructive side effects — callers can retry safely.
 *
 * <p><b>Empty collectors:</b> The builder accepts an empty collector list to support the Quarkus
 * producer path (where {@code @Produces} cannot easily condition on collection emptiness). In that
 * case {@link #export(SubjectId, UserId)} throws {@link IllegalStateException} at call time.
 */
public final class ExportSubjectDataService {

  /** A registered collector and the section name it was given when the service was built. */
  private record NamedCollector(String name, SubjectDataCollector collector) {}

  private final List<NamedCollector> collectors;
  private final GdprAuditWriter auditWriter;

  private ExportSubjectDataService(List<NamedCollector> collectors, AuditStore auditStore) {
    this.collectors = List.copyOf(collectors);
    this.auditWriter = new GdprAuditWriter(auditStore);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Invokes all registered collectors sequentially in registration order and returns the merged
   * export.
   *
   * @param subjectId the data subject identifier; must not be null
   * @param requesterUserId who requested the export (for audit); may be {@code null}
   * @return immutable {@link SubjectExport} with sections in registration order
   * @throws NullPointerException if {@code subjectId} is null
   * @throws IllegalStateException if no collectors were registered (Quarkus path)
   * @throws GdprExportException if any collector throws; carries partial sections plus failed
   *     collector name. An {@link Error} from a collector is audited as FAILURE and rethrown
   *     unchanged instead of wrapped.
   */
  public SubjectExport export(SubjectId subjectId, UserId requesterUserId) {
    Objects.requireNonNull(subjectId, "subjectId must not be null");
    if (collectors.isEmpty()) {
      throw new IllegalStateException("no SubjectDataCollector registered");
    }

    Instant requestedAt = Instant.now();
    Map<String, JsonNode> sections = new LinkedHashMap<>();

    for (NamedCollector entry : collectors) {
      String name = entry.name();
      try {
        JsonNode node = entry.collector().collect(subjectId);
        if (node != null) {
          // Names are unique (refused at build), so no section replaces another here.
          sections.put(name, node);
        }
      } catch (Throwable t) {
        String msg =
            "collector " + LogSanitizer.sanitizeForLog(name) + " failed: " + t.getMessage();
        try {
          auditWriter.write(
              GdprAction.EXPORT, AuditOutcome.FAILURE, subjectId, requesterUserId, msg);
        } catch (RuntimeException auditEx) {
          // The subject id may be PII (email/username) — log its hash, not the raw
          // value (parity with ForgetSubjectService's subject-hash= convention).
          GdprAuditWriter.logger()
              .warn(
                  "GDPR export audit FAILURE write failed for subject-hash={}: {}",
                  subjectId.redacted(),
                  auditEx.getMessage());
        }
        if (t instanceof Error error) {
          // Rule S1181: the FAILURE row above is written for an Error too, but the
          // Error itself propagates. Wrapped, an OutOfMemoryError read as a business failure with
          // a partial export that application code may handle and carry on from.
          throw error;
        }
        throw new GdprExportException(subjectId.value(), name, sections, t);
      }
    }

    var result = new SubjectExport(subjectId, requestedAt, sections);
    auditWriter.write(
        GdprAction.EXPORT,
        AuditOutcome.SUCCESS,
        subjectId,
        requesterUserId,
        "sections="
            + sections.keySet().stream()
                .map(LogSanitizer::sanitizeForLog)
                .collect(Collectors.joining(",")));
    return result;
  }

  /**
   * Reads a collector's name once, for {@link Builder#build()}, without letting {@code name()} stop
   * the build. A name that is {@code null} or blank, or a {@code name()} that throws an exception,
   * breaks the contract of {@link SubjectDataCollector#name()}: the collector is named by {@link
   * GdprNames#placeholder its class name} instead, and a WARN says which collector lacks a name. An
   * {@link Error} is not caught here; it propagates from {@code build()}.
   */
  private static String resolveName(SubjectDataCollector collector) {
    String name;
    try {
      name = collector.name();
    } catch (Exception nameFailure) {
      String placeholder = GdprNames.placeholder(collector);
      GdprAuditWriter.logger()
          .warn(
              "GDPR export: SubjectDataCollector {} threw from name() ({}); it is still collected,"
                  + " named by its class — give it a stable name",
              placeholder,
              LogSanitizer.sanitizeForLog(nameFailure.toString()));
      return placeholder;
    }
    if (name == null || name.isBlank()) {
      String placeholder = GdprNames.placeholder(collector);
      GdprAuditWriter.logger()
          .warn(
              "GDPR export: SubjectDataCollector {} returned a null or blank name(); it is still"
                  + " collected, named by its class — give it a stable name",
              placeholder);
      return placeholder;
    }
    return name;
  }

  /** Builder for {@link ExportSubjectDataService}. */
  public static final class Builder {
    private List<SubjectDataCollector> collectors;
    private AuditStore auditStore;

    private Builder() {}

    /**
     * Required. May be empty (in which case {@link #export(SubjectId, UserId)} throws lazily). The
     * list is defensively copied when the service is built. Each collector needs a name of its own
     * (see {@link #build()}).
     */
    public Builder collectors(List<SubjectDataCollector> collectors) {
      this.collectors = collectors;
      return this;
    }

    /** Optional. {@code null} disables audit writes. */
    public Builder auditStore(AuditStore auditStore) {
      this.auditStore = auditStore;
      return this;
    }

    /**
     * Builds the service. Reads every collector's {@code name()} once; each export keys that
     * collector's section by it (see the class javadoc). Nothing is collected or audited here.
     *
     * @throws NullPointerException if no collector list was set, or it holds a {@code null}
     * @throws IllegalArgumentException if two or more collectors go by one name, a class name
     *     standing in for a missing one included; the message names each shared name and the
     *     classes sharing it
     * @throws Error an {@link Error} from a collector's {@code name()}, unchanged
     */
    public ExportSubjectDataService build() {
      Objects.requireNonNull(collectors, "collectors is required (may be empty)");
      List<SubjectDataCollector> registered = List.copyOf(collectors);
      List<String> names = registered.stream().map(ExportSubjectDataService::resolveName).toList();
      GdprNames.requireUnique(
          "SubjectDataCollector",
          "each one keys its section of the export, and a second section under one key would"
              + " replace the first one's",
          registered,
          names);
      List<NamedCollector> named = new ArrayList<>(registered.size());
      for (int i = 0; i < registered.size(); i++) {
        named.add(new NamedCollector(names.get(i), registered.get(i)));
      }
      return new ExportSubjectDataService(named, auditStore);
    }
  }
}
