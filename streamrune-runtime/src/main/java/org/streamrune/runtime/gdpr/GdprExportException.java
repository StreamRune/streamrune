package org.streamrune.runtime.gdpr;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SubjectId;

/**
 * Thrown when a {@link org.streamrune.core.gdpr.SubjectDataCollector} fails during export. Carries
 * the partial sections collected before the failure so callers can decide whether to surface them
 * to the data subject or discard them.
 *
 * <p>The raw subject id (which may itself be PII — an email/username) is kept available via {@link
 * #subjectId()} for callers that legitimately need it, but the exception {@code message} embeds
 * only its SHA-256 hash ({@code subject-hash=}), so a leak into application logs or an HTTP error
 * handler never re-materializes the identifier — parity with {@code ForgetSubjectService}'s
 * redaction on the erasure path. The collector name in the message goes through {@link
 * LogSanitizer#sanitizeForLog} for the same reason: it is an identifier the collector chose, and
 * the message is logged. {@link #failedCollectorName()} returns it as the collector gave it, or its
 * class name when the collector had no usable name.
 */
public final class GdprExportException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final String subjectId;
  private final String failedCollectorName;
  private final transient Map<String, JsonNode> partialSections;

  public GdprExportException(
      String subjectId,
      String failedCollectorName,
      Map<String, JsonNode> partialSections,
      Throwable cause) {
    super(
        "GDPR export failed for subject-hash="
            + SubjectId.of(subjectId).redacted()
            + " at collector="
            + LogSanitizer.sanitizeForLog(failedCollectorName),
        cause);
    this.subjectId = subjectId;
    this.failedCollectorName = failedCollectorName;
    this.partialSections = partialSections == null ? Map.of() : Map.copyOf(partialSections);
  }

  public String subjectId() {
    return subjectId;
  }

  public String failedCollectorName() {
    return failedCollectorName;
  }

  /** Immutable snapshot of sections collected before the failure. */
  public Map<String, JsonNode> partialSections() {
    return partialSections;
  }
}
