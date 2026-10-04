package org.streamrune.core.gdpr;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.streamrune.core.types.SubjectId;

/**
 * Immutable result of a GDPR data export.
 *
 * @param subjectId the data subject identifier
 * @param requestedAt when the export was generated
 * @param sections section name → payload, in collector registration order; never null, must not
 *     contain null values (filtering is the caller's responsibility — see {@link
 *     SubjectDataCollector#collect})
 */
public record SubjectExport(
    SubjectId subjectId, Instant requestedAt, Map<String, JsonNode> sections) {

  public SubjectExport {
    Objects.requireNonNull(subjectId, "subjectId must not be null");
    Objects.requireNonNull(requestedAt, "requestedAt must not be null");
    if (sections == null) {
      sections = Map.of();
    } else {
      for (var entry : sections.entrySet()) {
        if (entry.getValue() == null) {
          throw new IllegalArgumentException(
              "sections must not contain null values (key: " + entry.getKey() + ")");
        }
      }
      sections = Collections.unmodifiableMap(new LinkedHashMap<>(sections));
    }
  }
}
