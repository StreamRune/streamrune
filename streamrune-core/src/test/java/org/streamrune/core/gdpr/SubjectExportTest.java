package org.streamrune.core.gdpr;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.SubjectId;

class SubjectExportTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void constructsWithSectionsCopy() {
    Map<String, JsonNode> sections = new LinkedHashMap<>();
    sections.put("profile", mapper.createObjectNode().put("email", "x@y.z"));
    var export =
        new SubjectExport(SubjectId.of("user-1"), Instant.parse("2026-04-16T10:00:00Z"), sections);

    assertEquals(SubjectId.of("user-1"), export.subjectId());
    assertEquals(Instant.parse("2026-04-16T10:00:00Z"), export.requestedAt());
    assertEquals(1, export.sections().size());
    assertTrue(export.sections().containsKey("profile"));

    // Mutating the source after construction must not affect the export.
    sections.put("orders", mapper.createObjectNode());
    assertFalse(export.sections().containsKey("orders"));
  }

  @Test
  void sectionsMapIsImmutable() {
    Map<String, JsonNode> sections = new LinkedHashMap<>();
    sections.put("profile", mapper.createObjectNode());
    var export = new SubjectExport(SubjectId.of("user-1"), Instant.now(), sections);

    assertThrows(
        UnsupportedOperationException.class,
        () -> export.sections().put("orders", mapper.createObjectNode()));
  }

  @Test
  void nullSectionsBecomesEmptyMap() {
    var export = new SubjectExport(SubjectId.of("user-1"), Instant.now(), null);
    assertNotNull(export.sections());
    assertTrue(export.sections().isEmpty());
  }

  @Test
  void rejectsNullSubjectId() {
    assertThrows(
        NullPointerException.class, () -> new SubjectExport(null, Instant.now(), Map.of()));
  }

  @Test
  void blankSubjectIdIsRejectedBySubjectIdBeforeSubjectExportSeesIt() {
    // SubjectExport checks only for null: SubjectId's own constructor rejects a blank value, so a
    // blank subject id cannot be constructed, let alone passed in.
    assertThrows(IllegalArgumentException.class, () -> SubjectId.of("  "));
  }

  @Test
  void rejectsNullRequestedAt() {
    assertThrows(
        NullPointerException.class,
        () -> new SubjectExport(SubjectId.of("user-1"), null, Map.of()));
  }

  @Test
  void rejectsNullSectionValueWithClearMessage() {
    Map<String, JsonNode> sections = new LinkedHashMap<>();
    sections.put("profile", null);

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SubjectExport(SubjectId.of("user-1"), Instant.now(), sections));
    assertTrue(
        ex.getMessage().contains("profile"),
        "exception message should identify offending key, was: " + ex.getMessage());
  }
}
