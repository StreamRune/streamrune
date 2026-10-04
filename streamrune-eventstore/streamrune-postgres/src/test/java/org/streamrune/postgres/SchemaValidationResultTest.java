package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class SchemaValidationResultTest {

  @Test
  void emptyResultIsValid() {
    var result = new SchemaValidationResult(List.of());
    assertTrue(result.isValid());
    assertTrue(result.issues().isEmpty());
  }

  @Test
  void resultWithWarningIsValid() {
    var result =
        new SchemaValidationResult(
            List.of(new SchemaIssue(SchemaIssue.Severity.WARNING, "saga_state", "Extra column")));
    assertTrue(result.isValid());
    assertEquals(1, result.issues().size());
  }

  @Test
  void resultWithErrorIsInvalid() {
    var result =
        new SchemaValidationResult(
            List.of(new SchemaIssue(SchemaIssue.Severity.ERROR, "event_stream", "Table missing")));
    assertFalse(result.isValid());
  }

  @Test
  void validationExceptionContainsResult() {
    var result =
        new SchemaValidationResult(
            List.of(new SchemaIssue(SchemaIssue.Severity.ERROR, "event_stream", "Table missing")));
    var ex = new SchemaValidationException(result);
    assertSame(result, ex.result());
    assertTrue(ex.getMessage().contains("event_stream"));
  }

  @Test
  void schemaIssueAccessors() {
    var issue =
        new SchemaIssue(SchemaIssue.Severity.ERROR, "event_stream", "Missing column: payload");
    assertEquals(SchemaIssue.Severity.ERROR, issue.severity());
    assertEquals("event_stream", issue.table());
    assertEquals("Missing column: payload", issue.message());
  }
}
