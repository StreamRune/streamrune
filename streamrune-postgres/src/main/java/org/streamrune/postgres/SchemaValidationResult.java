package org.streamrune.postgres;

import java.util.List;

/** Result of schema validation containing any discovered issues. */
public record SchemaValidationResult(List<SchemaIssue> issues) {

  /** Returns true if no ERROR-severity issues were found. */
  public boolean isValid() {
    return issues.stream().noneMatch(i -> i.severity() == SchemaIssue.Severity.ERROR);
  }
}
