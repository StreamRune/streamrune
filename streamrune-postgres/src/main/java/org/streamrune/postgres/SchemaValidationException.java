package org.streamrune.postgres;

/** Thrown when schema validation detects critical issues preventing EventStore operations. */
public class SchemaValidationException extends RuntimeException {

  private final SchemaValidationResult result;

  public SchemaValidationException(SchemaValidationResult result) {
    super(buildMessage(result));
    this.result = result;
  }

  public SchemaValidationResult result() {
    return result;
  }

  private static String buildMessage(SchemaValidationResult result) {
    var sb = new StringBuilder("Schema validation failed:");
    for (var issue : result.issues()) {
      if (issue.severity() == SchemaIssue.Severity.ERROR) {
        sb.append("\n  - [").append(issue.table()).append("] ").append(issue.message());
      }
    }
    return sb.toString();
  }
}
