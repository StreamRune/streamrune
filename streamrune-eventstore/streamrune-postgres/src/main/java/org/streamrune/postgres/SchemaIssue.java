package org.streamrune.postgres;

/** A single schema validation issue. */
public record SchemaIssue(Severity severity, String table, String message) {

  public enum Severity {
    WARNING,
    ERROR
  }
}
