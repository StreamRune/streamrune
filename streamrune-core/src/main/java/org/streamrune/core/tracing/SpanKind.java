package org.streamrune.core.tracing;

/**
 * The role of a span in a trace, with the meaning the OpenTelemetry specification gives the kind of
 * the same name. A StreamRune type, so the seam carries no OpenTelemetry type.
 */
public enum SpanKind {
  /** An operation inside the application, with no remote party. */
  INTERNAL,
  /** The handling of a synchronous request from a remote client. */
  SERVER,
  /** A synchronous request to a remote server. */
  CLIENT,
  /** The hand-off of a message to a broker or another asynchronous consumer. */
  PRODUCER,
  /** The processing of a message delivered asynchronously. */
  CONSUMER
}
