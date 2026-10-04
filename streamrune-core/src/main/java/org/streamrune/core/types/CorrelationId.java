package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Identifies the overarching business process or request that spans multiple commands and events.
 * All events produced within a single command execution share the same {@code correlationId}.
 *
 * <p>Each framework's request filter builds this from the unauthenticated {@code X-Correlation-Id}
 * header, and the value is then persisted into the append-only, plaintext {@code
 * event_stream.metadata} of every produced event and into {@code audit_log.correlation_id} (a
 * {@code TEXT} column — unbounded), so the value must be bounded and free of control characters.
 * That check lives at the INGRESS door — {@link
 * org.streamrune.core.StreamRuneContext.RequestContext#fromRequest}, which every framework filter
 * calls — and deliberately NOT here: this constructor also runs on the READ path (Jackson rebuilds
 * {@code EventMetadata} on every event load, projection page and subscription delivery, and the
 * DLQ/audit mappers rebuild ids from stored columns), where rejecting a value already persisted
 * through a path that never crosses the door (the plain {@code RequestContext} constructor, a
 * saga-derived correlation id, application-built {@code EventMetadata}) makes the affected stream
 * permanently unloadable rather than preventing anything. The same reasoning is why the check is
 * not in the {@code RequestContext} constructor either — that is also the DLQ-replay and
 * saga-dispatch reconstruction path. See {@link IdConstraints}.
 */
public record CorrelationId(@JsonValue String value) {

  @JsonCreator
  public CorrelationId {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("correlationId is required");
    }
  }

  public static CorrelationId of(String value) {
    return new CorrelationId(value);
  }

  @Override
  public String toString() {
    return value;
  }
}
