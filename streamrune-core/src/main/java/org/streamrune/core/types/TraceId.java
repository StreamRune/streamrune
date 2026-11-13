package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * A trace id: the caller's {@code X-Trace-Id} in {@link
 * org.streamrune.core.StreamRuneContext.RequestContext} and {@link
 * org.streamrune.core.EventMetadata#traceId()}, correlation data that never builds a link, and the
 * trace id of a {@link org.streamrune.core.tracing.SpanLink}, the trace of the command span the
 * link names.
 *
 * <p>As the caller's trace id it is built from the unauthenticated {@code X-Trace-Id} header and
 * persisted into the append-only, plaintext {@code event_stream.metadata}, so the value must be
 * bounded and free of control characters — a W3C trace id is 32 hex characters, far inside the
 * bound. That check lives at the INGRESS door ({@link
 * org.streamrune.core.StreamRuneContext.RequestContext#fromRequest}) and deliberately NOT here:
 * this constructor also runs on the READ path, where rejecting an already-persisted value makes the
 * affected stream permanently unloadable rather than preventing anything. Nor is it in the {@code
 * RequestContext} constructor, which is also the DLQ-replay and saga-dispatch reconstruction path.
 * See {@link IdConstraints}.
 */
public record TraceId(@JsonValue String value) {

  @JsonCreator
  public TraceId {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("traceId is required");
    }
  }

  public static TraceId of(String value) {
    return new TraceId(value);
  }

  @Override
  public String toString() {
    return value;
  }
}
