package org.streamrune.core.tracing;

import java.util.Objects;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.TraceId;

/**
 * A link from a new span to a span of another trace, for example from a subscription delivery to
 * the command that wrote the delivered events. The ids are the ones {@link
 * org.streamrune.core.EventMetadata} carries.
 *
 * @param traceId the trace of the linked span
 * @param spanId the linked span
 */
public record SpanLink(TraceId traceId, SpanId spanId) {

  /**
   * Creates a link.
   *
   * @throws NullPointerException if {@code traceId} or {@code spanId} is {@code null}
   */
  public SpanLink {
    Objects.requireNonNull(traceId, "traceId is required");
    Objects.requireNonNull(spanId, "spanId is required");
  }

  /**
   * The link to the span {@code traceId}/{@code spanId} names, or {@code null} when either is
   * {@code null}: event metadata carries both or neither, and a span without an origin has no link.
   *
   * @param traceId the trace of the linked span, or {@code null}
   * @param spanId the linked span, or {@code null}
   * @return the link, or {@code null}
   */
  public static SpanLink ofNullable(TraceId traceId, SpanId spanId) {
    return traceId != null && spanId != null ? new SpanLink(traceId, spanId) : null;
  }
}
