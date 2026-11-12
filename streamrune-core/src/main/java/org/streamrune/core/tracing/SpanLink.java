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

  /**
   * The link to the span that a W3C trace id and span id name, or {@code null} when either is
   * {@code null} or not a valid W3C id. A valid trace id is 32 lowercase hexadecimal characters, a
   * valid span id 16, and neither is all zeros. For ids read back from storage: a malformed stored
   * value gives no link instead of an exception.
   *
   * @param traceId the trace id, or {@code null}
   * @param spanId the span id, or {@code null}
   * @return the link, or {@code null}
   */
  public static SpanLink ofW3c(String traceId, String spanId) {
    return isW3cId(traceId, TRACE_ID_LENGTH) && isW3cId(spanId, SPAN_ID_LENGTH)
        ? new SpanLink(new TraceId(traceId), new SpanId(spanId))
        : null;
  }

  /**
   * Whether both ids are valid W3C ids: a trace id of 32 lowercase hexadecimal characters and a
   * span id of 16, neither all zeros. A tracing library ignores a link whose ids are not.
   *
   * @return {@code true} when both ids are valid W3C ids
   */
  public boolean isW3c() {
    return isW3cId(traceId.value(), TRACE_ID_LENGTH) && isW3cId(spanId.value(), SPAN_ID_LENGTH);
  }

  private static final int TRACE_ID_LENGTH = 32;
  private static final int SPAN_ID_LENGTH = 16;

  private static boolean isW3cId(String value, int length) {
    if (value == null || value.length() != length) {
      return false;
    }
    boolean nonZero = false;
    for (int i = 0; i < length; i++) {
      char c = value.charAt(i);
      if ((c < '0' || c > '9') && (c < 'a' || c > 'f')) {
        return false;
      }
      nonZero |= c != '0';
    }
    return nonZero;
  }
}
