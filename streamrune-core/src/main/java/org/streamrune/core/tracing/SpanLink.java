package org.streamrune.core.tracing;

import java.util.Objects;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.TraceId;

/**
 * A link from a new span to a span of another trace, for example from a subscription delivery to
 * the command that wrote the delivered events. The ids are the ones {@link EventMetadata} carries.
 *
 * <p><b>No link is {@link #NONE}, never {@code null}.</b> The tracing seam takes no {@code null}:
 * {@link StreamRuneTracing#start(TracingArea, String, SpanKind, SpanAttributes, SpanLink,
 * SpanBaggage) start} rejects a {@code null} link, and a component with nothing to link to passes
 * {@code NONE}. {@code NONE} is the one empty link: {@link #isPresent()} is {@code false} for it
 * alone, and a tracing implementation starts the span without a link, so {@code NONE} never reaches
 * an exporter. Its ids are the all-zero ids W3C calls invalid, and the constructor refuses that
 * pair, so no other instance equals it.
 *
 * @param traceId the trace of the linked span
 * @param spanId the linked span
 */
public record SpanLink(TraceId traceId, SpanId spanId) {

  private static final int TRACE_ID_LENGTH = 32;
  private static final int SPAN_ID_LENGTH = 16;
  private static final String ZERO_TRACE_ID = "0".repeat(TRACE_ID_LENGTH);
  private static final String ZERO_SPAN_ID = "0".repeat(SPAN_ID_LENGTH);

  /**
   * No link: the span starts without one. {@link #isPresent()} is {@code false}, {@link #isW3c()}
   * is {@code false}, and its ids are the all-zero ids.
   */
  public static final SpanLink NONE =
      new SpanLink(new TraceId(ZERO_TRACE_ID), new SpanId(ZERO_SPAN_ID));

  /**
   * Creates a link.
   *
   * @throws NullPointerException if {@code traceId} or {@code spanId} is {@code null}
   * @throws IllegalArgumentException if both ids are all zeros, the ids of {@link #NONE}
   */
  public SpanLink {
    Objects.requireNonNull(traceId, "traceId is required");
    Objects.requireNonNull(spanId, "spanId is required");
    // NONE is the first instance with the all-zero ids, created while the class initializes,
    // before any other code can call this constructor; every later one is refused.
    if (NONE != null
        && ZERO_TRACE_ID.equals(traceId.value())
        && ZERO_SPAN_ID.equals(spanId.value())) {
      throw new IllegalArgumentException("the all-zero ids are SpanLink.NONE");
    }
  }

  /**
   * Whether this is a link to a span, {@code false} for {@link #NONE} alone.
   *
   * @return {@code true} unless this is {@link #NONE}
   */
  public boolean isPresent() {
    return this != NONE;
  }

  /**
   * The link to the span that a W3C trace id and span id name, or {@link #NONE} when either is
   * {@code null} or not a valid W3C id. A valid trace id is 32 lowercase hexadecimal characters, a
   * valid span id 16, and neither is all zeros. For ids read back from storage: a malformed stored
   * value gives no link instead of an exception.
   *
   * @param traceId the trace id, or {@code null}
   * @param spanId the span id, or {@code null}
   * @return the link, or {@link #NONE}
   */
  public static SpanLink ofW3c(String traceId, String spanId) {
    return isW3cId(traceId, TRACE_ID_LENGTH) && isW3cId(spanId, SPAN_ID_LENGTH)
        ? new SpanLink(new TraceId(traceId), new SpanId(spanId))
        : NONE;
  }

  /**
   * The link to the span that wrote an event: the {@code traceId} and {@code spanId} of its
   * metadata when both are valid W3C ids, {@link #NONE} otherwise — no metadata, only one of the
   * two ids, or an id that is not a W3C id (a trace id an HTTP client sent, for example). Every
   * component that links a span to the trace of an event builds the link here.
   *
   * @param metadata the event's metadata, or {@code null}
   * @return the link, or {@link #NONE}
   */
  public static SpanLink of(EventMetadata metadata) {
    if (metadata == null || metadata.traceId() == null || metadata.spanId() == null) {
      return NONE;
    }
    return ofW3c(metadata.traceId().value(), metadata.spanId().value());
  }

  /**
   * Whether both ids are valid W3C ids: a trace id of 32 lowercase hexadecimal characters and a
   * span id of 16, neither all zeros. {@code false} for {@link #NONE}. A tracing library ignores a
   * link whose ids are not.
   *
   * @return {@code true} when both ids are valid W3C ids
   */
  public boolean isW3c() {
    return isW3cId(traceId.value(), TRACE_ID_LENGTH) && isW3cId(spanId.value(), SPAN_ID_LENGTH);
  }

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
