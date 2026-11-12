package org.streamrune.core.tracing;

/**
 * One started span, current on the thread that started it until {@link #close()}.
 *
 * <p>{@link #close()} ends the span and restores the context that was current before {@link
 * StreamRuneTracing#start}; it runs once, on the thread that started the scope. Scopes close in the
 * reverse order of their start, as {@code try}-with-resources closes them.
 *
 * <p>No method of a scope may throw: tracing must never fail the work it traces.
 *
 * <p>A value known only when the work ends (a count, an outcome) is set with {@code setAttribute}.
 * {@link #recordSuccess()} and {@link #recordError(Throwable)} set the span's status; a span closed
 * with neither keeps the unset status.
 */
public interface TraceScope extends AutoCloseable {

  /** The scope of a span that is not recorded: every method does nothing. A shared instance. */
  TraceScope NOOP = new NoopTraceScope();

  /**
   * Sets a string attribute; a {@code null} value sets nothing.
   *
   * @param key the attribute key
   * @param value the value, or {@code null}
   */
  void setAttribute(String key, String value);

  /**
   * Sets a {@code long} attribute.
   *
   * @param key the attribute key
   * @param value the value
   */
  void setAttribute(String key, long value);

  /**
   * Sets a {@code boolean} attribute.
   *
   * @param key the attribute key
   * @param value the value
   */
  void setAttribute(String key, boolean value);

  /** Marks the span as completed successfully. */
  void recordSuccess();

  /**
   * Records {@code error} on the span and marks the span as failed. An expected outcome, such as a
   * lock timeout the caller handles, is an attribute value and not an error. An implementation
   * records the exception's type and its own message, never the messages of its causes or of its
   * suppressed exceptions, which can quote the data the failing code was handling.
   *
   * @param error the failure
   */
  void recordError(Throwable error);

  /**
   * The link to the span this scope started: its trace id and span id, for a span of another trace
   * to link to. The command bus writes it into the metadata of the events a command appends ({@link
   * org.streamrune.core.EventMetadata#spanContext()}), so the spans that later handle those events
   * link back to the command. {@link SpanLink#NONE} when no span was started: the {@link #NOOP}
   * scope, a tracing that is off, an area that is off. A span the sampler does not record still has
   * a link when its context is valid, so the links hold under a parent-based sampler.
   *
   * @return the link to this scope's span, or {@link SpanLink#NONE}
   */
  default SpanLink link() {
    return SpanLink.NONE;
  }

  /** Ends the span and restores the context that was current before it started. */
  @Override
  void close();
}
