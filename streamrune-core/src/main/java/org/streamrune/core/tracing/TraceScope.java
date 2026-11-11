package org.streamrune.core.tracing;

/**
 * One started span, current on the thread that started it until {@link #close()}.
 *
 * <p>{@link #close()} ends the span and restores the context that was current before {@link
 * StreamRuneTracing#start}; it runs once, on the thread that started the scope. Scopes close in the
 * reverse order of their start, as {@code try}-with-resources closes them.
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
   * lock timeout the caller handles, is an attribute value and not an error.
   *
   * @param error the failure
   */
  void recordError(Throwable error);

  /** Ends the span and restores the context that was current before it started. */
  @Override
  void close();
}
