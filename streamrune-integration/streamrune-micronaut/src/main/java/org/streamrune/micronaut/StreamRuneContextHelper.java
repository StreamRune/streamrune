package org.streamrune.micronaut;

import org.streamrune.core.StreamRuneContext;

/**
 * Helper for accessing StreamRuneContext in environments where ScopedValue doesn't propagate
 * automatically (e.g., in reactive contexts).
 *
 * <p>This provides a ThreadLocal-based fallback for code that can't use ScopedValue directly.
 */
public final class StreamRuneContextHelper {

  private static final ThreadLocal<StreamRuneContext.RequestContext> CONTEXT = new ThreadLocal<>();

  private StreamRuneContextHelper() {}

  /**
   * Get the current context from ThreadLocal.
   *
   * @return the current context or null if not set
   */
  public static StreamRuneContext.RequestContext get() {
    return CONTEXT.get();
  }

  /**
   * Set the context in ThreadLocal.
   *
   * @param ctx the context to set
   */
  public static void set(StreamRuneContext.RequestContext ctx) {
    CONTEXT.set(ctx);
  }

  /** Remove the context from ThreadLocal. */
  public static void remove() {
    CONTEXT.remove();
  }
}
