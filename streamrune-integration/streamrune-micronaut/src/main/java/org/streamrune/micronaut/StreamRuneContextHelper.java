package org.streamrune.micronaut;

import io.micronaut.http.context.ServerRequestContext;
import org.streamrune.core.StreamRuneContext;

/**
 * The request context of the request the calling thread serves, on the threads where {@link
 * StreamRuneContext#CURRENT} is not bound.
 *
 * <p>{@link StreamRuneContextFilter} binds {@code CURRENT} around the rest of the filter chain, on
 * the thread the filter runs on. Micronaut can run the route on another thread, outside that
 * binding: a Netty event loop when the request body arrives after the request head (the default
 * {@code streamed} server type), or the executor of a controller annotated {@code @ExecuteOn}.
 * There {@link #get()} still answers the request's context: from this class's {@code ThreadLocal}
 * mirror, which Micronaut's {@code PropagatedContext} sets on the threads it propagates the
 * filter's context to, and otherwise from the request itself ({@link #ofCurrentRequest()}), which
 * Micronaut makes current on every thread a route runs on.
 *
 * <p>The command bus and the query bus this module builds take the context from {@link #get()} when
 * {@code CURRENT} is not bound, so a command or a query is authorized, audited and recorded as the
 * request's caller on every thread. Code of your own that reads the caller outside a bus dispatch —
 * in a controller, before it dispatches — reads it here, not from {@code CURRENT}.
 */
public final class StreamRuneContextHelper {

  private static final ThreadLocal<StreamRuneContext.RequestContext> CONTEXT = new ThreadLocal<>();

  private StreamRuneContextHelper() {}

  /**
   * The request context of the request the calling thread serves: the {@code ThreadLocal} mirror
   * when it is set on this thread, otherwise {@link #ofCurrentRequest()}.
   *
   * @return the context, or {@code null} when the thread serves no request that carries one (a
   *     background thread, a request the filter did not run for)
   */
  public static StreamRuneContext.RequestContext get() {
    StreamRuneContext.RequestContext mirrored = CONTEXT.get();
    return mirrored != null ? mirrored : ofCurrentRequest();
  }

  /**
   * The context {@link StreamRuneContextFilter} stored on the request the calling thread serves
   * (its request attribute {@link StreamRuneContextFilter#CONTEXT_ATTRIBUTE}), found through
   * Micronaut's {@link ServerRequestContext#currentRequest()}.
   *
   * @return the context, or {@code null} when no request is current on this thread or the current
   *     request carries none
   */
  public static StreamRuneContext.RequestContext ofCurrentRequest() {
    return ServerRequestContext.currentRequest()
        .map(StreamRuneContextFilter::requestContextOf)
        .orElse(null);
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

  /**
   * The {@code ThreadLocal} mirror alone, without the fallback to the current request: what the
   * filter's propagated element saves and restores, so that it never leaves a value behind on a
   * pooled thread.
   */
  static StreamRuneContext.RequestContext mirrored() {
    return CONTEXT.get();
  }
}
