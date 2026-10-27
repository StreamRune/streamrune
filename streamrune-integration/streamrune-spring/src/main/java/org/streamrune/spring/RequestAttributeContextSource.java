package org.streamrune.spring;

import java.util.function.Supplier;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.streamrune.core.StreamRuneContext;

/**
 * The request context {@link ScopedValueFilter} stored on the request the calling thread serves,
 * for a dispatch on a thread where {@link StreamRuneContext#CURRENT} is not bound: the source the
 * Spring integration's command bus ({@code VirtualThreadCommandBus.Builder.unboundRequestContext})
 * and query bus ({@code UnboundRequestContextQueryBus}) take the caller from.
 *
 * <p>The filter binds {@code CURRENT} around the rest of the filter chain on the servlet thread,
 * and stores the same context on the request as the attribute {@link #ATTRIBUTE}. Spring MVC runs a
 * {@code Callable} or a {@code WebAsyncTask} a controller returns on its task executor, outside
 * that binding, and makes the request's attributes current on that thread ({@link
 * RequestContextHolder}): there {@link #get()} answers the request's context, so a command or a
 * query the task dispatches is authorized, audited and recorded as the request's caller. On a
 * thread that serves no request it answers {@code null} and the dispatch stays anonymous: a
 * scheduler, a thread the application starts, an {@code @Async} method whose executor does not copy
 * the request's attributes, the thread that completes a {@code DeferredResult}, a WebFlux handler
 * (WebFlux has no servlet request). It answers {@code null} too once the request has completed, and
 * for a request the filter did not run for.
 *
 * <p>Keep {@code threadContextInheritable} at its default ({@code false}) on the {@code
 * DispatcherServlet} and on any {@code RequestContextFilter}: with it, a pooled thread created
 * while one request is handled inherits that request's attributes, and a dispatch from another
 * request on that thread, while the first is still in its handler, is attributed to the first
 * request's caller; the source cannot tell the two apart.
 *
 * <p>The auto-configuration registers one in a servlet web application and hands it to the command
 * bus and the query bus it builds. A {@code VirtualThreadCommandBus} or a {@code QueryBus} the
 * application defines itself takes the bean the same way: {@code .unboundRequestContext(source)},
 * and {@code new UnboundRequestContextQueryBus(bus, source)}.
 */
public final class RequestAttributeContextSource
    implements Supplier<StreamRuneContext.RequestContext> {

  /**
   * The name of the request attribute {@link ScopedValueFilter} stores the request's context under.
   */
  public static final String ATTRIBUTE =
      "org.streamrune.spring.RequestAttributeContextSource.context";

  /** Creates the source. */
  public RequestAttributeContextSource() {}

  /**
   * The context {@link ScopedValueFilter} stored on the request the calling thread serves.
   *
   * @return the context, or {@code null} when no request is current on this thread, the request
   *     carries none, or the request has completed
   */
  @Override
  public StreamRuneContext.RequestContext get() {
    RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
    if (attributes == null) {
      return null;
    }
    Object context;
    try {
      context = attributes.getAttribute(ATTRIBUTE, RequestAttributes.SCOPE_REQUEST);
    } catch (IllegalStateException requestCompleted) {
      // Attributes a thread still holds after the request completed (inherited, or kept by a task
      // decorator) refuse a request-scope read: no request is served here any more.
      return null;
    }
    return context instanceof StreamRuneContext.RequestContext requestContext
        ? requestContext
        : null;
  }
}
