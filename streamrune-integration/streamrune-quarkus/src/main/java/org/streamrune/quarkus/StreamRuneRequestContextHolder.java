package org.streamrune.quarkus;

import io.quarkus.arc.Arc;
import io.quarkus.arc.ArcContainer;
import io.quarkus.arc.InstanceHandle;
import jakarta.enterprise.context.RequestScoped;
import org.streamrune.core.StreamRuneContext;

/**
 * Request-scoped holder for the {@link StreamRuneContext.RequestContext} extracted from HTTP
 * headers by {@link StreamRuneRequestFilter}.
 *
 * <p>JAX-RS filters cannot wrap the resource-method invocation, so a {@link ScopedValue} binding
 * cannot be established by the filter itself. Instead the filter parses the headers into a context
 * and stores it here, and the command bus and the query bus the integration produces read it
 * through {@link #ofCurrentRequest()} for a dispatch that starts with {@link
 * StreamRuneContext#CURRENT} unbound: a resource method dispatches as the request's caller with
 * nothing bound around the call.
 *
 * <pre>{@code
 * @Inject CommandBus commandBus;
 *
 * @POST
 * public Response placeOrder(PlaceOrderCommand cmd) {
 *   var result = commandBus.execute(cmd); // authorized and audited as the request's caller
 *   ...
 * }
 * }</pre>
 *
 * <p>Code that reads the caller itself, outside a bus dispatch, binds the context around it with
 * {@link StreamRuneRequestFilter#withContext} or {@link StreamRuneRequestFilter#callWithContext}.
 * Quarkus propagates the CDI request context to the thread executing the resource method, so this
 * holder is safe to inject into blocking and {@code @RunOnVirtualThread} endpoints.
 */
@RequestScoped
public class StreamRuneRequestContextHolder {

  private StreamRuneContext.RequestContext context;

  /** Returns the request context for the current HTTP request, or {@code null} outside one. */
  public StreamRuneContext.RequestContext context() {
    return context;
  }

  /** Sets the request context. Called by {@link StreamRuneRequestFilter}. */
  public void set(StreamRuneContext.RequestContext context) {
    this.context = context;
  }

  /**
   * The request context {@link StreamRuneRequestFilter} stored for the HTTP request the calling
   * thread serves: what this holder holds while a CDI request context is active on the thread.
   * Quarkus REST activates a request's context on every thread it runs the request on — the event
   * loop, a worker thread, a virtual thread — so a resource method reads its own request's context
   * here, wherever it runs.
   *
   * <p>The produced {@code VirtualThreadCommandBus} takes it as its unbound-request-context source
   * ({@code VirtualThreadCommandBus.Builder#unboundRequestContext}), and the produced {@code
   * QueryBus} is an {@code UnboundRequestContextQueryBus} over it. A command or query bus an
   * application produces itself does the same to dispatch as the request's caller.
   *
   * @return the context, or {@code null} when no CDI request context is active on the thread (a
   *     runner, a scheduled job, a thread the application started) or the filter stored none for
   *     the request
   */
  public static StreamRuneContext.RequestContext ofCurrentRequest() {
    ArcContainer container = Arc.container();
    if (container == null || !container.requestContext().isActive()) {
      return null;
    }
    InstanceHandle<StreamRuneRequestContextHolder> holder =
        container.instance(StreamRuneRequestContextHolder.class);
    return holder.isAvailable() ? holder.get().context() : null;
  }
}
