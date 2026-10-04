package org.streamrune.quarkus;

import jakarta.enterprise.context.RequestScoped;
import org.streamrune.core.StreamRuneContext;

/**
 * Request-scoped holder for the {@link StreamRuneContext.RequestContext} extracted from HTTP
 * headers by {@link StreamRuneRequestFilter}.
 *
 * <p>JAX-RS filters cannot wrap the resource-method invocation, so a {@link ScopedValue} binding
 * cannot be established by the filter itself. Instead the filter parses the headers into a context
 * and stores it here; resource methods bind it explicitly around StreamRune calls:
 *
 * <pre>{@code
 * @Inject StreamRuneRequestContextHolder requestContext;
 * @Inject CommandBus commandBus;
 *
 * @POST
 * public Response placeOrder(PlaceOrderCommand cmd) {
 *   var result =
 *       StreamRuneRequestFilter.callWithContext(
 *           requestContext.context(), () -> commandBus.execute(cmd));
 *   ...
 * }
 * }</pre>
 *
 * <p>Quarkus propagates the CDI request context to the thread executing the resource method, so
 * this holder is safe to inject into blocking and {@code @RunOnVirtualThread} endpoints.
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
}
