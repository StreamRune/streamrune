package org.streamrune.runtime;

import java.util.Objects;
import java.util.function.Supplier;
import org.streamrune.core.Query;
import org.streamrune.core.QueryBus;
import org.streamrune.core.QueryHandler;
import org.streamrune.core.StreamRuneContext;

/**
 * A {@link QueryBus} that dispatches a query started where {@link StreamRuneContext#CURRENT} is not
 * bound under the request context a source answers — the query-side counterpart of {@link
 * VirtualThreadCommandBus.Builder#unboundRequestContext}.
 *
 * <p>The Spring, Micronaut and Quarkus integrations make their {@code QueryBus} bean this bus over
 * the composed chain, with the request context of the request the calling thread serves as the
 * source: a query dispatched from code that runs outside the request filter's binding (a {@code
 * Callable} a Spring MVC controller returns, a Micronaut route on a Netty event loop or an
 * {@code @ExecuteOn} executor, any Quarkus resource method, which a JAX-RS filter cannot wrap) is
 * audited as the request's caller, and its handler sees that caller in {@code CURRENT} ({@code
 * Authorization.currentUserId()}, {@code requireOwner}). An application that defines its own {@code
 * QueryBus} bean replaces the integration's, and wraps its bus in this one to keep that.
 *
 * <p>The source is asked once per dispatch, on the dispatching thread, and never while {@code
 * CURRENT} is bound, whatever it is bound to: a binding the caller made is never replaced. A {@code
 * null} answer dispatches the query without a context, as the delegate would.
 */
public final class UnboundRequestContextQueryBus implements QueryBus {

  private final QueryBus delegate;
  private final Supplier<StreamRuneContext.RequestContext> unboundRequestContext;

  /**
   * Creates the bus.
   *
   * @param delegate the bus that dispatches and registers
   * @param unboundRequestContext where the context of a dispatch that starts unbound comes from; it
   *     must answer only with the context of the request the calling thread serves
   * @throws NullPointerException if either argument is {@code null}
   */
  public UnboundRequestContextQueryBus(
      QueryBus delegate, Supplier<StreamRuneContext.RequestContext> unboundRequestContext) {
    this.delegate = Objects.requireNonNull(delegate, "delegate is required");
    this.unboundRequestContext =
        Objects.requireNonNull(unboundRequestContext, "unboundRequestContext is required");
  }

  @Override
  public <R> R dispatch(Query<R> query) {
    if (StreamRuneContext.CURRENT.isBound()) {
      return delegate.dispatch(query);
    }
    StreamRuneContext.RequestContext context = unboundRequestContext.get();
    if (context == null) {
      return delegate.dispatch(query);
    }
    return ScopedValue.where(StreamRuneContext.CURRENT, context)
        .call(() -> delegate.dispatch(query));
  }

  @Override
  public <Q extends Query<R>, R> void register(Class<Q> queryType, QueryHandler<Q, R> handler) {
    delegate.register(queryType, handler);
  }

  /**
   * The bus this one dispatches through.
   *
   * @return the delegate
   */
  public QueryBus delegate() {
    return delegate;
  }
}
