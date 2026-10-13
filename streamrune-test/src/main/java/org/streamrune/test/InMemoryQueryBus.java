package org.streamrune.test;

import java.util.LinkedHashMap;
import java.util.Map;
import org.streamrune.core.Query;
import org.streamrune.core.QueryBus;
import org.streamrune.core.QueryHandler;
import org.streamrune.core.StreamRuneContext;

/**
 * In-memory implementation of {@link QueryBus} for unit testing.
 *
 * <p>Handlers are registered at build time and dispatched by exact query class match.
 *
 * <p>Like the production query buses it refuses a query dispatched under a correlation id with the
 * reserved {@code streamrune:} prefix that is not the bound saga origin's, with a {@link
 * org.streamrune.core.ReservedCorrelationIdException}, before the handler is looked up ({@link
 * StreamRuneContext#requireNoForeignReservedCorrelationId()}).
 *
 * <pre>{@code
 * record GetOrderQuery(String orderId) implements Query<OrderView> {}
 *
 * var bus = InMemoryQueryBus.builder()
 *     .register(GetOrderQuery.class, q -> orderRepository.findById(q.orderId()))
 *     .build();
 *
 * OrderView view = bus.dispatch(new GetOrderQuery(orderId));
 * }</pre>
 */
public final class InMemoryQueryBus implements QueryBus {

  private final Map<Class<?>, QueryHandler<?, ?>> handlers;

  private InMemoryQueryBus(Map<Class<?>, QueryHandler<?, ?>> handlers) {
    this.handlers = Map.copyOf(handlers);
  }

  /** Creates a new builder. */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <R> R dispatch(Query<R> query) {
    if (query == null) {
      throw new IllegalArgumentException("query must not be null");
    }
    // As the production query buses refuse it, before the handler lookup.
    StreamRuneContext.requireNoForeignReservedCorrelationId();
    QueryHandler<Query<R>, R> handler = (QueryHandler<Query<R>, R>) handlers.get(query.getClass());
    if (handler == null) {
      throw new IllegalArgumentException(
          "No handler registered for query type: "
              + query.getClass().getSimpleName()
              + ". Register one via InMemoryQueryBus.builder().register(...)");
    }
    return handler.handle(query);
  }

  @Override
  public <Q extends Query<R>, R> void register(Class<Q> queryType, QueryHandler<Q, R> handler) {
    throw new UnsupportedOperationException(
        "InMemoryQueryBus is immutable. Use builder().register(...).build() instead.");
  }

  /** Builder for {@link InMemoryQueryBus}. */
  public static final class Builder {

    private final Map<Class<?>, QueryHandler<?, ?>> handlers = new LinkedHashMap<>();

    private Builder() {}

    /**
     * Registers a handler for the given query type.
     *
     * @param queryType the query class
     * @param handler functional handler for the query
     * @return this builder
     */
    public <Q extends Query<R>, R> Builder register(
        Class<Q> queryType, QueryHandler<Q, R> handler) {
      if (handlers.containsKey(queryType)) {
        throw new IllegalArgumentException(
            "Handler already registered for: "
                + queryType.getSimpleName()
                + ". Each query type can only have one handler.");
      }
      handlers.put(queryType, handler);
      return this;
    }

    /** Builds an immutable {@link InMemoryQueryBus} from the registered handlers. */
    public InMemoryQueryBus build() {
      return new InMemoryQueryBus(handlers);
    }
  }
}
