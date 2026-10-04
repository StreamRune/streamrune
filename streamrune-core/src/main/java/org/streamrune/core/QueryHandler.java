package org.streamrune.core;

/**
 * Marker interface for query handlers in the CQRS pattern.
 *
 * <p>Query handlers are responsible for reading data from projections or other read models. Unlike
 * commands, queries should not modify state.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * public record GetCartQuery(String cartId) implements Query<CartView> {}
 *
 * public class CartQueryHandler implements QueryHandler<GetCartQuery, CartView> {
 *     private final Map<String, CartView> views;
 *
 *     @Override
 *     public CartView handle(GetCartQuery query) {
 *         return views.getOrDefault(query.cartId(), null);
 *     }
 * }
 * }</pre>
 *
 * @param <Q> the query type, which must declare its result type via {@link Query}
 * @param <R> Result type
 */
@FunctionalInterface
public interface QueryHandler<Q extends Query<R>, R> {

  /**
   * Handles the query and returns the result.
   *
   * @param query the query to handle
   * @return the query result
   */
  R handle(Q query);
}
