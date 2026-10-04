package org.streamrune.core;

/**
 * Marker interface for queries in the CQRS pattern, parameterized by the query's result type.
 *
 * <p>This is the read side of CQRS. A query carries enough information to be answered from a read
 * model and must not modify state. The type parameter {@code R} binds the query type to the type it
 * resolves to, so {@link QueryBus#dispatch(Query)} can return that type with no caller-supplied
 * type witness and no unchecked cast at the call site.
 *
 * <p>Implementations are typically {@code record}s:
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
 *
 * // No type witness needed — the result type follows from the query type:
 * CartView view = queryBus.dispatch(new GetCartQuery("cart-1"));
 * }</pre>
 *
 * @param <R> the type this query resolves to
 */
public interface Query<R> {}
