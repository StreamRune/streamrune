package org.streamrune.core;

/**
 * Query bus for dispatching queries to handlers.
 *
 * <p>This is the read side of CQRS. Queries are dispatched to registered handlers based on query
 * type.
 *
 * <p><strong>Matching contract:</strong> handlers are looked up by the query's exact runtime class.
 * Registering a handler for an interface or superclass does not match subtypes — unlike {@link
 * CommandBus}, which matches commands via {@code isInstance} and so does handle interface
 * registrations.
 *
 * <p><strong>Type safety:</strong> the result type is carried by the query itself via {@link
 * Query}{@code <R>}, so {@link #dispatch(Query)} returns {@code R} with no caller-supplied type
 * witness and {@link #register(Class, QueryHandler)} binds the handler's result type to the query
 * type at compile time. A handler whose result type does not match the query's declared {@code R}
 * is a compile error at the registration site, not a {@code ClassCastException} at dispatch.
 */
public interface QueryBus {

  /**
   * Dispatches a query to its handler and returns the result.
   *
   * @param query the query to dispatch
   * @param <R> the result type, taken from the query's {@link Query}{@code <R>} declaration
   * @return the query result
   * @throws IllegalArgumentException if no handler is registered for the query's runtime class
   */
  <R> R dispatch(Query<R> query);

  /**
   * Registers a query handler for a query type.
   *
   * <p>Duplicate registration semantics are implementation-defined: {@code SimpleQueryBus} silently
   * replaces the previous handler (last registration wins), while immutable implementations may
   * reject duplicates or not support post-construction registration at all.
   *
   * @param queryType the query class (matched exactly against the dispatched query's runtime class)
   * @param handler the handler to register
   * @param <Q> the query type, whose declared {@link Query} result type binds {@code R}
   * @param <R> Result type
   */
  <Q extends Query<R>, R> void register(Class<Q> queryType, QueryHandler<Q, R> handler);
}
