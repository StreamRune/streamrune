package org.streamrune.core;

import org.streamrune.core.types.UserId;

/**
 * Pluggable authorization check consulted by {@code CachingQueryBus} on every dispatch of a {@link
 * Cacheable @Cacheable} query — hit or miss — before the cache is ever touched.
 *
 * <p><b>Why this exists.</b> A cache HIT is served without ever calling the delegate {@link
 * QueryBus}, so any authorization a handler performs internally never runs for it: a caller whose
 * access was revoked after a query was first cached keeps receiving the cached answer until the
 * entry expires or is invalidated. TTL and event-driven invalidation (see {@link Cacheable}) are
 * caching controls, not security controls — neither reacts to a permission change. Configuring a
 * {@code QueryAuthorizer} closes this gap: it runs on every dispatch, so a denial is enforced
 * immediately, on the very next call, regardless of what is or is not in the cache.
 *
 * <p><b>Authorize at the object level.</b> The query instance itself must carry whatever
 * identifiers the check needs (e.g. the order id a {@code GetOrder} query targets) — a purely
 * role-based check cannot express "does this caller own THIS resource", and object-level access
 * (ownership, an ACL entry, tenant membership) is exactly the kind of authorization a handler would
 * otherwise perform internally and a cache hit would then skip.
 *
 * <p><b>Scope.</b> When configured, this authorizer runs for every {@code @Cacheable} query
 * dispatch — both {@link Cacheable.Scope#USER} and {@link Cacheable.Scope#GLOBAL}, and even when no
 * caller identity is bound (a {@code null} {@code user}, e.g. an anonymous {@code GLOBAL}-scoped
 * query). It is NOT consulted for queries without {@link Cacheable} — those always reach the
 * delegate's handler on every call, so any authorization the handler performs internally already
 * runs unconditionally.
 *
 * <p>Absent (the default — no authorizer configured), {@code CachingQueryBus} behaves exactly as
 * before: caching is purely a performance optimization with no authorization opinion of its own,
 * and any authorization must live entirely inside the handler (with the cache-hit caveat above).
 *
 * @see Authorization
 */
@FunctionalInterface
public interface QueryAuthorizer {

  /**
   * Authorizes {@code query} for {@code user}. Returning normally allows the dispatch to proceed —
   * to a cache hit if one exists, or to the delegate on a miss. Throwing denies it: the dispatch
   * serves nothing (no stale hit) and stores nothing (the delegate is never called, so there is
   * nothing to cache).
   *
   * @param query the query about to be served from cache or dispatched to the delegate
   * @param user the calling user's {@link UserId} from {@link StreamRuneContext}, or {@code null}
   *     when no identity is bound
   * @throws AuthorizationException if {@code user} is not authorized for {@code query}
   */
  void authorize(Query<?> query, UserId user);
}
