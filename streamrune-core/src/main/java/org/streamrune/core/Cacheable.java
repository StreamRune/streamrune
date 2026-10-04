package org.streamrune.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a query record so that {@code CachingQueryBus} caches the dispatch result.
 *
 * <h2>Cache key</h2>
 *
 * <p>The cache key is the query instance itself, resolved via {@code equals}/{@code hashCode}. Java
 * {@code record} types provide canonical implementations automatically; non-record query classes
 * must implement {@code equals}/{@code hashCode} correctly or every call will be a cache miss.
 *
 * <h2>Scope</h2>
 *
 * <p>{@link #scope()} controls whether cached results are shared across users. The default, {@link
 * Scope#USER}, additionally keys every entry by the calling user's {@code UserId} from {@code
 * StreamRuneContext} — required for handlers that read the ambient user and return user-specific
 * data, where a query-only key would serve one user's results to another. {@link Scope#GLOBAL} is
 * an explicit opt-in for read models whose results are identical for every caller.
 *
 * <h2>Eviction</h2>
 *
 * <p>Two independent eviction triggers are supported and both apply when both are configured — an
 * entry is evicted on whichever happens first:
 *
 * <ol>
 *   <li><b>TTL</b> — entry is evicted after {@link #ttlSeconds()} seconds from insertion.
 *   <li><b>Event-driven</b> — entry is evicted when any event whose type appears in {@link
 *       #invalidateOn()} is processed. An empty {@code invalidateOn} (the default) disables
 *       event-driven eviction, leaving TTL as the only trigger.
 * </ol>
 *
 * <h2>Validation</h2>
 *
 * <p>{@link #ttlSeconds()} and {@link #maxEntries()} must both be {@literal >} 0. Violations are
 * not caught at compile time; {@code CachingQueryBus} throws {@link IllegalStateException} on the
 * first dispatch attempt for the offending query type.
 *
 * <h2>Interaction with auditing</h2>
 *
 * <p>For queries that are also {@link org.streamrune.core.audit.Auditable @Auditable}, the nesting
 * order of the caching and auditing decorators determines whether cache hits are audited — see
 * {@link org.streamrune.core.audit.Auditable}. The provided integrations wrap the caching bus with
 * the auditing bus (auditing outermost), so cache hits are recorded in the audit trail.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * @Cacheable(ttlSeconds = 300, invalidateOn = {OrderPlaced.class, OrderCancelled.class})
 * public record GetOrderCount(String userId) {}
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Cacheable {

  /** Controls whether cached results are shared across users. */
  enum Scope {
    /**
     * Cache entries are keyed by the query <em>and</em> the calling user's {@code UserId} from
     * {@code StreamRuneContext}. Each user gets their own entry; callers without a bound user
     * (anonymous requests) <b>bypass the cache entirely</b> — neither served from it nor stored
     * into it — rather than sharing one anonymous partition across every unidentified caller (see
     * {@code CachingQueryBus}'s javadoc). The safe default — query handlers frequently read the
     * ambient user and return user-specific data.
     *
     * <p><b>Caching is not authorization.</b> A cache HIT never calls the delegate, so a check the
     * handler performs internally does not run for it — a caller whose access was revoked after an
     * entry was cached keeps receiving it until TTL/invalidation, which are caching controls, not
     * security controls. Configure an {@code org.streamrune.core.QueryAuthorizer} (consulted on
     * every dispatch, hit or miss, before the cache) for any cached query whose result depends on
     * the caller's access — see {@code CachingQueryBus.Builder#authorizer} and the query-cache
     * guide's Authorization section.
     */
    USER,

    /**
     * Cache entries are keyed by the query alone and shared across all users. Opt in only for read
     * models whose results are identical for every caller; never use for handlers that read the
     * ambient user from {@code StreamRuneContext}.
     */
    GLOBAL
  }

  /**
   * Time-to-live in seconds. Cache entries for this query type are evicted after this many seconds
   * from insertion, regardless of access pattern. Must be {@literal >} 0. Default: 60.
   */
  long ttlSeconds() default 60;

  /**
   * Maximum number of entries kept in the cache for this query type. When the limit is reached the
   * cache evicts the entries it judges least likely to be used again: Caffeine's size-based
   * eviction weighs access frequency as well as recency, so it is not strict LRU. Must be {@literal
   * >} 0. Default: 1000.
   */
  long maxEntries() default 1000;

  /**
   * Event types that trigger immediate cache invalidation. When ANY event of a listed type is
   * processed, ALL cache entries for this query type are evicted. Both TTL and event-driven
   * eviction apply when both are configured; the entry is evicted on whichever trigger fires first.
   *
   * <p>Empty array (default) means TTL-only invalidation — no event-driven eviction is registered.
   */
  Class<?>[] invalidateOn() default {};

  /**
   * Whether cached results are per-user or shared. Defaults to {@link Scope#USER} so that
   * user-scoped query results are never served to a different user; use {@link Scope#GLOBAL} for
   * read models that are identical for every caller.
   */
  Scope scope() default Scope.USER;
}
