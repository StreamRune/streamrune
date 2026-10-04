package org.streamrune.runtime;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.Objects;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.UserId;

/**
 * A caching decorator for {@link UserRoleResolver} that memoizes resolved {@link UserAuthority}
 * instances using a Caffeine cache keyed by {@link UserId}.
 *
 * <p>Null user IDs are never cached and always delegate directly.
 */
public final class CachingUserRoleResolver implements UserRoleResolver {

  private static final Duration DEFAULT_TTL = Duration.ofMinutes(5);
  private static final long DEFAULT_MAX_SIZE = 10_000;

  private final UserRoleResolver delegate;
  private final Cache<UserId, UserAuthority> cache;

  public CachingUserRoleResolver(UserRoleResolver delegate) {
    this(delegate, DEFAULT_TTL, DEFAULT_MAX_SIZE);
  }

  public CachingUserRoleResolver(UserRoleResolver delegate, Duration ttl, long maxSize) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.cache = Caffeine.newBuilder().expireAfterWrite(ttl).maximumSize(maxSize).build();
  }

  @Override
  public UserAuthority resolve(UserId userId) {
    if (userId == null) {
      return delegate.resolve(null);
    }
    return cache.get(userId, delegate::resolve);
  }

  /**
   * Reports the delegate's own answer. The decorator changes when {@link #resolve(UserId)} is
   * called, never where it can be called from, so hiding a request-scoped delegate behind the
   * default {@code false} would tell the framework it may consult this resolver on a background
   * thread — and a cache miss there resolves to {@link UserAuthority#EMPTY} and caches that empty
   * answer against the user's id for the whole TTL, denying them on the request thread too.
   */
  @Override
  public boolean requiresRequestContext() {
    return delegate.requiresRequestContext();
  }

  /** Evicts the cached authority for a specific user. */
  public void invalidate(UserId userId) {
    cache.invalidate(userId);
  }

  /** Clears all cached authorities. */
  public void invalidateAll() {
    cache.invalidateAll();
  }
}
