package org.streamrune.runtime;

import com.github.benmanes.caffeine.cache.Ticker;
import java.util.Objects;
import org.streamrune.core.QueryAuthorizer;
import org.streamrune.core.QueryBus;
import org.streamrune.core.StreamRuneMetrics;

/**
 * Fluent builder for {@link CachingQueryBus}.
 *
 * <p>Obtain an instance via {@link CachingQueryBus#builder()}.
 *
 * <pre>{@code
 * CachingQueryBus bus = CachingQueryBus.builder()
 *     .delegate(simpleQueryBus)
 *     .build();
 * }</pre>
 */
public final class CachingQueryBusBuilder {

  private QueryBus delegate;
  private Ticker ticker = Ticker.systemTicker();
  private StreamRuneMetrics metrics;
  private QueryAuthorizer authorizer;

  /** Package-private — obtain via {@link CachingQueryBus#builder()}. */
  CachingQueryBusBuilder() {}

  /**
   * Sets the delegate {@link QueryBus} that actually executes handlers. Required.
   *
   * @param bus the delegate bus; must not be {@code null}
   * @return this builder
   */
  public CachingQueryBusBuilder delegate(QueryBus bus) {
    this.delegate = Objects.requireNonNull(bus, "delegate");
    return this;
  }

  /**
   * Overrides the {@link Ticker} used by Caffeine for TTL expiry. Defaults to {@link
   * Ticker#systemTicker()}. Primarily used in tests to control time.
   *
   * @param t the ticker to use; must not be {@code null}
   * @return this builder
   */
  public CachingQueryBusBuilder ticker(Ticker t) {
    this.ticker = Objects.requireNonNull(t, "ticker");
    return this;
  }

  /**
   * Sets the metrics collector. Optional; when absent (or {@code null}) cache metrics are not
   * recorded and behavior is unchanged. Records {@code queries.cache.hits} and {@code
   * queries.cache.misses} tagged with the query type for {@link
   * org.streamrune.core.Cacheable @Cacheable} queries.
   *
   * @param m the metrics collector; must not be {@code null}
   * @return this builder
   */
  public CachingQueryBusBuilder metrics(StreamRuneMetrics m) {
    this.metrics = Objects.requireNonNull(m, "metrics");
    return this;
  }

  /**
   * Sets the {@link QueryAuthorizer} consulted on every dispatch of a {@code @Cacheable} query —
   * hit or miss — before the cache is ever touched. Optional; when absent (or {@code null}) no
   * authorization check runs here and behavior is unchanged from before this option existed — see
   * {@link QueryAuthorizer} for why one is usually needed once caching is turned on for a query
   * whose results depend on the caller's access.
   *
   * @param authorizer the authorizer to consult; {@code null} disables the check
   * @return this builder
   */
  public CachingQueryBusBuilder authorizer(QueryAuthorizer authorizer) {
    this.authorizer = authorizer;
    return this;
  }

  /**
   * Builds the {@link CachingQueryBus}.
   *
   * @return a new {@link CachingQueryBus}
   * @throws IllegalStateException if {@link #delegate(QueryBus)} was not called
   */
  public CachingQueryBus build() {
    if (delegate == null) {
      throw new IllegalStateException("delegate is required");
    }
    return new CachingQueryBus(delegate, ticker, metrics, authorizer);
  }
}
