package org.streamrune.runtime;

import java.util.List;
import org.streamrune.core.EventEnvelope;

/**
 * Notified by projections after a successful event batch. Triggers eviction of caches whose
 * {@code @Cacheable.invalidateOn} includes any of the batch's event types.
 *
 * <p>Implementations must be cheap, non-blocking, and must not throw a runtime exception. An {@link
 * Error} may propagate; the projection runner then stops and reports it.
 */
public interface CacheInvalidator {

  /**
   * Called after a projection has successfully processed {@code batch}. The invalidator inspects
   * event types and evicts any matching query-type caches.
   *
   * @param batch the processed events; must not be {@code null}
   */
  void onEventsProcessed(List<EventEnvelope> batch);
}
