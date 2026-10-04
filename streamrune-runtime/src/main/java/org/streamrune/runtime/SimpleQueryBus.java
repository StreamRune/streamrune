package org.streamrune.runtime;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.Query;
import org.streamrune.core.QueryBus;
import org.streamrune.core.QueryHandler;
import org.streamrune.core.StreamRuneMetrics;

/**
 * In-process implementation of {@link org.streamrune.core.QueryBus}. Routes queries to handlers
 * registered via {@link #register(Class, QueryHandler)}. Not thread-safe for handler registration;
 * register all handlers during startup before serving requests.
 *
 * <p>When a {@link StreamRuneMetrics} is supplied, each dispatch records {@code queries.dispatched}
 * and {@code queries.duration} tagged with the query type. Wire metrics into exactly one bus in a
 * decorator chain (e.g. either this bus or a wrapping {@link CachingQueryBus}) to avoid
 * double-counting.
 */
public final class SimpleQueryBus implements QueryBus {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleQueryBus.class);

  private final Map<Class<?>, QueryHandler<?, ?>> handlers = new ConcurrentHashMap<>();
  private final StreamRuneMetrics metrics;

  /** Creates a bus without metrics. */
  public SimpleQueryBus() {
    this(null);
  }

  /**
   * Creates a bus with an optional metrics collector.
   *
   * @param metrics the metrics collector; {@code null} disables metrics (behavior unchanged)
   */
  public SimpleQueryBus(StreamRuneMetrics metrics) {
    this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <R> R dispatch(Query<R> query) {
    QueryHandler<Query<R>, R> handler = (QueryHandler<Query<R>, R>) handlers.get(query.getClass());
    if (handler == null) {
      throw new IllegalArgumentException(
          "No handler registered for: " + query.getClass().getSimpleName());
    }
    String queryType = query.getClass().getSimpleName();
    recordMetric(() -> metrics.recordQueryDispatched(queryType), "recordQueryDispatched");
    long start = System.nanoTime();
    try {
      return handler.handle(query);
    } finally {
      long duration = System.nanoTime() - start;
      recordMetric(() -> metrics.recordQueryDuration(queryType, duration), "recordQueryDuration");
    }
  }

  /** Runs a metric-recording action, logging and swallowing any failure. */
  private void recordMetric(Runnable recorder, String meter) {
    try {
      recorder.run();
    } catch (RuntimeException e) {
      LOG.warn("Metrics {} failed", meter, e);
    }
  }

  @Override
  public <Q extends Query<R>, R> void register(Class<Q> queryType, QueryHandler<Q, R> handler) {
    handlers.put(queryType, handler);
  }
}
