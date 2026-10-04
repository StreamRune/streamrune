package org.streamrune.micronaut;

import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import jakarta.inject.Singleton;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.reactivestreams.Publisher;
import org.streamrune.core.EventStore;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.SubscriptionHealthContributor;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Micronaut {@link HealthIndicator} for StreamRune. Reports DOWN when the PostgreSQL DataSource
 * does not accept connections, when a subscription is DOWN (stopped or terminally halted) or when a
 * started background relay has lost its thread; otherwise UP. A subscription that is behind (lag)
 * or failing (consecutive errors) is DEGRADED in its own detail and does not change the status.
 *
 * <p>Exposed at {@code /health} and, because it is not annotated {@code @Liveness}, at {@code
 * /health/readiness} too — Micronaut runs every indicator that is not a liveness indicator for
 * readiness. That is why lag must not turn it DOWN: lag is measured against the checkpoint all
 * replicas share, so a catch-up or rebuild would take every replica out of service at once. Only
 * active when a {@link DataSource} bean is available.
 */
@Singleton
@io.micronaut.context.annotation.Requires(beans = DataSource.class)
public class StreamRuneHealthIndicator implements HealthIndicator {

  private static final int VALIDATION_TIMEOUT_SECONDS = 3;

  private final DataSource dataSource;
  private final SubscriptionHealthContributor subscriptionHealthContributor;
  private final EventStore eventStore;
  private final BackgroundRelayHealthContributor relayHealthContributor;

  /**
   * Creates the health indicator.
   *
   * @param dataSource the JDBC data source to check
   * @param subscriptionHealthContributor optional subscription health contributor
   */
  public StreamRuneHealthIndicator(
      DataSource dataSource,
      @jakarta.annotation.Nullable SubscriptionHealthContributor subscriptionHealthContributor) {
    this(dataSource, subscriptionHealthContributor, null, null);
  }

  /**
   * Creates the health indicator.
   *
   * @param dataSource the JDBC data source to check
   * @param subscriptionHealthContributor optional subscription health contributor
   * @param eventStore optional event store for additional health details
   */
  public StreamRuneHealthIndicator(
      DataSource dataSource,
      @jakarta.annotation.Nullable SubscriptionHealthContributor subscriptionHealthContributor,
      @jakarta.annotation.Nullable EventStore eventStore) {
    this(dataSource, subscriptionHealthContributor, eventStore, null);
  }

  /**
   * Creates the health indicator.
   *
   * @param dataSource the JDBC data source to check
   * @param subscriptionHealthContributor optional subscription health contributor
   * @param eventStore optional event store for additional health details
   * @param relayHealthContributor optional background-relay (outbox / DLQ retry) liveness
   *     contributor
   */
  @jakarta.inject.Inject
  public StreamRuneHealthIndicator(
      DataSource dataSource,
      @jakarta.annotation.Nullable SubscriptionHealthContributor subscriptionHealthContributor,
      @jakarta.annotation.Nullable EventStore eventStore,
      @jakarta.annotation.Nullable BackgroundRelayHealthContributor relayHealthContributor) {
    this.dataSource = dataSource;
    this.subscriptionHealthContributor = subscriptionHealthContributor;
    this.eventStore = eventStore;
    this.relayHealthContributor = relayHealthContributor;
  }

  @Override
  public Publisher<HealthResult> getResult() {
    return Mono.fromCallable(this::checkHealth).subscribeOn(Schedulers.boundedElastic());
  }

  private HealthResult checkHealth() {
    try (Connection conn = dataSource.getConnection()) {
      if (!conn.isValid(VALIDATION_TIMEOUT_SECONDS)) {
        return HealthResult.builder("streamrune")
            .status(HealthStatus.DOWN)
            .details(Map.of("error", "Connection validation timed out"))
            .build();
      }
    } catch (SQLException e) {
      return HealthResult.builder("streamrune")
          .status(HealthStatus.DOWN)
          .details(Map.of("error", e.getMessage()))
          .build();
    }

    // DB is up — add event store details
    Map<String, Object> details = new LinkedHashMap<>();
    if (eventStore != null) {
      try {
        details.put("eventStore.lastGlobalOffset", eventStore.lastGlobalOffset().value());
      } catch (UnsupportedOperationException _) {
        details.put("eventStore.lastGlobalOffset", 0L);
      } catch (Exception e) {
        details.put("eventStore.error", e.getMessage());
      }
    }

    boolean down = false;

    // Check subscription health. One sample: the status is derived from the very entries reported
    // below, so the indicator can never be UP while its own details show a DOWN subscription.
    if (subscriptionHealthContributor != null) {
      var subscriptions = subscriptionHealthContributor.health();
      if (SubscriptionHealthContributor.overallStatus(subscriptions)
          == SubscriptionHealth.Status.DOWN) {
        down = true;
      }
      for (var sub : subscriptions) {
        Map<String, Object> subDetails = new LinkedHashMap<>();
        subDetails.put("state", sub.state().name());
        subDetails.put("lag", sub.lag());
        subDetails.put("errorCount", sub.errorCount());
        subDetails.put("status", sub.status().name());
        details.put("subscription." + sub.name(), subDetails);
      }
    }

    // Background relay liveness (outbox relay + DLQ retry runner): DOWN if a started relay's poll
    // thread has died — otherwise /health reports UP while a relay is silently stalled.
    // One sample, like the subscriptions above.
    if (relayHealthContributor != null) {
      var components = relayHealthContributor.components();
      if (components.stream()
          .anyMatch(c -> c.status() == BackgroundRelayHealthContributor.Status.DOWN)) {
        down = true;
      }
      for (var component : components) {
        Map<String, Object> relayDetails = new LinkedHashMap<>();
        relayDetails.put("status", component.status().name());
        relayDetails.put("started", component.started());
        relayDetails.put("alive", component.alive());
        relayDetails.put("consecutiveFailures", component.consecutiveFailures());
        details.put("relay." + component.name(), relayDetails);
      }
    }

    return HealthResult.builder("streamrune")
        .status(down ? HealthStatus.DOWN : HealthStatus.UP)
        .details(details.isEmpty() ? null : details)
        .build();
  }
}
