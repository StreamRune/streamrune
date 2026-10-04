package org.streamrune.spring;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.SubscriptionHealthContributor;

/**
 * Spring Boot Actuator {@link HealthIndicator} for StreamRune. Reports DOWN when the configured
 * PostgreSQL DataSource does not accept connections, when a subscription is DOWN (stopped or
 * terminally halted) or when a started background relay has lost its thread; otherwise UP. A
 * subscription that is behind (lag) or failing (consecutive errors) is DEGRADED in its own detail
 * and does not change the status, so the check can sit in the readiness group without a catch-up or
 * rebuild taking every replica out of service. The head of the event stream ({@code
 * eventStore.lastGlobalOffset} and {@code eventStore.lastEventTimestamp}) is included as details.
 *
 * <p>Auto-registered by {@link StreamRuneAutoConfiguration} when {@code DataSource} and {@code
 * HealthIndicator} are on the classpath.
 */
public class StreamRuneHealthIndicator implements HealthIndicator {

  private static final int VALIDATION_TIMEOUT_SECONDS = 3;

  /**
   * Head probe for the event stream: the highest {@code global_offset} and its insert timestamp.
   * Index-backed via the {@code event_stream} primary key, so this stays O(1) regardless of store
   * size.
   */
  static final String LATEST_EVENT_QUERY =
      "SELECT global_offset, created_at FROM event_stream ORDER BY global_offset DESC LIMIT 1";

  private final DataSource dataSource;
  private final SubscriptionHealthContributor subscriptionHealthContributor;
  private final BackgroundRelayHealthContributor relayHealthContributor;

  public StreamRuneHealthIndicator(DataSource dataSource) {
    this(dataSource, null, null);
  }

  public StreamRuneHealthIndicator(
      DataSource dataSource, SubscriptionHealthContributor subscriptionHealthContributor) {
    this(dataSource, subscriptionHealthContributor, null);
  }

  public StreamRuneHealthIndicator(
      DataSource dataSource,
      SubscriptionHealthContributor subscriptionHealthContributor,
      BackgroundRelayHealthContributor relayHealthContributor) {
    this.dataSource = dataSource;
    this.subscriptionHealthContributor = subscriptionHealthContributor;
    this.relayHealthContributor = relayHealthContributor;
  }

  @Override
  public Health health() {
    boolean dbUp;
    String dbError = null;
    Health.Builder builder = Health.up();
    try (Connection conn = dataSource.getConnection()) {
      dbUp = conn.isValid(VALIDATION_TIMEOUT_SECONDS);
      if (!dbUp) {
        dbError = "Connection validation timed out";
      } else {
        addEventStoreDetails(conn, builder);
      }
    } catch (SQLException e) {
      dbUp = false;
      dbError = e.getMessage();
    }

    if (!dbUp) {
      var down = Health.down();
      if (dbError != null) {
        down.withDetail("error", dbError);
      }
      return down.build();
    }

    // Check subscription health. One sample: the status is derived from the very entries reported
    // below, so the check can never be UP while its own details show a DOWN subscription.
    if (subscriptionHealthContributor != null) {
      var subscriptions = subscriptionHealthContributor.health();
      if (SubscriptionHealthContributor.overallStatus(subscriptions)
          == SubscriptionHealth.Status.DOWN) {
        builder.down();
      }
      for (var sub : subscriptions) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("state", sub.state().name());
        details.put("lag", sub.lag());
        details.put("errorCount", sub.errorCount());
        details.put("status", sub.status().name());
        builder.withDetail("subscription." + sub.name(), details);
      }
    }

    // Background relay liveness (outbox relay + DLQ retry runner): DOWN if a started relay's poll
    // thread has died — otherwise /health reports UP while a relay is silently stalled.
    // One sample, like the subscriptions above.
    if (relayHealthContributor != null) {
      var components = relayHealthContributor.components();
      if (components.stream()
          .anyMatch(c -> c.status() == BackgroundRelayHealthContributor.Status.DOWN)) {
        builder.down();
      }
      for (var component : components) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("status", component.status().name());
        details.put("started", component.started());
        details.put("alive", component.alive());
        details.put("consecutiveFailures", component.consecutiveFailures());
        builder.withDetail("relay." + component.name(), details);
      }
    }

    return builder.build();
  }

  /**
   * Adds the latest global offset and event timestamp to the health details by probing the head of
   * the {@code event_stream} table. A probe failure (e.g. schema not yet migrated) is reported as a
   * detail but does not flip the overall status.
   */
  private void addEventStoreDetails(Connection conn, Health.Builder builder) {
    try (var ps = conn.prepareStatement(LATEST_EVENT_QUERY);
        var rs = ps.executeQuery()) {
      if (rs.next()) {
        builder.withDetail("eventStore.lastGlobalOffset", rs.getLong("global_offset"));
        // event_stream.created_at is NOT NULL DEFAULT NOW() in the event-store baseline.
        builder.withDetail(
            "eventStore.lastEventTimestamp",
            rs.getObject("created_at", OffsetDateTime.class).toInstant().toString());
      } else {
        builder.withDetail("eventStore.lastGlobalOffset", 0L);
      }
    } catch (Exception e) {
      String message = e.getMessage();
      builder.withDetail("eventStore.error", message != null ? message : e.getClass().getName());
    }
  }
}
