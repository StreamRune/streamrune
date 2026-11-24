package org.streamrune.spring;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.streamrune.core.EventStore;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.SubscriptionHealthContributor;

/**
 * Spring Boot Actuator {@link HealthIndicator} for StreamRune. Reports DOWN when the configured
 * DataSource does not accept connections, when a subscription is DOWN (stopped or terminally
 * halted) or when a started background relay has lost its thread; otherwise UP. A subscription that
 * is behind (lag) or failing (consecutive errors) is DEGRADED in its own detail and does not change
 * the status, so the check can sit in the readiness group without a catch-up or rebuild taking
 * every replica out of service. The head of the event stream is included as the detail {@code
 * eventStore.lastGlobalOffset}, read through the application's {@link EventStore}, so it is the
 * same on every backend. A projection's entry also names the checkpoint target its checkpoint lives
 * in ({@code store}: the read-model store it is routed to, or {@code subscription_checkpoint}); its
 * lag is measured against that checkpoint.
 *
 * <p>Auto-registered by {@link StreamRuneAutoConfiguration} when {@code DataSource} and {@code
 * HealthIndicator} are on the classpath.
 */
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
   */
  public StreamRuneHealthIndicator(DataSource dataSource) {
    this(dataSource, null, null, null);
  }

  /**
   * Creates the health indicator.
   *
   * @param dataSource the JDBC data source to check
   * @param subscriptionHealthContributor the subscriptions to report, or {@code null}
   */
  public StreamRuneHealthIndicator(
      DataSource dataSource, SubscriptionHealthContributor subscriptionHealthContributor) {
    this(dataSource, subscriptionHealthContributor, null, null);
  }

  /**
   * Creates the health indicator.
   *
   * @param dataSource the JDBC data source to check
   * @param subscriptionHealthContributor the subscriptions to report, or {@code null}
   * @param eventStore the event store whose head is reported, or {@code null} to report none
   * @param relayHealthContributor the background relays to report, or {@code null}
   */
  public StreamRuneHealthIndicator(
      DataSource dataSource,
      SubscriptionHealthContributor subscriptionHealthContributor,
      EventStore eventStore,
      BackgroundRelayHealthContributor relayHealthContributor) {
    this.dataSource = dataSource;
    this.subscriptionHealthContributor = subscriptionHealthContributor;
    this.eventStore = eventStore;
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
    addEventStoreDetails(builder);

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
        details.put("role", sub.role().name());
        if (sub.store() != null) {
          details.put("store", sub.store());
        }
        builder.withDetail("subscription." + sub.name().value(), details);
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
   * Adds the head of the event stream, as the application's {@link EventStore} reports it: the same
   * call on every backend. A store without a global head reports {@code 0}. A failure (the schema
   * is not migrated yet, for example) is reported as the detail {@code eventStore.error} and does
   * not change the status.
   */
  private void addEventStoreDetails(Health.Builder builder) {
    if (eventStore == null) {
      return;
    }
    try {
      builder.withDetail("eventStore.lastGlobalOffset", eventStore.lastGlobalOffset().value());
    } catch (UnsupportedOperationException _) {
      builder.withDetail("eventStore.lastGlobalOffset", 0L);
    } catch (Exception e) {
      String message = e.getMessage();
      builder.withDetail("eventStore.error", message != null ? message : e.getClass().getName());
    }
  }
}
