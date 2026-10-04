package org.streamrune.quarkus;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;
import org.streamrune.core.EventStore;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.SubscriptionHealthContributor;

/**
 * Quarkus {@link HealthCheck} for StreamRune. Annotated {@code @Readiness} — included in {@code
 * /q/health/ready} (and {@code /q/health}) beside every other readiness check of the application,
 * such as the datasource check Quarkus registers by default.
 *
 * <p>Deliberately not {@code @DefaultBean}: SmallRye Health collects its checks through an {@code
 * Instance<HealthCheck>}, and Arc resolves that iteration like a single injection point — a
 * {@code @DefaultBean} candidate is dropped as soon as any other readiness check exists, which
 * would hide this check exactly in the deployments that have one. To turn it off, set {@code
 * quarkus.smallrye-health.check."org.streamrune.quarkus.StreamRuneHealthCheck".enabled=false}; to
 * replace it, turn it off and declare your own {@code @Readiness} check.
 *
 * <p>Reports UP when the PostgreSQL DataSource accepts connections, no subscription is DOWN and no
 * started background relay has lost its thread. A subscription is DOWN only when it is stopped or
 * terminally halted; one that is behind (lag) or failing (consecutive errors) is DEGRADED in the
 * response data and leaves the check UP. Lag is measured against the checkpoint all replicas share,
 * so a lag-driven DOWN would take every replica out of readiness during a catch-up or rebuild. The
 * DataSource is optional: a health check is unremovable, so a direct injection would fail the build
 * for applications that use a custom (non-JDBC) {@link EventStore} and have no DataSource bean at
 * all. When no DataSource exists the connectivity check is skipped and that is reported explicitly
 * in the response data.
 */
@Readiness
@ApplicationScoped
public class StreamRuneHealthCheck implements HealthCheck {

  private static final int VALIDATION_TIMEOUT_SECONDS = 3;

  private final DataSource dataSource;
  private final SubscriptionHealthContributor subscriptionHealthContributor;
  private final EventStore eventStore;
  private final BackgroundRelayHealthContributor relayHealthContributor;

  @jakarta.inject.Inject
  public StreamRuneHealthCheck(
      Instance<DataSource> dataSourceInstance,
      Instance<SubscriptionHealthContributor> healthContributor,
      Instance<EventStore> eventStoreInstance,
      Instance<BackgroundRelayHealthContributor> relayHealthContributor) {
    this.dataSource = dataSourceInstance.isUnsatisfied() ? null : dataSourceInstance.get();
    this.subscriptionHealthContributor =
        healthContributor.isUnsatisfied() ? null : healthContributor.get();
    this.eventStore = eventStoreInstance.isUnsatisfied() ? null : eventStoreInstance.get();
    this.relayHealthContributor =
        relayHealthContributor.isUnsatisfied() ? null : relayHealthContributor.get();
  }

  /**
   * Client-proxy constructor. A normal-scoped bean needs a non-private no-arg constructor for its
   * client proxy; Quarkus's build step adds one by bytecode transformation, which an application
   * can switch off ({@code quarkus.arc.transform-unproxyable-classes=false}) and which a plain Arc
   * container does not run, so the bean is proxyable as written. {@code protected} rather than
   * package-private: the proxy is a generated subclass that may live in a different classloader (a
   * different runtime package), where only protected access reaches a {@code super()} constructor.
   * Never used for a real instance — the injecting constructor above is the one Arc calls.
   */
  protected StreamRuneHealthCheck() {
    this.dataSource = null;
    this.subscriptionHealthContributor = null;
    this.eventStore = null;
    this.relayHealthContributor = null;
  }

  @Override
  public HealthCheckResponse call() {
    if (dataSource != null) {
      try (Connection conn = dataSource.getConnection()) {
        if (!conn.isValid(VALIDATION_TIMEOUT_SECONDS)) {
          return HealthCheckResponse.named("streamrune")
              .down()
              .withData("error", "Connection validation timed out")
              .build();
        }
      } catch (SQLException | RuntimeException e) {
        // RuntimeException covers client-proxy failures such as an Agroal datasource
        // that exists as a bean but was not activated by configuration.
        return HealthCheckResponse.named("streamrune")
            .down()
            .withData("error", e.getMessage())
            .build();
      }
    }

    // DB is up (or there is none to check) — add event store details
    HealthCheckResponseBuilder builder = HealthCheckResponse.named("streamrune");
    if (dataSource == null) {
      builder.withData("database", "no DataSource bean - connectivity check skipped");
    }
    if (eventStore != null) {
      try {
        builder.withData("eventStore.lastGlobalOffset", eventStore.lastGlobalOffset().value());
      } catch (UnsupportedOperationException _) {
        builder.withData("eventStore.lastGlobalOffset", 0L);
      } catch (Exception e) {
        builder.withData("eventStore.error", e.getMessage());
      }
    }

    // Check subscription health
    if (subscriptionHealthContributor != null) {
      // One snapshot: the overall status is derived from the very entries reported below, so the
      // check can never be UP while its own details show a DOWN subscription.
      var subscriptions = subscriptionHealthContributor.health();
      if (SubscriptionHealthContributor.overallStatus(subscriptions)
          == SubscriptionHealth.Status.DOWN) {
        builder.down();
      } else {
        builder.up();
      }
      for (var sub : subscriptions) {
        builder.withData("subscription." + sub.name() + ".state", sub.state().name());
        builder.withData("subscription." + sub.name() + ".lag", sub.lag());
        builder.withData("subscription." + sub.name() + ".errorCount", sub.errorCount());
        builder.withData("subscription." + sub.name() + ".status", sub.status().name());
      }
    } else {
      builder.up();
    }

    // Background relay liveness (outbox relay + DLQ retry runner): only ever DOWNGRADES to DOWN if
    // a started relay's poll thread has died; never upgrades an already-down status. One snapshot,
    // like the subscriptions above.
    if (relayHealthContributor != null) {
      var components = relayHealthContributor.components();
      if (components.stream()
          .anyMatch(c -> c.status() == BackgroundRelayHealthContributor.Status.DOWN)) {
        builder.down();
      }
      for (var component : components) {
        builder.withData("relay." + component.name() + ".status", component.status().name());
        builder.withData("relay." + component.name() + ".started", component.started());
        builder.withData("relay." + component.name() + ".alive", component.alive());
        builder.withData(
            "relay." + component.name() + ".consecutiveFailures", component.consecutiveFailures());
      }
    }
    return builder.build();
  }
}
