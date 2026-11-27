package org.streamrune.quarkus.it;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;

/** The application's projections: continuous ones, three of them named by a configuration key. */
final class Projections {

  private Projections() {}

  /** Named by no key: runs. */
  @ApplicationScoped
  @ProjectionConfig(name = "orders", deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class Orders implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /** Its error strategy is set in the configuration file: runs. */
  @ApplicationScoped
  @ProjectionConfig(
      name = "order-totals",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class OrderTotals implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /** Switched off in the configuration file. */
  @ApplicationScoped
  @ProjectionConfig(
      name = "audit-log",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class AuditLog implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /** Switched off by a system property. */
  @ApplicationScoped
  @ProjectionConfig(
      name = "legacy-feed",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class LegacyFeed implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }
}
