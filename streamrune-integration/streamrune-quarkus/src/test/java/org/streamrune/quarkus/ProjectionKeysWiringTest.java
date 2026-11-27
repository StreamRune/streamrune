package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;

/**
 * {@link ProjectionKeysValidator} in a real Arc container: its startup observer runs on the startup
 * event and refuses a projection key the discovery does not read, and finds the name of a
 * normal-scoped projection bean through its client proxy.
 */
class ProjectionKeysWiringTest {

  static volatile SmallRyeConfig applicationConfig;

  /** The application's configuration and its bound mapping. */
  public static class Infrastructure {
    @Produces
    @Singleton
    public org.eclipse.microprofile.config.Config config() {
      return applicationConfig;
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return applicationConfig.getConfigMapping(StreamRuneQuarkusProperties.class);
    }
  }

  /** A normal-scoped projection: the container hands out its client proxy. */
  @ApplicationScoped
  @ProjectionConfig(name = "orders", deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  public static class Orders implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @Test
  void aKeyOfAProjectionBeanPasses() throws Exception {
    assertNull(start(Map.of("streamrune.projections.orders.enabled", "false")));
  }

  @Test
  void aMisspeltKeyRefusesTheStart() throws Exception {
    Throwable failure = start(Map.of("streamrune.projections.orders.enabeld", "false"));

    assertNotNull(failure, "the start must be refused");
    assertTrue(
        messages(failure).contains("streamrune.projections.orders.enabeld is not a key"),
        messages(failure));
  }

  @Test
  void aNameNoProjectionBeanHasRefusesTheStart() throws Exception {
    Throwable failure = start(Map.of("streamrune.projections.order.enabled", "false"));

    assertNotNull(failure, "the start must be refused");
    assertTrue(
        messages(failure)
            .contains(
                "streamrune.projections.order.enabled names no @ProjectionConfig projection;"
                    + " the projections are orders."),
        messages(failure));
  }

  /** Boots the validator, the configuration and the projection, fires the startup event. */
  private static Throwable start(Map<String, String> properties) throws Exception {
    applicationConfig =
        new SmallRyeConfigBuilder()
            .withMapping(StreamRuneQuarkusProperties.class)
            .withSources(new PropertiesConfigSource(properties, "application.properties", 250))
            .build();
    try (var arc =
        RealArcTestContainer.boot(
            List.of(ProjectionKeysValidator.class, Infrastructure.class, Orders.class))) {
      arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());
      return null;
    } catch (RuntimeException e) {
      return e;
    }
  }

  private static String messages(Throwable failure) {
    var text = new StringBuilder();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      text.append(t.getMessage()).append('\n');
      if (t.getCause() == t) {
        break;
      }
    }
    return text.toString();
  }
}
