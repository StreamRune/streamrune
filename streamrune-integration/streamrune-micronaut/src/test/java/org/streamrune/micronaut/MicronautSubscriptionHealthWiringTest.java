package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.exceptions.BeanInstantiationException;
import jakarta.inject.Singleton;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.runtime.SubscriptionHealthContributor;
import org.streamrune.test.InMemoryEventStore;

/** The subscription health contributor's lag threshold comes from configuration. */
class MicronautSubscriptionHealthWiringTest {

  private static final String SPEC = "subscription-health";

  @Test
  void theLagThresholdDefaultsToOneThousandEvents() {
    try (var ctx = ApplicationContext.run(Map.of("spec.name", SPEC))) {
      assertEquals(1_000L, ctx.getBean(SubscriptionHealthContributor.class).lagThreshold());
    }
  }

  @Test
  void theLagThresholdBindsFromItsProperty() {
    try (var ctx =
        ApplicationContext.run(
            Map.of("spec.name", SPEC, "streamrune.subscription.health.lag-threshold", "25000"))) {
      assertEquals(25_000L, ctx.getBean(SubscriptionHealthContributor.class).lagThreshold());
    }
  }

  @Test
  void aLagThresholdBelowOneIsRefusedNamingTheProperty() {
    var e =
        assertThrows(
            BeanInstantiationException.class,
            () -> {
              try (var ctx =
                  ApplicationContext.run(
                      Map.of(
                          "spec.name",
                          SPEC,
                          "streamrune.subscription.health.lag-threshold",
                          "0"))) {
                ctx.getBean(SubscriptionHealthContributor.class);
              }
            });
    assertTrue(
        e.getMessage().contains("streamrune.subscription.health.lag-threshold must be at least 1"),
        e.getMessage());
  }

  @Factory
  @Requires(property = "spec.name", value = SPEC)
  static class Fixture {

    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Singleton
    OffsetStore offsetStore() {
      OffsetStore offsetStore = mock(OffsetStore.class);
      when(offsetStore.getLastOffset(any())).thenReturn(GlobalOffset.initial());
      return offsetStore;
    }
  }
}
