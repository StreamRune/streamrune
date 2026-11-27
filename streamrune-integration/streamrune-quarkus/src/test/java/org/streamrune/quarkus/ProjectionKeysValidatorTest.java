package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.smallrye.config.EnvConfigSource;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.inject.Instance;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;

/**
 * Under {@code streamrune.projections.} the configuration accepts every key (the mapping declares
 * them as one map); the start is refused for a key, from a source other than the environment, that
 * the projection discovery does not read.
 */
class ProjectionKeysValidatorTest {

  @ProjectionConfig(name = "orders", deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class Orders implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @ProjectionConfig(
      name = "order-totals",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class OrderTotals implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @ProjectionConfig(
      name = "orders.v2",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class OrdersV2 implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /** A projection without {@code @ProjectionConfig}: the discovery does not see it. */
  static class Unannotated implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  private static final List<Projection> PROJECTIONS =
      List.of(new Orders(), new OrderTotals(), new OrdersV2(), new Unannotated());

  @Test
  void theKeysTheDiscoveryReadsPass() {
    SmallRyeConfig config =
        configOf(
            Map.of(
                "streamrune.projections.auto-discovery.enabled", "true",
                "streamrune.projections.orders.enabled", "false",
                "streamrune.projections.\"orders.v2\".enabled", "false",
                "streamrune.projections.order-totals.error-strategy", "SKIP"),
            Map.of());

    assertDoesNotThrow(() -> validate(config, PROJECTIONS));
  }

  @Test
  void aKeyTheDiscoveryDoesNotReadRefusesTheStartNamingIt() {
    SmallRyeConfig config =
        configOf(
            Map.of(
                "streamrune.projections.orders.enabeld", "false",
                "streamrune.projections.orders.enabled", "false",
                "streamrune.projections.order-totals.batch-size", "10"),
            Map.of());

    IllegalStateException refused =
        assertThrows(IllegalStateException.class, () -> validate(config, PROJECTIONS));

    assertEquals(
        "streamrune.projections.order-totals.batch-size, streamrune.projections.orders.enabeld"
            + " are not keys the projection discovery reads: under streamrune.projections. it"
            + " reads auto-discovery.enabled, <name>.enabled and <name>.error-strategy.",
        refused.getMessage());
  }

  @Test
  void oneRefusedKeyIsNamedAlone() {
    SmallRyeConfig config = configOf(Map.of("streamrune.projections.enabled", "false"), Map.of());

    IllegalStateException refused =
        assertThrows(IllegalStateException.class, () -> validate(config, PROJECTIONS));

    assertEquals(
        "streamrune.projections.enabled is not a key the projection discovery reads: under"
            + " streamrune.projections. it reads auto-discovery.enabled, <name>.enabled and"
            + " <name>.error-strategy.",
        refused.getMessage());
  }

  @Test
  void aNameNoProjectionHasRefusesTheStartListingTheProjections() {
    SmallRyeConfig config =
        configOf(
            Map.of(
                "streamrune.projections.order-total.enabled", "false",
                "streamrune.projections.unannotated.error-strategy", "SKIP",
                "streamrune.projections.orders.paused", "true"),
            Map.of());

    IllegalStateException refused =
        assertThrows(IllegalStateException.class, () -> validate(config, PROJECTIONS));

    assertEquals(
        "streamrune.projections.orders.paused is not a key the projection discovery reads: under"
            + " streamrune.projections. it reads auto-discovery.enabled, <name>.enabled and"
            + " <name>.error-strategy. streamrune.projections.order-total.enabled,"
            + " streamrune.projections.unannotated.error-strategy name no @ProjectionConfig"
            + " projection; the projections are order-totals, orders, orders.v2.",
        refused.getMessage());
  }

  @Test
  void oneUnknownNameWithoutProjectionsIsNamedAlone() {
    SmallRyeConfig config =
        configOf(Map.of("streamrune.projections.orders.enabled", "false"), Map.of());

    IllegalStateException refused =
        assertThrows(IllegalStateException.class, () -> validate(config, List.of()));

    assertEquals(
        "streamrune.projections.orders.enabled names no @ProjectionConfig projection; the"
            + " projections are none.",
        refused.getMessage());
  }

  @Test
  void aKeyFromTheEnvironmentIsNotChecked() {
    // STREAMRUNE_PROJECTIONS_ORDER_TOTALS_ERROR_STRATEGY is the map key
    // order.totals.error.strategy, the configuration's reading of the variable, which the
    // discovery's lookup of streamrune.projections.order-totals.error-strategy finds.
    SmallRyeConfig config =
        configOf(
            Map.of(),
            Map.of(
                "STREAMRUNE_PROJECTIONS_ORDER_TOTALS_ERROR_STRATEGY", "SKIP",
                "STREAMRUNE_PROJECTIONS_ORDERS_ENABELD", "false"));

    assertDoesNotThrow(() -> validate(config, PROJECTIONS));
    assertEquals(
        "SKIP",
        config.getValue("streamrune.projections.order-totals.error-strategy", String.class));
  }

  @Test
  void theCheckIsOffWhereTheConfigurationValidatesNoUnknownKey() {
    for (String switchKey :
        List.of(
            "quarkus.config.mapping.validate-unknown",
            "smallrye.config.mapping.validate-unknown")) {
      // Every spelling the configuration's boolean converter reads as false.
      for (String off : List.of("false", "off", "no", "0")) {
        Map<String, String> file = new HashMap<>();
        file.put("streamrune.projections.orders.enabeld", "false");
        file.put(switchKey, off);

        assertDoesNotThrow(
            () -> validate(configOf(file, Map.of()), PROJECTIONS), switchKey + "=" + off);
      }
    }
    Map<String, String> on = new HashMap<>();
    on.put("streamrune.projections.orders.enabeld", "false");
    on.put("quarkus.config.mapping.validate-unknown", "true");
    assertThrows(IllegalStateException.class, () -> validate(configOf(on, Map.of()), PROJECTIONS));
  }

  @Test
  void whileTheDiscoveryIsOffANameIsNotCheckedAndNoProjectionIsLookedUp() {
    @SuppressWarnings("unchecked")
    Instance<Projection> projections = mock(Instance.class);
    SmallRyeConfig config =
        configOf(
            Map.of(
                "streamrune.projections.auto-discovery.enabled", "false",
                "streamrune.projections.hand-wired.enabled", "false"),
            Map.of());

    new ProjectionKeysValidator(
            config.getConfigMapping(StreamRuneQuarkusProperties.class),
            configInstance(config),
            projections)
        .validate(null);

    verifyNoInteractions(projections);
    SmallRyeConfig misspelt =
        configOf(
            Map.of(
                "streamrune.projections.auto-discovery.enabled", "false",
                "streamrune.projections.hand-wired.enabeld", "false"),
            Map.of());
    assertThrows(IllegalStateException.class, () -> validate(misspelt, PROJECTIONS));
  }

  @Test
  void withoutANameKeyOutsideTheEnvironmentNoProjectionIsLookedUp() {
    @SuppressWarnings("unchecked")
    Instance<Projection> projections = mock(Instance.class);
    SmallRyeConfig config =
        configOf(
            Map.of("streamrune.projections.auto-discovery.enabled", "true"),
            Map.of("STREAMRUNE_PROJECTIONS_ORDERS_ENABLED", "false"));

    new ProjectionKeysValidator(
            config.getConfigMapping(StreamRuneQuarkusProperties.class),
            configInstance(config),
            projections)
        .validate(null);

    verifyNoInteractions(projections);
  }

  @Test
  void withoutAProjectionKeyNoProjectionIsLookedUp() {
    @SuppressWarnings("unchecked")
    Instance<Projection> projections = mock(Instance.class);
    SmallRyeConfig config = configOf(Map.of(), Map.of());

    new ProjectionKeysValidator(
            config.getConfigMapping(StreamRuneQuarkusProperties.class),
            configInstance(config),
            projections)
        .validate(null);

    verifyNoInteractions(projections);
  }

  @Test
  void withoutAnInjectedConfigTheConfigOfTheClassLoaderIsRead() {
    StreamRuneQuarkusProperties properties =
        TestProperties.of(Map.of("streamrune.projections.orders.enabeld", "false"));
    @SuppressWarnings("unchecked")
    Instance<Config> none = mock(Instance.class);
    when(none.isResolvable()).thenReturn(false);

    assertThrows(
        IllegalStateException.class,
        () ->
            new ProjectionKeysValidator(properties, none, projections(PROJECTIONS)).validate(null));
  }

  private static void validate(SmallRyeConfig config, List<Projection> beans) {
    new ProjectionKeysValidator(
            config.getConfigMapping(StreamRuneQuarkusProperties.class),
            configInstance(config),
            projections(beans))
        .validate(null);
  }

  private static Instance<Config> configInstance(Config config) {
    @SuppressWarnings("unchecked")
    Instance<Config> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    when(instance.get()).thenReturn(config);
    return instance;
  }

  private static Instance<Projection> projections(List<Projection> beans) {
    @SuppressWarnings("unchecked")
    Instance<Projection> instance = mock(Instance.class);
    when(instance.iterator()).thenAnswer(invocation -> beans.iterator());
    return instance;
  }

  private static SmallRyeConfig configOf(Map<String, String> file, Map<String, String> env) {
    return new SmallRyeConfigBuilder()
        .withMapping(StreamRuneQuarkusProperties.class)
        .withSources(new PropertiesConfigSource(file, "application.properties", 250))
        .withSources(new EnvConfigSource(env, 300))
        .build();
  }
}
