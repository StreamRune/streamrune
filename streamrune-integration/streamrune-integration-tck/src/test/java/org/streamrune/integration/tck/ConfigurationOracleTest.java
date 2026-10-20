package org.streamrune.integration.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ConfigurationOracleTest {

  /** An explicit key, the way a framework annotation names one. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.RECORD_COMPONENT, ElementType.METHOD})
  @interface Key {
    String value();
  }

  record Retention7(Duration retentionMaxAge) {}

  record Properties(
      int snapshotEveryNEvents,
      boolean eventAuditEnabled,
      List<String> baggageAllowlist,
      Retention7 inbox,
      @Key("streamrune.renamed-explicitly") String forwardFence) {}

  interface Mapping {
    int stripeCount();

    Optional<String> token();

    Group saga();

    Optional<Group> missing();

    default String notAKey() {
      return "x";
    }

    interface Group {
      Duration compensationRetryInterval();
    }
  }

  @Test
  void aRecordTreeBecomesKebabKeysWithRenderedDefaults() {
    var properties =
        new Properties(100, false, List.of(), new Retention7(Duration.ofDays(7)), "auto");

    Map<String, String> keys = ConfigurationOracle.ofRecord(properties, "streamrune");

    assertEquals(
        Map.of(
            "streamrune.snapshot-every-n-events", "100",
            "streamrune.event-audit-enabled", "false",
            "streamrune.baggage-allowlist", "—",
            "streamrune.inbox.retention-max-age", "7d",
            "streamrune.forward-fence", "auto"),
        keys);
  }

  @Test
  void theNamingDecidesTheKeyOfEachElement() {
    var properties =
        new Properties(100, false, List.of(), new Retention7(Duration.ofDays(7)), "auto");
    ConfigurationOracle.KeyNaming explicitFirst =
        (parent, element, name) ->
            element.isAnnotationPresent(Key.class)
                ? element.getAnnotation(Key.class).value()
                : ConfigurationOracle.KeyNaming.KEBAB.key(parent, element, name);

    Map<String, String> keys =
        ConfigurationOracle.ofRecord(properties, "streamrune", explicitFirst);

    assertEquals("auto", keys.get("streamrune.renamed-explicitly"));
    assertFalse(keys.containsKey("streamrune.forward-fence"));
  }

  @Test
  void aMissingGroupStillListsItsKeysWithoutDefaults() {
    var properties = new Properties(1, true, null, null, null);

    Map<String, String> keys = ConfigurationOracle.ofRecord(properties, "streamrune");

    assertEquals("—", keys.get("streamrune.inbox.retention-max-age"));
    assertEquals("—", keys.get("streamrune.baggage-allowlist"));
  }

  @Test
  void anInterfaceMappingIsWalkedThroughItsAbstractMethods() {
    Mapping.Group group =
        (Mapping.Group)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {Mapping.Group.class},
                (proxy, method, args) -> Duration.ofSeconds(60));
    Mapping mapping =
        (Mapping)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {Mapping.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "stripeCount" -> 1024;
                      case "token" -> Optional.empty();
                      case "saga" -> group;
                      case "missing" -> Optional.empty();
                      default -> throw new AssertionError(method.getName());
                    });

    Map<String, String> keys =
        ConfigurationOracle.ofConfigMapping(
            mapping, Mapping.class, "streamrune", ConfigurationOracle.KeyNaming.KEBAB);

    assertEquals(
        Map.of(
            "streamrune.stripe-count", "1024",
            "streamrune.token", "—",
            "streamrune.saga.compensation-retry-interval", "1m",
            "streamrune.missing.compensation-retry-interval", "—"),
        keys);
  }

  @Test
  void aFailingAccessorAndAWrongArgumentAreReported() {
    Mapping throwing =
        (Mapping)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {Mapping.class},
                (proxy, method, args) -> {
                  throw new IllegalStateException("broken " + method.getName());
                });
    var failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                ConfigurationOracle.ofConfigMapping(
                    throwing, Mapping.class, "streamrune", ConfigurationOracle.KeyNaming.KEBAB));
    assertTrue(failure.getCause().getMessage().startsWith("broken "), failure.toString());

    assertThrows(
        IllegalArgumentException.class, () -> ConfigurationOracle.ofRecord("text", "streamrune"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ConfigurationOracle.ofConfigMapping(
                "text", Mapping.class, "streamrune", ConfigurationOracle.KeyNaming.KEBAB));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ConfigurationOracle.ofConfigMapping(
                new Retention7(null),
                Retention7.class,
                "streamrune",
                ConfigurationOracle.KeyNaming.KEBAB));
  }

  @Test
  void twoPropertiesBindingOneKeyAreRefused() {
    var properties =
        new Properties(100, false, List.of(), new Retention7(Duration.ofDays(7)), "auto");
    ConfigurationOracle.KeyNaming collapsing = (parent, element, name) -> "streamrune.same";

    assertThrows(
        IllegalStateException.class,
        () -> ConfigurationOracle.ofRecord(properties, "streamrune", collapsing));
  }

  @Test
  void kebabCaseIsSpringsDashedForm() {
    assertEquals("snapshot-every-n-events", ConfigurationOracle.kebab("snapshotEveryNEvents"));
    assertEquals(
        "max-in-flight-async-commands", ConfigurationOracle.kebab("maxInFlightAsyncCommands"));
    assertEquals("data-key-cache", ConfigurationOracle.kebab("dataKeyCache"));
    assertEquals("enabled", ConfigurationOracle.kebab("enabled"));
  }
}
