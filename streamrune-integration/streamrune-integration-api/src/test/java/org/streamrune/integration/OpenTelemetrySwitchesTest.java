package org.streamrune.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.integration.OpenTelemetrySwitches.Reading;
import org.streamrune.integration.OpenTelemetrySwitches.Switch;

class OpenTelemetrySwitchesTest {

  private static Optional<Switch> off(List<Switch> switches, String key, String value) {
    return OpenTelemetrySwitches.switchedOffBy(switches, Map.of(key, value)::get);
  }

  @Test
  void theSdksSwitch_isOffForTrueAlone_asTheSdkReadsIt() {
    for (List<Switch> switches :
        List.of(
            OpenTelemetrySwitches.SPRING,
            OpenTelemetrySwitches.QUARKUS,
            OpenTelemetrySwitches.MICRONAUT)) {
      assertThat(off(switches, "otel.sdk.disabled", " TRUE ")).isPresent();
      for (String on : List.of("false", "yes", "1", "on", "")) {
        assertThat(off(switches, "otel.sdk.disabled", on)).as(on).isEmpty();
      }
    }
  }

  @Test
  void springBootsSwitch_isOffForAnythingButTrue_asItsPropertyConditionReadsIt() {
    for (String value : List.of("false", " False ", "off", "no", "0", "")) {
      assertThat(off(OpenTelemetrySwitches.SPRING, "management.opentelemetry.enabled", value))
          .as(value)
          .map(Switch::toString)
          .contains("management.opentelemetry.enabled");
    }
    assertThat(off(OpenTelemetrySwitches.SPRING, "management.opentelemetry.enabled", "TRUE"))
        .isEmpty();
  }

  @Test
  void quarkusSwitches_areReadAsSmallRyeReadsABoolean() {
    for (String value : List.of("true", "1", "YES", "y", "on", "ja", "j", "si", "sim", "oui")) {
      assertThat(off(OpenTelemetrySwitches.QUARKUS, "quarkus.otel.sdk.disabled", value))
          .as(value)
          .isPresent();
      assertThat(off(OpenTelemetrySwitches.QUARKUS, "quarkus.otel.enabled", value))
          .as(value)
          .isEmpty();
      assertThat(off(OpenTelemetrySwitches.QUARKUS, "quarkus.otel.traces.enabled", value))
          .as(value)
          .isEmpty();
    }
    for (String value : List.of("false", "0", "no", "n", "OFF", "maybe")) {
      assertThat(off(OpenTelemetrySwitches.QUARKUS, "quarkus.otel.sdk.disabled", value))
          .as(value)
          .isEmpty();
      assertThat(off(OpenTelemetrySwitches.QUARKUS, "quarkus.otel.enabled", value))
          .as(value)
          .isPresent();
      assertThat(off(OpenTelemetrySwitches.QUARKUS, "quarkus.otel.traces.enabled", value))
          .as(value)
          .isPresent();
    }
  }

  @Test
  void aSettingThatOnlyRemovesTheExporter_orAnotherFrameworksKey_leavesTracingOn() {
    Map<String, String> notSwitches =
        Map.of(
            "otel.traces.exporter", "none",
            "management.tracing.export.enabled", "false",
            // Spring Boot 3's spelling of the export switch, which Spring Boot 4 does not read.
            "management.tracing.enabled", "false",
            "quarkus.otel.traces.exporter", "none");
    for (List<Switch> switches :
        List.of(
            OpenTelemetrySwitches.SPRING,
            OpenTelemetrySwitches.QUARKUS,
            OpenTelemetrySwitches.MICRONAUT)) {
      assertThat(OpenTelemetrySwitches.switchedOffBy(switches, notSwitches::get)).isEmpty();
      assertThat(OpenTelemetrySwitches.switchedOffBy(switches, key -> null)).isEmpty();
    }
    assertThat(off(OpenTelemetrySwitches.MICRONAUT, "quarkus.otel.sdk.disabled", "true")).isEmpty();
    assertThat(off(OpenTelemetrySwitches.QUARKUS, "management.opentelemetry.enabled", "false"))
        .isEmpty();
  }

  @Test
  void everyFrameworkReadsTheSdksOwnSwitch() {
    assertThat(OpenTelemetrySwitches.SPRING).containsAll(OpenTelemetrySwitches.SDK);
    assertThat(OpenTelemetrySwitches.QUARKUS).containsAll(OpenTelemetrySwitches.SDK);
    assertThat(OpenTelemetrySwitches.MICRONAUT).containsAll(OpenTelemetrySwitches.SDK);
  }

  @Test
  void nothingIsNull() {
    assertThatThrownBy(() -> new Switch(null, Reading.TRUE_DISABLES))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new Switch("k", null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> OpenTelemetrySwitches.switchedOffBy(null, key -> null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> OpenTelemetrySwitches.switchedOffBy(OpenTelemetrySwitches.SDK, null))
        .isInstanceOf(NullPointerException.class);
  }
}
