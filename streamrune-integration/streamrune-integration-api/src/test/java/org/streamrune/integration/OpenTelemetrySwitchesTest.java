package org.streamrune.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.integration.OpenTelemetrySwitches.Switch;

class OpenTelemetrySwitchesTest {

  @Test
  void aSwitchAtItsOffValue_switchesTracingOff_ignoringCaseAndSurroundingWhitespace() {
    assertThat(
            OpenTelemetrySwitches.switchedOffBy(
                OpenTelemetrySwitches.SPRING,
                Map.of("management.opentelemetry.enabled", " False ")::get))
        .contains(new Switch("management.opentelemetry.enabled", "false"));
    assertThat(
            OpenTelemetrySwitches.switchedOffBy(
                OpenTelemetrySwitches.QUARKUS, Map.of("quarkus.otel.sdk.disabled", "TRUE")::get))
        .map(Switch::toString)
        .contains("quarkus.otel.sdk.disabled=true");
    assertThat(
            OpenTelemetrySwitches.switchedOffBy(
                OpenTelemetrySwitches.MICRONAUT, Map.of("otel.sdk.disabled", "true")::get))
        .isPresent();
  }

  @Test
  void aSettingThatOnlyRemovesTheExporter_leavesTracingOn() {
    Map<String, String> exportOff =
        Map.of(
            "otel.traces.exporter", "none",
            "management.tracing.export.enabled", "false",
            "quarkus.otel.traces.exporter", "none");
    assertThat(OpenTelemetrySwitches.switchedOffBy(OpenTelemetrySwitches.SPRING, exportOff::get))
        .isEmpty();
    assertThat(OpenTelemetrySwitches.switchedOffBy(OpenTelemetrySwitches.QUARKUS, exportOff::get))
        .isEmpty();
    assertThat(OpenTelemetrySwitches.switchedOffBy(OpenTelemetrySwitches.MICRONAUT, exportOff::get))
        .isEmpty();
  }

  @Test
  void anUnsetKey_anotherValue_orAnotherFrameworksKey_leavesTracingOn() {
    assertThat(OpenTelemetrySwitches.switchedOffBy(OpenTelemetrySwitches.SPRING, key -> null))
        .isEmpty();
    assertThat(
            OpenTelemetrySwitches.switchedOffBy(
                OpenTelemetrySwitches.SPRING,
                Map.of(
                        "otel.sdk.disabled",
                        "false",
                        "management.opentelemetry.enabled",
                        "true",
                        // Spring Boot 3's spelling of the export switch, which Spring Boot 4
                        // ignores.
                        "management.tracing.enabled",
                        "false")
                    ::get))
        .isEmpty();
    assertThat(
            OpenTelemetrySwitches.switchedOffBy(
                OpenTelemetrySwitches.MICRONAUT, Map.of("quarkus.otel.sdk.disabled", "true")::get))
        .isEmpty();
  }

  @Test
  void everyFrameworkReadsTheSdksOwnSwitches() {
    assertThat(OpenTelemetrySwitches.SPRING).containsAll(OpenTelemetrySwitches.SDK);
    assertThat(OpenTelemetrySwitches.QUARKUS).containsAll(OpenTelemetrySwitches.SDK);
    assertThat(OpenTelemetrySwitches.MICRONAUT).containsAll(OpenTelemetrySwitches.SDK);
  }

  @Test
  void nothingIsNull() {
    assertThatThrownBy(() -> new Switch(null, "true")).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new Switch("k", null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> OpenTelemetrySwitches.switchedOffBy(null, key -> null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> OpenTelemetrySwitches.switchedOffBy(OpenTelemetrySwitches.SDK, null))
        .isInstanceOf(NullPointerException.class);
  }
}
