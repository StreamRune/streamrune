package org.streamrune.integration;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * The settings with which an application switches its OpenTelemetry tracing off, which an
 * integration reads before it builds its {@code StreamRuneTracing} bean: with one of them set, the
 * bean is {@code StreamRuneTracing.NOOP}, as without OpenTelemetry. An SDK that is present but
 * switched off still answers with a tracer provider of its own, so StreamRune would otherwise start
 * spans that no backend receives and write their span context into the metadata of every event and
 * into every outbox row.
 *
 * <p>The switches are those of the OpenTelemetry SDK itself ({@link #SDK}: {@code
 * otel.sdk.disabled=true}, as a system property, an environment variable or a configuration key)
 * and those of each framework's OpenTelemetry support ({@link #SPRING}, {@link #QUARKUS}, {@link
 * #MICRONAUT}). A value matches ignoring case and surrounding whitespace.
 *
 * <p>A setting that only removes the exporter ({@code otel.traces.exporter=none}, {@code
 * management.tracing.export.enabled=false}) is not among them: the SDK still runs, and a span
 * processor of the application, or of a test, still receives the spans. {@code
 * streamrune.tracing.enabled=false} switches StreamRune's spans off whatever the OpenTelemetry
 * settings are.
 */
public final class OpenTelemetrySwitches {

  /**
   * One setting and the value that switches tracing off.
   *
   * @param key the configuration key
   * @param offValue the value that switches tracing off
   */
  public record Switch(String key, String offValue) {

    /**
     * Checks both parts.
     *
     * @throws NullPointerException if a part is {@code null}
     */
    public Switch {
      Objects.requireNonNull(key, "key is required");
      Objects.requireNonNull(offValue, "offValue is required");
    }

    @Override
    public String toString() {
      return key + "=" + offValue;
    }
  }

  /** The OpenTelemetry SDK's own switches, which every integration reads. */
  public static final List<Switch> SDK = List.of(new Switch("otel.sdk.disabled", "true"));

  /**
   * The SDK's switches and Spring Boot's: {@code management.opentelemetry.enabled=false} replaces
   * the SDK Spring Boot builds with one that has no span processor, and Spring Boot maps the
   * environment variable {@code OTEL_SDK_DISABLED} onto it. {@code otel.sdk.disabled} stays among
   * them for an SDK that is not Spring Boot's: the OpenTelemetry Java agent's, which reads it as a
   * system property or an environment variable. {@code management.tracing.export.enabled=false}
   * only removes the exporters and is not a switch.
   */
  public static final List<Switch> SPRING =
      with(new Switch("management.opentelemetry.enabled", "false"));

  /** The SDK's switches and those of the Quarkus OpenTelemetry extension. */
  public static final List<Switch> QUARKUS =
      with(
          new Switch("quarkus.otel.enabled", "false"),
          new Switch("quarkus.otel.sdk.disabled", "true"),
          new Switch("quarkus.otel.traces.enabled", "false"));

  /** The SDK's switches, which Micronaut's OpenTelemetry tracing module reads as they are. */
  public static final List<Switch> MICRONAUT = SDK;

  private OpenTelemetrySwitches() {}

  private static List<Switch> with(Switch... framework) {
    return Stream.concat(SDK.stream(), Stream.of(framework)).toList();
  }

  /**
   * The first of {@code switches} that {@code property} holds at its off value.
   *
   * @param switches the switches to read, {@link #SPRING}, {@link #QUARKUS} or {@link #MICRONAUT}
   * @param property the configured value of a key, or {@code null} when the key is not set
   * @return the switch that switches tracing off, or empty
   * @throws NullPointerException if an argument is {@code null}
   */
  public static Optional<Switch> switchedOffBy(
      List<Switch> switches, Function<String, String> property) {
    Objects.requireNonNull(switches, "switches is required");
    Objects.requireNonNull(property, "property is required");
    for (Switch candidate : switches) {
      String value = property.apply(candidate.key());
      if (value != null && value.strip().toLowerCase(Locale.ROOT).equals(candidate.offValue())) {
        return Optional.of(candidate);
      }
    }
    return Optional.empty();
  }
}
