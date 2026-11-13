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
 * #MICRONAUT}). Each value is read as the component that owns the key reads it ({@link Reading}): a
 * spelling that turns the SDK off there turns StreamRune's tracing off here.
 *
 * <p>A setting that only removes the exporter ({@code otel.traces.exporter=none}, {@code
 * management.tracing.export.enabled=false}) is not among them: the SDK still runs, and a span
 * processor of the application, or of a test, still receives the spans. {@code
 * streamrune.tracing.enabled=false} switches StreamRune's spans off whatever the OpenTelemetry
 * settings are.
 */
public final class OpenTelemetrySwitches {

  /** How the component that owns a key reads its value; surrounding whitespace is ignored. */
  public enum Reading {
    /**
     * The OpenTelemetry SDK's {@code otel.sdk.disabled}: off when the value is {@code true},
     * ignoring case.
     */
    TRUE_DISABLES,

    /**
     * A Spring Boot property condition that matches {@code true} and also when the key is not set:
     * off when the key is set to anything but {@code true}, ignoring case ({@code false}, {@code
     * off}, {@code no}, {@code 0}).
     */
    ANYTHING_BUT_TRUE_DISABLES,

    /**
     * A SmallRye Config {@code boolean} that disables: off when the value is one of SmallRye's
     * {@code true} spellings, ignoring case ({@code true}, {@code 1}, {@code yes}, {@code y},
     * {@code on}, {@code ja}, {@code j}, {@code si}, {@code sim}, {@code oui}).
     */
    SMALLRYE_TRUE_DISABLES,

    /**
     * A SmallRye Config {@code boolean} that enables: off when the key is set to anything SmallRye
     * does not read as {@code true}, since SmallRye reads every other value as {@code false}.
     */
    SMALLRYE_ANYTHING_BUT_TRUE_DISABLES;

    /** The values SmallRye Config's {@code boolean} converter reads as {@code true}. */
    private static final java.util.Set<String> SMALLRYE_TRUE =
        java.util.Set.of("true", "1", "yes", "y", "on", "ja", "j", "si", "sim", "oui");

    /** Whether {@code value}, a set value, turns the SDK off. */
    boolean disables(String value) {
      String v = value.strip().toLowerCase(Locale.ROOT);
      return switch (this) {
        case TRUE_DISABLES -> v.equals("true");
        case ANYTHING_BUT_TRUE_DISABLES -> !v.equals("true");
        case SMALLRYE_TRUE_DISABLES -> SMALLRYE_TRUE.contains(v);
        case SMALLRYE_ANYTHING_BUT_TRUE_DISABLES -> !SMALLRYE_TRUE.contains(v);
      };
    }
  }

  /**
   * One setting and how its value is read.
   *
   * @param key the configuration key
   * @param reading how the component that owns the key reads its value
   */
  public record Switch(String key, Reading reading) {

    /**
     * Checks both parts.
     *
     * @throws NullPointerException if a part is {@code null}
     */
    public Switch {
      Objects.requireNonNull(key, "key is required");
      Objects.requireNonNull(reading, "reading is required");
    }

    @Override
    public String toString() {
      return key;
    }
  }

  /** The OpenTelemetry SDK's own switch, which every integration reads. */
  public static final List<Switch> SDK =
      List.of(new Switch("otel.sdk.disabled", Reading.TRUE_DISABLES));

  /**
   * The SDK's switch and Spring Boot's: {@code management.opentelemetry.enabled=false} replaces the
   * SDK Spring Boot builds with one that has no span processor, and Spring Boot maps the
   * environment variable {@code OTEL_SDK_DISABLED} onto it. {@code otel.sdk.disabled} stays among
   * them for an SDK that is not Spring Boot's: the OpenTelemetry Java agent's, which reads it as a
   * system property or an environment variable. {@code management.tracing.export.enabled=false}
   * only removes the exporters and is not a switch.
   */
  public static final List<Switch> SPRING =
      with(new Switch("management.opentelemetry.enabled", Reading.ANYTHING_BUT_TRUE_DISABLES));

  /** The SDK's switch and those of the Quarkus OpenTelemetry extension. */
  public static final List<Switch> QUARKUS =
      with(
          new Switch("quarkus.otel.enabled", Reading.SMALLRYE_ANYTHING_BUT_TRUE_DISABLES),
          new Switch("quarkus.otel.sdk.disabled", Reading.SMALLRYE_TRUE_DISABLES),
          new Switch("quarkus.otel.traces.enabled", Reading.SMALLRYE_ANYTHING_BUT_TRUE_DISABLES));

  /** The SDK's switch, which Micronaut's OpenTelemetry tracing module hands to the SDK. */
  public static final List<Switch> MICRONAUT = SDK;

  private OpenTelemetrySwitches() {}

  private static List<Switch> with(Switch... framework) {
    return Stream.concat(SDK.stream(), Stream.of(framework)).toList();
  }

  /**
   * The first of {@code switches} whose key {@code property} holds at a value that turns the SDK
   * off, as the key's owner reads it.
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
      if (value != null && candidate.reading().disables(value)) {
        return Optional.of(candidate);
      }
    }
    return Optional.empty();
  }
}
