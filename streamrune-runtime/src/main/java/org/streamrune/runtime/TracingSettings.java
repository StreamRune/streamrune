package org.streamrune.runtime;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import org.streamrune.core.tracing.TracingArea;

/**
 * The switches of {@link OpenTelemetryTracing}: a master switch and one switch per {@link
 * TracingArea}. An area is traced when the master switch and its own switch are both on. Immutable.
 *
 * <pre>{@code
 * TracingSettings settings =
 *     TracingSettings.builder().area(TracingArea.PROJECTION, false).build();
 * StreamRuneTracing tracing = OpenTelemetryTracing.of(openTelemetry, settings);
 * }</pre>
 *
 * <p>This class does not reference OpenTelemetry.
 */
public final class TracingSettings {

  private static final TracingSettings ALL_ENABLED = builder().build();

  private static final TracingSettings DISABLED = builder().enabled(false).build();

  private final boolean enabled;
  private final Set<TracingArea> areas;

  private TracingSettings(boolean enabled, Set<TracingArea> areas) {
    this.enabled = enabled;
    this.areas = areas;
  }

  /**
   * Every area traced: the master switch and every area switch on.
   *
   * @return the settings
   */
  public static TracingSettings allEnabled() {
    return ALL_ENABLED;
  }

  /**
   * Nothing traced: the master switch off.
   *
   * @return the settings
   */
  public static TracingSettings disabled() {
    return DISABLED;
  }

  /**
   * Starts a builder with the master switch and every area switch on.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * The master switch.
   *
   * @return {@code false} when no area is traced, whatever the area switches say
   */
  public boolean enabled() {
    return enabled;
  }

  /**
   * Whether {@code area} is traced: the master switch and the area's switch are both on.
   *
   * @param area the area
   * @return {@code true} when spans of the area are recorded
   */
  public boolean enabled(TracingArea area) {
    return enabled && areas.contains(Objects.requireNonNull(area, "area is required"));
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof TracingSettings that
        && enabled == that.enabled
        && areas.equals(that.areas);
  }

  @Override
  public int hashCode() {
    return Objects.hash(enabled, areas);
  }

  @Override
  public String toString() {
    return "TracingSettings{enabled=" + enabled + ", areas=" + areas + "}";
  }

  /** Builder for {@link TracingSettings}; every switch starts on. Not thread-safe. */
  public static final class Builder {

    private boolean enabled = true;
    private final EnumSet<TracingArea> areas = EnumSet.allOf(TracingArea.class);

    private Builder() {}

    /**
     * Sets the master switch (default {@code true}). Off, no area is traced.
     *
     * @param enabled the master switch
     * @return this builder
     */
    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    /**
     * Sets the switch of one area (default {@code true}).
     *
     * @param area the area
     * @param enabled whether its spans are recorded
     * @return this builder
     * @throws NullPointerException if {@code area} is {@code null}
     */
    public Builder area(TracingArea area, boolean enabled) {
      Objects.requireNonNull(area, "area is required");
      if (enabled) {
        areas.add(area);
      } else {
        areas.remove(area);
      }
      return this;
    }

    /**
     * Builds the settings.
     *
     * @return the settings
     */
    public TracingSettings build() {
      return new TracingSettings(enabled, Set.copyOf(areas));
    }
  }
}
