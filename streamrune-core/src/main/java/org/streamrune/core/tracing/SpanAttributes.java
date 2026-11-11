package org.streamrune.core.tracing;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * The attributes a span starts with: an immutable, ordered set of keys, each with a {@link String},
 * {@code long} or {@code boolean} value. A StreamRune type, so the seam carries no OpenTelemetry
 * type.
 *
 * <p>A {@code null} string value is not an attribute: {@link Builder#put(String, String)} skips it,
 * so a value the caller does not know leaves the key absent rather than empty. A key set twice
 * keeps the last value, in the position of its first.
 *
 * <p>A caller builds the attributes only after {@link StreamRuneTracing#enabled(TracingArea)}
 * answered {@code true}; {@link #empty()} is a shared instance.
 */
public final class SpanAttributes {

  private static final SpanAttributes EMPTY = new SpanAttributes(new String[0], new Object[0]);

  private final String[] keys;
  private final Object[] values;

  private SpanAttributes(String[] keys, Object[] values) {
    this.keys = keys;
    this.values = values;
  }

  /**
   * The attributes without any key, a shared instance.
   *
   * @return the empty attributes
   */
  public static SpanAttributes empty() {
    return EMPTY;
  }

  /**
   * Starts an empty builder.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * The number of attributes.
   *
   * @return the number of keys
   */
  public int size() {
    return keys.length;
  }

  /**
   * Whether there is no attribute.
   *
   * @return {@code true} when there is no key
   */
  public boolean isEmpty() {
    return keys.length == 0;
  }

  /**
   * Hands every attribute to {@code action} in the order the keys were first put. A value is a
   * {@link String}, a {@link Long} or a {@link Boolean}.
   *
   * @param action receives each key and its value
   */
  public void forEach(BiConsumer<String, Object> action) {
    for (int i = 0; i < keys.length; i++) {
      action.accept(keys[i], values[i]);
    }
  }

  /**
   * The attributes as an unmodifiable map that keeps the order of the keys.
   *
   * @return the attributes
   */
  public Map<String, Object> asMap() {
    Map<String, Object> map = new LinkedHashMap<>();
    forEach(map::put);
    return Collections.unmodifiableMap(map);
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof SpanAttributes that
        && Arrays.equals(keys, that.keys)
        && Arrays.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(keys) + Arrays.hashCode(values);
  }

  @Override
  public String toString() {
    return "SpanAttributes" + asMap();
  }

  /** Collects attributes in order; not thread-safe. */
  public static final class Builder {

    private final LinkedHashMap<String, Object> entries = new LinkedHashMap<>();

    private Builder() {}

    /**
     * Puts a string attribute; a {@code null} value puts nothing.
     *
     * @param key the attribute key, not blank
     * @param value the value, or {@code null}
     * @return this builder
     * @throws IllegalArgumentException if {@code key} is {@code null} or blank
     */
    public Builder put(String key, String value) {
      requireKey(key);
      if (value != null) {
        entries.put(key, value);
      }
      return this;
    }

    /**
     * Puts a {@code long} attribute.
     *
     * @param key the attribute key, not blank
     * @param value the value
     * @return this builder
     * @throws IllegalArgumentException if {@code key} is {@code null} or blank
     */
    public Builder put(String key, long value) {
      requireKey(key);
      entries.put(key, value);
      return this;
    }

    /**
     * Puts a {@code boolean} attribute.
     *
     * @param key the attribute key, not blank
     * @param value the value
     * @return this builder
     * @throws IllegalArgumentException if {@code key} is {@code null} or blank
     */
    public Builder put(String key, boolean value) {
      requireKey(key);
      entries.put(key, value);
      return this;
    }

    /**
     * Builds the attributes; the builder can be used again afterwards.
     *
     * @return the attributes put so far
     */
    public SpanAttributes build() {
      if (entries.isEmpty()) {
        return EMPTY;
      }
      String[] keys = entries.keySet().toArray(new String[0]);
      Object[] values = entries.values().toArray();
      return new SpanAttributes(keys, values);
    }

    private static void requireKey(String key) {
      if (Objects.requireNonNullElse(key, "").isBlank()) {
        throw new IllegalArgumentException("an attribute key is required");
      }
    }
  }
}
