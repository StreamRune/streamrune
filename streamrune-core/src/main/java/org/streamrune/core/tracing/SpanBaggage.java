package org.streamrune.core.tracing;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * What a new span does with the current baggage while its scope is open: {@link #KEEP} it, or
 * {@link #replace(Map) replace} it with other entries. An empty replacement clears it.
 *
 * <p>The tracing seam takes no {@code null}: {@link StreamRuneTracing#start(TracingArea, String,
 * SpanKind, SpanAttributes, SpanLink, SpanBaggage) start} rejects a {@code null} baggage, and a
 * component that leaves the baggage alone passes {@code KEEP}, as the overloads without a baggage
 * parameter do.
 */
public final class SpanBaggage {

  /** Keeps the current baggage. */
  public static final SpanBaggage KEEP = new SpanBaggage(null);

  /** The replacement entries; {@code null} for {@link #KEEP} alone. */
  private final Map<String, String> entries;

  private SpanBaggage(Map<String, String> entries) {
    this.entries = entries;
  }

  /**
   * Replaces the current baggage with {@code entries} while the scope is open; an empty map clears
   * it. The map is copied.
   *
   * @param entries the baggage entries
   * @return the replacement
   * @throws NullPointerException if {@code entries}, one of its keys or one of its values is {@code
   *     null}
   */
  public static SpanBaggage replace(Map<String, String> entries) {
    Objects.requireNonNull(entries, "entries is required");
    Map<String, String> copy = new LinkedHashMap<>(entries.size());
    entries.forEach(
        (key, value) ->
            copy.put(
                Objects.requireNonNull(key, "a baggage key is required"),
                Objects.requireNonNull(value, "a baggage value is required")));
    return new SpanBaggage(Collections.unmodifiableMap(copy));
  }

  /**
   * Whether the span keeps the current baggage: {@code true} for {@link #KEEP} alone.
   *
   * @return {@code true} for {@link #KEEP}
   */
  public boolean keepsCurrent() {
    return entries == null;
  }

  /**
   * The entries that replace the current baggage, in the order the map given to {@link
   * #replace(Map)} iterated them.
   *
   * @return the replacement entries, unmodifiable
   * @throws IllegalStateException for {@link #KEEP}, which replaces nothing
   */
  public Map<String, String> entries() {
    if (entries == null) {
      throw new IllegalStateException("SpanBaggage.KEEP replaces no entries");
    }
    return entries;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof SpanBaggage that && Objects.equals(entries, that.entries);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(entries);
  }

  @Override
  public String toString() {
    return entries == null ? "SpanBaggage.KEEP" : "SpanBaggage.replace(" + entries.keySet() + ")";
  }
}
