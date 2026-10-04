package org.streamrune.core.upcasting;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.streamrune.core.types.EventType;

/**
 * Chains upcasters to transform events through multiple schema versions in one pass.
 *
 * <p>For example, if an event is stored as V1 and the current version is V3, the chain will apply
 * transformations V1 to V2 then V2 to V3 sequentially. If no upcaster is registered for a given
 * event type, the data is returned unchanged.
 */
public final class UpcasterChain {

  private final Map<EventType, EventUpcaster> upcasters;

  /**
   * Creates a chain from the given list of upcasters.
   *
   * @param upcasters the upcasters to register; each event type must have at most one upcaster
   * @throws IllegalStateException if duplicate event types are detected
   */
  public UpcasterChain(List<EventUpcaster> upcasters) {
    if (upcasters == null || upcasters.isEmpty()) {
      this.upcasters = Map.of();
    } else {
      this.upcasters =
          upcasters.stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      EventUpcaster::eventType,
                      u -> u,
                      (a, b) -> {
                        throw new IllegalStateException(
                            "Duplicate upcaster for event type: " + a.eventType());
                      }));
    }
  }

  /**
   * Returns the current schema version for the given event type.
   *
   * @param eventType the event type
   * @return the current version from the registered upcaster, or 1 if no upcaster is registered
   */
  public int currentVersion(EventType eventType) {
    var upcaster = upcasters.get(eventType);
    return upcaster != null ? upcaster.currentVersion() : 1;
  }

  /**
   * Upcasts event data from {@code fromVersion} to the current version.
   *
   * <p>When a transformation runs, the registered upcaster receives a mutable copy of {@code data}
   * (per the {@link EventUpcaster#upcast} contract), so callers may pass immutable maps and the
   * caller's map is never modified.
   *
   * @param eventType the event type
   * @param data the event payload
   * @param fromVersion the version of the stored event (schema versions start at 1)
   * @return the transformed data, or the original data if no upcaster is registered or the data is
   *     already at the current version
   * @throws IllegalArgumentException if {@code data} is null or {@code fromVersion} is below 1
   */
  public Map<String, Object> upcast(
      EventType eventType, Map<String, Object> data, int fromVersion) {
    if (data == null) {
      throw new IllegalArgumentException("data must not be null");
    }
    if (fromVersion < 1) {
      throw new IllegalArgumentException("fromVersion must be >= 1, was: " + fromVersion);
    }
    var upcaster = upcasters.get(eventType);
    if (upcaster == null || fromVersion >= upcaster.currentVersion()) {
      return data;
    }
    Map<String, Object> result = new HashMap<>(data);
    for (int v = fromVersion; v < upcaster.currentVersion(); v++) {
      result = upcaster.upcast(result, v);
    }
    return result;
  }
}
