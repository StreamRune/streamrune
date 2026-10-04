package org.streamrune.core.upcasting;

import java.util.Map;
import org.streamrune.core.types.EventType;

/**
 * Transforms events from older schema versions to the current version.
 *
 * <p>Exactly one upcaster owns ALL version steps for its event type: {@link UpcasterChain} rejects
 * a second upcaster for the same type, and dispatches each single-version step (e.g., V1 to V2,
 * then V2 to V3) to the same upcaster — typically a {@code switch} on {@code fromVersion}.
 * Migration steps for one event type cannot be split across multiple upcaster classes.
 *
 * <p>Event types without a registered upcaster are treated as schema version 1 (see {@link
 * UpcasterChain#currentVersion}); their data passes through unchanged.
 */
public interface EventUpcaster {

  /** The event type this upcaster handles. */
  EventType eventType();

  /** The current (latest) schema version this upcaster produces. */
  int currentVersion();

  /**
   * Transforms event data from {@code fromVersion} to {@code fromVersion + 1}.
   *
   * <p>{@link UpcasterChain} always passes a mutable map: implementations may mutate {@code
   * eventData} in place and return it, or return a new map.
   *
   * <p>A step can receive data that already has the shape it produces. The stored schema version is
   * the {@link #currentVersion()} in effect for the writing store when the event was appended
   * ({@code 1} for a type with no upcaster), not a property of the payload: an event appended
   * before this upcaster was registered is stamped {@code 1} even if its class already had the new
   * field. A step must therefore keep a value the data already carries — for a new field with a
   * default, {@code putIfAbsent} rather than {@code put}, which would replace the real value on
   * every read.
   *
   * @param eventData the event payload as a mutable map
   * @param fromVersion the version of the incoming data
   * @return transformed event data for the next version
   */
  Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion);
}
