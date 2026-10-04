package org.streamrune.core;

import java.util.Collection;
import java.util.List;

/**
 * Thrown when a persisted event-type or state-type name has no class registered in the {@link
 * EventTypeRegistry}.
 *
 * <p>This is an operational/data condition, not a programmer error: it typically means a deployment
 * forgot to register a newly introduced event class, or an event class was renamed while historical
 * events still carry the old name. It surfaces on the read path (aggregate loading, projections,
 * subscriptions) when a stored event or snapshot is first deserialized. Extends {@link
 * EventStoreException} so read-path error handling can catch it together with other store failures
 * — or distinctly, to report the missing registration.
 */
public class UnknownEventTypeException extends EventStoreException {

  private final String typeName;
  private final List<String> registeredTypes;

  /**
   * Creates an exception for a failed type-name lookup.
   *
   * @param kind human-readable kind of lookup that failed, e.g. {@code "event type"} or {@code
   *     "state type"}
   * @param typeName the persisted type name that could not be resolved
   * @param registeredTypes the type names registered for that kind; copied and sorted
   */
  public UnknownEventTypeException(
      String kind, String typeName, Collection<String> registeredTypes) {
    super(message(kind, typeName, registeredTypes));
    this.typeName = typeName;
    this.registeredTypes = registeredTypes.stream().sorted().toList();
  }

  private static String message(String kind, String typeName, Collection<String> registeredTypes) {
    return "Unknown %s: '%s' is not registered. Registered %ss: %s"
        .formatted(kind, typeName, kind, registeredTypes.stream().sorted().toList());
  }

  /** The persisted type name that could not be resolved. */
  public String typeName() {
    return typeName;
  }

  /** The type names registered for the failed lookup kind, sorted; immutable. */
  public List<String> registeredTypes() {
    return registeredTypes;
  }
}
