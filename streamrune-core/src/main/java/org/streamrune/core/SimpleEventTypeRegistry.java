package org.streamrune.core;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import org.streamrune.core.types.EventType;

/**
 * Default builder-based implementation of {@link EventTypeRegistry}. Immutable after construction.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * var registry = SimpleEventTypeRegistry.builder()
 *     .registerEvent("OrderCreated", OrderCreated.class)
 *     .registerState("OrderState", OrderState.class)
 *     .build();
 * }</pre>
 */
public final class SimpleEventTypeRegistry implements EventTypeRegistry {

  private final Map<EventType, Class<?>> eventTypes;
  private final Map<String, Class<?>> stateTypes;

  private SimpleEventTypeRegistry(
      Map<EventType, Class<?>> eventTypes, Map<String, Class<?>> stateTypes) {
    this.eventTypes = Map.copyOf(eventTypes);
    this.stateTypes = Map.copyOf(stateTypes);
  }

  /** Creates a new builder for constructing a {@link SimpleEventTypeRegistry}. */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public Class<?> resolveEventType(EventType eventType) {
    var clazz = eventTypes.get(eventType);
    if (clazz == null) {
      throw new UnknownEventTypeException(
          "event type",
          eventType.name(),
          eventTypes.keySet().stream().map(EventType::name).toList());
    }
    return clazz;
  }

  @Override
  public Class<?> resolveStateType(String stateType) {
    var clazz = stateTypes.get(stateType);
    if (clazz == null) {
      throw new UnknownEventTypeException("state type", stateType, stateTypes.keySet());
    }
    return clazz;
  }

  @Override
  public Collection<Class<?>> registeredTypes() {
    // LinkedHashSet: stable iteration order (registration order) and de-duplicates a class
    // registered as both an event type and a state type.
    var all = new LinkedHashSet<Class<?>>(eventTypes.values());
    all.addAll(stateTypes.values());
    return java.util.Collections.unmodifiableCollection(all);
  }

  /** Builder for {@link SimpleEventTypeRegistry}. */
  public static final class Builder {

    private final Map<EventType, Class<?>> eventTypes = new HashMap<>();
    private final Map<String, Class<?>> stateTypes = new HashMap<>();

    private Builder() {}

    /**
     * Registers an event class under the name produced by {@link EventType#fromClass(Class)} (the
     * simple class name). Use this overload when the write path also derives event-type names via
     * {@code EventType.fromClass}, so the registered name is guaranteed to match what gets
     * persisted. Cross-package simple-name collisions are then rejected here at build time instead
     * of corrupting reads later.
     *
     * @throws IllegalStateException if the derived event type name is already registered
     */
    public Builder registerEvent(Class<?> type) {
      return registerEvent(EventType.fromClass(type), type);
    }

    /**
     * Registers an event type name to its Java class.
     *
     * @throws IllegalStateException if the event type name is already registered
     */
    public Builder registerEvent(String name, Class<?> type) {
      var eventType = new EventType(name);
      if (eventTypes.containsKey(eventType)) {
        throw new IllegalStateException("Duplicate event type: " + name);
      }
      eventTypes.put(eventType, type);
      return this;
    }

    /**
     * Registers an event type to its Java class.
     *
     * @throws IllegalStateException if the event type is already registered
     */
    public Builder registerEvent(EventType eventType, Class<?> type) {
      if (eventTypes.containsKey(eventType)) {
        throw new IllegalStateException("Duplicate event type: " + eventType);
      }
      eventTypes.put(eventType, type);
      return this;
    }

    /**
     * Registers a state type name to its Java class.
     *
     * @throws IllegalStateException if the state type name is already registered
     */
    public Builder registerState(String name, Class<?> type) {
      if (stateTypes.containsKey(name)) {
        throw new IllegalStateException("Duplicate state type: " + name);
      }
      stateTypes.put(name, type);
      return this;
    }

    /** Builds an immutable {@link SimpleEventTypeRegistry}. */
    public SimpleEventTypeRegistry build() {
      return new SimpleEventTypeRegistry(eventTypes, stateTypes);
    }
  }
}
