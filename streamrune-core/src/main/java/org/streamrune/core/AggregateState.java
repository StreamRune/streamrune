package org.streamrune.core;

/**
 * Marker interface for aggregate state objects.
 *
 * <p>Implement this interface (typically as a {@code record}) to represent the current state of an
 * aggregate. The framework casts loaded snapshot payloads to this type, so concrete state classes
 * must be registered with {@link SimpleEventTypeRegistry} before they can be deserialized.
 *
 * <p>Implementing classes should be immutable. Sealed interfaces with record permits are strongly
 * recommended.
 */
public interface AggregateState {}
