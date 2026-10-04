package org.streamrune.core;

/**
 * Marker interface for domain events.
 *
 * <p>All type parameters {@code E} on {@link Decider} must implement this interface.
 * Implementations are typically a sealed interface with one {@code record} per event variant.
 * Events are immutable data objects and must be serializable by the configured Jackson {@code
 * ObjectMapper}.
 */
public interface DomainEvent {}
