package org.streamrune.core;

/**
 * Marker interface for command types.
 *
 * <p>All type parameters {@code C} on {@link Decider} must implement this interface.
 * Implementations are typically a sealed interface with one {@code record} per command variant.
 *
 * <p>Unlike {@link DomainEvent} and {@link AggregateState}, commands are transient input: the
 * framework never writes one to the event store, the command inbox or the audit log. The marker
 * exists for type-safety and API consistency, not persistence.
 *
 * <p><b>The one exception is the dead letter queue.</b> When a {@link DeadLetterQueue} is
 * configured and a command fails with an infrastructure error after its retries ran out, the bus
 * serializes the command to JSON with its Jackson {@code ObjectMapper}, and the dead-letter retry
 * runner rebuilds it from that JSON, with a mapper configured alike, on every replay. So a command
 * type that can be dead-lettered must round-trip through Jackson:
 *
 * <ul>
 *   <li>A {@code record} does by default.
 *   <li>A plain class needs a default constructor, or a constructor or factory method annotated
 *       {@code @JsonCreator}, together with readable properties.
 *   <li>A component Jackson does not write (for example one marked {@code @JsonIgnore}) is {@code
 *       null} or zero in the replayed command.
 * </ul>
 *
 * <p>Nothing checks this when a command is registered. A command the mapper cannot write is
 * recorded as a metadata-only entry (a JSON {@code null} payload) that can never be replayed. A
 * command it can write but not rebuild is stored normally, then fails to deserialize on every
 * replay cycle: nothing is executed and each cycle spends one of the entry's retries.
 */
public interface Command {}
