package org.streamrune.runtime;

import java.util.function.Function;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;

/**
 * Binds a command type to its aggregate type, its decider and its aggregate ID extractor. A command
 * of {@code commandType} targets the stream {@code StreamId.of(aggregateType,
 * idExtractor.apply(command))}.
 *
 * @param aggregateType the aggregate type of the streams this registration's commands target — the
 *     first half of every StreamId it produces
 * @param commandType the sealed interface or class representing the command hierarchy
 * @param idExtractor extracts the aggregate ID from a command instance
 * @param decider the decider that handles commands of this type
 * @param <C> command type (must implement Command)
 * @param <S> state type (must implement AggregateState)
 * @param <E> event type (must implement DomainEvent)
 */
public record DeciderRegistration<
    C extends Command, S extends AggregateState, E extends DomainEvent>(
    AggregateType aggregateType,
    Class<C> commandType,
    Function<C, AggregateId> idExtractor,
    Decider<C, S, E> decider) {

  public DeciderRegistration {
    if (aggregateType == null) {
      throw new IllegalArgumentException("aggregateType is required");
    }
    if (commandType == null) {
      throw new IllegalArgumentException("commandType is required");
    }
    if (idExtractor == null) {
      throw new IllegalArgumentException("idExtractor is required");
    }
    if (decider == null) {
      throw new IllegalArgumentException("decider is required");
    }
  }
}
