package org.streamrune.core.saga;

import java.util.Objects;
import org.streamrune.core.Command;
import org.streamrune.core.types.AggregateId;

/**
 * A command to be dispatched as part of a saga step, paired with its target aggregate ID.
 *
 * <p>The target's aggregate type is not part of a saga command: the command bus resolves it from
 * the registration of the command's class. {@code aggregateId} is informational (routing logs, saga
 * tests).
 *
 * @param command the command to dispatch via {@code CommandBus.execute()}
 * @param aggregateId the aggregate ID that the command targets (used for routing/logging)
 */
public record SagaCommand(Command command, AggregateId aggregateId) {

  public SagaCommand {
    Objects.requireNonNull(command, "command must not be null");
    Objects.requireNonNull(aggregateId, "aggregateId must not be null");
  }

  public static SagaCommand of(Command command, AggregateId aggregateId) {
    return new SagaCommand(command, aggregateId);
  }
}
