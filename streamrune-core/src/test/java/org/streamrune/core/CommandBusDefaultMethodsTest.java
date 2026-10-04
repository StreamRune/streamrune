package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * Verifies that the {@link CommandBus#execute(Command, IdempotencyKey)} default method throws
 * {@link UnsupportedOperationException} for implementations that do not support idempotency keys.
 */
class CommandBusDefaultMethodsTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  record TestCmd() implements Command {}

  /** Minimal implementation that overrides only the abstract method. */
  private static final class MinimalCommandBus implements CommandBus {

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("s1")), Version.initial(), List.of());
    }
  }

  @Test
  void executeWithKeyDefault_throwsUnsupportedOperation() {
    var bus = new MinimalCommandBus();
    var ex =
        assertThrows(
            UnsupportedOperationException.class,
            () -> bus.execute(new TestCmd(), IdempotencyKey.of("k")));
    assertTrue(
        ex.getMessage().contains("idempotency"),
        "message should mention idempotency, got: " + ex.getMessage());
  }
}
