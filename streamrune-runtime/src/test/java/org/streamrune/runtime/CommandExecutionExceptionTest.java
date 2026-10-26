package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.StreamId;

class CommandExecutionExceptionTest {

  private static final CommandId COMMAND_ID = CommandId.of("cmd-1");
  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("customer"), AggregateId.of("c-1"));

  record SampleCommand(String value) implements Command {}

  /** A command whose payload is personal data, as an {@code @Encrypted} field holds it. */
  record PiiCommand(String email) implements Command {}

  @Test
  void carriesTheCommandItsIdsAndTheCause() {
    var cmd = new SampleCommand("v");
    var cause = new RuntimeException("root");

    var ex = new CommandExecutionException(cmd, COMMAND_ID, STREAM, "boom", cause);

    assertSame(cmd, ex.command());
    assertEquals(COMMAND_ID, ex.commandId());
    assertEquals(STREAM, ex.streamId());
    assertEquals("boom", ex.getMessage());
    assertSame(cause, ex.getCause());
  }

  @Test
  void theCommandAndItsIdsAreRequired() {
    var cmd = new SampleCommand("v");
    assertThrows(
        NullPointerException.class,
        () -> new CommandExecutionException(null, COMMAND_ID, STREAM, "boom", null));
    assertThrows(
        NullPointerException.class,
        () -> new CommandExecutionException(cmd, null, STREAM, "boom", null));
    assertThrows(
        NullPointerException.class,
        () -> new CommandExecutionException(cmd, COMMAND_ID, null, "boom", null));
  }

  @Test
  void toStringNamesTheCommandTypeItsIdsAndTheCause() {
    var ex =
        new CommandExecutionException(
            new SampleCommand("v"), COMMAND_ID, STREAM, "boom", new IllegalStateException("root"));

    assertEquals(
        CommandExecutionException.class.getName()
            + ": boom [command SampleCommand, id cmd-1, stream customer:c-1, cause "
            + IllegalStateException.class.getName()
            + "]",
        ex.toString());
  }

  @Test
  void toStringNeverRendersTheCommandsPayload() {
    var ex =
        new CommandExecutionException(
            new PiiCommand("alice@example.com"),
            COMMAND_ID,
            STREAM,
            "boom",
            new IllegalStateException("root"));

    assertFalse(ex.toString().contains("alice@example.com"), ex.toString());
  }

  @Test
  void toStringWritesTheIdsLogSafe() {
    var forged = StreamId.of(AggregateType.of("customer"), new AggregateId("c-1\nINFO forged"));

    var ex =
        new CommandExecutionException(new SampleCommand("v"), COMMAND_ID, forged, "boom", null);

    assertFalse(ex.toString().contains("\n"), ex.toString());
    assertTrue(ex.toString().endsWith(", stream customer:c-1INFO forged]"), ex.toString());
  }
}
