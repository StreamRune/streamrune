package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;

class CommandExecutionExceptionTest {

  record SampleCommand(String value) implements Command {}

  record AnotherCommand(String id) implements Command {}

  @Test
  void constructsWithCommandAndMessage() {
    var cmd = new SampleCommand("v");
    var ex = new CommandExecutionException(cmd, "boom");
    assertEquals(cmd, ex.command());
    assertEquals("boom", ex.getMessage());
    assertNull(ex.getCause());
  }

  @Test
  void constructsWithCause() {
    var cause = new RuntimeException("root");
    var cmd = new AnotherCommand("test");
    var ex = new CommandExecutionException(cmd, "boom", cause);
    assertSame(cause, ex.getCause());
    assertEquals(cmd, ex.command());
    assertEquals("boom", ex.getMessage());
  }

  @Test
  void toStringContainsMessageAndCommand() {
    var ex = new CommandExecutionException(new SampleCommand("v"), "boom");
    String s = ex.toString();
    assertTrue(s.contains("boom"));
    assertTrue(s.contains("SampleCommand"));
  }
}
