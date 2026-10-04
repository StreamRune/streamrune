package org.streamrune.runtime;

import org.streamrune.core.Command;

/** Thrown when command execution fails. */
public class CommandExecutionException extends RuntimeException {

  private final Command command;

  public CommandExecutionException(Command command, String message, Throwable cause) {
    super(message, cause);
    this.command = command;
  }

  public CommandExecutionException(Command command, String message) {
    super(message);
    this.command = command;
  }

  public Command command() {
    return command;
  }

  @Override
  public String toString() {
    return "CommandExecutionException{command=" + command + ", message=" + getMessage() + "}";
  }
}
