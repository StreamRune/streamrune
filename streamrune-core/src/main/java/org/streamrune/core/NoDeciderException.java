package org.streamrune.core;

import java.util.List;

/**
 * Exception thrown when no decider is registered for a command type. Provides helpful hints about
 * available commands and registration.
 *
 * <p>Extends {@link IllegalArgumentException} rather than {@link DomainException}: a missing
 * decider registration is a wiring/configuration error by the application developer, not a command
 * rejected by business rules, so it must not be swallowed by {@code catch (DomainException)}
 * handlers at API boundaries.
 */
public class NoDeciderException extends IllegalArgumentException {

  private final Class<?> commandType;
  private final List<Class<?>> availableCommands;

  public NoDeciderException(Class<?> commandType, List<Class<?>> availableCommands) {
    super(buildMessage(commandType, availableCommands));
    this.commandType = commandType;
    this.availableCommands = List.copyOf(availableCommands);
  }

  public Class<?> commandType() {
    return commandType;
  }

  public List<Class<?>> availableCommands() {
    return availableCommands;
  }

  private static String buildMessage(Class<?> commandType, List<Class<?>> available) {
    var sb = new StringBuilder();
    sb.append("No decider registered for command interface ")
        .append(commandType.getSimpleName())
        .append(".\n");
    sb.append("Did you forget to call: .registerDecider(")
        .append(commandType.getSimpleName())
        .append(".class, decider) ?\n");
    if (!available.isEmpty()) {
      sb.append("Available commands: ");
      sb.append(
          available.stream().map(Class::getSimpleName).reduce((a, b) -> a + ", " + b).orElse("[]"));
    } else {
      sb.append("Available commands: []");
    }
    sb.append(
        "\nHint: Make sure your command is a sealed interface or class registered in StreamRune builder.");
    return sb.toString();
  }
}
