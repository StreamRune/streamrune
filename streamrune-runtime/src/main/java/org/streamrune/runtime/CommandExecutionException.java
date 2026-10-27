package org.streamrune.runtime;

import java.util.Objects;
import org.streamrune.core.Command;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;

/**
 * A command's execution failed. {@link VirtualThreadCommandBus} throws it for a failure that is not
 * itself a {@link RuntimeException} or that it must state (an interrupted retry backoff), and its
 * subtype {@link CommandDeadLetteredException} for a command whose dead-letter entry is stored.
 *
 * <p><b>Rendered without the command's payload.</b> {@link #toString()} — the first line of every
 * logged stack trace — names the command's type, its command id, its stream and the type of the
 * cause, and never renders the command itself: a command's own {@code toString} writes every
 * component, {@code @Encrypted} ones included, in the clear. The ids are written through {@link
 * LogSanitizer#sanitizeForLog}. {@link #command()} still hands the command to code that asks.
 */
public class CommandExecutionException extends RuntimeException {

  private final transient Command command;
  private final transient CommandId commandId;
  private final transient StreamId streamId;

  /**
   * Creates the exception.
   *
   * @param command the command whose execution failed
   * @param commandId the id the command ran under
   * @param streamId the stream the command targets
   * @param message what failed
   * @param cause the failure, or {@code null}
   * @throws NullPointerException when {@code command}, {@code commandId} or {@code streamId} is
   *     {@code null}
   */
  public CommandExecutionException(
      Command command, CommandId commandId, StreamId streamId, String message, Throwable cause) {
    super(message, cause);
    this.command = Objects.requireNonNull(command, "command is required");
    this.commandId = Objects.requireNonNull(commandId, "commandId is required");
    this.streamId = Objects.requireNonNull(streamId, "streamId is required");
  }

  /**
   * The command whose execution failed. Hand it to code that needs it; it is not rendered into the
   * exception's text.
   *
   * @return the command
   */
  public Command command() {
    return command;
  }

  /**
   * The id the command ran under. For a {@link CommandDeadLetteredException} it is the id of the
   * dead-letter entry that holds the command, which for a re-send the queue already held an entry
   * for is the id of that earlier send (see {@link CommandDeadLetteredException#commandId()}).
   *
   * @return the command id
   */
  public CommandId commandId() {
    return commandId;
  }

  /**
   * The stream the command targets.
   *
   * @return the stream id
   */
  public StreamId streamId() {
    return streamId;
  }

  /**
   * The class name and the message, then the command's type, its command id, its stream and the
   * type of the cause — never the command's payload.
   */
  @Override
  public String toString() {
    Throwable cause = getCause();
    return getClass().getName()
        + ": "
        + getMessage()
        + " [command "
        + (command == null ? "unknown" : command.getClass().getSimpleName())
        + ", id "
        + (commandId == null ? "unknown" : LogSanitizer.sanitizeForLog(commandId.value()))
        + ", stream "
        + (streamId == null ? "unknown" : LogSanitizer.sanitizeForLog(streamId.value()))
        + (cause == null ? "" : ", cause " + cause.getClass().getName())
        + "]";
  }
}
