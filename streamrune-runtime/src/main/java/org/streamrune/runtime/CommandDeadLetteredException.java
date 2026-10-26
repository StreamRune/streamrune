package org.streamrune.runtime;

import java.util.Objects;
import org.streamrune.core.Command;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;

/**
 * A command failed and its dead-letter entry is stored: the command has not been applied, and it
 * may still be, when the entry is replayed. {@link VirtualThreadCommandBus#execute(Command)} and
 * {@link VirtualThreadCommandBus#execute(Command, IdempotencyKey)} throw it in place of the failure
 * once the publish to the {@link org.streamrune.core.DeadLetterQueue} has succeeded; the failure is
 * its {@linkplain #getCause() cause}, and {@link #commandId()} is the entry's id.
 *
 * <p><b>The outcome is pending, not final.</b> A caller that treats this as a final failure and
 * issues the operation again under a new idempotency key has it applied twice once the entry is
 * replayed: the replay runs under the command's own key, and the command inbox deduplicates only a
 * command that presents that key. What the caller does depends on the key:
 *
 * <ul>
 *   <li><b>A keyed command</b> ({@link #idempotencyKey()} is not {@code null}) is replayed under
 *       its key, by the dead-letter retry runner's schedule or by an operator. Send it again only
 *       under the same key, or wait for the replay: the inbox executes the command once, whichever
 *       of the two comes first, and answers the other with the recorded outcome. A new key is a new
 *       operation and is applied beside the replay.
 *   <li><b>An unkeyed command</b> ({@link #idempotencyKey()} is {@code null}) has nothing that
 *       pairs its replay with a send of the caller's own, so under the default retry policy its
 *       entry waits for an operator, who replays or discards it. A caller that sends it again has
 *       it applied again if the operator replays the entry as well.
 * </ul>
 *
 * <p>An HTTP endpoint answers it with {@code 503 Service Unavailable} and {@code Retry-After}, and
 * a body whose reason tells the two cases apart: queued for replay under the client's key, or
 * awaiting an operator. Not {@code 202 Accepted}: a client treats a 2xx as done and never sends the
 * request again, while the replay can itself fail, exhaust its attempts and be kept for an
 * operator.
 *
 * <p><b>When the failure itself propagates.</b> Only a stored entry makes the outcome pending, so
 * the bus throws the failure unchanged when there is none: a failure its {@code dlqEligible}
 * predicate refuses, which by default is a business rejection; a bus without a dead letter queue; a
 * failed publish, whose exception is attached to the failure as suppressed; the replay of an entry,
 * under {@link VirtualThreadCommandBus#DLQ_REPLAY}; a command a saga dispatches, and one dispatched
 * with a dispatch cause bound, which their callers retry and the bus does not dead-letter. The
 * future of {@link VirtualThreadCommandBus#executeAsync} fails with the failure itself, although
 * its entry is stored: an asynchronous command runs without an idempotency key, so the entry waits
 * for an operator. The interceptors' {@code onError} receives the failure, never this exception.
 *
 * <p>The message names the command type and the entry and is safe to log; it carries neither the
 * command's payload nor the key, which is a bearer token for the command's recorded result. {@link
 * #toString()} adds the command's type, its ids and the type of the cause, as {@link
 * CommandExecutionException#toString()} renders every command failure.
 */
public final class CommandDeadLetteredException extends CommandExecutionException {

  private final transient IdempotencyKey idempotencyKey;

  /**
   * Creates the exception for a command whose dead-letter entry is stored.
   *
   * @param command the command that failed
   * @param commandId the id the command ran under, which is its dead-letter entry's id
   * @param streamId the stream the command targets
   * @param idempotencyKey the key the command ran under, or {@code null} when it ran without one
   * @param cause the failure the command ended in
   * @throws NullPointerException when {@code command}, {@code commandId}, {@code streamId} or
   *     {@code cause} is {@code null}
   */
  public CommandDeadLetteredException(
      Command command,
      CommandId commandId,
      StreamId streamId,
      IdempotencyKey idempotencyKey,
      Throwable cause) {
    super(
        Objects.requireNonNull(command, "command is required"),
        Objects.requireNonNull(commandId, "commandId is required"),
        streamId,
        message(command, commandId, idempotencyKey),
        Objects.requireNonNull(cause, "cause is required"));
    this.idempotencyKey = idempotencyKey;
  }

  private static String message(Command command, CommandId commandId, IdempotencyKey key) {
    String recorded =
        "Command "
            + command.getClass().getSimpleName()
            + " failed and is recorded in the dead letter queue as entry "
            + LogSanitizer.sanitizeForLog(commandId.value());
    return key != null
        ? recorded
            + ", to be replayed under its idempotency key: its outcome is pending. Send it again"
            + " only under the same key, or wait for the replay; a new key is a new operation"
        : recorded
            + ", without an idempotency key: its outcome is pending until the entry is replayed"
            + " or discarded, which the default retry policy leaves to an operator";
  }

  /**
   * The id the command ran under, which is the id of its dead-letter entry: the argument of the
   * retry runner's {@code retry(CommandId)} and of the queue's {@code discard(CommandId)}.
   *
   * @return the command id
   */
  @Override
  public CommandId commandId() {
    return super.commandId();
  }

  /**
   * The idempotency key the command ran under, which its replay runs under too.
   *
   * @return the key, or {@code null} when the command ran without one
   */
  public IdempotencyKey idempotencyKey() {
    return idempotencyKey;
  }
}
