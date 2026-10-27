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
 * once the publish to the {@link org.streamrune.core.DeadLetterQueue} has succeeded, or once they
 * find the queue holding an entry of the same command under its idempotency key; the failure is its
 * {@linkplain #getCause() cause}, {@link #commandId()} is the entry's id and {@link
 * #pendingOutcome()} says whether the retry schedule replays the entry or an operator decides.
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
 *       operation and is applied beside the replay. A re-send under the same key that fails again
 *       stores no second entry: the queue holds one of the command under the key already, and the
 *       exception names it ({@link #commandId()}) with its outcome — {@link
 *       PendingOutcome#QUEUED_FOR_REPLAY} while the schedule replays it, {@link
 *       PendingOutcome#AWAITING_OPERATOR} once its replays have exhausted their attempts or its
 *       command was first received longer ago than the command-inbox window.
 *   <li><b>An unkeyed command</b> ({@link #idempotencyKey()} is {@code null}) has nothing that
 *       pairs its replay with a send of the caller's own, so under the default retry policy its
 *       entry waits for an operator, who replays or discards it ({@link
 *       PendingOutcome#AWAITING_OPERATOR}). A caller that sends it again has it applied again if
 *       the operator replays the entry as well.
 * </ul>
 *
 * <p>An HTTP endpoint answers the two outcomes differently, each with a body whose reason names it.
 * {@link PendingOutcome#QUEUED_FOR_REPLAY}: {@code 503 Service Unavailable} with {@code
 * Retry-After}, since a re-send under the same key is safe. {@link
 * PendingOutcome#AWAITING_OPERATOR}: {@code 500 Internal Server Error} with no {@code Retry-After},
 * since the schedule will not replay the entry — for an unkeyed command a re-send is a second
 * operation, applied beside an operator's replay of the entry — and the client does not re-send it
 * until the operator has decided. Never {@code 202 Accepted}: a client treats a 2xx as done and
 * never sends the request again, while the replay can itself fail, exhaust its attempts and be kept
 * for an operator. Under the retry policy's {@code discardAfterMaxRetries} the replay can also
 * exhaust its attempts and discard the command, and a client that waits for it is not told: where
 * callers are answered that their command is queued for replay, keep the default, under which an
 * exhausted entry is kept for an operator, or tell them through a channel of your own.
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

  /**
   * What happens to a dead-lettered command's entry, and so what its caller does: the reason an
   * HTTP endpoint answers with.
   */
  public enum PendingOutcome {
    /**
     * The dead-letter retry runner replays the entry on its schedule, under the command's
     * idempotency key. A re-send under the same key is safe — the command inbox executes the
     * command once, whichever of the re-send and the replay comes first — and so is waiting for the
     * replay. Only a keyed command is queued for replay.
     */
    QUEUED_FOR_REPLAY,
    /**
     * The retry schedule does not replay the entry; an operator replays or discards it. The command
     * ran without an idempotency key, so nothing pairs its replay with a send of the caller's own
     * and a re-send is a second operation; or it ran under one and its entry's replays exhausted
     * their attempts, or its command was first received longer ago than the command-inbox window.
     * The caller does not re-send until the operator has decided.
     */
    AWAITING_OPERATOR
  }

  private final transient IdempotencyKey idempotencyKey;
  private final PendingOutcome pendingOutcome;

  /**
   * Creates the exception for a command whose dead-letter entry is stored, with the outcome the key
   * implies: {@link PendingOutcome#QUEUED_FOR_REPLAY} for a keyed command, {@link
   * PendingOutcome#AWAITING_OPERATOR} for an unkeyed one.
   *
   * @param command the command that failed
   * @param commandId the id of the dead-letter entry that holds the command
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
    this(
        command,
        commandId,
        streamId,
        idempotencyKey,
        idempotencyKey != null
            ? PendingOutcome.QUEUED_FOR_REPLAY
            : PendingOutcome.AWAITING_OPERATOR,
        cause);
  }

  /**
   * Creates the exception for a command whose dead-letter entry is stored, with the outcome of that
   * entry: {@link PendingOutcome#AWAITING_OPERATOR} for a keyed command whose entry the retry
   * schedule does not replay.
   *
   * @param command the command that failed
   * @param commandId the id of the dead-letter entry that holds the command
   * @param streamId the stream the command targets
   * @param idempotencyKey the key the command ran under, or {@code null} when it ran without one
   * @param pendingOutcome what happens to the entry
   * @param cause the failure the command ended in
   * @throws NullPointerException when {@code command}, {@code commandId}, {@code streamId}, {@code
   *     pendingOutcome} or {@code cause} is {@code null}
   * @throws IllegalArgumentException when {@code pendingOutcome} is {@link
   *     PendingOutcome#QUEUED_FOR_REPLAY} and {@code idempotencyKey} is {@code null}: a command
   *     without a key has none to be replayed under
   */
  public CommandDeadLetteredException(
      Command command,
      CommandId commandId,
      StreamId streamId,
      IdempotencyKey idempotencyKey,
      PendingOutcome pendingOutcome,
      Throwable cause) {
    super(
        Objects.requireNonNull(command, "command is required"),
        Objects.requireNonNull(commandId, "commandId is required"),
        streamId,
        message(command, commandId, idempotencyKey, checked(pendingOutcome, idempotencyKey)),
        Objects.requireNonNull(cause, "cause is required"));
    this.idempotencyKey = idempotencyKey;
    this.pendingOutcome = pendingOutcome;
  }

  private static PendingOutcome checked(PendingOutcome outcome, IdempotencyKey key) {
    Objects.requireNonNull(outcome, "pendingOutcome is required");
    if (outcome == PendingOutcome.QUEUED_FOR_REPLAY && key == null) {
      throw new IllegalArgumentException(
          "A command without an idempotency key cannot be queued for replay under one");
    }
    return outcome;
  }

  private static String message(
      Command command, CommandId commandId, IdempotencyKey key, PendingOutcome outcome) {
    String recorded =
        "Command "
            + command.getClass().getSimpleName()
            + " failed and is recorded in the dead letter queue as entry "
            + LogSanitizer.sanitizeForLog(commandId.value());
    if (key == null) {
      return recorded
          + ", without an idempotency key: its outcome is pending until the entry is replayed"
          + " or discarded, which the default retry policy leaves to an operator";
    }
    return outcome == PendingOutcome.QUEUED_FOR_REPLAY
        ? recorded
            + ", to be replayed under its idempotency key: its outcome is pending. Send it again"
            + " only under the same key, or wait for the replay; a new key is a new operation"
        : recorded
            + ", which is not replayed on the retry schedule: its replays exhausted their attempts"
            + " or it is older than the command-inbox window. Its outcome is pending until an"
            + " operator replays or discards the entry";
  }

  /**
   * The id of the dead-letter entry that holds the command: the argument of the retry runner's
   * {@code retry(CommandId)} and of the queue's {@code discard(CommandId)}. It is the id this send
   * ran under when the send stored the entry, and the id of the entry an earlier send stored when
   * the queue held one of the command under its key already.
   *
   * @return the entry's command id
   */
  @Override
  public CommandId commandId() {
    return super.commandId();
  }

  /**
   * What happens to the entry: replayed on the retry schedule under the command's key, or left to
   * an operator.
   *
   * @return the pending outcome
   */
  public PendingOutcome pendingOutcome() {
    return pendingOutcome;
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
