package org.streamrune.core;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;

/**
 * Stores commands that failed after all retry attempts were exhausted. Implementations persist the
 * command payload, error details, and metadata so operators can inspect, replay, or investigate
 * failures.
 *
 * <p>Implementations should be thread-safe and transactional — a command must not be silently lost.
 *
 * <p><b>Keying.</b> {@code commandId} uniquely identifies an entry: the queue holds at most one
 * entry per command, and {@link #discard(CommandId)} and {@link #updateAttempts(CommandId, int,
 * Instant)} address entries by it. An entry also records the idempotency key its command ran under,
 * and {@link #findByIdempotencyKey(IdempotencyKey)} finds the entries stored under a key: a command
 * bus that meets an entry for a keyed command's key stores no second one when the command fails
 * again under that key.
 *
 * <p><b>Optional capabilities.</b> The methods that back an optional feature — scheduled DLQ retry
 * ({@link #updateAttempts}, {@link #readRetryable}), the depth gauge ({@link #countPending}),
 * retention pruning ({@link #deleteOlderThan}) and the lookup by key ({@link
 * #findByIdempotencyKey}) — are {@code default} methods that throw {@link
 * UnsupportedOperationException}, so an implementation that does not support a feature fails loudly
 * — never silently — when that feature is enabled. Override them to opt in.
 */
public interface DeadLetterQueue {

  /**
   * Data required to publish a failed command to the dead letter queue.
   *
   * @param commandPayload serialized command payload (JSON string); must not be null
   * @param commandType fully-qualified class name ({@link Class#getName()}) of the command; must
   *     not be null or blank
   * @param commandId unique ID of the command; must not be null
   * @param streamId the stream the command targeted — its aggregate type and id — or {@code null}
   *     if not applicable
   * @param errorType fully-qualified class name of the exception that caused the failure; must not
   *     be null or blank
   * @param errorMessage the exception message; may be null (exceptions can lack a message)
   * @param attempts number of attempts made before giving up; must not be negative
   * @param timestamp when the command was first received; must not be null
   * @param correlationId the correlation id of the originating request context, or null if no
   *     context was bound when the command ran. Persisted so a replay can rebind the original
   *     business flow instead of generating a fresh correlation id.
   * @param userId the authenticated user from the originating request context, or null if anonymous
   *     / no context was bound. Persisted so fail-closed authorization re-evaluates the replay as
   *     the original user rather than as anonymous.
   * @param traceId the trace id from the originating request context, or null if none was bound
   * @param idempotencyKey the caller-supplied {@link IdempotencyKey} the command originally ran
   *     under (via {@code execute(command, key)}), or null if the command ran unkeyed. Persisted so
   *     the retry runner replays under the ORIGINAL key: a keyed command that failed on transient
   *     infra then reconciles a DLQ replay AND the client's own at-least-once redelivery against
   *     the same {@link CommandInbox} row, so the events are produced exactly once. Unkeyed
   *     commands store null: nothing can deduplicate their replay against a retry by the caller.
   */
  record DeadLetterPublishRequest(
      String commandPayload,
      String commandType,
      CommandId commandId,
      StreamId streamId,
      String errorType,
      String errorMessage,
      int attempts,
      Instant timestamp,
      CorrelationId correlationId,
      UserId userId,
      TraceId traceId,
      IdempotencyKey idempotencyKey) {

    /** Compact constructor — validates required fields. */
    public DeadLetterPublishRequest {
      Objects.requireNonNull(commandPayload, "commandPayload is required");
      Objects.requireNonNull(commandType, "commandType is required");
      if (commandType.isBlank()) {
        throw new IllegalArgumentException("commandType must not be blank");
      }
      Objects.requireNonNull(commandId, "commandId is required");
      Objects.requireNonNull(errorType, "errorType is required");
      if (errorType.isBlank()) {
        throw new IllegalArgumentException("errorType must not be blank");
      }
      Objects.requireNonNull(timestamp, "timestamp is required");
      if (attempts < 0) {
        throw new IllegalArgumentException("attempts must not be negative");
      }
    }

    /**
     * The aggregate id of {@link #streamId()}, or {@code null} when there is no stream. The record
     * stores the id once, inside the stream.
     */
    public AggregateId aggregateId() {
      return streamId == null ? null : streamId.aggregateId();
    }
  }

  /**
   * Publishes a failed command to the dead letter queue. Called when a command has exhausted all
   * retry attempts and cannot be processed.
   *
   * <p>Implementations should store enough information to replay the command later: the serialized
   * command payload, the exception type and message, and contextual metadata.
   *
   * <p>Callers must publish a given {@code commandId} at most once — the queue keeps one entry per
   * command, and implementations may reject a duplicate (the PostgreSQL implementation has a
   * primary key on the command ID). When a replayed entry fails again, update it via {@link
   * #updateAttempts(CommandId, int, Instant)} instead of publishing it a second time; when a keyed
   * command fails again under a key the queue holds an entry of the same command for ({@link
   * #findByIdempotencyKey}), the command bus publishes nothing.
   *
   * @param request the dead letter publish request containing all failure details
   */
  void publish(DeadLetterPublishRequest request);

  /**
   * Result of a dead letter queue entry returned by {@link #read(int)}.
   *
   * @param commandId unique identifier for this entry; must not be null
   * @param commandType fully-qualified class name ({@link Class#getName()}) of the command; must
   *     not be null or blank
   * @param commandPayload serialized command payload (JSON string); must not be null
   * @param streamId the stream the command targeted — its aggregate type and id — or {@code null}
   *     if not applicable
   * @param errorType the exception class that caused the failure; must not be null or blank
   * @param errorMessage the exception message; may be null
   * @param attempts number of attempts before giving up; must not be negative
   * @param firstAttemptAt when the command was first received; must not be null
   * @param publishedAt when this entry was added to the DLQ; must not be null
   * @param dlqAttempts number of DLQ retry attempts made so far; must not be negative
   * @param lastAttemptAt timestamp of the most recent DLQ retry attempt, or null if never retried
   * @param correlationId the correlation id of the originating request context, or null if no
   *     context was bound when the command ran. Rebound on replay so events keep their original
   *     correlation instead of a freshly generated one.
   * @param userId the authenticated user from the originating request context, or null if anonymous
   *     / no context was bound. Rebound on replay so fail-closed authorization re-evaluates the
   *     replay as the original user rather than as anonymous.
   * @param traceId the trace id from the originating request context, or null if none was bound
   * @param idempotencyKey the caller-supplied {@link IdempotencyKey} the command originally ran
   *     under, or null if it ran unkeyed. When present, the retry runner replays under this key so
   *     a cross-instance duplicate replay AND the client's at-least-once redelivery reconcile
   *     against the same {@link CommandInbox} row (events produced exactly once), as long as the
   *     inbox still holds that row. When null the runner's scheduled retry leaves the entry to an
   *     operator unless its policy replays unkeyed entries automatically, and a replay runs under a
   *     synthetic {@code dlq-replay:<commandId>} key. The PostgreSQL {@code
   *     dead_letter_queue.idempotency_key} column is part of the shipped schema, so every stored
   *     entry records its key and null always means the command ran unkeyed.
   */
  record DeadLetterEntry(
      CommandId commandId,
      String commandType,
      String commandPayload,
      StreamId streamId,
      String errorType,
      String errorMessage,
      int attempts,
      Instant firstAttemptAt,
      Instant publishedAt,
      int dlqAttempts,
      Instant lastAttemptAt,
      CorrelationId correlationId,
      UserId userId,
      TraceId traceId,
      IdempotencyKey idempotencyKey) {

    /** Compact constructor — validates required fields. */
    public DeadLetterEntry {
      Objects.requireNonNull(commandId, "commandId is required");
      Objects.requireNonNull(commandType, "commandType is required");
      if (commandType.isBlank()) {
        throw new IllegalArgumentException("commandType must not be blank");
      }
      Objects.requireNonNull(commandPayload, "commandPayload is required");
      Objects.requireNonNull(errorType, "errorType is required");
      if (errorType.isBlank()) {
        throw new IllegalArgumentException("errorType must not be blank");
      }
      Objects.requireNonNull(firstAttemptAt, "firstAttemptAt is required");
      Objects.requireNonNull(publishedAt, "publishedAt is required");
      if (attempts < 0) {
        throw new IllegalArgumentException("attempts must not be negative");
      }
      if (dlqAttempts < 0) {
        throw new IllegalArgumentException("dlqAttempts must not be negative");
      }
    }

    /**
     * The aggregate id of {@link #streamId()}, or {@code null} when there is no stream. The record
     * stores the id once, inside the stream.
     */
    public AggregateId aggregateId() {
      return streamId == null ? null : streamId.aggregateId();
    }
  }

  /**
   * Reads up to {@code limit} dead letter entries, ordered by {@code publishedAt} descending
   * (newest first).
   *
   * @param limit maximum number of entries to return
   * @return list of dead letter entries, may be empty
   */
  List<DeadLetterEntry> read(int limit);

  /**
   * Finds a single dead letter entry by its command id, or {@link Optional#empty()} if none
   * matches.
   *
   * <p>Used for operator-triggered single-entry retry. The default implementation scans {@link
   * #read(int)}; implementations backed by a database should override it with an indexed lookup.
   *
   * @param commandId the command id of the entry to find
   * @return the matching entry, or empty if not present
   */
  default Optional<DeadLetterEntry> find(CommandId commandId) {
    return read(Integer.MAX_VALUE).stream()
        .filter(entry -> entry.commandId().equals(commandId))
        .findFirst();
  }

  /**
   * Finds every entry stored under {@code key} — the entries of the commands that ran under it —
   * oldest first (ascending {@code publishedAt}); empty when none is.
   *
   * <p>The command bus asks before it stores an entry for a keyed command: when the queue already
   * holds an entry of the same command (its type and its stream) under the key, the command failed
   * again after a send under that key was dead-lettered, so the bus stores nothing and answers the
   * caller with that entry. The client that re-sends under its key during an outage therefore adds
   * no entry per re-send. Two sends that fail at the same instant can still store one entry each;
   * the command inbox deduplicates their replays.
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}: the lookup is an
   * optional capability. Without it the bus stores an entry for every failed send, as it does for
   * an unkeyed command. Implementations backed by a database override it with an indexed lookup.
   *
   * @param key the idempotency key the commands ran under
   * @return the entries stored under the key, oldest first; may be empty
   * @throws UnsupportedOperationException if the implementation does not look entries up by key
   */
  default List<DeadLetterEntry> findByIdempotencyKey(IdempotencyKey key) {
    throw new UnsupportedOperationException(
        getClass().getName()
            + " does not support findByIdempotencyKey(IdempotencyKey) — override it so a keyed"
            + " command that fails again under the same key stores no second entry");
  }

  /**
   * Discards a dead letter entry after it has been handled (e.g., moved to a retry queue, manually
   * resolved, etc.). Discarding a {@code commandId} with no matching entry is a no-op.
   *
   * @param commandId the command ID of the entry to discard
   */
  void discard(CommandId commandId);

  /**
   * Updates the DLQ retry attempt count and last attempt timestamp for the given command. Updating
   * a {@code commandId} with no matching entry is a no-op.
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}: scheduled DLQ retry
   * is an optional capability, and an implementation must override this method (with {@link
   * #readRetryable}) before it can be enabled.
   *
   * @param commandId the command ID of the entry to update
   * @param dlqAttempts the new total number of DLQ retry attempts
   * @param lastAttemptAt the timestamp of the most recent DLQ retry attempt
   * @throws UnsupportedOperationException if the implementation does not track DLQ retry attempts
   */
  default void updateAttempts(CommandId commandId, int dlqAttempts, Instant lastAttemptAt) {
    throw new UnsupportedOperationException(
        getClass().getName()
            + " does not support DLQ retry tracking — override"
            + " updateAttempts(CommandId, int, Instant) to record retry attempts");
  }

  /**
   * Reads entries eligible for DLQ retry, ordered by least-recently-active first — ascending {@code
   * COALESCE(lastAttemptAt, publishedAt)} (a never-retried entry sorts by its {@code publishedAt}).
   * Only entries with {@code dlqAttempts < maxRetries} are returned; unless {@code includeUnkeyed}
   * is {@code true}, only entries that carry an {@link DeadLetterEntry#idempotencyKey() idempotency
   * key}; and, when {@code keyedFirstAttemptAtOrAfter} is non-null, a keyed entry only if its
   * {@code firstAttemptAt} is at or after it (an unkeyed entry is not affected by the cutoff).
   *
   * <p>The ordering puts the entries most likely to be past their retry backoff at the front so a
   * recently-retried, still-backing-off entry cannot head-of-line block genuinely-due entries
   * behind it. The caller ({@code DeadLetterRetryRunner}) evaluates the per-entry backoff window
   * and over-fetches beyond its retry batch size, so returning a not-yet-due entry here is expected
   * — it is simply skipped that cycle.
   *
   * <p>Both filters belong to the store, not the caller: a runner that leaves unkeyed entries, or
   * keyed entries older than the command-inbox window, to an operator never touches them, so they
   * keep their place at the head of this ordering for as long as they are retained. Filtering them
   * out after the read would let a backlog of them fill every page and starve the keyed entries
   * behind it.
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}: scheduled DLQ retry
   * is an optional capability, and an implementation must override this method (with {@link
   * #updateAttempts}) before it can be enabled.
   *
   * @param maxRetries maximum number of DLQ retry attempts allowed; entries at or above this
   *     threshold are excluded
   * @param limit maximum number of entries to return
   * @param includeUnkeyed whether entries without an idempotency key are returned too; {@code
   *     false} returns keyed entries only
   * @param keyedFirstAttemptAtOrAfter the earliest {@code firstAttemptAt} of a keyed entry to
   *     return, or {@code null} to return keyed entries of any age
   * @return list of retryable dead letter entries, may be empty
   * @throws UnsupportedOperationException if the implementation does not track DLQ retry attempts
   */
  default List<DeadLetterEntry> readRetryable(
      int maxRetries, int limit, boolean includeUnkeyed, Instant keyedFirstAttemptAtOrAfter) {
    throw new UnsupportedOperationException(
        getClass().getName()
            + " does not support DLQ retry tracking — override readRetryable(int, int, boolean,"
            + " Instant) to return retry-eligible entries");
  }

  /**
   * Returns the current dead-letter-queue depth — entries queued (published, not yet discarded),
   * regardless of retry-eligibility. Read-only: it takes no claim/lease, so it is safe to call
   * while the retry runner polls. Used for backlog observability — the {@code
   * DeadLetterRetryRunner} samples it each poll cycle and reports it as the {@code
   * streamrune.dlq.pending} gauge, so a growing command DLQ is visible before any entry exhausts
   * its retry ladder.
   *
   * <p>The default throws {@link UnsupportedOperationException}: a store that does not support
   * counting is simply excluded from the depth gauge (the runner degrades gracefully); retry is
   * unaffected. Override it to enable the gauge.
   *
   * @return the number of queued dead-letter entries
   * @throws UnsupportedOperationException if the implementation does not support counting
   */
  default long countPending() {
    throw new UnsupportedOperationException(
        getClass().getName()
            + " does not support countPending() — override it to enable the command DLQ depth"
            + " gauge (streamrune.dlq.pending).");
  }

  /**
   * Counts the entries the scheduled retry will never replay and that are not exhausted — entries
   * with {@code dlqAttempts < maxRetries} that are either unkeyed (when {@code countUnkeyed}) or
   * keyed with {@code firstAttemptAt} strictly before {@code keyedFirstAttemptBefore} (when it is
   * non-null). Read-only, like {@link #countPending()}. The {@code DeadLetterRetryRunner} samples
   * it each poll cycle as the {@code streamrune.dlq.awaiting_operator} gauge: every entry it counts
   * waits for an operator to retry or discard it, and none of them ever reaches {@code
   * streamrune.dlq.exhausted}.
   *
   * <p>The default throws {@link UnsupportedOperationException}: a store that does not support it
   * is simply excluded from the gauge (the runner degrades gracefully); retry is unaffected.
   *
   * @param maxRetries entries at or above this many DLQ attempts are exhausted and not counted
   * @param countUnkeyed whether entries without an idempotency key count
   * @param keyedFirstAttemptBefore keyed entries first received strictly before this instant count;
   *     {@code null} counts no keyed entry
   * @return the number of entries awaiting an operator
   * @throws UnsupportedOperationException if the implementation does not support counting
   */
  default long countAwaitingOperator(
      int maxRetries, boolean countUnkeyed, Instant keyedFirstAttemptBefore) {
    throw new UnsupportedOperationException(
        getClass().getName()
            + " does not support countAwaitingOperator(int, boolean, Instant) — override it to"
            + " enable the streamrune.dlq.awaiting_operator gauge.");
  }

  /**
   * Deletes entries published strictly before {@code cutoff}, returning the number of rows removed.
   * Used by the retention sweeper to bound how long failed-command payloads (including any
   * encrypted PII) remain queryable — a GDPR storage-limitation concern, not a correctness one.
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}: retention pruning
   * is an optional capability, and an implementation must override this method before the retention
   * sweeper can be enabled — an implementation that does not support it makes the sweeper fail
   * loudly (not silently) on its first cycle.
   *
   * @param cutoff entries with {@code publishedAt} strictly before this instant are eligible for
   *     deletion; entries at or after {@code cutoff} are retained
   * @return the number of entries deleted
   * @throws UnsupportedOperationException if the implementation does not support retention pruning
   */
  default int deleteOlderThan(Instant cutoff) {
    throw new UnsupportedOperationException(
        getClass().getName()
            + " does not support retention pruning — override deleteOlderThan(Instant) to"
            + " delete entries older than a cutoff");
  }
}
