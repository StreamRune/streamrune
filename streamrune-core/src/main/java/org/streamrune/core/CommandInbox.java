package org.streamrune.core;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * Records commands already processed under a caller-supplied {@link IdempotencyKey} so a
 * redelivered command (e.g. a saga command after broker at-least-once redelivery) applies its
 * events effectively once. The read and prune sides of the command inbox live here; the atomic
 * write of a new inbox row happens inside the event-store append transaction (see {@link
 * EventStore#appendWithKey}) — the same split used by the transactional outbox between {@code
 * OutboxStore} reads and its same-transaction writes.
 *
 * <p>The typical call sequence is:
 *
 * <ol>
 *   <li>{@link #find} — before executing a command, check whether the key was already processed. If
 *       present, return the recorded result immediately (no handler, no events).
 *   <li>{@link EventStore#appendWithKey} — on a cache miss, execute the handler and commit the
 *       inbox row + events atomically.
 *   <li>{@link #deleteProcessedBefore} — called periodically by {@code InboxRetentionSweeper} to
 *       prune old rows; the window must exceed the maximum redelivery horizon.
 * </ol>
 */
public interface CommandInbox {

  /** Returns the recorded result for {@code key}, or empty if this command was never processed. */
  Optional<InboxResult> find(IdempotencyKey key);

  /** Deletes inbox rows processed strictly before {@code cutoff}. Returns the number deleted. */
  int deleteProcessedBefore(Instant cutoff);

  /** The recorded outcome of an already-processed command, enough to reconstruct its result. */
  record InboxResult(
      IdempotencyKey key,
      String commandType,
      StreamId streamId,
      Version finalVersion,
      List<GlobalOffset> globalOffsets,
      Instant processedAt) {
    public InboxResult {
      Objects.requireNonNull(key, "key is required");
      Objects.requireNonNull(commandType, "commandType is required");
      Objects.requireNonNull(streamId, "streamId is required");
      Objects.requireNonNull(finalVersion, "finalVersion is required");
      Objects.requireNonNull(globalOffsets, "globalOffsets is required");
      globalOffsets = List.copyOf(globalOffsets);
      Objects.requireNonNull(processedAt, "processedAt is required");
    }

    /**
     * Fails closed unless this recorded row was produced by the very command now presenting the
     * same key: both {@link #commandType()} and {@link #streamId()} must match. Every path that
     * turns a recorded row back into a caller-visible result — the command bus's pre-execution
     * inbox HIT, and {@link EventStore#appendWithKey}'s claim-loser branch in <em>every</em>
     * implementation — must call this BEFORE returning the row's offsets and version.
     *
     * <p><b>Why it lives here.</b> The guard used to exist only on the command bus's sequential
     * pre-check. Two concurrent executions presenting the same key both miss that pre-check
     * (neither inbox row is committed yet), so the loser reached the store's claim-loser branch,
     * which returned the WINNER's {@code globalOffsets}/{@code finalVersion} with {@code
     * alreadyApplied == true} and no comparison at all. The losing caller was told its command had
     * succeeded — its events were never appended and never would be, the key being permanently
     * claimed — and the result carried another aggregate's stream metadata. The same two inputs
     * therefore produced either a loud rejection or a silent false success depending purely on
     * timing. One shared guard, called from every path, is what keeps the verdict identical; a
     * per-call-site copy is exactly how the paths diverged in the first place.
     *
     * <p>Both discriminators are {@code NOT NULL} columns of the inbox table in the event-store
     * baseline, so every recorded row carries them.
     *
     * <p>The key and both stream ids render through {@link LogSanitizer#sanitizeForLog}: the key is
     * caller-supplied, a key rebuilt through {@link IdempotencyKey}'s decode door keeps no charset
     * rule, and this message is persisted — {@code AuditCommandInterceptor} writes it into {@code
     * audit_log} — as well as logged.
     *
     * @param presentedCommandType the command type presenting the key now (the same fully-qualified
     *     name form that was recorded)
     * @param presentedStreamId the typed stream the presenting command targets; the same id value
     *     under another aggregate type is another stream
     * @throws IllegalArgumentException if either discriminator differs from the recorded one
     */
    public void requireBoundTo(String presentedCommandType, StreamId presentedStreamId) {
      if (commandType.equals(presentedCommandType) && streamId.equals(presentedStreamId)) {
        return;
      }
      throw new IllegalArgumentException(
          "idempotency key '"
              + LogSanitizer.sanitizeForLog(key.value())
              + "' was recorded for command type "
              + commandType
              + " on stream "
              + LogSanitizer.sanitizeForLog(streamId.value())
              + " but was presented with "
              + presentedCommandType
              + " on stream "
              + LogSanitizer.sanitizeForLog(presentedStreamId.value())
              + " — keys must not be reused across commands");
    }
  }
}
