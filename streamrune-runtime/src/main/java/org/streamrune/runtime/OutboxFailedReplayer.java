package org.streamrune.runtime;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.types.LogSanitizer;

/**
 * The operator surface for terminal {@code FAILED} outbox entries: replay (back to {@code PENDING},
 * position-preserving) or skip (to the audited terminal {@code SKIPPED}). Deliberately <b>no
 * polling runner and no lifecycle</b> (mirroring {@link SagaDeadLetterReplayer}): an entry reaches
 * {@code FAILED} only after exhausting its counted ladder on a per-entry rejection — a
 * deterministic condition a fix must resolve.
 *
 * <p>On a {@code STRICT_PER_AGGREGATE} channel a {@code FAILED} entry is its aggregate's head and
 * blocks every later entry of that aggregate; {@link #replay} delivers it before its successors,
 * {@link #skip} releases them (the only other exit from {@code FAILED}). On an {@code
 * AVAILABILITY_FIRST} channel nothing was blocked; both operations still record the decision.
 *
 * <p><b>A skip is not a delivery.</b> The downstream did not receive the skipped change and will
 * receive the successors as if it had. If the consumer's state depends on that change, emit a
 * corrective domain event through the normal command path, or reconcile out of band; record the
 * ticket in {@code reason}.
 *
 * <p>{@code skippedBy} and {@code reason} are caller-supplied (the framework has no principal), are
 * sanitized ({@link LogSanitizer#sanitizeFreeText}) and capped (255 / 2000 UTF-16 units, never
 * splitting a surrogate pair) before they are persisted, and must still be non-blank after that —
 * they are read by humans, never compared or keyed on.
 */
public final class OutboxFailedReplayer {

  private static final Logger LOG = LoggerFactory.getLogger(OutboxFailedReplayer.class);

  static final int MAX_SKIPPED_BY_LENGTH = 255;
  static final int MAX_REASON_LENGTH = 2000;

  /** Outcome of {@link #replay(OutboxEntryId)}. */
  public enum ReplayOutcome {
    REPLAYED,
    /** The entry exists but is not {@code FAILED}: already replayed, skipped, or never failed. */
    NOT_FAILED,
    NOT_FOUND
  }

  /** Outcome of {@link #skip(OutboxEntryId, String, String)}. */
  public enum SkipOutcome {
    SKIPPED,
    /** The entry exists but is not {@code FAILED}: already replayed, skipped, or never failed. */
    NOT_FAILED,
    NOT_FOUND
  }

  private final OutboxStore store;
  private final StreamRuneMetrics metrics;

  /**
   * Creates a replayer.
   *
   * @param store the outbox store whose {@code FAILED} entries are replayed or skipped (required)
   * @param metrics metrics collector (required; pass {@link StreamRuneMetrics#NOOP} to disable)
   */
  public OutboxFailedReplayer(OutboxStore store, StreamRuneMetrics metrics) {
    this.store = Objects.requireNonNull(store, "store is required");
    this.metrics = Objects.requireNonNull(metrics, "metrics is required");
  }

  /**
   * Resets up to {@code maxEntries} {@code FAILED} entries back to {@code PENDING} for redelivery,
   * <b>oldest first</b>. Enumerates via {@link OutboxStore#findByStatus} (oldest {@code seq} first)
   * and resets each via {@link OutboxStore#resetFailedToPending} — position-preserving, same row,
   * same {@code seq}. Records {@link StreamRuneMetrics#recordOutboxReplayed(long)} with the number
   * actually reset and logs a summary.
   *
   * <p>An entry that another actor already moved off {@code FAILED} between enumeration and reset
   * is simply skipped (the reset is a no-op) and not counted — safe to call repeatedly.
   *
   * <p>On a {@code STRICT_PER_AGGREGATE} channel every reset entry is its aggregate's head again
   * and is delivered before its successors. On an {@code AVAILABILITY_FIRST} channel the successors
   * may already have been delivered while the entry sat terminal; the replayed entry then arrives
   * after them, so consumers of such a channel must be idempotent and order-tolerant for replayed
   * traffic.
   *
   * @param maxEntries the maximum number of entries to reset; must be positive
   * @return the number of entries actually reset from {@code FAILED} to {@code PENDING}
   */
  public int replayFailed(int maxEntries) {
    if (maxEntries < 1) throw new IllegalArgumentException("maxEntries must be >= 1");
    List<OutboxEntry> failed = store.findByStatus(OutboxStatus.FAILED, maxEntries);
    int reset = 0;
    for (OutboxEntry entry : failed) {
      if (store.resetFailedToPending(entry.id())) {
        reset++;
      }
    }
    recordReplayed(reset);
    LOG.info(
        "Outbox failed-replay reset {} of {} enumerated FAILED entries back to PENDING (maxEntries={})",
        reset,
        failed.size(),
        maxEntries);
    return reset;
  }

  /**
   * Replays one {@code FAILED} entry: {@code FAILED → PENDING} at its original position (same row,
   * same {@code seq}). One statement; a rerun finds the row no longer {@code FAILED} and returns
   * {@link ReplayOutcome#NOT_FAILED}.
   */
  public ReplayOutcome replay(OutboxEntryId id) {
    Objects.requireNonNull(id, "id is required");
    Optional<OutboxEntry> found = store.findById(id);
    if (found.isEmpty()) {
      return ReplayOutcome.NOT_FOUND;
    }
    if (!store.resetFailedToPending(id)) {
      return ReplayOutcome.NOT_FAILED;
    }
    recordReplayed(1);
    LOG.info(
        "Outbox entry {} of stream {} replayed to PENDING at its original position",
        LogSanitizer.sanitizeForLog(id.value()),
        streamOf(found.get()));
    return ReplayOutcome.REPLAYED;
  }

  /**
   * Skips one {@code FAILED} entry: {@code FAILED → SKIPPED} with the audit columns, in one
   * statement. A rerun returns {@link SkipOutcome#NOT_FAILED}. Racing a replay, exactly one of the
   * two wins; the loser sees {@code NOT_FAILED} and re-reads with {@code findById}.
   *
   * @param skippedBy the operator identity the application resolved (required, non-blank)
   * @param reason why the entry will never be delivered — the ticket (required, non-blank)
   */
  public SkipOutcome skip(OutboxEntryId id, String skippedBy, String reason) {
    Objects.requireNonNull(id, "id is required");
    String by = requireSanitized(skippedBy, "skippedBy", MAX_SKIPPED_BY_LENGTH);
    String why = requireSanitized(reason, "reason", MAX_REASON_LENGTH);
    Optional<OutboxEntry> found = store.findById(id);
    if (found.isEmpty()) {
      return SkipOutcome.NOT_FOUND;
    }
    if (!store.skipFailed(id, by, why)) {
      return SkipOutcome.NOT_FAILED;
    }
    recordSkipped();
    // Deliberately conditional: a FAILED row is not always a head (legacy availability-first data
    // can hold a PENDING head backing off below a FAILED row), and this class reads no seq.
    LOG.warn(
        "Outbox entry {} of stream {} SKIPPED by {}: \"{}\" — this entry will never be"
            + " delivered; the downstream did NOT receive this change. If it was its stream's"
            + " blocking head, the stream's later entries are claimable from the next poll",
        LogSanitizer.sanitizeForLog(id.value()),
        streamOf(found.get()),
        by,
        why);
    return SkipOutcome.SKIPPED;
  }

  /**
   * Bulk skip, the mirror of {@link #replayFailed(int)}: enumerates the oldest {@code maxEntries}
   * {@code FAILED} rows and skips each with ONE {@code status = 'FAILED'} CAS carrying the same
   * audit. Exists because {@code FAILED} is never swept in either mode: an {@code
   * AVAILABILITY_FIRST} channel can accumulate unresolvable rows that nothing else removes. On a
   * strict channel prefer {@link #skip} per head — every bulk-skipped head drops one aggregate's
   * change. A crash after {@code k} skips leaves {@code k} fully audited {@code SKIPPED} rows; a
   * rerun enumerates only the rows still {@code FAILED}.
   *
   * @return the number of rows that became {@code SKIPPED}
   */
  public int skipFailed(int maxEntries, String skippedBy, String reason) {
    if (maxEntries < 1) throw new IllegalArgumentException("maxEntries must be >= 1");
    String by = requireSanitized(skippedBy, "skippedBy", MAX_SKIPPED_BY_LENGTH);
    String why = requireSanitized(reason, "reason", MAX_REASON_LENGTH);
    List<OutboxEntry> failed = store.findByStatus(OutboxStatus.FAILED, maxEntries);
    int skipped = 0;
    for (OutboxEntry entry : failed) {
      if (store.skipFailed(entry.id(), by, why)) {
        skipped++;
        recordSkipped();
      }
    }
    LOG.warn(
        "Outbox bulk skip: {} of {} enumerated FAILED entries SKIPPED by {}: \"{}\" (maxEntries={});"
            + " none of them was delivered downstream",
        skipped,
        failed.size(),
        by,
        why,
        maxEntries);
    return skipped;
  }

  /**
   * Sanitizes, caps, and only then judges blankness — on the value that is persisted. Judging the
   * raw input would admit a value the sanitizer empties: it removes format code points (U+200B)
   * outright and turns a control run (U+0007) into one space, neither of which {@link
   * String#isBlank()} treats as blank. The cap never splits a surrogate pair.
   */
  private static String requireSanitized(String value, String name, int max) {
    String sanitized = LogSanitizer.sanitizeFreeText(value);
    if (sanitized != null && sanitized.length() > max) {
      int end = Character.isHighSurrogate(sanitized.charAt(max - 1)) ? max - 1 : max;
      sanitized = sanitized.substring(0, end);
    }
    if (sanitized == null || sanitized.isBlank()) {
      throw new IllegalArgumentException(
          name + " must be non-blank once sanitized and capped to " + max + " characters");
    }
    return sanitized;
  }

  private static String streamOf(OutboxEntry entry) {
    return entry.streamId() == null
        ? "<none>"
        : LogSanitizer.sanitizeForLog(entry.streamId().value());
  }

  // Guarded — the store writes above already committed; a throwing metrics backend must not
  // turn a successful operation into an exception for the caller.
  private void recordReplayed(int rows) {
    try {
      metrics.recordOutboxReplayed(rows);
    } catch (RuntimeException e) {
      LOG.warn("Metrics recordOutboxReplayed failed", e);
    }
  }

  private void recordSkipped() {
    try {
      metrics.recordOutboxSkipped();
    } catch (RuntimeException e) {
      LOG.warn("Metrics recordOutboxSkipped failed", e);
    }
  }
}
