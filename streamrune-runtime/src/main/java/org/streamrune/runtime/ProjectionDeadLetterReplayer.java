package org.streamrune.runtime;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * Replays dead-lettered projection batches back through their projection. This is the recovery half
 * of the projection DLQ: the runners only <em>write</em> entries. Under the DLQ error strategy both
 * {@link ScheduledProjectionRunner} and {@link ContinuousProjectionRunner} (in catch-up and in live
 * mode) dead-letter a failed batch, advance the checkpoint past it (leaving a hole in the read
 * model), and continue — a dead-letter no longer halts the runner. Once the underlying cause is
 * fixed (bug deployed, poison data corrected), an operator invokes {@link #replay(ProjectionName,
 * Projection, int)} to re-read each dead-lettered range from the event store, run it through the
 * projection, and discard the entry on success.
 *
 * <p>Replay is explicit and operator-driven by design — there is no background loop. A
 * dead-lettered batch failed deterministically; retrying it on a schedule before the cause is fixed
 * would only burn cycles and spam logs.
 *
 * <p><b>Ordering and idempotency.</b> Entries replay oldest range first, but later events were
 * already processed when the failed batch was skipped past, so a replay is inherently an
 * out-of-order patch of the read model — safe because {@link Projection#process(List)} must be
 * idempotent. Because the runner already advanced its checkpoint past the dead-lettered range,
 * replay backfills the hole <em>behind</em> the checkpoint without moving it; consumers of the read
 * model must therefore tolerate a range being filled in after later ranges are already visible.
 *
 * <p><b>At-least-once.</b> If the projection succeeds but discarding the entry fails, the entry
 * stays queued and the next replay processes the range again. An entry whose range reads back empty
 * — no event of it is left in the event store — is discarded without processing: there is nothing
 * left to apply. A GDPR erasure does not empty a range: crypto-shredding destroys the subject's
 * key, not the events, so the erased subject's events read back with their encrypted fields as
 * {@code [REDACTED]} ({@code CryptoShreddingModule}'s fallback, reported on {@code
 * streamrune.crypto.subject_redacted}) and are fed to the projection like any other event. An event
 * whose type rejects the {@code [REDACTED]} value fails the read instead, and its entry is kept and
 * counted in {@link ReplayResult#failed()}.
 *
 * <p>This holds under every {@code ProjectionDeliveryMode}: a replay calls the one-arg {@code
 * process} outside any checkpoint transaction — a {@code BaseProjection} writes through its
 * captured repository, autocommit — possibly concurrently with the live runner's later batches on
 * the same rows. The mode does not make a replay transactional.
 *
 * <p><b>A feed that applied nothing keeps the entry.</b> The batch is fed through {@link
 * Projection#processDeadLetterReplay(List)}, which reports whether the projection applied anything;
 * discard happens only on a non-exceptional feed that DID apply. A self-fencing projection — {@link
 * WindowedProjection}, which dedups on the highest offset it ever accumulated — fences a
 * dead-lettered range out wholesale once the live runner has advanced its fence past the hole the
 * entry records, so its {@code process} returns normally having changed nothing. Pre-fix that
 * silent no-op discarded the entry: the recovery path destroyed the only record of a range that was
 * never accumulated. Such entries are now counted as {@link ReplayResult#fenced()} and kept, with
 * the remedy logged: replay from a fresh process (the fence and the windows are per-JVM — see
 * {@code WindowedProjection}'s rebuild contract), before the live runner advances again. The fence
 * itself is deliberately not weakened — a genuinely re-delivered range must still dedup.
 */
public final class ProjectionDeadLetterReplayer {

  private static final Logger logger = LoggerFactory.getLogger(ProjectionDeadLetterReplayer.class);

  private final EventStore eventStore;
  private final ProjectionDeadLetterStore deadLetterStore;

  /**
   * Creates a replayer.
   *
   * @param eventStore the event store to re-read dead-lettered ranges from
   * @param deadLetterStore the store holding the entries to replay
   */
  public ProjectionDeadLetterReplayer(
      EventStore eventStore, ProjectionDeadLetterStore deadLetterStore) {
    if (eventStore == null) throw new IllegalArgumentException("eventStore is required");
    if (deadLetterStore == null) throw new IllegalArgumentException("deadLetterStore is required");
    this.eventStore = eventStore;
    this.deadLetterStore = deadLetterStore;
  }

  /**
   * Replays up to {@code maxEntries} dead-letter entries for the given projection, oldest range
   * first. For each entry the original {@code [fromOffset, toOffset]} range is re-read from the
   * global stream and processed through the projection; on success the entry is discarded. A
   * failing entry is kept (and logged) and the remaining entries are still attempted, so one
   * still-poisoned batch does not block recovery of the others.
   *
   * <p>The projection's offset checkpoint is never touched — replay fills holes behind the
   * checkpoint, it does not move it.
   *
   * @param projectionName the projection whose dead letters to replay; must not be null
   * @param projection the projection to feed the re-read batches to; must not be null
   * @param maxEntries maximum number of entries to replay in this pass; must be > 0
   * @return how many entries were replayed and discarded, how many failed and were kept, and how
   *     many the projection fenced out wholesale and were kept
   */
  public ReplayResult replay(ProjectionName projectionName, Projection projection, int maxEntries) {
    if (projectionName == null) {
      throw new IllegalArgumentException("projectionName is required");
    }
    if (projection == null) throw new IllegalArgumentException("projection is required");
    if (maxEntries <= 0) throw new IllegalArgumentException("maxEntries must be positive");

    // The store returns oldest-first; the sort is a defensive no-op pinning stream order.
    List<ProjectionDeadLetterEntry> entries =
        deadLetterStore.read(projectionName, maxEntries).stream()
            .sorted(Comparator.comparingLong(entry -> entry.fromOffset().value()))
            .toList();

    int replayed = 0;
    int failed = 0;
    int fenced = 0;
    for (var entry : entries) {
      try {
        List<EventEnvelope> batch = readRange(entry);
        if (batch.isEmpty()) {
          logger.info(
              "Dead-letter batch [{}-{}] for projection '{}' no longer exists in the event store"
                  + " (no event of the range could be read back) — discarding the entry",
              entry.fromOffset().value(),
              entry.toOffset().value(),
              projectionName.value());
        } else if (!projection.processDeadLetterReplay(batch)) {
          // The feed returned normally but the projection applied NOTHING — a
          // self-fencing projection read the never-accumulated hole as already counted. The
          // entry is the range's only record; keep it and say why.
          fenced++;
          logger.warn(
              "Replay of dead-letter batch [{}-{}] for projection '{}' applied NOTHING: the"
                  + " projection reports the whole range as already accumulated (a self-fencing"
                  + " projection such as WindowedProjection, whose offset fence has moved past this"
                  + " hole while later batches were applied). The range was never accumulated, so"
                  + " the entry is KEPT rather than discarded. Replay it from a fresh process —"
                  + " the fence and the open windows are per-JVM — before the live runner advances"
                  + " again, or discard the entry deliberately if the range is not needed.",
              entry.fromOffset().value(),
              entry.toOffset().value(),
              projectionName.value());
          continue;
        }
        deadLetterStore.discard(projectionName, entry.fromOffset());
        replayed++;
      } catch (RuntimeException e) {
        failed++;
        logger.error(
            "Replay of dead-letter batch [{}-{}] for projection '{}' failed — the entry is kept"
                + " for a later replay",
            entry.fromOffset().value(),
            entry.toOffset().value(),
            projectionName.value(),
            e);
      }
    }
    return new ReplayResult(replayed, failed, fenced);
  }

  /**
   * Re-reads the entry's inclusive {@code [fromOffset, toOffset]} range from the global stream,
   * paging until the range end is reached. The store may return shorter pages than requested, and
   * the {@link EventStore#readGlobalStream} contract does not promise contiguous offsets — both are
   * handled by following the offsets, not the counts.
   */
  private List<EventEnvelope> readRange(ProjectionDeadLetterEntry entry) {
    long toOffset = entry.toOffset().value();
    var batch = new ArrayList<EventEnvelope>(entry.batchSize());
    // readGlobalStream is exclusive of afterOffset, so start one before the range to include it.
    GlobalOffset cursor = GlobalOffset.of(Math.max(0, entry.fromOffset().value() - 1));
    while (cursor.value() < toOffset) {
      List<EventEnvelope> page = eventStore.readGlobalStream(cursor, entry.batchSize());
      if (page.isEmpty()) {
        break;
      }
      for (var envelope : page) {
        if (envelope.globalOffset().value() > toOffset) {
          return batch;
        }
        batch.add(envelope);
      }
      cursor = page.getLast().globalOffset();
    }
    return batch;
  }

  /**
   * Outcome of one {@link #replay(ProjectionName, Projection, int)} pass.
   *
   * @param replayed entries successfully processed (or empty) and discarded
   * @param failed entries that failed again and were kept in the store
   * @param fenced entries whose feed returned normally but applied NOTHING — the projection fenced
   *     the whole range out — and were kept in the store; replay them from a fresh process
   */
  public record ReplayResult(int replayed, int failed, int fenced) {}
}
