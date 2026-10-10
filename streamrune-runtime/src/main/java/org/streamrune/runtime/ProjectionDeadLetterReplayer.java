package org.streamrune.runtime;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * Replays dead-lettered projection batches back through their projection. This is the recovery half
 * of the projection DLQ: the runners only <em>write</em> entries. Under the DLQ error strategy both
 * {@link ScheduledProjectionRunner} and {@link ContinuousProjectionRunner} (in catch-up and in live
 * mode) dead-letter a failed batch, advance the checkpoint past it (leaving a hole in the read
 * model), and continue. Once the underlying cause is fixed (bug deployed, poison data corrected),
 * an operator invokes {@link #replay(ProjectionName, Projection, ProjectionDeliveryMode, int)} to
 * re-read each dead-lettered range from the event store, run it through the projection, and discard
 * the entry on success.
 *
 * <p>Replay is explicit and operator-driven by design — there is no background loop. A
 * dead-lettered batch failed deterministically; retrying it on a schedule before the cause is fixed
 * would only burn cycles and spam logs.
 *
 * <p><b>Serialized with the live runner.</b> {@link #replay} feeds each range through {@link
 * AtomicBatchProcessor#executeReplay} on the processor the projection's runner commits through, so
 * the replay takes the lock a live batch takes — the projection's checkpoint row, {@code FOR
 * UPDATE}, on {@code JdbcProjectionRepository}. A replay and a live batch of one projection never
 * run at the same time, in one process or across replicas: whichever comes second reads the rows
 * the first one committed. That is what keeps a read-modify-write projection ({@code findById} →
 * change → {@code save}) correct; without the lock both sides can read a row before either saves
 * it, and one of the two writes is lost although neither side applied anything twice. The
 * projection is handed what a live batch of its registration is handed: the replay transaction's
 * repository under {@code TRANSACTIONAL_LOCAL} and {@code EXTERNAL_EFFECT}, so the range is applied
 * all-or-nothing, and {@code null} under {@code AT_LEAST_ONCE_IDEMPOTENT}, where the projection
 * writes through its own repository while the replay holds the lock.
 *
 * <p><b>A processor without that lock is refused.</b> {@link
 * AtomicBatchProcessor#nonAtomicAtLeastOnce()} holds no per-projection lock, so {@link #replay}
 * throws for it before it reads an entry: running the range beside a live batch would be the lost
 * update above, and idempotency does not prevent it. Stop the projection's runner and call {@link
 * #replayWithRunnerStopped}; the name is the caller's statement that nothing else is processing the
 * projection.
 *
 * <p><b>Ordering and idempotency.</b> Entries replay oldest range first, but later events were
 * already processed when the failed batch was skipped past, so a replay is an out-of-order patch of
 * the read model, and it is at-least-once (see the crash points below). {@link
 * Projection#process(List)} must therefore be idempotent under every delivery mode. Replay
 * backfills the hole <em>behind</em> the checkpoint without moving it; consumers of the read model
 * must tolerate a range being filled in after later ranges are already visible.
 *
 * <p><b>Crash points.</b> For each entry the replay commits first and discards the entry second:
 *
 * <ul>
 *   <li>A crash before the replay commits. Under {@code TRANSACTIONAL_LOCAL} and {@code
 *       EXTERNAL_EFFECT} the transaction rolls back and the read model is unchanged; under {@code
 *       AT_LEAST_ONCE_IDEMPOTENT} the writes the projection made before the crash stand. The entry
 *       is still queued, and the next replay applies the whole range.
 *   <li>A crash after the replay commits and before the entry is discarded (or a discard that
 *       fails). The range is applied and the entry is still queued: the next replay applies the
 *       range a second time, which an idempotent projection absorbs, and then discards the entry.
 *   <li>A crash after the discard. The range is applied and the entry is gone; nothing is left to
 *       do.
 * </ul>
 *
 * <p>The checkpoint is not written at any of these points, so a rerun starts from the same
 * dead-letter store and converges on a read model with the range applied and no entry.
 *
 * <p><b>Empty ranges.</b> An entry whose range reads back empty — no event of it is left in the
 * event store — is discarded without processing: there is nothing left to apply. A GDPR erasure
 * does not empty a range: crypto-shredding destroys the subject's key, not the events, so the
 * erased subject's events read back with their encrypted fields as {@code [REDACTED]} ({@code
 * CryptoShreddingModule}'s fallback, reported on {@code streamrune.crypto.subject_redacted}) and
 * are fed to the projection like any other event. An event whose type rejects the {@code
 * [REDACTED]} value fails the read instead, and its entry is kept and counted in {@link
 * ReplayResult#failed()}.
 *
 * <p><b>A feed that applied nothing keeps the entry.</b> The batch is fed through {@link
 * Projection#processDeadLetterReplay(List, org.streamrune.core.projection.ProjectionRepository)},
 * which reports whether the projection applied anything; discard happens only on a non-exceptional
 * feed that DID apply. A self-fencing projection — {@link WindowedProjection}, which dedups on the
 * highest offset it ever accumulated — fences a dead-lettered range out wholesale once the live
 * runner has advanced its fence past the hole the entry records, so its {@code process} returns
 * normally having changed nothing. Such entries are counted as {@link ReplayResult#fenced()} and
 * kept, with the remedy logged: replay from a fresh process (the fence and the windows are per-JVM
 * — see {@code WindowedProjection}'s rebuild contract), before the live runner advances again. The
 * fence itself is deliberately not weakened — a genuinely re-delivered range must still dedup.
 */
public final class ProjectionDeadLetterReplayer {

  private static final Logger logger = LoggerFactory.getLogger(ProjectionDeadLetterReplayer.class);

  private final EventStore eventStore;
  private final ProjectionDeadLetterStore deadLetterStore;
  private final AtomicBatchProcessor processor;

  /**
   * Creates a replayer.
   *
   * @param eventStore the event store to re-read dead-lettered ranges from
   * @param deadLetterStore the store holding the entries to replay
   * @param processor the processor the runner of the replayed projections commits through; its
   *     per-projection lock is what serializes a replay with a live batch, so a different processor
   *     here serializes nothing
   */
  public ProjectionDeadLetterReplayer(
      EventStore eventStore,
      ProjectionDeadLetterStore deadLetterStore,
      AtomicBatchProcessor processor) {
    if (eventStore == null) throw new IllegalArgumentException("eventStore is required");
    if (deadLetterStore == null) throw new IllegalArgumentException("deadLetterStore is required");
    if (processor == null) {
      throw new IllegalArgumentException(
          "processor is required: pass the AtomicBatchProcessor the projection's runner commits"
              + " through");
    }
    this.eventStore = eventStore;
    this.deadLetterStore = deadLetterStore;
    this.processor = processor;
  }

  /**
   * Replays up to {@code maxEntries} dead-letter entries for the given projection, oldest range
   * first, each under the processor's per-projection lock. For each entry the original {@code
   * [fromOffset, toOffset]} range is re-read from the global stream and processed through the
   * projection; on success the entry is discarded. A failing entry is kept (and logged) and the
   * remaining entries are still attempted, so one still-poisoned batch does not block recovery of
   * the others.
   *
   * <p>Safe while the projection's runner is live. The projection's offset checkpoint is never
   * touched — replay fills holes behind the checkpoint, it does not move it.
   *
   * @param projectionName the projection whose dead letters to replay; must not be null
   * @param projection the projection to feed the re-read batches to; must not be null
   * @param deliveryMode the delivery mode the projection is registered with; decides what the
   *     projection is handed, exactly as for a live batch
   * @param maxEntries maximum number of entries to replay in this pass; must be > 0
   * @return how many entries were replayed and discarded, how many failed and were kept, and how
   *     many the projection fenced out wholesale and were kept
   * @throws IllegalArgumentException if the projection cannot run under {@code deliveryMode} on
   *     this replayer's processor (the check a runner makes at registration)
   * @throws IllegalStateException if the processor cannot serialize a replay with a live batch
   *     ({@link AtomicBatchProcessor#serializesReplay()} is {@code false})
   */
  public ReplayResult replay(
      ProjectionName projectionName,
      Projection projection,
      ProjectionDeliveryMode deliveryMode,
      int maxEntries) {
    requireArguments(projectionName, projection, maxEntries);
    ProjectionDeliveryPolicy.require(
        projection,
        deliveryMode,
        processor,
        "ProjectionDeadLetterReplayer",
        projectionName.value());
    if (!processor.serializesReplay()) {
      throw new IllegalStateException(
          "Dead letters of projection '"
              + LogSanitizer.sanitizeForLog(projectionName.value())
              + "' cannot be replayed beside a live runner on "
              + ProjectionDeliveryPolicy.processorLabel(processor)
              + ": that processor holds no per-projection lock, so the replay and a live batch"
              + " could both read a row before either saves it and one of the two writes would be"
              + " lost — idempotency does not prevent that. Stop the projection's runner and call"
              + " replayWithRunnerStopped(...), or run the projection on a processor that"
              + " serializes replays (JdbcProjectionRepository).");
    }
    boolean transactional = deliveryMode.writesInCheckpointTransaction();
    return replayEntries(
        projectionName,
        maxEntries,
        batch -> {
          if (transactional) {
            processor.prepareReadModel(projectionName);
          }
          boolean[] applied = {false};
          processor.executeReplay(
              projectionName,
              batch,
              txRepository ->
                  applied[0] =
                      projection.processDeadLetterReplay(
                          batch, transactional ? txRepository : null));
          return applied[0];
        });
  }

  /**
   * Replays up to {@code maxEntries} dead-letter entries for a projection whose processor holds no
   * per-projection lock ({@link AtomicBatchProcessor#nonAtomicAtLeastOnce()}), with no lock and no
   * transaction: each range is fed to {@code projection.processDeadLetterReplay(batch, null)}.
   *
   * <p><b>The caller guarantees that no runner is processing this projection</b>, in this process
   * or any other, for the whole call. Nothing here can verify it. A batch that runs beside this
   * replay can read a row before the replay saves it and overwrite the replayed change, or the
   * other way round, and the entry is discarded either way.
   *
   * <p>Refused on a processor whose {@link AtomicBatchProcessor#serializesReplay()} is {@code
   * true}: there {@link #replay} gives the same result with the runner live or stopped, and applies
   * a transactional registration's range all-or-nothing.
   *
   * @param projectionName the projection whose dead letters to replay; must not be null
   * @param projection the projection to feed the re-read batches to; must not be null
   * @param maxEntries maximum number of entries to replay in this pass; must be > 0
   * @return how many entries were replayed and discarded, how many failed and were kept, and how
   *     many the projection fenced out wholesale and were kept
   * @throws IllegalStateException if the processor serializes replays
   */
  public ReplayResult replayWithRunnerStopped(
      ProjectionName projectionName, Projection projection, int maxEntries) {
    requireArguments(projectionName, projection, maxEntries);
    if (processor.serializesReplay()) {
      throw new IllegalStateException(
          "replayWithRunnerStopped is for a processor without a per-projection lock; "
              + ProjectionDeliveryPolicy.processorLabel(processor)
              + " serializes a replay with a live batch, so call replay(...) for projection '"
              + LogSanitizer.sanitizeForLog(projectionName.value())
              + "' — with the runner live or stopped.");
    }
    return replayEntries(
        projectionName, maxEntries, batch -> projection.processDeadLetterReplay(batch, null));
  }

  private static void requireArguments(
      ProjectionName projectionName, Projection projection, int maxEntries) {
    if (projectionName == null) {
      throw new IllegalArgumentException("projectionName is required");
    }
    if (projection == null) throw new IllegalArgumentException("projection is required");
    if (maxEntries <= 0) throw new IllegalArgumentException("maxEntries must be positive");
  }

  /**
   * The replay pass shared by both entry points: read the entries, re-read each range, hand it to
   * {@code feed} (which reports whether the projection applied anything) and discard the entry
   * after a feed that applied.
   */
  private ReplayResult replayEntries(
      ProjectionName projectionName, int maxEntries, Predicate<List<EventEnvelope>> feed) {
    String name = LogSanitizer.sanitizeForLog(projectionName.value());
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
              "Dead-letter batch [{}-{}] for projection '{}' is not in the event store (no event of"
                  + " the range could be read back) — discarding the entry",
              entry.fromOffset().value(),
              entry.toOffset().value(),
              name);
        } else if (!feed.test(batch)) {
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
              name);
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
            name,
            e);
      }
    }
    return new ReplayResult(replayed, failed, fenced);
  }

  /**
   * Re-reads the entry's inclusive {@code [fromOffset, toOffset]} range from the global stream,
   * paging until the range end is reached. The store may return shorter pages than requested, so
   * the loop follows the offsets, not the counts. {@code PostgresEventStore} commits offsets
   * without gaps, and a dead-lettered range lies behind the checkpoint, below any append still in
   * flight; an empty page therefore means the store holds none of the remaining events of the
   * range.
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
   * Outcome of one replay pass.
   *
   * @param replayed entries successfully processed (or empty) and discarded
   * @param failed entries that failed again and were kept in the store
   * @param fenced entries whose feed returned normally but applied NOTHING — the projection fenced
   *     the whole range out — and were kept in the store; replay them from a fresh process
   */
  public record ReplayResult(int replayed, int failed, int fenced) {}
}
