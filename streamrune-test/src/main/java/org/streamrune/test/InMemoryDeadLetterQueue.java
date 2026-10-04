package org.streamrune.test;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.types.CommandId;

/**
 * In-memory {@link DeadLetterQueue} for unit tests.
 *
 * <p>Thread-safe via {@link CopyOnWriteArrayList}. Not suitable for production use.
 *
 * <p>{@code publishedAt} (the sort key of {@link #read} and {@link #readRetryable}) is stamped from
 * the injected {@link Clock} — pass a {@link MutableClock} for deterministic ordering. The no-arg
 * constructor uses {@link Clock#systemUTC()}.
 *
 * <p><b>Concurrency divergence from PostgresDeadLetterQueue (documented, not modeled).</b> The
 * production {@code PostgresDeadLetterQueue#readRetryable} selects rows with {@code FOR UPDATE SKIP
 * LOCKED} so that two retry schedulers polling at the <em>same instant</em> never pick up the same
 * rows. That row lock is held only for the duration of the read transaction and is released the
 * moment the connection is returned; it is <em>not</em> a persistent claim. Consequently — in both
 * production and this double — a <em>sequential</em> second {@link #readRetryable} call returns the
 * same entry again: nothing has been claimed, only locked-then-released. An entry stops being
 * returned solely because {@link #updateAttempts} pushed its {@code dlqAttempts} to or above {@code
 * maxRetries}, or it was {@link #discard discarded} — exactly how {@code DeadLetterRetryRunner}
 * drives the queue. This double therefore reproduces the production sequential behavior faithfully.
 *
 * <p>The single behavior an in-memory double cannot reproduce is the true <em>concurrent</em>
 * row-lock: two threads calling {@link #readRetryable} at the same instant here both observe every
 * eligible entry (no SKIP LOCKED), so a test must not rely on this double to deduplicate concurrent
 * pollers. Modeling a persistent claimed-set was deliberately rejected: production releases the
 * lock on commit, so a non-releasing claim would itself diverge (a second sequential poll would
 * wrongly see nothing). The faithful, lighter choice is to match the sequential contract and
 * document the concurrent gap here. See {@link InMemoryOutboxStore} for a store whose production
 * counterpart <em>does</em> persist claims and which therefore models them.
 *
 * <p>Use {@link #all()} in assertions to inspect all stored entries directly.
 */
public final class InMemoryDeadLetterQueue implements DeadLetterQueue {

  private final CopyOnWriteArrayList<DeadLetterEntry> entries = new CopyOnWriteArrayList<>();
  private final Clock clock;

  /** Creates a queue reading time from {@link Clock#systemUTC()}. */
  public InMemoryDeadLetterQueue() {
    this(Clock.systemUTC());
  }

  /** Creates a queue reading time from the given clock (e.g. a {@link MutableClock}). */
  public InMemoryDeadLetterQueue(Clock clock) {
    this.clock = Objects.requireNonNull(clock, "clock is required");
  }

  @Override
  public void publish(DeadLetterPublishRequest request) {
    Objects.requireNonNull(request, "request is required");
    entries.add(
        new DeadLetterEntry(
            request.commandId(),
            request.commandType(),
            request.commandPayload(),
            request.streamId(),
            request.errorType(),
            request.errorMessage(),
            request.attempts(),
            request.timestamp(),
            clock.instant(),
            0,
            null,
            request.correlationId(),
            request.userId(),
            request.traceId(),
            request.idempotencyKey()));
  }

  @Override
  public List<DeadLetterEntry> read(int limit) {
    return entries.stream()
        .sorted(Comparator.comparing(DeadLetterEntry::publishedAt).reversed())
        .limit(limit)
        .toList();
  }

  @Override
  public long countPending() {
    return entries.size();
  }

  @Override
  public void discard(CommandId commandId) {
    Objects.requireNonNull(commandId, "commandId is required");
    entries.removeIf(e -> e.commandId().equals(commandId));
  }

  @Override
  public void updateAttempts(CommandId commandId, int dlqAttempts, Instant lastAttemptAt) {
    Objects.requireNonNull(commandId, "commandId is required");
    for (int i = 0; i < entries.size(); i++) {
      DeadLetterEntry e = entries.get(i);
      if (e.commandId().equals(commandId)) {
        entries.set(
            i,
            new DeadLetterEntry(
                e.commandId(),
                e.commandType(),
                e.commandPayload(),
                e.streamId(),
                e.errorType(),
                e.errorMessage(),
                e.attempts(),
                e.firstAttemptAt(),
                e.publishedAt(),
                dlqAttempts,
                lastAttemptAt,
                e.correlationId(),
                e.userId(),
                e.traceId(),
                e.idempotencyKey()));
        return;
      }
    }
  }

  @Override
  public List<DeadLetterEntry> readRetryable(int maxRetries, int limit) {
    // Order by least-recently-active — the most recent of (lastAttemptAt,
    // publishedAt) — ascending, so the entries most likely to be past their backoff sort first and
    // a recently-attempted (still backing-off) entry never sits at the head starving due entries
    // behind it. Mirrors PostgresDeadLetterQueue.SELECT_RETRYABLE's
    // `ORDER BY COALESCE(last_attempt_at, published_at) ASC`.
    return entries.stream()
        .filter(e -> e.dlqAttempts() < maxRetries)
        .sorted(Comparator.comparing(InMemoryDeadLetterQueue::lastActivity))
        .limit(limit)
        .toList();
  }

  /**
   * The COALESCE(last_attempt_at, published_at) sort key: last attempt if retried, else publish.
   */
  private static Instant lastActivity(DeadLetterEntry entry) {
    return entry.lastAttemptAt() != null ? entry.lastAttemptAt() : entry.publishedAt();
  }

  @Override
  public int deleteOlderThan(Instant cutoff) {
    Objects.requireNonNull(cutoff, "cutoff is required");
    int before = entries.size();
    entries.removeIf(e -> e.publishedAt().isBefore(cutoff));
    return before - entries.size();
  }

  /** Returns all entries regardless of order. Use in test assertions. */
  public List<DeadLetterEntry> all() {
    return List.copyOf(entries);
  }
}
