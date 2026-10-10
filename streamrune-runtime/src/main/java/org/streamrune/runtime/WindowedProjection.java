package org.streamrune.runtime;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.LateDataPolicy;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.Window;
import org.streamrune.core.projection.WindowResult;
import org.streamrune.core.projection.WindowSink;

/**
 * {@link Projection} decorator. Buckets events into time windows by {@code
 * event.metadata().timestamp()}, accumulates state of type {@code W}, emits via {@link WindowSink}
 * on close + (REOPEN policy) on each late update that arrives while the closed window's state is
 * still retained (until the watermark passes {@code window.end + 2×grace}).
 *
 * <p>Tumbling: {@code slide == size} (default). Sliding: {@code slide < size} — each event lands in
 * {@code ceil(size/slide)} overlapping windows.
 *
 * <p>Watermark = highest event-time seen across all batches. Window closed when {@code watermark
 * &gt;= window.end + grace}. On close: sink fires with {@code isFinal = true}; window state then
 * evicted (under REOPEN, retained until the watermark passes {@code window.end + 2×grace}).
 *
 * <p><b>Cross-restart contract:</b> open-window state is in-memory only. Restart loses open
 * windows.
 *
 * <p><b>Idempotency:</b> accumulation is not idempotent — applying an event twice counts it twice —
 * so this class fences itself on the only axis it can trust, the global offset: {@link
 * #process(List)} ignores any envelope at or below the highest offset it has already accumulated.
 * Register it {@code AT_LEAST_ONCE_IDEMPOTENT}: it writes to memory and is self-fencing on offset;
 * it exposes no write target.
 *
 * <p>That self-fencing is load-bearing, not belt-and-braces. Accumulation happens on the JVM heap
 * inside the updater lambda an {@link org.streamrune.core.projection.AtomicBatchProcessor} runs
 * within its transaction, and a rollback undoes the <em>database</em> work only — the heap keeps
 * the accumulation while the checkpoint stays put. The runners' documented response to an
 * uncommitted batch is to re-read the very same range from the un-advanced checkpoint, so the
 * checkpoint guarantees forward-progressing batches only ACROSS COMMITS, never across the retry of
 * an uncommitted one. At least five ordinary (non-crash) paths re-deliver an identical batch: a
 * transient offset-save failure under HALT, a DLQ transient retry, a {@code
 * ProjectionCommitFencedException} abandon-and-re-read, a leadership-loss abandon-and-re-read, and
 * {@code ScheduledProjectionRunner}'s rethrow-and-re-drain. Without the offset fence each of those
 * doubles a window's aggregate, and the window then closes with {@code isFinal = true} carrying the
 * wrong number — permanently, with nothing to correct it.
 *
 * <p><b>Retry atomicity requires a non-mutating accumulator.</b> Each event is applied
 * buffer-then-commit: its new window states are computed first and written back only after every
 * accumulator call and REOPEN re-emission for that event has returned, so an accumulator or sink
 * that throws leaves the event wholly un-applied and the re-delivered batch applies it exactly
 * once. That holds only when the {@code accumulator} returns a new state and leaves the state it is
 * handed untouched: the state it receives is the live window state held by this instance, so an
 * accumulator that updates it in place has already changed the window when a later callback for the
 * same event throws, and the retry then counts that event twice. The same goes for the value a
 * {@link WindowSink} receives — it is the live state, not a copy; treat it as read-only.
 *
 * <p><b>Rebuilds require a fresh process.</b> The offset fence lives in the heap alongside the
 * windows, so rewinding the checkpoint ({@code OffsetStore.reset}) and letting the same instance
 * re-read is a no-op. That path was already unusable for this class before the fence — the retained
 * watermark pushed every replayed event down the late-past-grace branch — so replay from a
 * restarted process, consistent with the cross-restart contract above.
 *
 * <p><b>Per-instance state.</b> Windows, watermark and the offset fence are all per-JVM. Under
 * multi-replica leadership only the leader accumulates, so a failover starts the new leader's
 * windows empty; this class is for single-writer or restart-tolerant aggregation, not for a read
 * model that must survive a takeover mid-window.
 *
 * <p><b>Thread safety: {@link #process(List)} is mutually exclusive.</b> All of this class's state
 * — {@code openWindows}, {@code closedWindows}, {@code watermark} and the offset fence — is
 * mutable, unshared and read-modify-written across a whole batch, and the framework itself
 * sanctions more than one thread driving the SAME projection instance: a {@code
 * ProjectionDeadLetterReplayer.replay(...)} running while the owning runner is LIVE (the exposure
 * {@code BaseProjection} thread-confines its transactional binding for), and the inline+runner dual
 * registration this class's own runners recommend, where {@code InlineProjectionInterceptor} calls
 * {@code process} on every command thread. Without exclusion those drivers interleave the {@code
 * get → accumulate → put} of a shared window (an event silently vanishes from an aggregate that
 * then closes {@code isFinal = true} and is never corrected), structurally modify the {@code
 * TreeMap} another driver is iterating in the close scan (a {@link
 * java.util.ConcurrentModificationException} escapes {@code process} and aborts the batch, leaving
 * the windows it had already emitted un-removed so the next driver emits them final a second time),
 * and read {@code watermark} / the offset fence with no happens-before.
 *
 * <p>The monitor is held for the whole batch — including the {@code accumulator} and {@link
 * WindowSink} callbacks, which is what makes "accumulate then emit then evict" atomic against a
 * second driver. Windowed aggregation is not a throughput-critical path (batches are already
 * serialized per runner, and a second driver is by definition an exceptional backfill), so the
 * contention cost is deliberate. A sink that blocks therefore blocks the other drivers too, and a
 * sink that acquires further locks must not do so in an order that can invert against this monitor;
 * re-entering {@code process} from the sink on the same thread is safe (the monitor is reentrant)
 * but re-enters the batch state mid-mutation and is not supported.
 */
public final class WindowedProjection<W> implements Projection {

  private static final Logger logger = LoggerFactory.getLogger(WindowedProjection.class);

  private final Duration size;
  private final Duration slide;
  private final Duration grace;
  private final LateDataPolicy lateDataPolicy;
  private final Supplier<W> initialState;
  private final BiFunction<W, EventEnvelope, W> accumulator;
  private final WindowSink<W> sink;

  /** Open window state, keyed by window-start. TreeMap → ordered iteration for close detection. */
  private final TreeMap<Instant, W> openWindows = new TreeMap<>();

  /**
   * Recently-closed window state, keyed by window-start. Retained for REOPEN re-emit semantics.
   * Evicted when watermark exceeds windowEnd + 2×grace (strict greater-than).
   */
  private final TreeMap<Instant, W> closedWindows = new TreeMap<>();

  /** Highest event-time seen so far. */
  private Instant watermark = Instant.MIN;

  /**
   * Highest {@link org.streamrune.core.types.GlobalOffset} already accumulated into a window.
   * Anything at or below it is a re-delivery and is skipped. {@code -1} (not {@code 0}, a legal
   * first offset) so the very first event is always accumulated. Advanced per event, right after it
   * lands in all of its windows, so an accumulator that throws mid-batch leaves the fence exactly
   * at the last fully-applied event and the retry resumes there instead of re-counting the prefix.
   * The event being processed when the throw happens is itself left fully un-applied — its window
   * states are buffered and committed only after its accumulations and late re-emissions have all
   * succeeded — so "at or below the fence" and "accumulated" are the same set even mid-batch, and
   * the retry never re-applies a partially-landed event.
   */
  private long highestAccumulatedOffset = -1L;

  private WindowedProjection(Builder<W> b) {
    this.size = b.size;
    this.slide = b.slide != null ? b.slide : b.size;
    this.grace = b.grace;
    this.lateDataPolicy = b.lateDataPolicy;
    this.initialState = b.initialState;
    this.accumulator = b.accumulator;
    this.sink = b.sink;
  }

  public static <W> Builder<W> builder() {
    return new Builder<>();
  }

  /**
   * {@inheritDoc}
   *
   * <p>{@code synchronized} — see the class javadoc. This is the single mutation entry point:
   * {@link org.streamrune.core.projection.Projection#process(List,
   * org.streamrune.core.projection.ProjectionRepository)} is not overridden, so its default
   * delegation lands here and the transactional driver is covered by the same monitor.
   */
  @Override
  public synchronized void process(List<EventEnvelope> batch) {
    accumulate(batch);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reports {@code false} when the offset fence excluded the WHOLE batch. A dead-lettered range
   * is a hole the live runner skipped past; once later batches have advanced the fence beyond it,
   * the fence — a high-water mark, not a per-offset record — reads the never- accumulated hole as
   * "already counted" and the replay applies nothing. A replayer that discarded the entry on that
   * silent no-op would destroy the range's only record. The fence semantics are deliberately NOT
   * weakened here (a genuinely re-delivered range must still dedup); the replayer keeps the entry
   * instead, and the operator replays from a fresh process (see the class doc's rebuild contract).
   * A PARTIALLY fenced batch reports {@code true}: the fence advances per applied event, so a fence
   * sitting inside the range proves its prefix was genuinely accumulated — only later batches can
   * move it past a hole, and then the whole range is below it.
   */
  @Override
  public synchronized boolean processDeadLetterReplay(List<EventEnvelope> batch) {
    return accumulate(batch) > 0;
  }

  /** Accumulates the batch; returns how many of its events passed the offset fence. */
  private int accumulate(List<EventEnvelope> batch) {
    if (batch.isEmpty()) {
      return 0;
    }

    // Fence out re-deliveries before anything else. A rolled-back commit, a fenced or
    // leadership-lost abandon, a DLQ transient retry and a scheduled re-drain all hand back the
    // SAME range from the un-advanced checkpoint, and the accumulation from the previous attempt
    // is still on the heap — the rollback only undid the database work. Everything at or below
    // the fence is therefore already counted; re-applying it would inflate the window's aggregate,
    // which then closes as final and is never corrected.
    List<EventEnvelope> fresh = new ArrayList<>(batch.size());
    for (var event : batch) {
      if (event.globalOffset().value() > highestAccumulatedOffset) {
        fresh.add(event);
      }
    }
    if (fresh.size() < batch.size()) {
      logger.debug(
          "Skipping {} of {} re-delivered event(s) at or below the highest accumulated offset {}",
          batch.size() - fresh.size(),
          batch.size(),
          highestAccumulatedOffset);
    }
    if (fresh.isEmpty()) {
      // Wholly re-delivered batch: nothing accumulates and the watermark does not move. But this
      // is EXACTLY the shape a retry takes after a sink threw inside the close scan of the previous
      // attempt: that attempt had committed every event, advanced the fence AND the
      // watermark, and aborted the scan with windows still open past end+grace. On a quiescent
      // stream no later event will ever re-run the scan, so those isFinal emissions would be
      // stranded forever if this branch returned early as a complete no-op, sink included.
      // Re-run the close scan and the eviction against the
      // current watermark: both are idempotent per window (a closed window leaves openWindows; a
      // window whose emission threw before it.remove() is re-emitted with the SAME correct value —
      // the duplicate-never-wrong discipline). No accumulation, no fence move, no
      // watermark move, so the buffer-then-commit invariants are untouched.
      closeAndEvict();
      return 0;
    }

    // Snapshot watermark before this batch — used to determine if a window was already
    // closed past grace BEFORE this batch arrived (i.e. the event is truly late).
    Instant preBatchWatermark = watermark;

    // Compute the new watermark from this batch (max event-time seen across all events). NOT
    // assigned to the field until the whole batch has accumulated — see below the loop.
    Instant batchMax = watermark;
    for (var event : fresh) {
      Instant t = event.metadata().timestamp();
      if (t.isAfter(batchMax)) {
        batchMax = t;
      }
    }

    // Accumulate events into their windows. Use preBatchWatermark for "past grace" check so
    // that within-batch out-of-order events are not incorrectly dropped.
    //
    // Buffer-then-commit: an event's application is atomic with its fence advance.
    // Phase 1 computes every owning window's new state WITHOUT touching the maps; phase 2
    // performs the event's REOPEN re-emissions; phase 3 — reached only once every callback has
    // returned — writes the buffered states and advances the fence. A throw from the accumulator
    // or the sink therefore leaves the event fully un-applied (maps unchanged, fence unmoved),
    // and the re-delivered batch re-applies it cleanly instead of double-counting the windows an
    // earlier iteration had already mutated. Emissions deliberately fire BEFORE the commit: were
    // they after it, a throwing sink would strand an already-fenced event and the retry would
    // fence-skip its re-emission entirely (silent under-emission). The cost of this ordering is
    // a possible duplicate of the SAME correct re-emission when a sink throws after delivering —
    // never a wrong value, because the value is recomputed from the unmutated maps.
    for (var event : fresh) {
      Instant t = event.metadata().timestamp();

      List<PendingState<W>> pending = new ArrayList<>(2);
      List<WindowResult<W>> reEmissions = new ArrayList<>(1);

      // Phase 1: compute. No map mutation, no sink call.
      for (Instant windowStart : owningWindowStarts(t)) {
        Instant windowEnd = windowStart.plus(size);
        boolean closedPastGrace = !preBatchWatermark.isBefore(windowEnd.plus(grace));

        if (closedPastGrace) {
          // Late event past grace: handle per policy
          if (lateDataPolicy == LateDataPolicy.DROP) {
            logger.warn(
                "Dropping late event {} for window [{},{}) (DROP policy)",
                event.metadata().eventId().value(),
                windowStart,
                windowEnd);
            continue;
          }
          W closed = closedWindows.get(windowStart);
          if (closed == null) {
            logger.warn(
                "Dropping late event {} for window [{},{}) (REOPEN but state evicted)",
                event.metadata().eventId().value(),
                windowStart,
                windowEnd);
            continue;
          }
          closed = accumulator.apply(closed, event);
          pending.add(new PendingState<>(windowStart, closed, true));
          reEmissions.add(new WindowResult<>(new Window(windowStart, windowEnd), closed, false));
          continue;
        }

        W state = openWindows.get(windowStart);
        if (state == null) {
          state = initialState.get();
        }
        state = accumulator.apply(state, event);
        pending.add(new PendingState<>(windowStart, state, false));
      }

      // Phase 2: emit this event's REOPEN re-emissions while it is still fully un-applied.
      for (var emission : reEmissions) {
        sink.onWindow(emission);
      }

      // Phase 3: commit. Heap-map puts on Instant keys cannot fail, so the event lands whole.
      for (var p : pending) {
        if (p.intoClosed()) {
          closedWindows.put(p.windowStart(), p.state());
        } else {
          openWindows.put(p.windowStart(), p.state());
        }
      }

      // Advance the fence only once this event has landed in every window that owns it. An
      // accumulator that throws part-way through the batch therefore leaves the fence at the last
      // fully-applied event, and the re-delivered batch resumes from there rather than re-counting
      // the prefix it already applied.
      highestAccumulatedOffset = Math.max(highestAccumulatedOffset, event.globalOffset().value());
    }

    // The watermark advances only after every fresh event has committed. It feeds the
    // next attempt's preBatchWatermark, and that classification must be identical on the retry of
    // a thrown batch — an advance made before the loop would re-classify the still-un-applied
    // events as late-past-grace on retry and silently drop them (their windows are still OPEN, so
    // the REOPEN lookup finds no closed state). After a mid-batch throw the field can lag the
    // committed prefix's timestamps until a later event tops them — the safe direction: windows
    // close later, never earlier, and the close scan and the lateness check read the same field,
    // so an open window is never treated as closed.
    watermark = batchMax;

    closeAndEvict();
    return fresh.size();
  }

  /**
   * Closes every open window the current watermark has passed ({@code end + grace}) — emitting it
   * {@code isFinal} and, under REOPEN, retaining its state for late re-emission — then evicts
   * closed state too old to re-emit ({@code watermark > end + 2×grace}). Idempotent per window
   * against the SAME watermark, which is what lets the wholly-fenced retry path call it again after
   * a sink threw mid-scan: a window emitted-then-removed is gone; a window whose emission threw
   * before {@code it.remove()} is re-emitted with the same value.
   */
  private void closeAndEvict() {
    // Close all windows whose watermark passed end+grace
    Iterator<Map.Entry<Instant, W>> it = openWindows.entrySet().iterator();
    while (it.hasNext()) {
      var entry = it.next();
      Instant winEnd = entry.getKey().plus(size);
      if (!watermark.isBefore(winEnd.plus(grace))) {
        sink.onWindow(
            new WindowResult<>(new Window(entry.getKey(), winEnd), entry.getValue(), true));
        if (lateDataPolicy == LateDataPolicy.REOPEN) {
          closedWindows.put(entry.getKey(), entry.getValue());
        }
        it.remove();
      }
    }

    // Evict closed window state that is too old for re-emission (watermark > end + 2×grace)
    if (!closedWindows.isEmpty()) {
      Iterator<Map.Entry<Instant, W>> cit = closedWindows.entrySet().iterator();
      while (cit.hasNext()) {
        var entry = cit.next();
        Instant evictAfter = entry.getKey().plus(size).plus(grace).plus(grace);
        if (watermark.isAfter(evictAfter)) {
          cit.remove();
        }
      }
    }
  }

  /**
   * Returns all window-start instants whose window contains {@code t}. Tumbling: single window.
   * Sliding: multiple overlapping windows.
   */
  private List<Instant> owningWindowStarts(Instant t) {
    long sizeMillis = size.toMillis();
    long slideMillis = slide.toMillis();
    long tMillis = t.toEpochMilli();

    if (slideMillis >= sizeMillis) {
      // Tumbling
      long start = Math.floorDiv(tMillis, sizeMillis) * sizeMillis;
      return List.of(Instant.ofEpochMilli(start));
    }

    // Sliding: enumerate all starts s such that s <= t < s + size, with stride = slide
    // First start = floor((t - size + slide) / slide) * slide
    long firstStart = Math.floorDiv(tMillis - sizeMillis + slideMillis, slideMillis) * slideMillis;
    var starts = new ArrayList<Instant>();
    for (long s = firstStart; s <= tMillis; s += slideMillis) {
      if (s + sizeMillis > tMillis) {
        starts.add(Instant.ofEpochMilli(s));
      }
    }
    return starts;
  }

  /**
   * One owning window's not-yet-committed new state for the event being processed. {@code
   * intoClosed} selects the destination map — {@code true} for a REOPEN late update to {@link
   * #closedWindows}, {@code false} for {@link #openWindows}.
   */
  private record PendingState<W>(Instant windowStart, W state, boolean intoClosed) {}

  public static final class Builder<W> {
    private Duration size;
    private Duration slide;
    private Duration grace = Duration.ofMinutes(5);
    private LateDataPolicy lateDataPolicy = LateDataPolicy.REOPEN;
    private Supplier<W> initialState;
    private BiFunction<W, EventEnvelope, W> accumulator;
    private WindowSink<W> sink;

    public Builder<W> size(Duration size) {
      this.size = size;
      return this;
    }

    public Builder<W> slide(Duration slide) {
      this.slide = slide;
      return this;
    }

    public Builder<W> grace(Duration grace) {
      this.grace = grace;
      return this;
    }

    public Builder<W> lateData(LateDataPolicy policy) {
      this.lateDataPolicy = policy;
      return this;
    }

    public Builder<W> initialState(Supplier<W> initialState) {
      this.initialState = initialState;
      return this;
    }

    /**
     * The fold applied to each event for every window that owns it: {@code (state, event) ->
     * newState}. Required. It must return a new state and must not mutate the {@code state} it is
     * handed: that argument is the live window state, and the retry atomicity described on the
     * class (an event whose accumulator or sink call throws is left wholly un-applied) holds only
     * when the accumulator leaves it untouched. An in-place update is already visible when a later
     * callback for the same event throws, and the re-delivered batch then counts that event twice.
     */
    public Builder<W> accumulator(BiFunction<W, EventEnvelope, W> accumulator) {
      this.accumulator = accumulator;
      return this;
    }

    public Builder<W> sink(WindowSink<W> sink) {
      this.sink = sink;
      return this;
    }

    public WindowedProjection<W> build() {
      if (size == null) {
        throw new IllegalArgumentException("size is required");
      }
      if (size.isZero() || size.isNegative()) {
        throw new IllegalArgumentException("size must be > 0");
      }
      if (slide != null && (slide.isZero() || slide.isNegative())) {
        throw new IllegalArgumentException("slide must be > 0");
      }
      if (grace == null || grace.isNegative()) {
        throw new IllegalArgumentException("grace must be >= 0");
      }
      if (lateDataPolicy == null) {
        throw new IllegalArgumentException("lateDataPolicy is required");
      }
      if (initialState == null) {
        throw new IllegalArgumentException("initialState is required");
      }
      if (accumulator == null) {
        throw new IllegalArgumentException("accumulator is required");
      }
      if (sink == null) {
        throw new IllegalArgumentException("sink is required");
      }
      return new WindowedProjection<>(this);
    }
  }
}
