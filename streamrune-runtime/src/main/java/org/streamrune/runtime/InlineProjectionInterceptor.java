package org.streamrune.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.types.LogSanitizer;

/**
 * {@link CommandInterceptor} that runs registered projections inline after {@code
 * eventStore.append()} succeeds, on the calling thread.
 *
 * <p><b>Contract (explicit):</b> INLINE is <b>post-commit</b> (events are durable before a
 * projection ever runs), <b>same-JVM</b> (it runs on the thread that called {@link
 * org.streamrune.core.CommandBus#execute}, with no cross-process coordination), and
 * <b>per-aggregate ordered</b> — {@link VirtualThreadCommandBus} holds the target aggregate's lock
 * for this interceptor's entire {@link #after(CommandContext)} call, so two commands committed
 * against the SAME aggregate apply their inline projections in commit order; commands on DIFFERENT
 * aggregates give no ordering guarantee relative to each other. INLINE has <b>no recovery path</b>:
 * a throwing {@link Projection#process(List)} is logged and swallowed by the command bus (see
 * Failure isolation below) — the command still reports success, because the event log, not the
 * projection, is the bus's source of truth. There is no offset, no checkpoint, and nothing retries
 * a failed inline update. For a read model that must stay correct even after a transient projection
 * failure — i.e. an <b>authoritative</b> read model — register the SAME projection logic with a
 * durable runner ({@code CONTINUOUS} via {@link MultiProjectionRunner} or {@code SCHEDULED} via
 * {@link ScheduledProjectionRunner}) backed by a transactional {@link
 * org.streamrune.core.projection.ProjectionRepository}, and treat INLINE purely as a low-latency,
 * best-effort head start that the durable runner's offset replay reconciles.
 *
 * <p><b>Semantics — weak inline:</b> events ARE persisted before projection runs. A projection
 * failure propagates out of {@link #after(CommandContext)}; the command bus logs it and the command
 * still reports success (the events are committed — see the {@link CommandInterceptor} lifecycle
 * contract). Continuous / scheduled runner catches up via offset replay — projections registered
 * here SHOULD also be registered with a {@link MultiProjectionRunner} or {@link
 * ScheduledProjectionRunner} so a failed inline update is eventually applied.
 *
 * <p><b>Failure isolation:</b> each registered projection is attempted even when an earlier one
 * fails — one broken projection must not starve its siblings of an update the events already paid
 * for. Every failure is logged with its registration name; the first failure (with later failures
 * attached as suppressed exceptions) propagates after all registrations ran.
 *
 * <p><b>Locking / reentrancy:</b> {@code VirtualThreadCommandBus} tracks, per thread, which
 * aggregate locks that thread already holds, so a projection here that dispatches a follow-up
 * command against the SAME aggregate on the SAME thread re-enters instead of deadlocking or
 * self-blocking — reentrant on EVERY configured {@link org.streamrune.core.AggregateLocker}, not
 * only a {@link LocalStripedLocker} whose {@link java.util.concurrent.locks.ReentrantLock} happens
 * to re-enter for free. This matters because the documented multi-instance production locker,
 * {@code PgAdvisoryLocker}, checks out a NEW pool connection and takes a session-scoped advisory
 * lock on every acquisition — without the bus's tracking, the identical same-aggregate nested
 * dispatch would self-block on it instead. A follow-up dispatched from a DIFFERENT thread — or
 * against a DIFFERENT aggregate — still contends for a real, independent lock and waits for this
 * call to finish, bounded by the bus's configured lock timeout like any other lock wait; it does
 * not hang indefinitely.
 *
 * <p>Short-circuited commands (see {@link
 * org.streamrune.core.CommandBus.CommandResult#shortCircuited()}) are skipped — nothing was
 * executed, so there is nothing to project. This also covers a successful idempotent replay: it
 * appends no new events and carries no envelopes, so a registered projection is not re-run for it.
 */
public final class InlineProjectionInterceptor implements CommandInterceptor {

  private static final Logger logger = LoggerFactory.getLogger(InlineProjectionInterceptor.class);

  private final List<Registration> registrations;

  private InlineProjectionInterceptor(List<Registration> registrations) {
    this.registrations = List.copyOf(registrations);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void after(CommandContext ctx) {
    var result = ctx.result();
    if (result == null || result.shortCircuited()) {
      // No command was executed (short-circuit) or no result is available — nothing to project.
      return;
    }
    var envelopes = result.envelopes();
    RuntimeException firstFailure = null;
    for (var reg : registrations) {
      try {
        reg.projection().process(envelopes);
      } catch (RuntimeException e) {
        logger.error(
            "Inline projection '{}' failed for command {} — the events are committed and the"
                + " remaining inline projections still run",
            reg.name(),
            LogSanitizer.sanitizeForLog(ctx.commandId().value()),
            e);
        if (firstFailure == null) {
          firstFailure = e;
        } else {
          firstFailure.addSuppressed(e);
        }
      }
    }
    if (firstFailure != null) {
      throw firstFailure;
    }
  }

  private record Registration(String name, Projection projection) {}

  public static final class Builder {
    private final List<Registration> registrations = new ArrayList<>();
    private final AtomicInteger anonCounter = new AtomicInteger();

    public Builder register(Projection projection) {
      Objects.requireNonNull(projection, "projection");
      registrations.add(new Registration("inline-" + anonCounter.incrementAndGet(), projection));
      return this;
    }

    public Builder register(String name, Projection projection) {
      if (name == null || name.isBlank()) {
        throw new IllegalArgumentException("name is required");
      }
      Objects.requireNonNull(projection, "projection");
      registrations.add(new Registration(name, projection));
      return this;
    }

    public InlineProjectionInterceptor build() {
      if (registrations.isEmpty()) {
        throw new IllegalArgumentException("at least one projection must be registered");
      }
      return new InlineProjectionInterceptor(registrations);
    }
  }
}
