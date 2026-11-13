package org.streamrune.runtime;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanBaggage;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;

/**
 * A {@link StreamRuneTracing} that breaks the seam's contract on purpose: every area is on, and the
 * operations named by its {@link Fault}s throw. It counts what it threw and how many scopes it
 * started and closed.
 */
final class ThrowingTracing implements StreamRuneTracing {

  /** The operation that throws. */
  enum Fault {
    ENABLED,
    START,
    SET_ATTRIBUTE,
    RECORD_SUCCESS,
    RECORD_ERROR,
    LINK,
    CLOSE,
    PROPAGATING
  }

  private final Set<Fault> faults;
  private final Supplier<? extends Throwable> failure;
  final AtomicInteger thrown = new AtomicInteger();
  final AtomicInteger started = new AtomicInteger();
  final AtomicInteger closed = new AtomicInteger();

  private ThrowingTracing(Set<Fault> faults, Supplier<? extends Throwable> failure) {
    this.faults = faults;
    this.failure = failure;
  }

  /** A tracing whose {@code faults} throw an {@link IllegalStateException}. */
  static ThrowingTracing on(Fault... faults) {
    return new ThrowingTracing(
        Set.of(faults), () -> new IllegalStateException("the tracing failed"));
  }

  /** A tracing whose {@code faults} throw what {@code failure} supplies, an {@link Error} too. */
  static ThrowingTracing throwing(Supplier<? extends Throwable> failure, Fault... faults) {
    return new ThrowingTracing(Set.of(faults), failure);
  }

  /** A tracing whose every scope method throws; its {@code start} works. */
  static ThrowingTracing onEveryScopeMethod() {
    return on(
        Fault.SET_ATTRIBUTE, Fault.RECORD_SUCCESS, Fault.RECORD_ERROR, Fault.LINK, Fault.CLOSE);
  }

  private void maybeThrow(Fault fault) {
    if (faults.contains(fault)) {
      thrown.incrementAndGet();
      sneakyThrow(failure.get());
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void sneakyThrow(Throwable t) throws T {
    throw (T) t;
  }

  @Override
  public boolean enabled(TracingArea area) {
    maybeThrow(Fault.ENABLED);
    return true;
  }

  @Override
  public TraceScope start(
      TracingArea area,
      String spanName,
      SpanKind kind,
      SpanAttributes attributes,
      SpanLink link,
      SpanBaggage baggage) {
    maybeThrow(Fault.START);
    started.incrementAndGet();
    return new Scope();
  }

  @Override
  public String toString() {
    Throwable sample = failure.get();
    return "ThrowingTracing"
        + new java.util.TreeSet<>(faults)
        + "/"
        + sample.getClass().getSimpleName();
  }

  @Override
  public Runnable propagating(Runnable task) {
    maybeThrow(Fault.PROPAGATING);
    return task;
  }

  private final class Scope implements TraceScope {
    @Override
    public void setAttribute(String key, String value) {
      maybeThrow(Fault.SET_ATTRIBUTE);
    }

    @Override
    public void setAttribute(String key, long value) {
      maybeThrow(Fault.SET_ATTRIBUTE);
    }

    @Override
    public void setAttribute(String key, boolean value) {
      maybeThrow(Fault.SET_ATTRIBUTE);
    }

    @Override
    public void recordSuccess() {
      maybeThrow(Fault.RECORD_SUCCESS);
    }

    @Override
    public void recordError(Throwable error) {
      maybeThrow(Fault.RECORD_ERROR);
    }

    @Override
    public SpanLink link() {
      maybeThrow(Fault.LINK);
      return SpanLink.NONE;
    }

    @Override
    public void close() {
      closed.incrementAndGet();
      maybeThrow(Fault.CLOSE);
    }
  }
}
