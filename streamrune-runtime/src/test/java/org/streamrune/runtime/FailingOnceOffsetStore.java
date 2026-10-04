package org.streamrune.runtime;

import java.util.concurrent.atomic.AtomicInteger;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/** Fails the first {@code saveOffset} (store outage), then forwards. Counts every attempt. */
final class FailingOnceOffsetStore implements OffsetStore {
  private final OffsetStore delegate;
  final AtomicInteger saveAttempts = new AtomicInteger();
  private volatile boolean failed;

  FailingOnceOffsetStore(OffsetStore delegate) {
    this.delegate = delegate;
  }

  @Override
  public GlobalOffset getLastOffset(ProjectionName projectionName) {
    return delegate.getLastOffset(projectionName);
  }

  @Override
  public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
    saveAttempts.incrementAndGet();
    if (!failed) {
      failed = true;
      throw new IllegalStateException("offset store down");
    }
    delegate.saveOffset(projectionName, offset);
  }

  @Override
  public void reset(ProjectionName projectionName) {
    delegate.reset(projectionName);
  }
}
