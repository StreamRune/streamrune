package org.streamrune.core.tracing;

/** {@link TraceScope#NOOP}: a scope whose methods do nothing. */
final class NoopTraceScope implements TraceScope {

  @Override
  public void setAttribute(String key, String value) {
    // not recorded
  }

  @Override
  public void setAttribute(String key, long value) {
    // not recorded
  }

  @Override
  public void setAttribute(String key, boolean value) {
    // not recorded
  }

  @Override
  public void recordSuccess() {
    // not recorded
  }

  @Override
  public void recordError(Throwable error) {
    // not recorded
  }

  @Override
  public void close() {
    // nothing to end
  }

  @Override
  public String toString() {
    return "TraceScope.NOOP";
  }
}
