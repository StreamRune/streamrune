package org.streamrune.core.outbox;

import org.streamrune.core.tracing.TraceScope;

/**
 * Starts the span of one outbox entry's hand-off to its transport. The {@code OutboxPoller} passes
 * one to {@link OutboxPublisher#publishBatch}: the span {@code outbox/publish} when its tracing
 * records the outbox area, {@link #NONE} otherwise. A publisher calls {@link #start} right before
 * it hands an entry to its transport and closes the scope once the hand-off returns.
 */
@FunctionalInterface
public interface OutboxEntrySpans {

  /** Starts no span: every entry gets {@link TraceScope#NOOP}. */
  OutboxEntrySpans NONE = entry -> TraceScope.NOOP;

  /**
   * Starts the span of {@code entry}'s hand-off and makes it current on this thread until the
   * returned scope is closed.
   *
   * @param entry the entry about to be handed to the transport
   * @return the scope to close when the hand-off returns
   */
  TraceScope start(OutboxEntry entry);
}
