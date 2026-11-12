package org.streamrune.core.outbox;

import org.streamrune.core.tracing.TraceScope;

/**
 * Starts the span of one outbox entry's hand-off to its transport. The {@code OutboxPoller} passes
 * one to {@link OutboxPublisher#publishBatch}: the span {@code outbox/publish} when its tracing
 * records the outbox area, {@link #NONE} otherwise. A publisher calls {@link #start} right before
 * it hands an entry to its transport and closes the scope once the hand-off returns.
 *
 * <p><b>Threading.</b> A publisher starts and closes each entry's scope on the publishing thread,
 * one entry at a time, and closes a scope before the next {@code start}: a scope makes its span
 * current on the thread that started it, so a broker client's own spans nest under it.
 *
 * <p><b>Status.</b> {@code recordSuccess} means that the transport accepted the hand-off, {@code
 * recordError} that the hand-off threw. An outcome the transport reports for the whole batch later
 * — a RabbitMQ nack or a confirm timeout — is not on the entry's span: it counts in {@code
 * streamrune.outbox.failed} of the {@code outbox/relay} span.
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
