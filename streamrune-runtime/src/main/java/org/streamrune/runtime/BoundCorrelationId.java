package org.streamrune.runtime;

import org.slf4j.Logger;
import org.streamrune.core.ReservedCorrelationIdException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.CorrelationId;

/**
 * The two rules the buses of this module share about the correlation id bound to a dispatch.
 *
 * <p><b>Which id a dispatch records.</b> {@link #inEffect()}: the bound saga origin's when an
 * origin is bound, otherwise the bound request context's. A command with neither has the id the
 * command bus generates for it before its interceptors run ({@link #ofTheCommandInProgress()}). The
 * command bus writes that id into the events of the command and the command audit into its row, so
 * an audit row and the events of its command never name different correlation ids. The query audit
 * records {@link #inEffect()}: a query stores no event, and with neither bound its row names none.
 *
 * <p><b>Which dispatch is refused.</b> {@link #refuseForeignReserved}: one whose bound correlation
 * id is in the reserved form and is not the bound saga origin's ({@link
 * StreamRuneContext#requireNoForeignReservedCorrelationId()}). The refusal is logged here, once, at
 * {@code WARN}: it is a wiring error of the code that bound the context, and the caller may be a
 * listener that catches what it is thrown.
 */
final class BoundCorrelationId {

  private BoundCorrelationId() {}

  /**
   * The correlation id {@code VirtualThreadCommandBus} generated for the command dispatch in
   * progress on this thread, bound for the length of that dispatch when neither a saga origin nor a
   * request context names one. Read through {@link #ofTheCommandInProgress()} only.
   */
  static final ScopedValue<CorrelationId> GENERATED = ScopedValue.newInstance();

  /**
   * The correlation id the dispatch in progress records.
   *
   * @return the bound saga origin's correlation id ({@link SagaOrigin#correlationId()}) when an
   *     origin is bound; otherwise the bound request context's; {@code null} when neither is bound
   *     (or the context is bound to {@code null})
   */
  static CorrelationId inEffect() {
    StreamRuneContext.DispatchCause cause = StreamRuneContext.cause();
    if (cause != null && cause.sagaOrigin() != null) {
      return cause.sagaOrigin().correlationId();
    }
    StreamRuneContext.RequestContext context = StreamRuneContext.capture();
    return context == null ? null : context.correlationId();
  }

  /**
   * The correlation id the command dispatch in progress records, in its events and in its audit
   * row: {@link #inEffect()}, or — when that names none — the one the command bus generated for
   * this dispatch.
   *
   * <p>The generated id is the command's own. A query executed inside the dispatch, and a command
   * executed inside it under a context or an origin, do not see it ({@link #inEffect()}); a command
   * executed inside it with nothing bound gets one of its own.
   *
   * @return the id; {@code null} only outside a dispatch of {@code VirtualThreadCommandBus} with
   *     neither an origin nor a context bound
   */
  static CorrelationId ofTheCommandInProgress() {
    CorrelationId bound = inEffect();
    if (bound != null) {
      return bound;
    }
    return GENERATED.isBound() ? GENERATED.get() : null;
  }

  /**
   * Refuses the dispatch in progress when its bound correlation id is reserved and is not the bound
   * saga origin's, and logs the refusal at {@code WARN} with its fixed reason. Neither the log line
   * nor the exception holds the bound value.
   *
   * @param log the logger of the refusing bus
   * @param kind what is dispatched, for the log line: {@code "command"} or {@code "query"}
   * @param type the class of the dispatched command or query; its simple name is logged
   * @throws ReservedCorrelationIdException if the dispatch is refused
   */
  static void refuseForeignReserved(Logger log, String kind, Class<?> type) {
    try {
      StreamRuneContext.requireNoForeignReservedCorrelationId();
    } catch (ReservedCorrelationIdException refused) {
      log.warn("Refusing {} {}: {}", kind, type.getSimpleName(), refused.getMessage());
      throw refused;
    }
  }
}
