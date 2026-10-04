package org.streamrune.runtime;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.LogSanitizer;

/**
 * The trusted system principals under which an authorization interceptor may execute a command with
 * the per-user check deliberately skipped, plus the recording discipline that makes every such skip
 * visible to an operator.
 *
 * <p><b>Why this is one shared mechanism and not a per-interceptor convention.</b> StreamRune ships
 * two authorization interceptors — {@link AnnotationAuthorizationInterceptor} (declarative
 * {@code @RequireRole}/{@code @RequirePermission}) and {@link AuthorizationCommandInterceptor} (a
 * programmatic {@code CommandAuthorizationPolicy}) — and both are auto-registered by all three
 * integrations. Only the first of them originally learned that a saga acts as the SYSTEM; the
 * second was left behind and kept asking its policy about an anonymous caller on every saga path.
 * Holding the marker test, the principal labels and the WARN-once-then-DEBUG recording in one place
 * is what stops the next fix from landing on one twin and not the other.
 *
 * <p>The recorded log line is the reason a skip is safe to have at all: the absence of such a
 * record means the per-user check ran, so "the system executed this" is always distinguishable from
 * "authorization was not checked". A saga skip is high-volume and expected, so it WARNs once per
 * command type per process (bounded, and enough to inventory which command types execute unchecked)
 * and DEBUGs thereafter. The identity is referenced by command id rather than interpolated. That is
 * a privacy choice, not a dependency one: {@code LogSanitizer} lives in {@code streamrune-core}, so
 * this module could render a {@code UserId} safely — it still does not, because a user id may be
 * personal data and the command id already joins this record to the audit row that carries the
 * identity.
 */
final class AuthorizationSystemPrincipal {

  /**
   * Identifies which system principal an execution ran under when the per-user check was skipped.
   * Part of the log record so an operator can tell the two apart in a search.
   */
  enum Kind {
    SAGA("saga"),
    DLQ_REPLAY("dlq-replay");

    private final String label;

    Kind(String label) {
      this.label = label;
    }

    @Override
    public String toString() {
      return label;
    }
  }

  private static final String SAGA_SKIP_MESSAGE =
      "Authorization check SKIPPED for {} ({}): executing under the '{}' system principal."
          + " No per-user authorization check was made. Set honorSagaSystemPrincipal=false to"
          + " enforce per-user authz on saga-dispatched commands.";

  private final Logger log;

  /**
   * Command types already WARN-logged for a saga-principal skip. Keeps the saga record bounded at
   * one WARN per type per process instead of one per dispatched command.
   */
  private final Set<Class<?>> sagaSkipWarned = ConcurrentHashMap.newKeySet();

  /**
   * @param log the owning interceptor's logger, so the record is attributed to the class that
   *     actually skipped the check
   */
  AuthorizationSystemPrincipal(Logger log) {
    this.log = log;
  }

  /**
   * Whether this execution is a saga-issued dispatch — {@code SagaCommandDispatch} binds {@link
   * StreamRuneContext#SAGA_OWNED} around every forward and compensation command a saga issues.
   * Those run on a subscription poll thread, a {@code SagaTimeoutRunner} thread or the {@code
   * SagaCompensationRetrySweeper} thread, where no {@code StreamRuneContext} user is bound.
   */
  static boolean sagaOwnedDispatch() {
    return StreamRuneContext.isSagaDispatch();
  }

  /** Whether this execution replays an existing dead-letter entry. */
  static boolean dlqReplay() {
    return isBoundTrue(VirtualThreadCommandBus.DLQ_REPLAY);
  }

  /** Records a saga-principal skip: WARN once per command type per process, DEBUG thereafter. */
  void recordSagaSkip(CommandContext ctx) {
    Class<?> type = ctx.command().getClass();
    if (sagaSkipWarned.add(type)) {
      log.warn(
          SAGA_SKIP_MESSAGE,
          LogSanitizer.sanitizeForLog(ctx.commandId().value()),
          type.getName(),
          Kind.SAGA);
    } else {
      log.debug(
          SAGA_SKIP_MESSAGE,
          LogSanitizer.sanitizeForLog(ctx.commandId().value()),
          type.getName(),
          Kind.SAGA);
    }
  }

  /** Whether {@code flag} is bound to {@code TRUE} on the current thread. */
  private static boolean isBoundTrue(ScopedValue<Boolean> flag) {
    return flag.isBound() && Boolean.TRUE.equals(flag.get());
  }
}
