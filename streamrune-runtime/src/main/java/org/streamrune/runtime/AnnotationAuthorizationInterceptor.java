package org.streamrune.runtime;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.AuthorizationAnnotations;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.RequirePermission;
import org.streamrune.core.RequireRole;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.UserId;

/**
 * A {@link CommandInterceptor} that enforces {@link RequireRole} and {@link RequirePermission}
 * annotations on command classes.
 *
 * <p>Annotations are looked up via {@link AuthorizationAnnotations}, so they are honored whether
 * declared on the command class itself or anywhere in its hierarchy — including the documented
 * pattern of annotating a sealed command interface shared by several command records. Lookups are
 * memoized per command class.
 *
 * <p>Reads the caller's {@link UserId} from {@link StreamRuneContext#CURRENT}. If the command class
 * has no annotations, the interceptor is a no-op. If the command class has annotations but no user
 * is authenticated, an {@link AuthorizationException} is thrown.
 *
 * <p>When both {@link RequireRole} and {@link RequirePermission} are present, both must be
 * satisfied (AND). Within a single annotation, any match suffices (OR).
 *
 * <h2>Where the authority comes from</h2>
 *
 * <p>A command does not only execute on the thread that submitted it. {@code executeAsync} runs it
 * on a fresh virtual thread and {@code DeadLetterRetryRunner} replays it on a background poll
 * thread. Every shipped {@link UserRoleResolver} reads framework request-scoped state, which does
 * not follow the command onto those threads and cannot be revived on them — so calling the resolver
 * there returns {@link UserAuthority#EMPTY}, which is indistinguishable from an honest "this user
 * has no roles". Treating that as a denial fails an authorized caller ({@code executeAsync}) and
 * permanently loses an already-authorized command (DLQ replay burns its whole retry ladder on
 * {@code AuthorizationException}). Treating it as permission would be an authorization bypass. The
 * only sound answer is to stop asking a question the resolver cannot answer, so {@code before()}
 * decides in this fixed order:
 *
 * <ol>
 *   <li><b>No annotations</b> — allow, no record. Unchanged.
 *   <li><b>Saga-system principal</b> — {@link StreamRuneContext#SAGA_OWNED} is bound and {@code
 *       honorSagaSystemPrincipal} is on: allow, RECORDED (see below). Unchanged behaviour, newly
 *       visible.
 *   <li><b>No user</b> — {@code userId == null}: deny with "Authentication required". Unchanged,
 *       and deliberately ahead of every remaining branch: an execution with neither a principal nor
 *       a system marker is exactly the case that must fail closed.
 *   <li><b>Captured authority</b> — the bound {@link StreamRuneContext.RequestContext} carries an
 *       {@link StreamRuneContext.RequestContext#authority() authority} the request edge resolved
 *       for this very {@code userId} (the record holds both, so they cannot disagree): enforce
 *       against it. This is the {@code executeAsync} path — the command bus already re-binds the
 *       whole request context onto the async thread, so the captured answer rides along and
 *       off-request execution decides exactly as the request thread did.
 *   <li><b>DLQ-replay system principal</b> — {@link VirtualThreadCommandBus#DLQ_REPLAY} is bound,
 *       {@code honorDlqReplaySystemPrincipal} is on, and {@link
 *       UserRoleResolver#requiresRequestContext()} says the resolver provably cannot answer on this
 *       thread: allow, RECORDED. See the safety argument below.
 *   <li><b>Otherwise</b> — {@code resolver.resolve(userId)} and enforce, exactly as before. A
 *       resolver that is a pure function of the {@code userId} therefore keeps being consulted on
 *       every path, including DLQ replay, so a genuine role revocation still denies a queued
 *       command.
 * </ol>
 *
 * <h2>Why the DLQ-replay system principal is not a bypass</h2>
 *
 * <p>An entry can only reach the command dead letter queue after the command passed this
 * interceptor: {@code before()} throwing propagates straight out of {@code executeCounted} and
 * never reaches {@code executeInternal}, the only place that publishes; and {@code
 * VirtualThreadCommandBus.DEFAULT_DLQ_ELIGIBLE} excludes {@code DomainException} (which {@link
 * AuthorizationException} is) in any case. So a DLQ replay is the framework finishing a unit of
 * work whose authorization was already decided — not a fresh, unauthorized request. Re-deciding it
 * is impossible with a request-scoped resolver and merely optional with a pure one, where it still
 * happens.
 *
 * <p>The one construction this does not cover is an entry written into the queue by something other
 * than the bus (application code calling {@code DeadLetterQueue.publish} directly, or an operator
 * with write access to the table). That is outside the framework's trust boundary and is precisely
 * what {@code honorDlqReplaySystemPrincipal = false} is for: it restores strict per-user
 * enforcement on the replay path, at the documented cost that a request-scoped resolver then loses
 * every authz-gated dead-lettered command.
 *
 * <h2>What an operator sees</h2>
 *
 * <p>Every execution that skips the per-user check emits a log record naming the system principal,
 * the command, and the identity the command originally ran as — so "the system ran this" is always
 * distinguishable from "authorization was not checked", and the absence of such a record means the
 * check ran. A DLQ-replay skip is rare and WARNs on every occurrence. A saga skip is high-volume
 * and expected, so it WARNs once per command type per process (bounded, and enough to inventory
 * which command types execute unchecked) and DEBUGs thereafter.
 *
 * <p><b>Saga-system principal:</b> commands a saga dispatches — forward and, critically,
 * <em>compensation</em> commands (e.g. {@code RefundPayment}) — run on a subscription poll thread,
 * a {@code SagaTimeoutRunner} thread, or the {@code SagaCompensationRetrySweeper} thread, where
 * <em>no</em> {@link org.streamrune.core.StreamRuneContext} user is bound. A saga acts as the
 * SYSTEM, not as an end user; the per-user {@code @RequireRole}/{@code @RequirePermission} gates
 * exist to authorize USER-initiated commands. If this interceptor rejected saga-dispatched
 * compensation with "Authentication required", the saga would be silently terminalized {@code
 * FAILED} and the undo (refund/stock release) would never run — a fail-closed authz interaction
 * that abandons compensation across every saga. To prevent that, commands dispatched under {@link
 * StreamRuneContext#SAGA_OWNED} (which {@code SagaCommandDispatch} binds around every saga-issued
 * command) are treated as the trusted saga-system principal and bypass the per-user role/permission
 * check — the same marker, labels and recording its policy-based twin {@link
 * AuthorizationCommandInterceptor} uses, shared through {@link AuthorizationSystemPrincipal} so the
 * two cannot drift. This is a documented, opt-out behavior: construct with {@code
 * honorSagaSystemPrincipal = false} to enforce per-user authz even for saga-dispatched commands (in
 * which case such commands must NOT be {@code @RequireRole}/{@code @RequirePermission}-gated, or
 * the saga's compensation will fail closed — see {@code docs/guide/advanced/saga.md}).
 */
public final class AnnotationAuthorizationInterceptor implements CommandInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(AnnotationAuthorizationInterceptor.class);

  /**
   * The shared saga/DLQ-replay system-principal mechanism (marker tests, principal labels, and the
   * WARN-once-then-DEBUG saga record). Held rather than duplicated so this interceptor and its
   * policy-based twin {@link AuthorizationCommandInterceptor} cannot drift apart again.
   */
  private final AuthorizationSystemPrincipal systemPrincipal =
      new AuthorizationSystemPrincipal(LOG);

  private final UserRoleResolver resolver;
  private final boolean honorSagaSystemPrincipal;
  private final boolean honorDlqReplaySystemPrincipal;

  /** Memoized per-command-class annotation lookup — the hierarchy walk runs once per type. */
  private final Map<Class<?>, Requirements> requirementsCache = new ConcurrentHashMap<>();

  /**
   * Creates an interceptor that honors both system principals: saga-dispatched commands (bound
   * under {@link StreamRuneContext#SAGA_OWNED}) bypass the per-user authz check so a saga's
   * compensation is never killed by fail-closed authorization on an unauthenticated saga thread,
   * and a DLQ replay driven by a resolver that {@linkplain
   * UserRoleResolver#requiresRequestContext() cannot answer off-request} runs the
   * already-authorized command under the DLQ-replay system principal rather than losing it. Both
   * skips are logged. Equivalent to {@code new AnnotationAuthorizationInterceptor(resolver, true,
   * true)}.
   */
  public AnnotationAuthorizationInterceptor(UserRoleResolver resolver) {
    this(resolver, true, true);
  }

  /**
   * @param resolver resolves a {@link UserId} to its roles/permissions (required)
   * @param honorSagaSystemPrincipal when {@code true} (recommended), commands dispatched by a saga
   *     (bound under {@link StreamRuneContext#SAGA_OWNED}) run as the trusted saga-system principal
   *     and bypass the per-user {@code @RequireRole}/{@code @RequirePermission} check; when {@code
   *     false}, per-user authz is enforced even for saga-dispatched commands (which then must not
   *     be authz-gated, or the saga's compensation fails closed)
   */
  public AnnotationAuthorizationInterceptor(
      UserRoleResolver resolver, boolean honorSagaSystemPrincipal) {
    this(resolver, honorSagaSystemPrincipal, true);
  }

  /**
   * @param resolver resolves a {@link UserId} to its roles/permissions (required)
   * @param honorSagaSystemPrincipal see {@link
   *     #AnnotationAuthorizationInterceptor(UserRoleResolver, boolean)}
   * @param honorDlqReplaySystemPrincipal when {@code true} (recommended), a DLQ replay whose {@code
   *     resolver} {@linkplain UserRoleResolver#requiresRequestContext() requires a request context}
   *     — and therefore provably cannot answer on the runner's background thread — executes under
   *     the recorded DLQ-replay system principal instead of being denied. The command already
   *     passed this interceptor before it could be dead-lettered, so this completes accepted work
   *     rather than admitting new work. When {@code false}, the resolver is consulted on the replay
   *     thread regardless: with a request-scoped resolver every authz-gated dead-lettered command
   *     is then rejected on every attempt and permanently lost once its retry ladder is exhausted.
   *     Choose {@code false} only when dead-letter entries can be written by something outside the
   *     command bus and that risk outweighs losing legitimate commands.
   */
  public AnnotationAuthorizationInterceptor(
      UserRoleResolver resolver,
      boolean honorSagaSystemPrincipal,
      boolean honorDlqReplaySystemPrincipal) {
    if (resolver == null) throw new IllegalArgumentException("resolver is required");
    this.resolver = resolver;
    this.honorSagaSystemPrincipal = honorSagaSystemPrincipal;
    this.honorDlqReplaySystemPrincipal = honorDlqReplaySystemPrincipal;
  }

  @Override
  public boolean before(CommandContext ctx) {
    Requirements requirements =
        requirementsCache.computeIfAbsent(
            ctx.command().getClass(), AnnotationAuthorizationInterceptor::resolveRequirements);

    if (requirements.role() == null && requirements.permission() == null) {
      return true;
    }

    if (honorSagaSystemPrincipal && AuthorizationSystemPrincipal.sagaOwnedDispatch()) {
      // A saga-dispatched command runs as the trusted saga-system principal — the saga
      // acts as the SYSTEM (not an end user), so per-user authz must not kill its compensation.
      systemPrincipal.recordSagaSkip(ctx);
      return true;
    }

    StreamRuneContext.RequestContext requestContext = StreamRuneContext.capture();
    UserId userId = requestContext == null ? null : requestContext.userId();
    if (userId == null) {
      throw new AuthorizationException("Authentication required");
    }

    UserAuthority authority = requestContext.authority();
    if (authority == null) {
      if (honorDlqReplaySystemPrincipal
          && AuthorizationSystemPrincipal.dlqReplay()
          && resolver.requiresRequestContext()) {
        // This replay runs on the retry runner's own thread, where
        // a request-scoped resolver can only ever answer EMPTY. Denying here would reject a command
        // that already passed this very interceptor before it could be dead-lettered, burn its
        // retry ladder on AuthorizationException, and permanently lose a legitimate business
        // operation while the logs blamed "authorization". Complete the accepted work under the
        // recorded DLQ-replay system principal instead.
        recordDlqReplaySkip(ctx);
        return true;
      }
      authority = resolver.resolve(userId);
    }

    RequireRole roleAnn = requirements.role();
    if (roleAnn != null && !authority.hasAnyRole(roleAnn.value())) {
      throw new AuthorizationException(
          "Required role: " + String.join(" or ", roleAnn.value()) + offRequestHint(authority));
    }

    RequirePermission permAnn = requirements.permission();
    if (permAnn != null && !authority.hasAnyPermission(permAnn.value())) {
      throw new AuthorizationException(
          "Required permission: "
              + String.join(" or ", permAnn.value())
              + offRequestHint(authority));
    }

    return true;
  }

  /**
   * Appends a diagnostic to a denial that a request-scoped resolver produced with no authorities at
   * all. That is the fingerprint of "the resolver could not see a request", which reads identically
   * to "this user has no roles" — the ambiguity at the heart of the off-request rule — so name it
   * rather than leaving an operator to guess why a caller who works synchronously is denied
   * asynchronously.
   */
  private String offRequestHint(UserAuthority authority) {
    if (!resolver.requiresRequestContext()
        || !authority.roles().isEmpty()
        || !authority.permissions().isEmpty()) {
      return "";
    }
    return " (resolver "
        + resolver.getClass().getName()
        + " requires a request context and returned no authorities — if this command executed off"
        + " the request thread, the request edge captured no authority for it)";
  }

  /**
   * Records a DLQ-replay-principal skip. Always WARN: it is rare (only a request-scoped resolver on
   * the replay path reaches it) and each occurrence is a distinct business command completing
   * without a fresh per-user check.
   */
  private void recordDlqReplaySkip(CommandContext ctx) {
    // The identity is deliberately NOT interpolated: the replay re-binds the user its dead-letter
    // entry recorded, and the events and audit row of this execution carry it under the command id
    // below, so repeating a possibly-personal identifier here adds a sink and no diagnostic. The
    // entry is no join target: its id is the original execution's, and a successful replay
    // removes it.
    LOG.warn(
        "Authorization check SKIPPED for {} ({}): executing under the '{}' system principal because"
            + " resolver {} requires a request context and this replay runs on the dead-letter"
            + " retry thread. The command already passed authorization when it was submitted; the"
            + " replay runs as the user its dead-letter entry recorded, which the events and the"
            + " audit row (when auditing is on) of command {} carry. No fresh per-user check was"
            + " made now."
            + " Set honorDlqReplaySystemPrincipal=false to enforce per-user authz on replays"
            + " (dropping every authz-gated dead-lettered command instead).",
        LogSanitizer.sanitizeForLog(ctx.commandId().value()),
        ctx.command().getClass().getName(),
        AuthorizationSystemPrincipal.Kind.DLQ_REPLAY,
        resolver.getClass().getName(),
        LogSanitizer.sanitizeForLog(ctx.commandId().value()));
  }

  private static Requirements resolveRequirements(Class<?> commandClass) {
    return new Requirements(
        AuthorizationAnnotations.findRequireRole(commandClass),
        AuthorizationAnnotations.findRequirePermission(commandClass));
  }

  /** Resolved annotations for one command class; both components are nullable. */
  private record Requirements(RequireRole role, RequirePermission permission) {}
}
