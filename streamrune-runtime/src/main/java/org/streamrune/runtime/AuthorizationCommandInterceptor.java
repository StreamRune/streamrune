package org.streamrune.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.CommandAuthorizationPolicy;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.UserId;

/**
 * A {@link CommandInterceptor} that enforces a {@link CommandAuthorizationPolicy} before every
 * command reaches the Decider.
 *
 * <p>Reads the caller's {@link UserId} from {@link StreamRuneContext#CURRENT} when the scoped value
 * is bound; passes {@code null} to the policy for unauthenticated requests (no context set or no
 * user in context). The policy decides whether to permit or reject the command by throwing {@link
 * org.streamrune.core.AuthorizationException}.
 *
 * <h2>Saga-dispatched commands run as the system principal</h2>
 *
 * <p>Commands a saga issues — forward and, critically, <em>compensation</em> commands (e.g. {@code
 * RefundPayment}) — are dispatched from a subscription poll thread, a {@code SagaTimeoutRunner}
 * thread or the {@code SagaCompensationRetrySweeper} thread. None of those binds a {@link
 * StreamRuneContext} user: there is no logged-in caller driving a timeout or a crash-resume, and a
 * saga acts as the SYSTEM. This interceptor therefore used to hand the policy {@code userId ==
 * null} on every saga path — and the shape the guide documents ({@code if (userId == null) throw
 * new AuthorizationException("Authentication required")}) rejects that. Under {@code
 * SagaCommandDispatch} an {@code AuthorizationException} is a business rejection, so the saga
 * claim-first compensated and then had its <em>compensation</em> rejected by the same policy: the
 * episode terminalized with the refund never issued, with no dead-letter entry and an error that
 * reads like a legitimate authorization failure.
 *
 * <p>The system-principal rule closed exactly that hole for {@link
 * AnnotationAuthorizationInterceptor} and left this twin — its policy-based sibling,
 * auto-registered by all three integrations whenever a {@code CommandAuthorizationPolicy} bean
 * exists — open. It now honors the same marker, through the same {@link
 * AuthorizationSystemPrincipal} mechanism: a command dispatched under {@link
 * StreamRuneContext#SAGA_OWNED} runs as the trusted saga-system principal and the policy is not
 * consulted. The skip is RECORDED (WARN once per command type per process, DEBUG thereafter), so
 * the absence of such a line means the policy did run.
 *
 * <p>The bypass is scoped strictly to a {@code SAGA_OWNED} dispatch: an ordinary {@code
 * bus.execute(...)} on an unauthenticated thread still reaches the policy and still fails closed.
 * There is deliberately no DLQ-replay principal here, unlike the annotation interceptor: that one
 * exists only because a request-scoped {@code UserRoleResolver} provably cannot answer off-request,
 * whereas a {@code CommandAuthorizationPolicy} is a function of the {@code userId} the replay
 * re-binds from the dead-letter entry — so it can and must still be consulted there.
 */
public final class AuthorizationCommandInterceptor implements CommandInterceptor {

  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationCommandInterceptor.class);

  private final CommandAuthorizationPolicy policy;
  private final boolean honorSagaSystemPrincipal;

  /**
   * Shared with {@link AnnotationAuthorizationInterceptor}: the {@code SAGA_OWNED} marker test, the
   * principal labels, and the WARN-once-then-DEBUG recording discipline.
   */
  private final AuthorizationSystemPrincipal systemPrincipal =
      new AuthorizationSystemPrincipal(LOG);

  /**
   * Creates an interceptor that honors the saga-system principal: commands dispatched by a saga
   * (bound under {@link StreamRuneContext#SAGA_OWNED}) bypass the policy so a saga's compensation
   * is never killed by a fail-closed policy on an unauthenticated saga thread. The skip is logged.
   * Equivalent to {@code new AuthorizationCommandInterceptor(policy, true)}.
   */
  public AuthorizationCommandInterceptor(CommandAuthorizationPolicy policy) {
    this(policy, true);
  }

  /**
   * @param policy the policy consulted for every non-system-principal command (required)
   * @param honorSagaSystemPrincipal when {@code true} (recommended), commands dispatched by a saga
   *     run as the trusted saga-system principal and the policy is not consulted; when {@code
   *     false}, the policy is consulted for saga-dispatched commands too — in which case it MUST
   *     permit them (it will see {@code userId == null} on every background saga path), or the
   *     saga's compensation fails closed and the undo is never issued
   */
  public AuthorizationCommandInterceptor(
      CommandAuthorizationPolicy policy, boolean honorSagaSystemPrincipal) {
    if (policy == null) throw new IllegalArgumentException("policy is required");
    this.policy = policy;
    this.honorSagaSystemPrincipal = honorSagaSystemPrincipal;
  }

  @Override
  public boolean before(CommandContext ctx) {
    if (honorSagaSystemPrincipal && AuthorizationSystemPrincipal.sagaOwnedDispatch()) {
      // A saga-dispatched command runs as the trusted saga-system principal — the saga
      // acts as the SYSTEM (not an end user), so a per-user policy must not kill its compensation.
      systemPrincipal.recordSagaSkip(ctx);
      return true;
    }
    UserId userId = null;
    if (StreamRuneContext.CURRENT.isBound()) {
      StreamRuneContext.RequestContext requestCtx = StreamRuneContext.CURRENT.get();
      if (requestCtx != null) {
        userId = requestCtx.userId();
      }
    }
    policy.authorize(userId, ctx.command());
    return true;
  }
}
