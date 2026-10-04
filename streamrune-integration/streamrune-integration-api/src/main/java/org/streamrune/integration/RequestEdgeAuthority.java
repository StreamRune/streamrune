package org.streamrune.integration;

import java.util.function.Consumer;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.StreamRuneContext.RequestContext;
import org.streamrune.core.UserRoleResolver;

/**
 * Captures the caller's authorities once, at the request edge, into the {@link RequestContext} that
 * every async boundary already carries — the shared half of off-request authorization.
 *
 * <p><b>The problem.</b> All three shipped {@link UserRoleResolver}s read their framework's
 * request-scoped security state. None of it follows a command onto {@code executeAsync}'s virtual
 * thread, so re-resolving there yields no authorities and fail-closed authorization rejects a
 * caller it authorized moments earlier on the same request. Each framework's native propagation
 * story is different (a ThreadLocal, a CDI request scope, a request lookup) and Quarkus's request
 * scope cannot be revived off-request at all — but all three already thread one object across that
 * boundary: the {@link RequestContext}. Resolving once here and putting the answer in that object
 * fixes all three identically, with no framework-specific machinery.
 *
 * <p><b>Why this is not a per-request tax.</b> The capture runs only for a resolver that declares
 * {@link UserRoleResolver#requiresRequestContext()} — which is exactly the class of resolver that
 * reads cheap ambient state. A user-supplied resolver that is a pure function of the user id (a
 * database lookup, an LDAP call) declares {@code false}, is never called here, and already works
 * off-request unaided.
 *
 * <p><b>What a resolver declaring {@code requiresRequestContext()} observes here.</b> {@link
 * StreamRuneContext#CURRENT} is bound to {@code ctx} for the duration of the {@link
 * UserRoleResolver#resolve(org.streamrune.core.types.UserId) resolve} call, so the resolver sees
 * the fully-built context of the very request being authorized — {@code traceId}, the
 * already-authenticated {@code userId}, {@code correlationId}, {@code timestamp} and the
 * allowlisted baggage — plus, because each filter calls this after its framework's authentication
 * filter has run, that framework's own request-scoped security state. Everything except {@link
 * RequestContext#authority()} itself, which is the value being computed.
 *
 * <p>The binding is required, not a convenience. Without it, declaring {@code
 * requiresRequestContext()} on a resolver that reads {@code StreamRuneContext.CURRENT} — the one
 * form of "ambient per-request state" that is the same on all three integrations — made that
 * resolver answer {@code UserAuthority.EMPTY}, and {@code AnnotationAuthorizationInterceptor}
 * <em>enforces</em> a captured authority rather than re-resolving it. The declaration that is
 * supposed to make such a resolver work was therefore the thing that broke it, on the request
 * thread, synchronously, with the caller's roles plainly present.
 *
 * <p>The captured value is process-local and never persisted; see {@link
 * RequestContext#authority()}.
 */
public final class RequestEdgeAuthority {

  private RequestEdgeAuthority() {}

  /**
   * Returns {@code ctx} with the caller's resolved authorities attached, or {@code ctx} unchanged
   * when there is nothing to capture — no resolver, an off-request-capable resolver, or an
   * anonymous request.
   *
   * <p>A resolver that throws here must not fail the request: an authority capture is an
   * optimisation for work that may never happen, and a request that executes no authz-gated command
   * would otherwise start failing for a reason it never depended on. The exception is reported to
   * {@code onCaptureFailure} (each integration passes its own logger — this module deliberately has
   * no logging dependency) and {@code ctx} is returned unchanged, so authorization falls back to
   * calling the resolver at command time exactly as it did before this capture existed and the same
   * failure resurfaces there, attached to the command it belongs to.
   *
   * @param ctx the request context built from the inbound request; {@code null} is returned as-is
   * @param resolver the configured resolver, or {@code null} when none is wired
   * @param onCaptureFailure receives a resolver failure that was swallowed (required)
   * @return {@code ctx}, possibly carrying a captured authority
   */
  public static RequestContext capture(
      RequestContext ctx, UserRoleResolver resolver, Consumer<RuntimeException> onCaptureFailure) {
    if (ctx == null || resolver == null || ctx.userId() == null) {
      return ctx;
    }
    if (!resolver.requiresRequestContext()) {
      return ctx;
    }
    try {
      // Bind the context the resolver was told it may rely on. The
      // binding is scoped to this call only — each filter binds its own (authority-carrying) copy
      // around the downstream chain afterwards, and on Quarkus there is no chain to wrap at all, so
      // a binding that outlived this call would either shadow the filter's or leak past the filter.
      // ctx cannot carry an authority yet (that is what this method produces), so a resolver that
      // recursed into authorization would see the same null it saw before this fix — no new cycle.
      return ScopedValue.where(StreamRuneContext.CURRENT, ctx)
          .call(() -> ctx.withAuthority(resolver.resolve(ctx.userId())));
    } catch (RuntimeException e) {
      onCaptureFailure.accept(e);
      return ctx;
    }
  }
}
