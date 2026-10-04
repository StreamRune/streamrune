package org.streamrune.micronaut;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.order.Ordered;
import io.micronaut.core.propagation.PropagatedContext;
import io.micronaut.core.propagation.ThreadPropagatedContextElement;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.RequestFilter;
import io.micronaut.http.annotation.ServerFilter;
import io.micronaut.http.filter.FilterContinuation;
import io.micronaut.http.filter.ServerFilterPhase;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.BaggageAllowlist;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestEdgeAuthority;
import org.streamrune.integration.RequestIdentityPolicy;

/**
 * Micronaut HTTP server filter that propagates HTTP request metadata into {@link
 * StreamRuneContext}. Extracts {@code X-Trace-Id} and {@code X-Correlation-Id} headers, and {@code
 * X-User-Id} and {@code X-User-Role} only in the trusted-gateway mode (see the
 * authorization-identity and role notes below). Missing headers default: trace and correlation IDs
 * are generated, the user is anonymous ({@code null} user ID) and there is no role.
 *
 * <p><b>Role (security):</b> baggage {@code role} — which every event the request produces carries
 * into its plaintext metadata — is the {@code X-User-Role} header in the trusted-gateway mode only,
 * where the gateway owns that header exactly as it owns {@code X-User-Id}; a repeated header is no
 * role. In the authenticated-principal and anonymous modes the header is ignored and there is no
 * {@code role}: an authenticated principal's verified roles are the context's captured authority
 * (when the configured {@link UserRoleResolver} declares {@link
 * UserRoleResolver#requiresRequestContext()}), never metadata baggage. A W3C {@code baggage: role=}
 * entry never reaches the key, in any mode and whatever the baggage allowlist says. The rule is
 * {@link RequestIdentityPolicy#applyRole(Map, List)}, identical on the three integrations.
 *
 * <p>Propagation is two-fold:
 *
 * <ul>
 *   <li>{@link StreamRuneContext#CURRENT} ({@code ScopedValue}) is bound around the downstream
 *       chain. The filter runs on a blocking executor thread and proceeds synchronously, so
 *       handlers executing on the request thread (and the command bus they call) observe the bound
 *       context.
 *   <li>A {@link ThreadPropagatedContextElement} mirrors the context into {@link
 *       StreamRuneContextHelper}'s ThreadLocal on every thread Micronaut propagates the request
 *       context to (e.g., {@code @ExecuteOn} executors or reactive pipelines), where {@code
 *       ScopedValue} bindings cannot follow.
 * </ul>
 *
 * <p><b>Authorization identity (security):</b> the request-context {@code userId} is the identity
 * that {@code CommandAuthorizationPolicy}, {@code @RequireRole}/{@code @RequirePermission} and
 * {@code Authorization.requireOwner(...)} authorize against and that the command audit records, so
 * it must never be a client-supplied value. It is resolved by the {@link RequestIdentityPolicy} the
 * filter is built with — the same policy bean the {@link SseController} uses, and the same rule as
 * the Spring and Quarkus integrations' request filters — and fails closed:
 *
 * <ul>
 *   <li>with an {@link AuthenticatedUserResolver} (Micronaut Security present and enabled, or an
 *       application bean), the {@code userId} is the authenticated principal ({@code
 *       SecurityService}); the {@code X-User-Id} header is ignored, and a header naming another
 *       user is logged (sanitized). An unauthenticated request is anonymous;
 *   <li>with {@code streamrune.security.trust-user-id-header=true} (the explicit trusted-gateway
 *       mode) the {@code userId} is the {@code X-User-Id} header — only safe when a gateway
 *       authenticates every caller, overwrites the header and strips any client-supplied value (and
 *       does the same for {@code X-User-Role});
 *   <li>with neither, every request is anonymous ({@code userId == null}) and the header is
 *       ignored. Nothing is logged per request; {@link StreamRuneRequestIdentityValidator} logs the
 *       mode once at startup and refuses to start when command authorization is configured in this
 *       mode.
 * </ul>
 *
 * <p>The policy sees every {@code X-User-Id} value the request carried ({@link
 * #userIdHeaderValues(HttpRequest)}, which the SSE endpoint reads through too), and every {@code
 * X-User-Role} value, so a repeated header is anonymous (no role) in the trusted-gateway mode
 * instead of resolving to the first value the single-valued {@code get} returns.
 *
 * <p>The filter runs after Micronaut Security's filter ({@link ServerFilterPhase#SECURITY}) so the
 * authentication is available when the identity is derived.
 *
 * <p><b>Baggage allowlisting:</b> OpenTelemetry baggage is populated from the client-supplied,
 * unauthenticated W3C {@code baggage:} HTTP header, and every baggage entry ends up in every
 * produced event's immutable, plaintext {@code event_stream.metadata}. To prevent a caller from
 * smuggling arbitrary PII permanently into the event log, only {@link
 * BaggageAllowlist#DEFAULT_ALLOWED_KEYS} (the causation-id key) plus any keys configured via {@code
 * streamrune.metadata.baggage-allowlist} are copied into the request context. Non-allowlisted keys
 * are dropped here, before they ever reach {@code RequestContext} or {@code EventMetadata}; {@code
 * role} is the role rule's alone (above).
 */
@ServerFilter("/**")
public class StreamRuneContextFilter implements Ordered {

  private static final Logger logger = LoggerFactory.getLogger(StreamRuneContextFilter.class);

  private final Set<String> extraAllowedBaggageKeys;
  private final RequestIdentityPolicy identityPolicy;
  private final UserRoleResolver userRoleResolver;

  /**
   * Creates the context filter with no additional allowlisted baggage keys. Its identity policy is
   * {@link RequestIdentityPolicy.Mode#ANONYMOUS} — every request runs without a user or a role, and
   * the {@code X-User-Id} and {@code X-User-Role} headers are ignored — and no {@link
   * UserRoleResolver} is wired, so no authority is captured at the request edge. Micronaut uses the
   * {@link Inject}-annotated constructor.
   */
  public StreamRuneContextFilter() {
    this(Set.of(), RequestIdentityPolicy.of(null, false), null);
  }

  /**
   * Creates the context filter, seeding the OTel baggage allowlist from {@code
   * streamrune.metadata.baggage-allowlist} on top of the framework defaults ({@link
   * BaggageAllowlist#DEFAULT_ALLOWED_KEYS}), and resolving identities through the application's
   * {@link RequestIdentityPolicy} bean. This is the constructor Micronaut uses for dependency
   * injection.
   *
   * @param properties the bound StreamRune Micronaut properties
   * @param identityPolicy where the request identity and role come from (authenticated principal,
   *     trusted gateway headers, or anonymous); the {@code StreamRuneMicronautModule} bean, shared
   *     with the SSE endpoint
   * @param userRoleResolver the configured role resolver; the caller's authority is captured at the
   *     request edge only when it is non-null and declares {@link
   *     UserRoleResolver#requiresRequestContext()}
   * @throws IllegalArgumentException if {@code identityPolicy} is {@code null}
   */
  @Inject
  public StreamRuneContextFilter(
      StreamRuneMicronautProperties properties,
      RequestIdentityPolicy identityPolicy,
      @Nullable UserRoleResolver userRoleResolver) {
    this(
        Set.copyOf(properties.metadataBaggageAllowlist()),
        identityPolicy,
        // The SAME resolver bean the
        // AnnotationAuthorizationInterceptor is built with, so the authority captured at the
        // request edge is byte-identical to the one a synchronous execute would have computed.
        userRoleResolver);
  }

  StreamRuneContextFilter(
      Set<String> extraAllowedBaggageKeys,
      RequestIdentityPolicy identityPolicy,
      UserRoleResolver userRoleResolver) {
    if (identityPolicy == null) {
      throw new IllegalArgumentException("identityPolicy is required");
    }
    this.extraAllowedBaggageKeys =
        extraAllowedBaggageKeys == null ? Set.of() : extraAllowedBaggageKeys;
    this.identityPolicy = identityPolicy;
    this.userRoleResolver = userRoleResolver;
  }

  /**
   * Runs after Micronaut Security's filter ({@link ServerFilterPhase#SECURITY}) so the
   * authenticated principal is available when the request-context identity is derived from it.
   */
  @Override
  public int getOrder() {
    return ServerFilterPhase.SECURITY.after();
  }

  /**
   * Builds the request context from headers and runs the rest of the filter chain with that context
   * bound.
   *
   * @param request the HTTP request
   * @param continuation the downstream filter chain
   * @return the downstream response
   */
  @RequestFilter
  @ExecuteOn(TaskExecutors.BLOCKING)
  public MutableHttpResponse<?> filter(
      HttpRequest<?> request, FilterContinuation<MutableHttpResponse<?>> continuation) {
    var ctx = buildContext(request);
    try (var _ = PropagatedContext.getOrEmpty().plus(new RequestContextElement(ctx)).propagate()) {
      return ScopedValue.where(StreamRuneContext.CURRENT, ctx).call(continuation::proceed);
    }
  }

  /**
   * Builds a {@link StreamRuneContext.RequestContext} from request headers, generating defaults for
   * missing trace and correlation IDs, with the user the {@link RequestIdentityPolicy} resolves
   * ({@code null} when anonymous) and the baggage its role rule leaves.
   *
   * @param request the HTTP request
   * @return the request context
   */
  StreamRuneContext.RequestContext buildContext(HttpRequest<?> request) {
    var headers = request.getHeaders();
    var traceId = headers.get("X-Trace-Id");
    if (traceId == null || traceId.isBlank()) {
      traceId = UUID.randomUUID().toString();
    }
    var correlationId = headers.get("X-Correlation-Id");
    if (correlationId == null || correlationId.isBlank()) {
      correlationId = UUID.randomUUID().toString();
    }
    var userIdHeaderValues = userIdHeaderValues(request);
    // Baggage `role` is the X-User-Role header behind a trusted gateway only, never a
    // client's W3C baggage entry — every value read, like X-User-Id, so a repeated header is none.
    var baggage =
        identityPolicy.applyRole(
            BaggageAllowlist.filter(readOtelBaggage(), extraAllowedBaggageKeys),
            headers.getAll(RequestIdentityPolicy.USER_ROLE_HEADER));
    // fromRequest, not the constructor. These two ids
    // came straight off the wire unauthenticated, so this is the ingress door where the
    // IdConstraints bound applies. The plain constructor deliberately does not enforce it — it is
    // also the DLQ-replay / saga-dispatch reconstruction path for ids already at rest.
    var ctx =
        StreamRuneContext.RequestContext.fromRequest(
            TraceId.of(traceId),
            resolveUserId(userIdHeaderValues),
            CorrelationId.of(correlationId),
            Instant.now(),
            baggage);
    // Resolve the caller's authorities HERE, while
    // SecurityService can still see this request's authentication, so a command that continues off
    // this thread (bus.executeAsync) authorizes exactly as a synchronous execute would. A no-op
    // unless the configured resolver declares requiresRequestContext().
    return RequestEdgeAuthority.capture(
        ctx,
        userRoleResolver,
        e ->
            logger.warn(
                "Capturing request-edge authorities via {} failed; authorization will resolve"
                    + " at command time instead, so an authz-gated command executed off this"
                    + " request thread will be denied",
                userRoleResolver.getClass().getName(),
                e));
  }

  /**
   * Resolves the authorization identity for the request through the {@link RequestIdentityPolicy}
   * (no header identity unless the trusted-gateway mode is explicit).
   */
  private UserId resolveUserId(List<String> userIdHeaderValues) {
    return identityPolicy.resolve(userIdHeaderValues, StreamRuneContextFilter::logMismatch);
  }

  /**
   * Every {@code X-User-Id} value the request carried, as received — the one reading the filter and
   * the {@link SseController} share, so both resolve the same caller. Not the single-valued {@code
   * get} (first value only) and not a {@code List}-typed {@code @Header} (a single value would be
   * split on commas).
   *
   * @param request the HTTP request
   * @return the header's values; empty when absent
   */
  static List<String> userIdHeaderValues(HttpRequest<?> request) {
    return request.getHeaders().getAll(RequestIdentityPolicy.USER_ID_HEADER);
  }

  private static void logMismatch(UserId headerUserId, UserId principal) {
    // headerUserId is attacker-controlled (UserId performs no
    // charset/length validation beyond non-blank) and reaches this WARN unconditionally on every
    // mismatching request — sanitize before it reaches the log record (CWE-117).
    logger.warn(
        "X-User-Id header '{}' does not match authenticated principal '{}'; ignoring the header "
            + "and authorizing as the authenticated principal",
        LogSanitizer.sanitizeForLog(headerUserId.value()),
        LogSanitizer.sanitizeForLog(principal.value()));
  }

  private Map<String, String> readOtelBaggage() {
    try {
      var baggage = io.opentelemetry.api.baggage.Baggage.current();
      if (baggage.isEmpty()) {
        return Map.of();
      }
      var result = new HashMap<String, String>();
      baggage.forEach((key, entry) -> result.put(key, entry.getValue()));
      return result;
    } catch (NoClassDefFoundError _) {
      return Map.of();
    }
  }

  /**
   * Mirrors the request context into {@link StreamRuneContextHelper}'s ThreadLocal on each thread
   * Micronaut binds the propagated context to, restoring the previous value afterwards.
   */
  record RequestContextElement(StreamRuneContext.RequestContext ctx)
      implements ThreadPropagatedContextElement<StreamRuneContext.RequestContext> {

    @Override
    public StreamRuneContext.RequestContext updateThreadContext() {
      var previous = StreamRuneContextHelper.get();
      StreamRuneContextHelper.set(ctx);
      return previous;
    }

    @Override
    public void restoreThreadContext(StreamRuneContext.RequestContext oldState) {
      if (oldState == null) {
        StreamRuneContextHelper.remove();
      } else {
        StreamRuneContextHelper.set(oldState);
      }
    }
  }
}
