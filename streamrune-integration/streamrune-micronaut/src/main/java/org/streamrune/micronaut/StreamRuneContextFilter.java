package org.streamrune.micronaut;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.order.Ordered;
import io.micronaut.core.propagation.PropagatedContext;
import io.micronaut.core.propagation.ThreadPropagatedContextElement;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
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
 * <p>Propagation is three-fold:
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
 *   <li>The context is the request attribute {@link #CONTEXT_ATTRIBUTE}, which goes wherever the
 *       request goes: a filter behind this one that runs on another executor can take the route to
 *       a thread neither of the two above reaches. The {@link SseController} reads the caller of a
 *       subscription there ({@link #requestContextOf(HttpRequest)}).
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
 * smuggling arbitrary PII permanently into the event log, only the keys configured via {@code
 * streamrune.metadata.baggage-allowlist} are copied into the request context; with none configured
 * no baggage entry is ({@link BaggageAllowlist#DEFAULT_ALLOWED_KEYS} is empty). Non-allowlisted
 * keys are dropped here, before they ever reach {@code RequestContext} or {@code EventMetadata};
 * {@code role} is the role rule's alone (above).
 *
 * <p><b>A refused request context.</b> The correlation id, the trace id and the user id pass
 * through {@link StreamRuneContext.RequestContext#fromRequest}, which refuses a value longer than
 * 255 characters, a value that holds a control character, and a correlation id that starts with
 * {@code streamrune:} — the prefix of the correlation ids the framework itself binds for a saga's
 * commands. The filter answers such a request with status {@code 400 Bad Request}, content type
 * {@code text/plain;charset=UTF-8} and the body {@link
 * RequestIdentityPolicy#REFUSED_REQUEST_CONTEXT} — the answer of the Spring and Quarkus filters,
 * byte for byte — and does not proceed: no controller runs, no context is bound or stored on the
 * request, nothing is stored. The refused value is neither echoed nor logged. Any other correlation
 * id is bound exactly as the client sent it.
 *
 * <p><b>A header sent more than once.</b> Of a repeated {@code X-Correlation-Id} or {@code
 * X-Trace-Id} header the first value is the header ({@code HttpHeaders#get}), on the three
 * integrations alike: a blank first value counts as an absent header and an id is generated,
 * whatever follows it. One line that holds a comma is one value. The correlation id, the trace id
 * and the baggage are caller-supplied in every identity mode; nothing may be addressed, authorized
 * or deduplicated by them.
 *
 * <p><b>What a request cannot set.</b> This filter builds a request context and nothing else. It
 * does not bind {@link StreamRuneContext#CAUSE}, so no header and no baggage entry, allowlisted or
 * not, reaches the saga origin or the causation id of an event: the command bus takes both from
 * that binding alone.
 */
@ServerFilter("/**")
public class StreamRuneContextFilter implements Ordered {

  /**
   * The request attribute that holds the {@link StreamRuneContext.RequestContext} this filter built
   * for the request. A filter that replaces this one sets it too, with the user id the {@link
   * RequestIdentityPolicy} bean resolves: the {@link SseController} refuses a request that has
   * none.
   */
  public static final String CONTEXT_ATTRIBUTE = "org.streamrune.micronaut.requestContext";

  private static final Logger logger = LoggerFactory.getLogger(StreamRuneContextFilter.class);

  /** The content type of the answer to a refused request context, the same on the integrations. */
  private static final MediaType REFUSAL_CONTENT_TYPE = MediaType.of("text/plain;charset=UTF-8");

  private final Set<String> extraAllowedBaggageKeys;
  private final RequestIdentityPolicy identityPolicy;
  private final UserRoleResolver userRoleResolver;

  /**
   * Creates the context filter with no allowlisted baggage key. Its identity policy is {@link
   * RequestIdentityPolicy.Mode#ANONYMOUS} — every request runs without a user or a role, and the
   * {@code X-User-Id} and {@code X-User-Role} headers are ignored — and no {@link UserRoleResolver}
   * is wired, so no authority is captured at the request edge. Micronaut uses the {@link
   * Inject}-annotated constructor.
   */
  public StreamRuneContextFilter() {
    this(Set.of(), RequestIdentityPolicy.of(null, false), null);
  }

  /**
   * Creates the context filter, taking the OTel baggage allowlist from {@code
   * streamrune.metadata.baggage-allowlist} (the framework adds no key of its own: {@link
   * BaggageAllowlist#DEFAULT_ALLOWED_KEYS} is empty), and resolving identities through the
   * application's {@link RequestIdentityPolicy} bean. This is the constructor Micronaut uses for
   * dependency injection.
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
   * bound. A request whose correlation id, trace id or user id the ingress door refuses ({@link
   * StreamRuneContext.RequestContext#fromRequest}) is answered with {@code 400 Bad Request} and the
   * body {@link RequestIdentityPolicy#REFUSED_REQUEST_CONTEXT} instead; the chain does not run.
   *
   * @param request the HTTP request
   * @param continuation the downstream filter chain
   * @return the downstream response, or the {@code 400} answer to a refused request context
   */
  @RequestFilter
  @ExecuteOn(TaskExecutors.BLOCKING)
  public MutableHttpResponse<?> filter(
      HttpRequest<?> request, FilterContinuation<MutableHttpResponse<?>> continuation) {
    // The baggage and the identity are resolved outside the try below: a failure of the identity
    // policy or of the application's AuthenticatedUserResolver is not a refused header.
    Map<String, String> baggage = baggageOf(request);
    UserId userId = resolveUserId(userIdHeaderValues(request));
    StreamRuneContext.RequestContext ctx;
    try {
      ctx = contextFromHeaders(request, userId, baggage);
    } catch (IllegalArgumentException refused) {
      // The door refused an id: too long, a control character, or a correlation id with the
      // reserved prefix. That is the client's error, answered here before the chain runs. The
      // message names the field and never the value; the value itself is not logged.
      logger.debug("Refusing the request with 400: {}", refused.getMessage());
      // A principal the identity provider asserted and the door refuses is refused on every
      // request of that user: reported once, at a level an operator sees.
      identityPolicy.unacceptablePrincipalWarning(userId).ifPresent(logger::warn);
      return HttpResponse.badRequest(RequestIdentityPolicy.REFUSED_REQUEST_CONTEXT)
          .contentType(REFUSAL_CONTENT_TYPE);
    }
    ctx = withCapturedAuthority(ctx);
    request.setAttribute(CONTEXT_ATTRIBUTE, ctx);
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
   * @throws IllegalArgumentException if the ingress door refuses the correlation id, the trace id
   *     or the user id ({@link StreamRuneContext.RequestContext#fromRequest}); {@link #filter}
   *     answers that with {@code 400}
   */
  StreamRuneContext.RequestContext buildContext(HttpRequest<?> request) {
    Map<String, String> baggage = baggageOf(request);
    UserId userId = resolveUserId(userIdHeaderValues(request));
    return withCapturedAuthority(contextFromHeaders(request, userId, baggage));
  }

  /**
   * The request's baggage: the allow-listed OpenTelemetry entries, with the role rule applied.
   * Baggage {@code role} is the {@code X-User-Role} header behind a trusted gateway only, never a
   * client's W3C baggage entry — every value read, like {@code X-User-Id}, so a repeated header is
   * none.
   */
  private Map<String, String> baggageOf(HttpRequest<?> request) {
    return identityPolicy.applyRole(
        BaggageAllowlist.filter(readOtelBaggage(), extraAllowedBaggageKeys),
        request.getHeaders().getAll(RequestIdentityPolicy.USER_ROLE_HEADER));
  }

  /**
   * The one step that reads the unauthenticated id headers and takes them through the ingress door.
   * {@code fromRequest}, not the constructor: these ids came straight off the wire, so this is
   * where the {@code IdConstraints} bound and the refusal of the reserved correlation prefix apply.
   * The plain constructor deliberately enforces neither — it is also the DLQ-replay / saga-dispatch
   * reconstruction path for ids already at rest.
   *
   * @throws IllegalArgumentException if the door refuses the correlation id, the trace id or the
   *     user id
   */
  private static StreamRuneContext.RequestContext contextFromHeaders(
      HttpRequest<?> request, UserId userId, Map<String, String> baggage) {
    var headers = request.getHeaders();
    var traceId = headers.get("X-Trace-Id");
    if (traceId == null || traceId.isBlank()) {
      traceId = UUID.randomUUID().toString();
    }
    var correlationId = headers.get("X-Correlation-Id");
    if (correlationId == null || correlationId.isBlank()) {
      correlationId = UUID.randomUUID().toString();
    }
    return StreamRuneContext.RequestContext.fromRequest(
        TraceId.of(traceId), userId, CorrelationId.of(correlationId), Instant.now(), baggage);
  }

  /**
   * Resolves the caller's authorities while {@code SecurityService} can still see this request's
   * authentication, so a command that continues off this thread ({@code bus.executeAsync})
   * authorizes exactly as a synchronous execute would. A no-op unless the configured resolver
   * declares {@code requiresRequestContext()}.
   */
  private StreamRuneContext.RequestContext withCapturedAuthority(
      StreamRuneContext.RequestContext ctx) {
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

  /**
   * The request context stored for the request under {@link #CONTEXT_ATTRIBUTE}.
   *
   * @param request the HTTP request
   * @return the request context, or {@code null} when the request carries none (no filter stored
   *     one for it)
   */
  static StreamRuneContext.RequestContext requestContextOf(HttpRequest<?> request) {
    return request
        .getAttribute(CONTEXT_ATTRIBUTE, StreamRuneContext.RequestContext.class)
        .orElse(null);
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
