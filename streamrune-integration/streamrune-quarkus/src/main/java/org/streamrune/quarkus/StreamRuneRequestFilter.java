package org.streamrune.quarkus;

import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
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
 * JAX-RS request filter that extracts HTTP request metadata into a {@link
 * StreamRuneContext.RequestContext}. Auto-registered by Quarkus REST via {@code @Provider} (this
 * library is an indexed bean archive).
 *
 * <p>Header handling, matching the Spring integration's {@code ScopedValueFilter}:
 *
 * <ul>
 *   <li>{@code X-Trace-Id} — generated (random UUID) when missing or blank
 *   <li>{@code X-User-Id} — an identity only in the trusted-gateway mode; see the
 *       authorization-identity note below
 *   <li>{@code X-Correlation-Id} — generated (random UUID) when missing or blank
 *   <li>{@code X-User-Role} — baggage {@code role} only in the trusted-gateway mode; see the role
 *       note below
 * </ul>
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
 * <p><b>Authorization identity (security):</b> the request-context {@code userId} is the identity
 * that {@code CommandAuthorizationPolicy}, {@code @RequireRole}/{@code @RequirePermission} and
 * {@code Authorization.requireOwner(...)} authorize against and that the command audit records, so
 * it must never be a client-supplied value. It is resolved by the {@link RequestIdentityPolicy} the
 * filter is built with — the same policy bean the {@link SseController} uses, and the same rule as
 * the Spring integration's request filter — and fails closed:
 *
 * <ul>
 *   <li>with an {@link AuthenticatedUserResolver} (Quarkus Security present, or an application
 *       bean), the {@code userId} is the authenticated {@code SecurityIdentity}'s principal; the
 *       {@code X-User-Id} header is ignored, and a header naming another user is logged
 *       (sanitized). An unauthenticated request is anonymous;
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
 * <p>The policy sees every {@code X-User-Id} and {@code X-User-Role} value the request carried
 * ({@code getHeaders().get(...)}, the same multivalued view the {@link SseController} reads through
 * {@code HttpHeaders#getRequestHeader}), so a repeated header is anonymous (no role) in the
 * trusted-gateway mode instead of the comma-joined string {@code getHeaderString} would return.
 *
 * <p>The parsed context is stored in the request-scoped {@link StreamRuneRequestContextHolder} and
 * as the request property {@link #CONTEXT_PROPERTY}. JAX-RS filters cannot wrap the resource-method
 * invocation, so the filter binds no {@link ScopedValue}: the command bus and the query bus the
 * integration produces read the holder ({@link StreamRuneRequestContextHolder#ofCurrentRequest()})
 * for a dispatch that starts with {@link StreamRuneContext#CURRENT} unbound, so a resource method
 * dispatches as the request's caller with nothing bound. Code that reads the caller itself, outside
 * a bus dispatch, binds it with {@link #withContext(StreamRuneContext.RequestContext, Runnable)} or
 * {@link #callWithContext(StreamRuneContext.RequestContext, Supplier)}.
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
 * commands. The filter aborts such a request with status {@code 400 Bad Request}, content type
 * {@code text/plain;charset=UTF-8} and the body {@link
 * RequestIdentityPolicy#REFUSED_REQUEST_CONTEXT} — the answer of the Spring and Micronaut filters,
 * byte for byte: the resource method does not run, and neither the {@link
 * StreamRuneRequestContextHolder} nor the {@link #CONTEXT_PROPERTY} is set. The refused value is
 * neither echoed nor logged. Any other correlation id is kept exactly as the client sent it.
 *
 * <p><b>A header sent more than once.</b> Of a repeated {@code X-Correlation-Id} or {@code
 * X-Trace-Id} header the first value is the header ({@code getHeaders().getFirst}, not the
 * comma-joined {@code getHeaderString}), on the three integrations alike: a blank first value
 * counts as an absent header and an id is generated, whatever follows it. One line that holds a
 * comma is one value. The correlation id, the trace id and the baggage are caller-supplied in every
 * identity mode; nothing may be addressed, authorized or deduplicated by them.
 *
 * <p><b>What a request cannot set.</b> This filter builds a request context and nothing else. It
 * does not bind {@link StreamRuneContext#CAUSE}, so no header and no baggage entry, allowlisted or
 * not, reaches the saga origin or the causation id of an event: the command bus takes both from
 * that binding alone.
 */
@Provider
public class StreamRuneRequestFilter implements ContainerRequestFilter {

  /** Request property under which the parsed {@code RequestContext} is stored. */
  public static final String CONTEXT_PROPERTY = "org.streamrune.quarkus.requestContext";

  private static final Logger logger = LoggerFactory.getLogger(StreamRuneRequestFilter.class);

  /** The content type of the answer to a refused request context, the same on the integrations. */
  private static final MediaType REFUSAL_CONTENT_TYPE =
      MediaType.TEXT_PLAIN_TYPE.withCharset("UTF-8");

  /** The policy of the static anonymous builders: no user, no role. */
  private static final RequestIdentityPolicy ANONYMOUS = RequestIdentityPolicy.of(null, false);

  private final StreamRuneRequestContextHolder holder;
  private final Set<String> extraAllowedBaggageKeys;
  private final RequestIdentityPolicy identityPolicy;
  private final UserRoleResolver userRoleResolver;

  /**
   * Creates the filter with no allowlisted baggage key. Its identity policy is {@link
   * RequestIdentityPolicy.Mode#ANONYMOUS} — every request runs without a user or a role, and the
   * {@code X-User-Id} and {@code X-User-Role} headers are ignored — and no {@link UserRoleResolver}
   * is wired, so no authority is captured at the request edge. Quarkus/Arc uses the {@link
   * Inject}-annotated constructor.
   *
   * @param holder the request-scoped context holder
   */
  public StreamRuneRequestFilter(StreamRuneRequestContextHolder holder) {
    this(holder, Set.of(), RequestIdentityPolicy.of(null, false), null);
  }

  /**
   * Creates the filter, taking the OTel baggage allowlist from {@code
   * streamrune.metadata.baggage-allowlist} (the framework adds no key of its own: {@link
   * BaggageAllowlist#DEFAULT_ALLOWED_KEYS} is empty), and resolving identities through the
   * application's {@link RequestIdentityPolicy} bean. This is the constructor Quarkus/Arc uses for
   * dependency injection.
   *
   * @param holder the request-scoped context holder
   * @param properties the bound StreamRune Quarkus properties
   * @param identityPolicy where the request identity and role come from (authenticated principal,
   *     trusted gateway headers, or anonymous); the {@code StreamRuneProducers} bean, shared with
   *     the SSE endpoint
   * @param userRoleResolver the configured role resolver; the caller's authority is captured at the
   *     request edge only when it is resolvable and declares {@link
   *     UserRoleResolver#requiresRequestContext()}
   * @throws IllegalArgumentException if {@code identityPolicy} is {@code null}
   */
  @Inject
  public StreamRuneRequestFilter(
      StreamRuneRequestContextHolder holder,
      StreamRuneQuarkusProperties properties,
      RequestIdentityPolicy identityPolicy,
      Instance<UserRoleResolver> userRoleResolver) {
    this(
        holder,
        Set.copyOf(properties.metadata().baggageAllowlist().orElse(java.util.List.of())),
        identityPolicy,
        // The SAME resolver bean the
        // AnnotationAuthorizationInterceptor is produced with, so the authority captured at the
        // request edge is byte-identical to the one a synchronous execute would have computed.
        userRoleResolver != null && userRoleResolver.isResolvable()
            ? userRoleResolver.get()
            : null);
  }

  StreamRuneRequestFilter(
      StreamRuneRequestContextHolder holder,
      Set<String> extraAllowedBaggageKeys,
      RequestIdentityPolicy identityPolicy,
      UserRoleResolver userRoleResolver) {
    if (identityPolicy == null) {
      throw new IllegalArgumentException("identityPolicy is required");
    }
    this.holder = holder;
    this.extraAllowedBaggageKeys =
        extraAllowedBaggageKeys == null ? Set.of() : extraAllowedBaggageKeys;
    this.identityPolicy = identityPolicy;
    this.userRoleResolver = userRoleResolver;
  }

  @Override
  public void filter(ContainerRequestContext requestContext) {
    UserId userId =
        resolveUserId(headerValues(requestContext, RequestIdentityPolicy.USER_ID_HEADER));
    // Baggage `role` is the X-User-Role header behind a trusted gateway only, never a
    // client's W3C baggage entry — every value read, like X-User-Id, so a repeated header is none.
    var baggage =
        identityPolicy.applyRole(
            BaggageAllowlist.filter(readOtelBaggage(), extraAllowedBaggageKeys),
            headerValues(requestContext, RequestIdentityPolicy.USER_ROLE_HEADER));
    StreamRuneContext.RequestContext ctx;
    try {
      ctx = buildContext(name -> firstHeaderValue(requestContext, name), baggage, userId);
    } catch (IllegalArgumentException refused) {
      // The ingress door refused an id: too long, a control character, or a correlation id with
      // the reserved prefix. That is the client's error, answered here before the resource method
      // runs; the holder and the request property stay unset. The identity and the baggage were
      // resolved above, outside this try, so a failure of theirs is not answered as a refusal. The
      // message names the field and never the value; the value itself is not logged.
      logger.debug("Refusing the request with 400: {}", refused.getMessage());
      // A principal the identity provider asserted and the door refuses is refused on every
      // request of that user: reported once, at a level an operator sees.
      identityPolicy.unacceptablePrincipalWarning(userId).ifPresent(logger::warn);
      requestContext.abortWith(
          Response.status(Response.Status.BAD_REQUEST)
              .type(REFUSAL_CONTENT_TYPE)
              .entity(RequestIdentityPolicy.REFUSED_REQUEST_CONTEXT)
              .build());
      return;
    }
    // Resolve the caller's authorities HERE, while the CDI request
    // scope holding SecurityIdentity is still active, and store the result on the context object
    // itself. That object is the ONLY thing this integration can carry across the boundary — a
    // JAX-RS filter cannot wrap the resource method, so the ScopedValue is bound later by
    // withContext(...)/callWithContext(...) — which makes carrying the authority in the context the
    // only mechanism that reaches executeAsync here at all.
    ctx =
        RequestEdgeAuthority.capture(
            ctx,
            userRoleResolver,
            e ->
                logger.warn(
                    "Capturing request-edge authorities via {} failed; authorization will resolve"
                        + " at command time instead, so an authz-gated command executed off this"
                        + " request thread will be denied",
                    userRoleResolver.getClass().getName(),
                    e));
    requestContext.setProperty(CONTEXT_PROPERTY, ctx);
    holder.set(ctx);
  }

  /**
   * Resolves the authorization identity for the request through the {@link RequestIdentityPolicy}
   * (no header identity unless the trusted-gateway mode is explicit).
   */
  private UserId resolveUserId(List<String> userIdHeaderValues) {
    return identityPolicy.resolve(userIdHeaderValues, StreamRuneRequestFilter::logMismatch);
  }

  /**
   * Every value of the named header the request carried, as received — for {@code X-User-Id} the
   * multivalued view the {@link SseController}'s {@code HttpHeaders#getRequestHeader} reads too, so
   * both resolve the same caller. Not {@code getHeaderString}, which joins repeated values with a
   * comma.
   */
  private static List<String> headerValues(ContainerRequestContext requestContext, String name) {
    var headers = requestContext.getHeaders();
    return headers == null ? null : headers.get(name);
  }

  /**
   * The first value of the named header the request carried, or {@code null} when it carried none —
   * what the servlet API's {@code getHeader} and Micronaut's {@code HttpHeaders#get} return, so the
   * three request filters bind the same value for a header sent more than once. Not {@code
   * getHeaderString}, which joins repeated values with a comma: the joined text is a value no
   * client sent as one, and a blank first value would turn an absent header into a refused one.
   */
  private static String firstHeaderValue(ContainerRequestContext requestContext, String name) {
    var headers = requestContext.getHeaders();
    return headers == null ? null : headers.getFirst(name);
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

  /**
   * Builds an anonymous {@link StreamRuneContext.RequestContext} from a header lookup, applying the
   * defaulting rules documented on this class, with no allowlisted baggage key. Neither {@code
   * X-User-Id} nor {@code X-User-Role} is read: the identity and the role are only ever resolved by
   * the filter's {@link RequestIdentityPolicy}, so the context has no user and no {@code role}.
   *
   * @param headers header lookup that answers one value per name: the first the request carried
   * @return the request context with no user, never {@code null}
   */
  static StreamRuneContext.RequestContext buildContext(Function<String, String> headers) {
    return buildContext(headers, Set.of());
  }

  /**
   * Builds an anonymous {@link StreamRuneContext.RequestContext} from a header lookup, applying the
   * defaulting rules documented on this class. Neither {@code X-User-Id} nor {@code X-User-Role} is
   * read: the context has no user and no {@code role}.
   *
   * @param headers header lookup that answers one value per name: the first the request carried
   * @param extraAllowedBaggageKeys the OTel baggage keys to allow (the framework's own {@link
   *     BaggageAllowlist#DEFAULT_ALLOWED_KEYS} is empty)
   * @return the request context with no user and no role, never {@code null}
   */
  static StreamRuneContext.RequestContext buildContext(
      Function<String, String> headers, Set<String> extraAllowedBaggageKeys) {
    return buildContext(
        headers,
        ANONYMOUS.applyRole(
            BaggageAllowlist.filter(readOtelBaggage(), extraAllowedBaggageKeys), List.of()),
        null);
  }

  /**
   * Builds a {@link StreamRuneContext.RequestContext} from a header lookup with an explicit,
   * already-resolved authorization identity and baggage. Used by the instance filter after its
   * {@link RequestIdentityPolicy} resolved the user and applied the role rule.
   *
   * @param headers header lookup that answers one value per name: the first the request carried
   * @param baggage the request's baggage, already allow-listed and with the role rule applied
   * @param userId the resolved authorization identity, or {@code null} for anonymous
   * @return the request context, never {@code null}
   */
  private static StreamRuneContext.RequestContext buildContext(
      Function<String, String> headers, Map<String, String> baggage, UserId userId) {
    var traceId = headers.apply("X-Trace-Id");
    if (traceId == null || traceId.isBlank()) {
      traceId = UUID.randomUUID().toString();
    }
    var correlationId = headers.apply("X-Correlation-Id");
    if (correlationId == null || correlationId.isBlank()) {
      correlationId = UUID.randomUUID().toString();
    }

    // fromRequest, not the constructor. These two ids
    // came straight off the wire unauthenticated, so this is the ingress door where the
    // IdConstraints bound applies. The plain constructor deliberately does not enforce it — it is
    // also the DLQ-replay / saga-dispatch reconstruction path for ids already at rest.
    return StreamRuneContext.RequestContext.fromRequest(
        TraceId.of(traceId), userId, CorrelationId.of(correlationId), Instant.now(), baggage);
  }

  /**
   * Executes the given runnable with the given context bound to {@link StreamRuneContext#CURRENT}.
   *
   * @param ctx the context to bind; when {@code null} the runnable executes unbound
   * @param runnable the code to execute
   */
  public static void withContext(StreamRuneContext.RequestContext ctx, Runnable runnable) {
    if (ctx == null) {
      runnable.run();
      return;
    }
    ScopedValue.where(StreamRuneContext.CURRENT, ctx).run(runnable);
  }

  /**
   * Executes the given supplier with the given context bound to {@link StreamRuneContext#CURRENT}
   * and returns its result.
   *
   * @param ctx the context to bind; when {@code null} the supplier executes unbound
   * @param supplier the code to execute
   * @param <T> the result type
   * @return the supplier's result
   */
  public static <T> T callWithContext(StreamRuneContext.RequestContext ctx, Supplier<T> supplier) {
    if (ctx == null) {
      return supplier.get();
    }
    return ScopedValue.where(StreamRuneContext.CURRENT, ctx).call(supplier::get);
  }

  private static Map<String, String> readOtelBaggage() {
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
}
