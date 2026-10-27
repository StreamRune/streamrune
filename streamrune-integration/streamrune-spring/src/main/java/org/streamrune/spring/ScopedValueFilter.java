package org.streamrune.spring;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Enumeration;
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
 * Servlet filter that creates a {@link StreamRuneContext.RequestContext} from HTTP headers and
 * binds it to the current scope via {@link ScopedValue}. Reads {@code X-Trace-Id} and {@code
 * X-Correlation-Id} (each generated when missing or blank), and {@code X-User-Id} and {@code
 * X-User-Role} only as far as the {@link RequestIdentityPolicy} allows (below).
 *
 * <p><b>Role (security):</b> baggage {@code role} — which every event the request produces carries
 * into its plaintext metadata — is the {@code X-User-Role} header in the trusted-gateway mode only,
 * where the gateway owns that header exactly as it owns {@code X-User-Id}; a repeated header is no
 * role. In the authenticated-principal and anonymous modes the header is ignored and there is no
 * {@code role}: an authenticated principal's verified roles are the context's captured authority
 * (see {@link #ScopedValueFilter(Set, RequestIdentityPolicy, UserRoleResolver)}), never metadata
 * baggage. A W3C {@code baggage: role=} entry never reaches the key, in any mode and whatever the
 * baggage allowlist says. The framework itself does not authorize on baggage {@code role}:
 * annotation-based authorization resolves authorities through the configured {@code
 * UserRoleResolver} (by default {@link SpringSecurityUserRoleResolver}, which reads Spring
 * Security's context). The rule is {@link RequestIdentityPolicy#applyRole(Map, List)}, identical on
 * the three integrations.
 *
 * <p><b>Authorization identity (security):</b> the request-context {@code userId} is the identity
 * that {@code CommandAuthorizationPolicy}, {@code @RequireRole}/{@code @RequirePermission} and
 * {@code Authorization.requireOwner(...)} authorize against and that the command audit records, so
 * it must never be a client-supplied value. It is resolved by the {@link RequestIdentityPolicy} the
 * filter is built with — the same policy the SSE endpoint uses — and fails closed:
 *
 * <ul>
 *   <li>with an {@link AuthenticatedUserResolver} (Spring Security on the classpath, or an
 *       application bean), the {@code userId} is the <em>authenticated principal</em>; the {@code
 *       X-User-Id} header is ignored, and a header naming another user is logged (sanitized). An
 *       unauthenticated request is anonymous;
 *   <li>with {@code streamrune.security.trust-user-id-header=true} (the explicit trusted-gateway
 *       mode) the {@code userId} is the {@code X-User-Id} header — only safe when a gateway
 *       authenticates every caller, overwrites the header and strips any client-supplied value (and
 *       does the same for {@code X-User-Role});
 *   <li>with neither, every request is anonymous ({@code userId == null}) and the header is
 *       ignored. Nothing is logged per request; the auto-configuration logs the mode once at
 *       startup and refuses to start when command authorization is configured in this mode.
 * </ul>
 *
 * <p>The policy sees every {@code X-User-Id} value the request carried ({@link
 * #userIdHeaderValues(HttpServletRequest)}, which the SSE endpoint reads through too), so a
 * repeated header is anonymous in the trusted-gateway mode instead of resolving to whichever value
 * {@code getHeader} returns first.
 *
 * <p>Because identity is read from the authenticated principal, this filter is ordered to run
 * <em>after</em> Spring Security's filter chain (see {@code
 * StreamRuneAutoConfiguration.SCOPED_VALUE_FILTER_ORDER}); the {@code SecurityContextHolder} is
 * only populated once authentication has run.
 *
 * <p><b>Baggage allowlisting:</b> OpenTelemetry baggage is populated from the client-supplied,
 * unauthenticated W3C {@code baggage:} HTTP header, and every baggage entry ends up in every
 * produced event's immutable, plaintext {@code event_stream.metadata}. To prevent a caller from
 * smuggling arbitrary PII permanently into the event log, only the keys configured via {@code
 * streamrune.metadata.baggage-allowlist} are copied into the request context — see {@link
 * #ScopedValueFilter(Set)}; with none configured no baggage entry is ({@link
 * BaggageAllowlist#DEFAULT_ALLOWED_KEYS} is empty). Non-allowlisted keys are dropped here, before
 * they ever reach {@code RequestContext} or {@code EventMetadata}; {@code role} is the role rule's
 * alone (above).
 *
 * <p><b>A refused request context.</b> The correlation id, the trace id and the user id pass
 * through {@link StreamRuneContext.RequestContext#fromRequest}, which refuses a value longer than
 * 255 characters, a value that holds a control character, and a correlation id that starts with
 * {@code streamrune:} — the prefix of the correlation ids the framework itself binds for a saga's
 * commands. The filter answers such a request itself: status {@code 400 Bad Request}, content type
 * {@code text/plain;charset=UTF-8}, and the body {@link
 * RequestIdentityPolicy#REFUSED_REQUEST_CONTEXT} — the answer of the Quarkus and Micronaut filters,
 * byte for byte. It does not call {@code sendError}: no error dispatch is started, so the
 * container's error page, the application's error handling and a security filter chain's rule for
 * the error path take no part, and the rest of the chain does not run — no controller, no command,
 * nothing stored. The refused value is neither echoed nor logged. Any other correlation id is bound
 * exactly as the client sent it.
 *
 * <p><b>A header sent more than once.</b> Of a repeated {@code X-Correlation-Id} or {@code
 * X-Trace-Id} header the first value is the header ({@code getHeader}), on the three integrations
 * alike: a blank first value counts as an absent header and an id is generated, whatever follows
 * it. One line that holds a comma is one value. The correlation id, the trace id and the baggage
 * are caller-supplied in every identity mode; nothing may be addressed, authorized or deduplicated
 * by them.
 *
 * <p><b>What a request cannot set.</b> This filter binds a request context and nothing else. It
 * does not bind {@link StreamRuneContext#CAUSE}, so no header and no baggage entry, allowlisted or
 * not, reaches the saga origin or the causation id of an event: the command bus takes both from
 * that binding alone.
 *
 * <p><b>Work off the servlet thread.</b> The binding holds on the thread the filter runs on. The
 * filter also stores the context it binds on the request, as the attribute {@link
 * RequestAttributeContextSource#ATTRIBUTE}, and the command bus and the query bus the integration
 * builds read it back through {@link RequestAttributeContextSource} for a dispatch that starts with
 * {@code CURRENT} unbound: a {@code Callable} or a {@code WebAsyncTask} a controller returns runs
 * on the MVC task executor, where Spring MVC makes the request's attributes current, and dispatches
 * as the request's caller. A thread that serves no request dispatches anonymously. A refused
 * request stores nothing.
 */
public class ScopedValueFilter implements Filter {

  private static final Logger logger = LoggerFactory.getLogger(ScopedValueFilter.class);

  /** The content type of the answer to a refused request context, the same on the integrations. */
  private static final String REFUSAL_CONTENT_TYPE = "text/plain;charset=UTF-8";

  private final Set<String> extraAllowedBaggageKeys;
  private final RequestIdentityPolicy identityPolicy;
  private final UserRoleResolver userRoleResolver;

  /**
   * Creates a filter that allowlists no baggage key. See {@link #ScopedValueFilter(Set)} for what
   * it does not wire.
   */
  public ScopedValueFilter() {
    this(Set.of());
  }

  /**
   * Creates a filter that allowlists the {@code extraAllowedBaggageKeys} baggage keys (the
   * framework's own {@link BaggageAllowlist#DEFAULT_ALLOWED_KEYS} is empty). Its identity policy is
   * {@link RequestIdentityPolicy.Mode#ANONYMOUS} — every request runs without a user or a role, and
   * the {@code X-User-Id} and {@code X-User-Role} headers are ignored — and no {@link
   * UserRoleResolver} is wired, so no authority is captured at the request edge. The
   * auto-configuration uses {@link #ScopedValueFilter(Set, RequestIdentityPolicy,
   * UserRoleResolver)}.
   *
   * @param extraAllowedBaggageKeys additional permitted baggage keys, typically sourced from {@code
   *     streamrune.metadata.baggage-allowlist}; {@code null} is treated as empty
   */
  public ScopedValueFilter(Set<String> extraAllowedBaggageKeys) {
    this(extraAllowedBaggageKeys, RequestIdentityPolicy.of(null, false), null);
  }

  /**
   * Creates the fully wired filter (the constructor the auto-configuration uses). The
   * request-context {@code userId} and baggage {@code role} are whatever {@code identityPolicy}
   * resolves for the request, and the caller's resolved {@link org.streamrune.core.UserAuthority}
   * is captured into the request context when {@code userRoleResolver} is non-null and declares
   * {@link UserRoleResolver#requiresRequestContext()}, so authorization decides the same way for
   * work that continues off the request thread ({@code executeAsync}).
   *
   * @param extraAllowedBaggageKeys additional permitted baggage keys, typically sourced from {@code
   *     streamrune.metadata.baggage-allowlist}; {@code null} is treated as empty
   * @param identityPolicy where the request identity and role come from (authenticated principal,
   *     trusted gateway headers, or anonymous); the auto-configured {@link RequestIdentityPolicy}
   *     bean
   * @param userRoleResolver the configured role resolver; the capture happens only when it is
   *     non-null and declares {@link UserRoleResolver#requiresRequestContext()}
   * @throws IllegalArgumentException if {@code identityPolicy} is {@code null}
   */
  public ScopedValueFilter(
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

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    var httpReq = (HttpServletRequest) request;
    var traceId = httpReq.getHeader("X-Trace-Id");
    if (traceId == null || traceId.isBlank()) {
      traceId = UUID.randomUUID().toString();
    }
    var correlationId = httpReq.getHeader("X-Correlation-Id");
    if (correlationId == null || correlationId.isBlank()) {
      correlationId = UUID.randomUUID().toString();
    }
    var userIdHeaderValues = userIdHeaderValues(httpReq);
    // Baggage `role` is the X-User-Role header behind a trusted gateway only, never a
    // client's W3C baggage entry — every value read, like X-User-Id, so a repeated header is none.
    var baggage =
        identityPolicy.applyRole(
            BaggageAllowlist.filter(readOtelBaggage(), extraAllowedBaggageKeys),
            headerValues(httpReq, RequestIdentityPolicy.USER_ROLE_HEADER));
    // Resolved before the context is built, outside the try below: a failure of the identity
    // policy or of the application's AuthenticatedUserResolver is not a refused header.
    UserId userId = resolveUserId(userIdHeaderValues);
    // fromRequest, not the constructor. These two ids
    // came straight off the wire unauthenticated, so this is the ingress door where the
    // IdConstraints bound applies. The plain constructor deliberately does not enforce it — it is
    // also the DLQ-replay / saga-dispatch reconstruction path for ids already at rest.
    StreamRuneContext.RequestContext ctx;
    try {
      ctx =
          StreamRuneContext.RequestContext.fromRequest(
              TraceId.of(traceId), userId, CorrelationId.of(correlationId), Instant.now(), baggage);
    } catch (IllegalArgumentException refused) {
      // The door refused an id: too long, a control character, or a correlation id with the
      // reserved prefix. That is the client's error, answered here before the chain runs. The
      // message names the field and never the value; the value itself is not logged.
      logger.debug("Refusing the request with 400: {}", refused.getMessage());
      // A principal the identity provider asserted and the door refuses is refused on every
      // request of that user: reported once, at a level an operator sees.
      identityPolicy.unacceptablePrincipalWarning(userId).ifPresent(logger::warn);
      // Written here, not handed to sendError: an error dispatch gives the status line and the
      // body to the application's error handling, and a security filter chain that does not admit
      // the caller to the error path answers that dispatch itself (401 or 403 instead of 400).
      var refusal = (HttpServletResponse) response;
      refusal.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      refusal.setContentType(REFUSAL_CONTENT_TYPE);
      refusal.getWriter().write(RequestIdentityPolicy.REFUSED_REQUEST_CONTEXT);
      return;
    }
    // Resolve the caller's authorities HERE, while Spring
    // Security's SecurityContextHolder ThreadLocal is still populated, so a command that continues
    // off this thread (bus.executeAsync) authorizes exactly as a synchronous execute would. A
    // no-op unless the configured resolver declares requiresRequestContext().
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
    // The same context, for the work Spring MVC runs for this request on another thread (a
    // Callable a controller returns), which this binding does not reach. Left on the request after
    // the chain returns: an asynchronous request is still being served then, and the container
    // drops the request's attributes when it recycles the request.
    httpReq.setAttribute(RequestAttributeContextSource.ATTRIBUTE, ctx);
    // call() (not run()) so checked exceptions from the chain propagate unchanged — the servlet
    // container and Spring's error handling special-case ServletException/IOException, so wrapping
    // them in RuntimeException would corrupt error-page resolution and stack traces.
    try {
      ScopedValue.where(StreamRuneContext.CURRENT, ctx)
          .call(
              () -> {
                chain.doFilter(request, response);
                return null;
              });
    } catch (IOException | ServletException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      // Unreachable: chain.doFilter declares only IOException and ServletException.
      throw new ServletException(e);
    }
  }

  /**
   * Resolves the authorization identity for the request through the {@link RequestIdentityPolicy}
   * (no header identity unless the trusted-gateway mode is explicit).
   */
  private UserId resolveUserId(List<String> userIdHeaderValues) {
    return identityPolicy.resolve(userIdHeaderValues, ScopedValueFilter::logMismatch);
  }

  /**
   * Every {@code X-User-Id} value the request carried, as received — the one reading the filter and
   * the {@link SseController} share, so both resolve the same caller. Not {@code getHeader} (first
   * value only) and not a {@code List}-typed {@code @RequestHeader} (Spring splits a single value
   * on commas). Public so an application endpoint that derives a caller through the {@link
   * RequestIdentityPolicy} bean reads the header the same way.
   *
   * @param request the HTTP request
   * @return the header's values; empty when absent or when the container withholds headers
   */
  public static List<String> userIdHeaderValues(HttpServletRequest request) {
    return headerValues(request, RequestIdentityPolicy.USER_ID_HEADER);
  }

  private static List<String> headerValues(HttpServletRequest request, String name) {
    Enumeration<String> values = request.getHeaders(name);
    return values == null ? List.of() : Collections.list(values);
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
}
