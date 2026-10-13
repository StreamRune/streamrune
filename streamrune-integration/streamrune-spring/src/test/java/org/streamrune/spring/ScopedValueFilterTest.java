package org.streamrune.spring;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.streamrune.core.Authorization;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.CommandBusProvenanceContract;
import org.streamrune.test.InMemoryEventStore;

/** Tests for {@link ScopedValueFilter}. */
class ScopedValueFilterTest {

  /** A W3C baggage key a client may send to name a causing event; the framework reads none. */
  private static final String CAUSATION_KEY = "streamrune.causation-id";

  /** A W3C baggage key a client may send to name a saga; the framework reads none. */
  private static final String ORIGIN_KEY = "sagaOrigin";

  // The authorization identity must come from the authenticated principal, never the
  // client-supplied X-User-Id header.

  // The id bound is enforced at the INGRESS DOOR the
  // filter takes (RequestContext.fromRequest), not in the RequestContext constructor — which is
  // also the DLQ-replay / saga-dispatch reconstruction path. This test pins the door: were the
  // filter to fall back to the plain constructor, an unauthenticated header would reach
  // event_stream.metadata unbounded again.

  // The filter answers a refusal of the door itself: 400 Bad Request with one fixed reason, before
  // the chain runs.

  private static ScopedValueFilter anonymousFilter() {
    return new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(Optional::empty, false), null);
  }

  /** A request that carries one header. */
  private static MockHttpServletRequest requestWith(String header, String value) {
    var request = new MockHttpServletRequest();
    request.addHeader(header, value);
    return request;
  }

  /**
   * {@code filter} answers {@code request} with {@code 400} and the shared reason, calls no chain
   * and leaves nothing bound; the response repeats nothing of {@code refusedValue}.
   */
  private static void assertAnswered400(
      ScopedValueFilter filter, HttpServletRequest request, String refusedValue) throws Exception {
    var response = new MockHttpServletResponse();
    var chainCalls = new ArrayList<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> chainCalls.add(StreamRuneContext.capture());

    filter.doFilter(request, response, chain);

    assertEquals(400, response.getStatus());
    assertEquals(RequestIdentityPolicy.REFUSED_REQUEST_CONTEXT, response.getErrorMessage());
    assertTrue(chainCalls.isEmpty(), "the chain is not called");
    assertFalse(StreamRuneContext.CURRENT.isBound());
    assertFalse(response.getErrorMessage().contains(refusedValue), "the value is not echoed");
    assertEquals("", response.getContentAsString());
  }

  @Test
  void aReservedCorrelationId_isAnswered400() throws Exception {
    for (String reserved :
        List.of("streamrune:saga:order-saga-42", "streamrune:", "streamrune:anything")) {
      assertAnswered400(anonymousFilter(), requestWith("X-Correlation-Id", reserved), reserved);
      assertAnswered400(
          new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(null, true), null),
          requestWith("X-Correlation-Id", reserved),
          reserved);
      assertAnswered400(
          new ScopedValueFilter(
              Set.of(), RequestIdentityPolicy.of(() -> Optional.of(UserId.of("bob")), false), null),
          requestWith("X-Correlation-Id", reserved),
          reserved);
    }
  }

  @Test
  void aCorrelationIdThatIsNotInTheReservedForm_isBoundAsSent() throws Exception {
    for (String ordinary :
        List.of(
            "order-saga-42",
            "saga:order-saga-42",
            "Streamrune:saga:order-saga-42",
            "streamrune",
            "upstream/streamrune:saga:order-saga-42",
            "c".repeat(255))) {
      var bound = new AtomicReference<StreamRuneContext.RequestContext>();
      var response = new MockHttpServletResponse();

      anonymousFilter()
          .doFilter(
              requestWith("X-Correlation-Id", ordinary),
              response,
              (req, res) -> bound.set(StreamRuneContext.CURRENT.get()));

      assertEquals(200, response.getStatus());
      assertEquals(ordinary, bound.get().correlationId().value());
    }
  }

  @Test
  void aTraceIdWithTheReservedPrefix_isBoundAsSent() throws Exception {
    var bound = new AtomicReference<StreamRuneContext.RequestContext>();

    anonymousFilter()
        .doFilter(
            requestWith("X-Trace-Id", "streamrune:saga:order-saga-42"),
            new MockHttpServletResponse(),
            (req, res) -> bound.set(StreamRuneContext.CURRENT.get()));

    assertEquals("streamrune:saga:order-saga-42", bound.get().traceId().value());
  }

  @Test
  void anOverlongCorrelationId_isAnswered400() throws Exception {
    String overlong = "c".repeat(256);

    assertAnswered400(anonymousFilter(), requestWith("X-Correlation-Id", overlong), overlong);
    assertAnswered400(anonymousFilter(), requestWith("X-Trace-Id", overlong), overlong);
  }

  @Test
  void aControlCharacterInTheTraceId_isAnswered400() throws Exception {
    assertAnswered400(
        anonymousFilter(), requestWith("X-Trace-Id", "trace" + (char) 0x01), "trace" + (char) 0x01);
    assertAnswered400(
        anonymousFilter(), requestWith("X-Correlation-Id", "corr\r\nX-Injected: 1"), "X-Injected");
  }

  /**
   * The identity crosses the same door. Both arms are covered — the header arm ({@code
   * trust-user-id-header=true}, the trusted-gateway mode) and the authenticated-principal arm,
   * whose value is whatever the IdP asserted and is therefore neither automatically bounded nor
   * charset-safe. An identity over 255 characters cannot be written to {@code
   * dead_letter_queue.user_id VARCHAR(255)} at all, and the bus catches and logs that INSERT
   * failure — so accepting the request would leave an accepted command with no recovery channel and
   * no signal. Identical on Quarkus and Micronaut.
   */
  @Test
  void anOverlongUserId_isAnswered400() throws Exception {
    // Header-identity arm: the trusted-gateway mode (the only mode that reads the header).
    var headerFilter = new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(null, true), null);
    assertAnswered400(headerFilter, requestWith("X-User-Id", "u".repeat(256)), "u".repeat(256));
    assertAnswered400(headerFilter, requestWith("X-User-Id", "alice\r\nadmin"), "admin");

    // Authenticated-principal arm: "already authenticated" is not "bounded".
    AuthenticatedUserResolver oversizedPrincipal = () -> Optional.of(UserId.of("p".repeat(256)));
    var principalFilter =
        new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(oversizedPrincipal, false), null);
    assertAnswered400(principalFilter, new MockHttpServletRequest(), "p".repeat(256));

    // At the bound it is accepted — the ceiling is the DLQ column width, not a style preference.
    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain capturing = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());
    headerFilter.doFilter(
        requestWith("X-User-Id", "u".repeat(255)), new MockHttpServletResponse(), capturing);
    assertEquals(255, captured.get().userId().value().length());
  }

  /**
   * Only the door's refusal is answered {@code 400}. An {@link IllegalArgumentException} of the
   * application's identity resolver, or of the application behind the filter, is not a refused
   * header and leaves the filter as it is.
   */
  @Test
  void anIllegalArgumentExceptionThatIsNotTheDoors_isNotAnswered400() {
    var resolverFailure = new IllegalArgumentException("the resolver's own failure");
    AuthenticatedUserResolver failing =
        () -> {
          throw resolverFailure;
        };
    var failingResolverFilter =
        new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(failing, false), null);
    var response = new MockHttpServletResponse();

    assertSame(
        resolverFailure,
        assertThrows(
            IllegalArgumentException.class,
            () ->
                failingResolverFilter.doFilter(
                    new MockHttpServletRequest(), response, (req, res) -> {})));
    assertEquals(200, response.getStatus());

    var applicationFailure = new IllegalArgumentException("the application's own failure");
    assertSame(
        applicationFailure,
        assertThrows(
            IllegalArgumentException.class,
            () ->
                anonymousFilter()
                    .doFilter(
                        new MockHttpServletRequest(),
                        response,
                        (req, res) -> {
                          throw applicationFailure;
                        })));
    assertEquals(200, response.getStatus());
  }

  @Test
  void authenticatedPrincipalOverridesSpoofedUserIdHeader() throws Exception {
    // Authenticated as "bob" but the request carries X-User-Id: alice. Identity must be bob.
    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var filter = new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(resolver, false), null);
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("alice"));

    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());
    filter.doFilter(request, response, chain);

    assertEquals("bob", captured.get().userId().value());
  }

  @Test
  void ownershipCheckDeniesWhenHeaderSpoofsAnotherOwner() throws Exception {
    // Authenticated as bob, header claims alice; an ownership guard for alice's aggregate must
    // DENY.
    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var filter = new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(resolver, false), null);
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("alice"));

    var denied = new AtomicBoolean(false);
    FilterChain chain =
        (req, res) -> {
          try {
            Authorization.requireOwner(UserId.of("alice"));
          } catch (AuthorizationException _) {
            denied.set(true);
          }
        };
    filter.doFilter(request, response, chain);

    assertTrue(denied.get(), "ownership command for alice must be denied when principal is bob");
  }

  @Test
  void unauthenticatedRequestIgnoresUserIdHeader() throws Exception {
    // A resolver that reports no authenticated principal: the header must not confer an identity.
    AuthenticatedUserResolver resolver = Optional::empty;
    var filter = new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(resolver, false), null);
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("alice"));

    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());
    filter.doFilter(request, response, chain);

    assertNull(captured.get().userId());
  }

  @Test
  void trustUserIdHeaderOptInUsesHeaderIdentity() throws Exception {
    // Opt-in trusted-gateway model: the header is authoritative even with a resolver present.
    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var filter = new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(resolver, true), null);
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("alice"));

    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());
    filter.doFilter(request, response, chain);

    assertEquals("alice", captured.get().userId().value());
  }

  @Test
  void populatesContextFromHeaders() throws Exception {
    var filter = new ScopedValueFilter();
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeader("X-Trace-Id")).thenReturn("trace-1");
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("user-1"));
    when(request.getHeader("X-Correlation-Id")).thenReturn("corr-1");

    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());

    filter.doFilter(request, response, chain);

    assertNotNull(captured.get());
    assertEquals("trace-1", captured.get().traceId().value());
    // The no-argument filter is ANONYMOUS — X-User-Id is not an identity without the
    // trusted-gateway flag or an authenticated principal.
    assertNull(captured.get().userId());
    assertEquals("corr-1", captured.get().correlationId().value());
  }

  @Test
  void anAnonymousFilterIgnoresEvenAnOversizedOrForgedUserIdHeader() throws Exception {
    // In the ANONYMOUS mode the header is not read for identity at all, so a value the
    // trusted-gateway door would reject cannot fail (or reach) the request either.
    var filter = new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(null, false), null);
    var response = mock(HttpServletResponse.class);
    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());

    for (String header : new String[] {"u".repeat(256), "alice\r\nadmin", "alice"}) {
      var request = mock(HttpServletRequest.class);
      when(request.getHeaders("X-User-Id"))
          .thenAnswer(invocation -> TestServletHeaders.userIdHeader(header));
      filter.doFilter(request, response, chain);
      assertNull(captured.get().userId());
    }
  }

  @Test
  void aFilterWithoutAnIdentityPolicyIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> new ScopedValueFilter(Set.of(), null, null));
  }

  @Test
  void generatesCorrelationIdWhenMissing() throws Exception {
    var filter = new ScopedValueFilter();
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeader("X-Trace-Id")).thenReturn("trace-1");
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("user-1"));
    when(request.getHeader("X-Correlation-Id")).thenReturn(null);

    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());

    filter.doFilter(request, response, chain);

    assertNotNull(captured.get());
    assertNotNull(captured.get().correlationId());
    assertFalse(captured.get().correlationId().value().isBlank());
  }

  @Test
  void generatesCorrelationIdWhenBlank() throws Exception {
    var filter = new ScopedValueFilter();
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeader("X-Trace-Id")).thenReturn("trace-1");
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("user-1"));
    when(request.getHeader("X-Correlation-Id")).thenReturn("  ");

    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());

    filter.doFilter(request, response, chain);
    assertFalse(captured.get().correlationId().value().isBlank());
  }

  @Test
  void nonAllowlistedBaggageIsDroppedByDefault() throws Exception {
    // A non-allowlisted OTel baggage key (e.g. from an unauthenticated client-supplied
    // `baggage:` header) must never reach RequestContext/EventMetadata baggage.
    var filter = new ScopedValueFilter();
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeader("X-Trace-Id")).thenReturn("trace-1");
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("user-1"));
    when(request.getHeader("X-Correlation-Id")).thenReturn("corr-1");

    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder().put("tenant", "acme").build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());

    try (var _ = otelCtx.makeCurrent()) {
      filter.doFilter(request, response, chain);
    }

    assertNotNull(captured.get());
    assertFalse(captured.get().baggage().containsKey("tenant"));
  }

  @Test
  void configuredExtraAllowlistedKeyIsKept() throws Exception {
    var filter = new ScopedValueFilter(java.util.Set.of("tenant"));
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeader("X-Trace-Id")).thenReturn("trace-1");
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("user-1"));
    when(request.getHeader("X-Correlation-Id")).thenReturn("corr-1");

    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder().put("tenant", "acme").build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());

    try (var _ = otelCtx.makeCurrent()) {
      filter.doFilter(request, response, chain);
    }

    assertNotNull(captured.get());
    assertEquals("acme", captured.get().baggage().get("tenant"));
  }

  @Test
  void onlyAllowlistedKeysSurvive_defaultAllowlistDropsPiiAndTracking() throws Exception {
    // The role comes from the X-User-Role header, so the filter runs behind a trusted gateway
    // (the only mode in which the header becomes baggage `role`).
    // The gateway's role survives; every W3C baggage entry is dropped, a causation entry included.
    var filter = gatewayFilter(Set.of());
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeader("X-Trace-Id")).thenReturn("trace-1");
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("user-1"));
    when(request.getHeader("X-Correlation-Id")).thenReturn("corr-1");
    when(request.getHeaders("X-User-Role"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("admin"));

    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder()
            .put(CAUSATION_KEY, "evt-1")
            .put("email", "pii@example.com")
            .put("tracking", "leak")
            .build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());

    try (var _ = otelCtx.makeCurrent()) {
      filter.doFilter(request, response, chain);
    }

    assertNotNull(captured.get());
    var resultBaggage = captured.get().baggage();
    assertEquals(Map.of("role", "admin"), resultBaggage);
    assertFalse(resultBaggage.containsKey(CAUSATION_KEY), "no baggage key is kept by default");
    assertFalse(resultBaggage.containsKey("email"));
    assertFalse(resultBaggage.containsKey("tracking"));
  }

  @Test
  void configuredAllowlistKeepsTrackingButStillDropsEmail() throws Exception {
    var filter = gatewayFilter(Set.of("tracking"));
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeader("X-Trace-Id")).thenReturn("trace-1");
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("user-1"));
    when(request.getHeader("X-Correlation-Id")).thenReturn("corr-1");
    when(request.getHeaders("X-User-Role"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("admin"));

    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder()
            .put(CAUSATION_KEY, "evt-1")
            .put("email", "pii@example.com")
            .put("tracking", "leak")
            .build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());

    try (var _ = otelCtx.makeCurrent()) {
      filter.doFilter(request, response, chain);
    }

    assertNotNull(captured.get());
    var resultBaggage = captured.get().baggage();
    assertFalse(resultBaggage.containsKey(CAUSATION_KEY), "only the configured key is kept");
    assertEquals("admin", resultBaggage.get("role"));
    assertEquals("leak", resultBaggage.get("tracking"));
    assertFalse(resultBaggage.containsKey("email"));
  }

  /** A trusted-gateway filter: the only mode in which X-User-Role becomes baggage role. */
  private static ScopedValueFilter gatewayFilter(Set<String> extraAllowedBaggageKeys) {
    return new ScopedValueFilter(
        extraAllowedBaggageKeys, RequestIdentityPolicy.of(null, true), null);
  }

  /**
   * The context {@code filter} binds for a request carrying the given {@code X-User-Role} lines.
   */
  private static StreamRuneContext.RequestContext boundWithRoles(
      ScopedValueFilter filter, String... roles) throws Exception {
    var request = mock(HttpServletRequest.class);
    when(request.getHeader("X-Trace-Id")).thenReturn("trace-1");
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("user-1"));
    when(request.getHeader("X-Correlation-Id")).thenReturn("corr-1");
    when(request.getHeaders("X-User-Role"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader(roles));
    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());
    filter.doFilter(request, mock(HttpServletResponse.class), chain);
    assertNotNull(captured.get());
    return captured.get();
  }

  @Test
  void theTrustedGatewayPopulatesRoleBaggageFromXUserRoleHeader() throws Exception {
    assertEquals("ADMIN", boundWithRoles(gatewayFilter(Set.of()), "ADMIN").baggage().get("role"));
  }

  @Test
  void roleBaggageAbsentWhenHeaderMissing() throws Exception {
    assertFalse(boundWithRoles(gatewayFilter(Set.of())).baggage().containsKey("role"));
  }

  @Test
  void theRoleHeaderIsIgnoredOutsideTheTrustedGateway() throws Exception {
    // The no-argument filter is anonymous, and the principal mode ignores the header too.
    assertFalse(boundWithRoles(new ScopedValueFilter(), "ADMIN").baggage().containsKey("role"));
    var principal =
        new ScopedValueFilter(
            Set.of(), RequestIdentityPolicy.of(() -> Optional.of(UserId.of("bob")), false), null);
    var ctx = boundWithRoles(principal, "ADMIN");
    assertEquals(UserId.of("bob"), ctx.userId());
    assertFalse(ctx.baggage().containsKey("role"));
  }

  @Test
  void aRepeatedRoleHeaderIsNoRoleBehindATrustedGateway() throws Exception {
    assertFalse(
        boundWithRoles(gatewayFilter(Set.of()), "CUSTOMER", "ADMIN").baggage().containsKey("role"));
  }

  @Test
  void aW3cBaggageRoleEntryIsDroppedEvenWhenAllowlisted() throws Exception {
    // `role` is not a default allow-listed key, and an application listing it does not
    // reopen the door — the role rule owns the key.
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current()
            .with(io.opentelemetry.api.baggage.Baggage.builder().put("role", "ADMIN").build());
    try (var _ = otelCtx.makeCurrent()) {
      assertFalse(boundWithRoles(new ScopedValueFilter()).baggage().containsKey("role"));
      assertFalse(
          boundWithRoles(gatewayFilter(Set.of("role"))).baggage().containsKey("role"),
          "behind a trusted gateway the header is the only source");
      assertEquals(
          "CUSTOMER",
          boundWithRoles(gatewayFilter(Set.of("role")), "CUSTOMER").baggage().get("role"));
    }
  }

  @Test
  void servletExceptionFromChainPropagatesUnwrapped() {
    // The servlet container and Spring's error handling special-case ServletException; wrapping
    // it in RuntimeException would break error-page resolution.
    var filter = new ScopedValueFilter();
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    var failure = new jakarta.servlet.ServletException("handler failed");
    FilterChain chain =
        (req, res) -> {
          throw failure;
        };

    var thrown =
        assertThrows(
            jakarta.servlet.ServletException.class,
            () -> filter.doFilter(request, response, chain));
    assertSame(failure, thrown);
  }

  @Test
  void ioExceptionFromChainPropagatesUnwrapped() {
    var filter = new ScopedValueFilter();
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    var failure = new java.io.IOException("broken pipe");
    FilterChain chain =
        (req, res) -> {
          throw failure;
        };

    var thrown =
        assertThrows(java.io.IOException.class, () -> filter.doFilter(request, response, chain));
    assertSame(failure, thrown);
  }

  @Test
  void runtimeExceptionFromChainPropagatesUnwrapped() {
    var filter = new ScopedValueFilter();
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    var failure = new IllegalStateException("boom");
    FilterChain chain =
        (req, res) -> {
          throw failure;
        };

    var thrown =
        assertThrows(IllegalStateException.class, () -> filter.doFilter(request, response, chain));
    assertSame(failure, thrown);
  }

  @Test
  void roleBaggageMergesWithOtelBaggage() throws Exception {
    // "tenant" is allowlisted explicitly here to verify role-header merging still works
    // alongside a configured extra allowlisted OTel baggage key.
    var filter = gatewayFilter(Set.of("tenant"));
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeader("X-Trace-Id")).thenReturn("trace-1");
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("user-1"));
    when(request.getHeader("X-Correlation-Id")).thenReturn("corr-1");
    when(request.getHeaders("X-User-Role"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("CUSTOMER"));

    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder().put("tenant", "acme").build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());

    try (var _ = otelCtx.makeCurrent()) {
      filter.doFilter(request, response, chain);
    }

    assertNotNull(captured.get());
    assertEquals("acme", captured.get().baggage().get("tenant"));
    assertEquals("CUSTOMER", captured.get().baggage().get("role"));
  }

  @Test
  void aCausationBaggageEntryIsDroppedUnlessTheApplicationAllowlistsIt() throws Exception {
    assertFalse(
        baggageBoundBy(new ScopedValueFilter()).containsKey(CAUSATION_KEY),
        "the default allowlist is empty");
    // Allowlisted, it is an ordinary baggage entry; nothing reads it as a causation id.
    assertEquals(
        "evt-trigger-1",
        baggageBoundBy(new ScopedValueFilter(Set.of(CAUSATION_KEY))).get(CAUSATION_KEY));
  }

  private static Map<String, String> baggageBoundBy(ScopedValueFilter filter) throws Exception {
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeader("X-Trace-Id")).thenReturn("trace-1");
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("user-1"));
    when(request.getHeader("X-Correlation-Id")).thenReturn("corr-1");

    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder().put(CAUSATION_KEY, "evt-trigger-1").build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());

    try (var _ = otelCtx.makeCurrent()) {
      filter.doFilter(request, response, chain);
    }

    assertNotNull(captured.get());
    return captured.get().baggage();
  }

  // ==== No header reaches an event's saga origin or causation id ====

  /**
   * In every identity mode, a request that carries every header a client can send — a correlation
   * id (a random one, and the id of somebody else's saga), a trace id, a user, a role, invented
   * provenance headers and W3C baggage naming a causing event and a saga — runs a command on the
   * production bus, and the stored events carry no saga origin and no causation id. Run with the
   * baggage keys dropped (the default) and allowlisted.
   */
  @TestFactory
  Stream<DynamicTest> noHeaderSetsAnOriginOrACausationId() {
    Map<String, RequestIdentityPolicy> modes = new LinkedHashMap<>();
    modes.put(
        "authenticated principal",
        RequestIdentityPolicy.of(() -> Optional.of(UserId.of("bob")), false));
    modes.put("trusted gateway", RequestIdentityPolicy.of(null, true));
    modes.put("anonymous", RequestIdentityPolicy.of(null, false));

    List<DynamicTest> tests = new ArrayList<>();
    for (var mode : modes.entrySet()) {
      for (String correlationId : List.of(UUID.randomUUID().toString(), "order-saga-42")) {
        for (Set<String> allowlist : List.of(Set.<String>of(), Set.of(CAUSATION_KEY, ORIGIN_KEY))) {
          tests.add(
              DynamicTest.dynamicTest(
                  mode.getKey()
                      + ", X-Correlation-Id="
                      + correlationId
                      + ", allowlist="
                      + allowlist,
                  () ->
                      assertNoHeaderSetsProvenance(
                          mode.getKey(), mode.getValue(), correlationId, allowlist)));
        }
      }
    }
    return tests.stream();
  }

  private static void assertNoHeaderSetsProvenance(
      String mode, RequestIdentityPolicy policy, String correlationId, Set<String> allowlist)
      throws Exception {
    var request = new MockHttpServletRequest("POST", "/orders");
    request.addHeader("X-Correlation-Id", correlationId);
    request.addHeader("X-Trace-Id", "trace-1");
    request.addHeader("X-User-Id", "mallory");
    request.addHeader("X-User-Role", "ADMIN");
    request.addHeader("X-Causation-Id", "evt-1");
    request.addHeader(
        "X-Saga-Origin", "{\"sagaType\":\"x.S\",\"sagaId\":\"order-saga-42\",\"hop\":0}");
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current()
            .with(
                io.opentelemetry.api.baggage.Baggage.builder()
                    .put(CAUSATION_KEY, "evt-1")
                    .put(ORIGIN_KEY, "order-saga-42")
                    .build());

    var eventStore = new InMemoryEventStore();
    try (var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(new LocalStripedLocker(16))
            .register(
                CommandBusProvenanceContract.AGGREGATE_TYPE,
                CommandBusProvenanceContract.Mark.class,
                CommandBusProvenanceContract::aggregateIdOf,
                CommandBusProvenanceContract.DECIDER)
            .build()) {
      FilterChain chain =
          (req, res) -> {
            assertNull(StreamRuneContext.cause(), "a request filter binds no dispatch cause");
            assertFalse(StreamRuneContext.isSagaDispatch());
            bus.execute(new CommandBusProvenanceContract.Mark("marked-1"));
          };
      try (var _ = otelCtx.makeCurrent()) {
        new ScopedValueFilter(allowlist, policy, null)
            .doFilter(request, new MockHttpServletResponse(), chain);
      }
    }

    var stored =
        eventStore
            .load(
                StreamId.of(
                    CommandBusProvenanceContract.AGGREGATE_TYPE, AggregateId.of("marked-1")))
            .events();
    assertEquals(2, stored.size());
    for (var envelope : stored) {
      var metadata = envelope.metadata();
      assertNull(metadata.sagaOrigin());
      assertNull(metadata.causationId());
      // The request's own values did arrive: what a client may set, it sets.
      assertEquals(correlationId, metadata.correlationId().value());
      assertEquals("trace-1", metadata.traceId().value());
      switch (mode) {
        case "authenticated principal" -> assertEquals("bob", metadata.userId().value());
        case "trusted gateway" -> {
          assertEquals("mallory", metadata.userId().value());
          assertEquals("ADMIN", metadata.baggage().get("role"));
        }
        default -> assertNull(metadata.userId());
      }
      if (allowlist.isEmpty()) {
        assertFalse(metadata.baggage().containsKey(CAUSATION_KEY));
        assertFalse(metadata.baggage().containsKey(ORIGIN_KEY));
      } else {
        // Allowlisted, both are ordinary baggage entries and still decide nothing.
        assertEquals("evt-1", metadata.baggage().get(CAUSATION_KEY));
        assertEquals("order-saga-42", metadata.baggage().get(ORIGIN_KEY));
      }
    }
  }
}
