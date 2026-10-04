package org.streamrune.spring;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Authorization;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;

/** Tests for {@link ScopedValueFilter}. */
class ScopedValueFilterTest {

  // The authorization identity must come from the authenticated principal, never the
  // client-supplied X-User-Id header.

  // The id bound is enforced at the INGRESS DOOR the
  // filter takes (RequestContext.fromRequest), not in the RequestContext constructor — which is
  // also the DLQ-replay / saga-dispatch reconstruction path. This test pins the door: were the
  // filter to fall back to the plain constructor, an unauthenticated header would reach
  // event_stream.metadata unbounded again.

  @Test
  void controlCharactersInAnUnauthenticatedIdHeaderAreRejectedAtTheRequestEdge() throws Exception {
    var filter =
        new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(Optional::empty, false), null);
    var response = mock(HttpServletResponse.class);
    FilterChain chain = (req, res) -> {};

    var forgedCorrelation = mock(HttpServletRequest.class);
    when(forgedCorrelation.getHeader("X-Correlation-Id")).thenReturn("corr\r\nX-Injected: 1");
    assertThrows(
        IllegalArgumentException.class, () -> filter.doFilter(forgedCorrelation, response, chain));

    var forgedTrace = mock(HttpServletRequest.class);
    when(forgedTrace.getHeader("X-Trace-Id")).thenReturn("trace" + (char) 0x01);
    assertThrows(
        IllegalArgumentException.class, () -> filter.doFilter(forgedTrace, response, chain));
  }

  @Test
  void anOversizedIdHeaderIsRejectedAtTheRequestEdge() throws Exception {
    var filter =
        new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(Optional::empty, false), null);
    var response = mock(HttpServletResponse.class);
    FilterChain chain = (req, res) -> {};

    var request = mock(HttpServletRequest.class);
    when(request.getHeader("X-Correlation-Id")).thenReturn("c".repeat(256));
    assertThrows(IllegalArgumentException.class, () -> filter.doFilter(request, response, chain));
  }

  /**
   * The identity crosses the same door and is now bounded there too. Both arms are covered — the
   * header arm ({@code trust-user-id-header=true}, the trusted-gateway mode) and the
   * authenticated-principal arm, whose value is whatever the IdP asserted and is therefore neither
   * automatically bounded nor charset-safe. An identity over 255 characters cannot be written to
   * {@code dead_letter_queue.user_id VARCHAR(255)} at all, and the bus catches and logs that INSERT
   * failure — so accepting the request would leave an accepted command with no recovery channel and
   * no signal. Identical rejection on Quarkus and Micronaut.
   */
  @Test
  void anOversizedOrControlBearingUserIdIsRejectedAtTheRequestEdge() throws Exception {
    var response = mock(HttpServletResponse.class);
    FilterChain chain = (req, res) -> {};

    // Header-identity arm: the trusted-gateway mode (the only mode that reads the header).
    var headerFilter = new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(null, true), null);
    var oversizedHeader = mock(HttpServletRequest.class);
    when(oversizedHeader.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("u".repeat(256)));
    assertThrows(
        IllegalArgumentException.class,
        () -> headerFilter.doFilter(oversizedHeader, response, chain));

    var forgedHeader = mock(HttpServletRequest.class);
    when(forgedHeader.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("alice\r\nadmin"));
    assertThrows(
        IllegalArgumentException.class, () -> headerFilter.doFilter(forgedHeader, response, chain));

    // Authenticated-principal arm: "already authenticated" is not "bounded".
    AuthenticatedUserResolver oversizedPrincipal = () -> Optional.of(UserId.of("p".repeat(256)));
    var principalFilter =
        new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(oversizedPrincipal, false), null);
    assertThrows(
        IllegalArgumentException.class,
        () -> principalFilter.doFilter(mock(HttpServletRequest.class), response, chain));

    // At the bound it is accepted — the ceiling is the DLQ column width, not a style preference.
    var atBound = mock(HttpServletRequest.class);
    when(atBound.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("u".repeat(255)));
    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain capturing = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());
    headerFilter.doFilter(atBound, response, capturing);
    assertEquals(255, captured.get().userId().value().length());
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
    // Scenario from the GDPR hardening spec: causationId + role survive, email/tracking dropped.
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
            .put(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY, "evt-1")
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
    assertEquals(
        "evt-1", resultBaggage.get(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY));
    assertEquals("admin", resultBaggage.get("role"));
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
            .put(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY, "evt-1")
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
    assertEquals(
        "evt-1", resultBaggage.get(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY));
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
  void causationIdBaggageSurvivesAllowlistAndPropagatesUnfiltered() throws Exception {
    // Causation-id propagation must keep working exactly as before: an allowlisted
    // causationId still flows through into RequestContext baggage.
    var filter = new ScopedValueFilter();
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeader("X-Trace-Id")).thenReturn("trace-1");
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("user-1"));
    when(request.getHeader("X-Correlation-Id")).thenReturn("corr-1");

    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder()
            .put(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY, "evt-trigger-1")
            .build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> captured.set(StreamRuneContext.CURRENT.get());

    try (var _ = otelCtx.makeCurrent()) {
      filter.doFilter(request, response, chain);
    }

    assertNotNull(captured.get());
    assertEquals(
        "evt-trigger-1",
        captured
            .get()
            .baggage()
            .get(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY));
  }
}
