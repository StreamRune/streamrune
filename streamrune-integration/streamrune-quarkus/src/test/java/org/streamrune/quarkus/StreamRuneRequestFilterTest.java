package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.ext.Provider;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;

/** Tests for {@link StreamRuneRequestFilter}. */
class StreamRuneRequestFilterTest {

  private static Function<String, String> headers(Map<String, String> values) {
    return values::get;
  }

  // The authorization identity must come from the authenticated principal, never the
  // client-supplied X-User-Id header.

  // The id bound is enforced at the INGRESS DOOR the
  // filter takes (RequestContext.fromRequest), not in the RequestContext constructor — which is
  // also the DLQ-replay / saga-dispatch reconstruction path. This test pins the door: were the
  // filter to fall back to the plain constructor, an unauthenticated header would reach
  // event_stream.metadata unbounded again.

  @Test
  void controlCharactersInAnUnauthenticatedIdHeaderAreRejectedAtTheRequestEdge() {
    var holder = new StreamRuneRequestContextHolder();
    var filter =
        new StreamRuneRequestFilter(
            holder, Set.of(), RequestIdentityPolicy.of(Optional::empty, false), null);

    ContainerRequestContext forgedCorrelation = mock(ContainerRequestContext.class);
    when(forgedCorrelation.getHeaderString("X-Correlation-Id")).thenReturn("corr\r\nX-Injected: 1");
    assertThrows(IllegalArgumentException.class, () -> filter.filter(forgedCorrelation));

    ContainerRequestContext forgedTrace = mock(ContainerRequestContext.class);
    when(forgedTrace.getHeaderString("X-Trace-Id")).thenReturn("trace" + (char) 0x01);
    assertThrows(IllegalArgumentException.class, () -> filter.filter(forgedTrace));
  }

  @Test
  void anOversizedIdHeaderIsRejectedAtTheRequestEdge() {
    var holder = new StreamRuneRequestContextHolder();
    var filter =
        new StreamRuneRequestFilter(
            holder, Set.of(), RequestIdentityPolicy.of(Optional::empty, false), null);

    ContainerRequestContext rc = mock(ContainerRequestContext.class);
    when(rc.getHeaderString("X-Correlation-Id")).thenReturn("c".repeat(256));
    assertThrows(IllegalArgumentException.class, () -> filter.filter(rc));
  }

  /**
   * The identity crosses the same door and is now bounded there too. Both arms are covered — the
   * header arm (only the trusted-gateway mode, {@code trust-user-id-header=true}) and the
   * authenticated-principal arm, whose value is whatever the IdP asserted and is therefore neither
   * automatically bounded nor charset-safe. An identity over 255 characters cannot be written to
   * {@code dead_letter_queue.user_id VARCHAR(255)} at all, and the bus catches and logs that INSERT
   * failure — so accepting the request would leave an accepted command with no recovery channel and
   * no signal. Identical rejection on Spring and Micronaut.
   */
  @Test
  void anOversizedOrControlBearingUserIdIsRejectedAtTheRequestEdge() {
    var holder = new StreamRuneRequestContextHolder();

    // Header-identity arm: the trusted-gateway mode, the only one that reads the header.
    var headerFilter =
        new StreamRuneRequestFilter(holder, Set.of(), RequestIdentityPolicy.of(null, true), null);
    ContainerRequestContext oversized = mock(ContainerRequestContext.class);
    when(oversized.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader("u".repeat(256)));
    assertThrows(IllegalArgumentException.class, () -> headerFilter.filter(oversized));

    ContainerRequestContext forged = mock(ContainerRequestContext.class);
    when(forged.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader("alice\r\nadmin"));
    assertThrows(IllegalArgumentException.class, () -> headerFilter.filter(forged));

    // Authenticated-principal arm: "already authenticated" is not "bounded".
    AuthenticatedUserResolver oversizedPrincipal = () -> Optional.of(UserId.of("p".repeat(256)));
    var principalFilter =
        new StreamRuneRequestFilter(
            holder, Set.of(), RequestIdentityPolicy.of(oversizedPrincipal, false), null);
    assertThrows(
        IllegalArgumentException.class,
        () -> principalFilter.filter(mock(ContainerRequestContext.class)));

    // At the bound it is accepted — the ceiling is the DLQ column width, not a style preference.
    ContainerRequestContext atBound = mock(ContainerRequestContext.class);
    when(atBound.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader("u".repeat(255)));
    headerFilter.filter(atBound);
    assertEquals(255, holder.context().userId().value().length());
  }

  @Test
  void authenticatedPrincipalOverridesSpoofedUserIdHeader() {
    var holder = new StreamRuneRequestContextHolder();
    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var filter =
        new StreamRuneRequestFilter(
            holder, Set.of(), RequestIdentityPolicy.of(resolver, false), null);

    ContainerRequestContext rc = mock(ContainerRequestContext.class);
    when(rc.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader("alice"));
    filter.filter(rc);

    assertEquals("bob", holder.context().userId().value());
  }

  @Test
  void unauthenticatedRequestIgnoresUserIdHeader() {
    var holder = new StreamRuneRequestContextHolder();
    AuthenticatedUserResolver resolver = Optional::empty;
    var filter =
        new StreamRuneRequestFilter(
            holder, Set.of(), RequestIdentityPolicy.of(resolver, false), null);

    ContainerRequestContext rc = mock(ContainerRequestContext.class);
    when(rc.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader("alice"));
    filter.filter(rc);

    assertNull(holder.context().userId());
  }

  @Test
  void trustUserIdHeaderOptInUsesHeaderIdentity() {
    var holder = new StreamRuneRequestContextHolder();
    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var filter =
        new StreamRuneRequestFilter(
            holder, Set.of(), RequestIdentityPolicy.of(resolver, true), null);

    ContainerRequestContext rc = mock(ContainerRequestContext.class);
    when(rc.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader("alice"));
    filter.filter(rc);

    assertEquals("alice", holder.context().userId().value());
  }

  /**
   * Without an authenticated-principal resolver and without the trust flag the header is NOT an
   * identity — this filter used to bind it whatever the flag said.
   */
  @Test
  void withoutAResolverOrTheTrustFlagTheHeaderIsIgnored() {
    var holder = new StreamRuneRequestContextHolder();
    var filter =
        new StreamRuneRequestFilter(holder, Set.of(), RequestIdentityPolicy.of(null, false), null);

    ContainerRequestContext rc = mock(ContainerRequestContext.class);
    when(rc.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader("alice"));
    filter.filter(rc);

    assertNull(holder.context().userId());
  }

  @Test
  void theTrustedGatewayWithoutAResolverTakesTheHeader() {
    var holder = new StreamRuneRequestContextHolder();
    var filter =
        new StreamRuneRequestFilter(holder, Set.of(), RequestIdentityPolicy.of(null, true), null);

    ContainerRequestContext rc = mock(ContainerRequestContext.class);
    when(rc.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader("alice"));
    filter.filter(rc);

    assertEquals("alice", holder.context().userId().value());
  }

  @Test
  void aNullIdentityPolicyIsRejected() {
    var holder = new StreamRuneRequestContextHolder();
    assertThrows(
        IllegalArgumentException.class,
        () -> new StreamRuneRequestFilter(holder, Set.of(), null, null));
  }

  @Test
  void isRegisteredAsJaxRsProvider() {
    // Quarkus REST auto-registers @Provider classes from indexed archives — without
    // the annotation the filter never runs.
    assertNotNull(StreamRuneRequestFilter.class.getAnnotation(Provider.class));
    assertTrue(
        jakarta.ws.rs.container.ContainerRequestFilter.class.isAssignableFrom(
            StreamRuneRequestFilter.class));
  }

  /** The static builder never reads X-User-Id — only the policy resolves an identity. */
  @Test
  void buildContextPopulatesFromHeadersButNeverTakesTheUserFromThem() {
    var ctx =
        StreamRuneRequestFilter.buildContext(
            headers(
                Map.of(
                    "X-Correlation-Id", "corr-1",
                    "X-Trace-Id", "trace-1",
                    "X-User-Id", "user-1")));

    assertEquals("corr-1", ctx.correlationId().value());
    assertEquals("trace-1", ctx.traceId().value());
    assertNull(ctx.userId());
    assertNotNull(ctx.timestamp());
    assertTrue(ctx.baggage().isEmpty());
  }

  @Test
  void missingHeadersAreDefaulted_notAnError() {
    // No X-Trace-Id, no X-User-Id, no X-Correlation-Id: the filter must default
    // (generated trace/correlation ids, anonymous user) instead of throwing.
    var ctx = StreamRuneRequestFilter.buildContext(headers(Map.of()));

    assertNotNull(ctx.traceId());
    assertFalse(ctx.traceId().value().isBlank());
    assertNotNull(ctx.correlationId());
    assertFalse(ctx.correlationId().value().isBlank());
    assertNull(ctx.userId());
  }

  @Test
  void blankHeadersAreDefaulted() {
    var ctx =
        StreamRuneRequestFilter.buildContext(
            headers(
                Map.of(
                    "X-Correlation-Id", "  ",
                    "X-Trace-Id", "  ",
                    "X-User-Id", "  ")));

    assertFalse(ctx.traceId().value().isBlank());
    assertFalse(ctx.correlationId().value().isBlank());
    assertNull(ctx.userId());
  }

  private static final RequestIdentityPolicy GATEWAY = RequestIdentityPolicy.of(null, true);

  /**
   * The context a filter with {@code policy} binds for a request carrying {@code X-User-Id: user-1}
   * and one {@code X-User-Role} line per element of {@code roles}. the header becomes baggage
   * {@code role} behind a trusted gateway ({@link #GATEWAY}) only.
   */
  private static StreamRuneContext.RequestContext boundBy(
      RequestIdentityPolicy policy, Set<String> extraAllowedBaggageKeys, String... roles) {
    var holder = new StreamRuneRequestContextHolder();
    ContainerRequestContext rc = mock(ContainerRequestContext.class);
    var all = TestRequestHeaders.userIdHeader("user-1");
    for (String role : roles) {
      all.add("X-User-Role", role);
    }
    when(rc.getHeaders()).thenReturn(all);
    new StreamRuneRequestFilter(holder, extraAllowedBaggageKeys, policy, null).filter(rc);
    return holder.context();
  }

  @Test
  void theTrustedGatewayPopulatesRoleBaggageFromXUserRoleHeader() {
    assertEquals("admin", boundBy(GATEWAY, Set.of(), "admin").baggage().get("role"));
  }

  @Test
  void theRoleHeaderIsIgnoredOutsideTheTrustedGatewayAndByTheStaticBuilders() {
    assertFalse(
        boundBy(RequestIdentityPolicy.of(null, false), Set.of(), "admin")
            .baggage()
            .containsKey("role"));
    var principal =
        boundBy(
            RequestIdentityPolicy.of(() -> Optional.of(UserId.of("bob")), false),
            Set.of(),
            "admin");
    assertEquals(UserId.of("bob"), principal.userId());
    assertFalse(principal.baggage().containsKey("role"));
    assertFalse(
        StreamRuneRequestFilter.buildContext(headers(Map.of("X-User-Role", "admin")))
            .baggage()
            .containsKey("role"),
        "the static builders are anonymous: no user, no role");
  }

  @Test
  void blankUserRoleHeaderIgnored() {
    assertFalse(boundBy(GATEWAY, Set.of(), "  ").baggage().containsKey("role"));
  }

  @Test
  void aRepeatedRoleHeaderIsNoRoleBehindATrustedGateway() {
    assertFalse(boundBy(GATEWAY, Set.of(), "CUSTOMER", "admin").baggage().containsKey("role"));
  }

  @Test
  void aW3cBaggageRoleEntryIsDroppedEvenWhenAllowlisted() {
    // `role` is not a default allow-listed key, and an application listing it does not
    // reopen the door — the role rule owns the key.
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current()
            .with(io.opentelemetry.api.baggage.Baggage.builder().put("role", "ADMIN").build());
    try (var _ = otelCtx.makeCurrent()) {
      assertFalse(
          StreamRuneRequestFilter.buildContext(headers(Map.of()), Set.of("role"))
              .baggage()
              .containsKey("role"));
      assertFalse(boundBy(GATEWAY, Set.of("role")).baggage().containsKey("role"));
      assertEquals("CUSTOMER", boundBy(GATEWAY, Set.of("role"), "CUSTOMER").baggage().get("role"));
    }
  }

  @Test
  void nonAllowlistedOtelBaggageDroppedByDefault_roleStillMerged() {
    // A non-allowlisted OTel baggage key (e.g. from an unauthenticated client-supplied
    // `baggage:` header) must never reach RequestContext/EventMetadata baggage.
    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder().put("tenant", "acme").build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    StreamRuneContext.RequestContext ctx;
    try (var _ = otelCtx.makeCurrent()) {
      ctx = boundBy(GATEWAY, Set.of(), "admin");
    }

    assertFalse(ctx.baggage().containsKey("tenant"));
    assertEquals("admin", ctx.baggage().get("role"));
  }

  @Test
  void configuredExtraAllowlistedKeyIsKept() {
    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder().put("tenant", "acme").build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    StreamRuneContext.RequestContext ctx;
    try (var _ = otelCtx.makeCurrent()) {
      ctx = boundBy(GATEWAY, Set.of("tenant"), "admin");
    }

    assertEquals("acme", ctx.baggage().get("tenant"));
    assertEquals("admin", ctx.baggage().get("role"));
  }

  @Test
  void onlyAllowlistedKeysSurvive_defaultAllowlistDropsPiiAndTracking() {
    // Scenario from the GDPR hardening spec: causationId + role survive, email/tracking dropped.
    // The
    // role is the X-User-Role header, so the filter runs behind a trusted gateway.
    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder()
            .put(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY, "evt-1")
            .put("email", "pii@example.com")
            .put("tracking", "leak")
            .build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    StreamRuneContext.RequestContext ctx;
    try (var _ = otelCtx.makeCurrent()) {
      ctx = boundBy(GATEWAY, Set.of(), "admin");
    }

    assertEquals(
        "evt-1", ctx.baggage().get(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY));
    assertEquals("admin", ctx.baggage().get("role"));
    assertFalse(ctx.baggage().containsKey("email"));
    assertFalse(ctx.baggage().containsKey("tracking"));
  }

  @Test
  void configuredAllowlistKeepsTrackingButStillDropsEmail() {
    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder()
            .put(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY, "evt-1")
            .put("email", "pii@example.com")
            .put("tracking", "leak")
            .build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    StreamRuneContext.RequestContext ctx;
    try (var _ = otelCtx.makeCurrent()) {
      ctx = boundBy(GATEWAY, Set.of("tracking"), "admin");
    }

    assertEquals(
        "evt-1", ctx.baggage().get(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY));
    assertEquals("admin", ctx.baggage().get("role"));
    assertEquals("leak", ctx.baggage().get("tracking"));
    assertFalse(ctx.baggage().containsKey("email"));
  }

  @Test
  void causationIdSurvivesAllowlistAndPropagatesUnfiltered() {
    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder()
            .put(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY, "evt-trigger-1")
            .build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    StreamRuneContext.RequestContext ctx;
    try (var _ = otelCtx.makeCurrent()) {
      ctx = StreamRuneRequestFilter.buildContext(headers(Map.of()));
    }

    assertEquals(
        "evt-trigger-1",
        ctx.baggage().get(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY));
  }

  /**
   * The no-argument-policy filter is anonymous: the X-User-Id and X-User-Role headers are ignored.
   */
  @Test
  void filterStoresContextInHolderAndRequestProperty() {
    var holder = new StreamRuneRequestContextHolder();
    var filter = new StreamRuneRequestFilter(holder);

    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    when(requestContext.getHeaderString("X-Correlation-Id")).thenReturn("corr-1");
    when(requestContext.getHeaderString("X-Trace-Id")).thenReturn("trace-1");
    var all = TestRequestHeaders.userIdHeader("user-1");
    all.add("X-User-Role", "admin");
    when(requestContext.getHeaders()).thenReturn(all);

    filter.filter(requestContext);

    assertNotNull(holder.context());
    assertEquals("corr-1", holder.context().correlationId().value());
    assertEquals("trace-1", holder.context().traceId().value());
    assertNull(holder.context().userId());
    assertFalse(holder.context().baggage().containsKey("role"));

    var captor = ArgumentCaptor.forClass(Object.class);
    verify(requestContext)
        .setProperty(eq(StreamRuneRequestFilter.CONTEXT_PROPERTY), captor.capture());
    assertSame(holder.context(), captor.getValue());
  }

  @Test
  void filterDoesNotThrowOnHeaderlessRequest() {
    var holder = new StreamRuneRequestContextHolder();
    var filter = new StreamRuneRequestFilter(holder);

    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    // all getHeaderString calls return null

    assertDoesNotThrow(() -> filter.filter(requestContext));
    assertNotNull(holder.context());
    assertNull(holder.context().userId());
  }

  @Test
  void withContextBindsContextOnCurrentThread() {
    var ctx =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("corr-1"), Instant.now(), Map.of());
    var captured = new AtomicReference<StreamRuneContext.RequestContext>();
    StreamRuneRequestFilter.withContext(ctx, () -> captured.set(StreamRuneContext.CURRENT.get()));
    assertSame(ctx, captured.get());
  }

  @Test
  void withContextRunsUnboundWhenContextNull() {
    var ran = new AtomicReference<>(false);
    StreamRuneRequestFilter.withContext(
        null,
        () -> {
          assertFalse(StreamRuneContext.CURRENT.isBound());
          ran.set(true);
        });
    assertTrue(ran.get());
  }

  @Test
  void callWithContextBindsContextAndReturnsResult() {
    var ctx =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("corr-2"), Instant.now(), Map.of());
    String result =
        StreamRuneRequestFilter.callWithContext(
            ctx, () -> StreamRuneContext.CURRENT.get().correlationId().value());
    assertEquals("corr-2", result);
  }

  @Test
  void callWithContextRunsUnboundWhenContextNull() {
    String result = StreamRuneRequestFilter.callWithContext(null, () -> "plain");
    assertEquals("plain", result);
  }
}
