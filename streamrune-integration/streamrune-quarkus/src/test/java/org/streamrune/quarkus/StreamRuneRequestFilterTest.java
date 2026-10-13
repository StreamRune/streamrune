package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.ArgumentCaptor;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.CommandBusProvenanceContract;
import org.streamrune.test.InMemoryEventStore;

/** Tests for {@link StreamRuneRequestFilter}. */
class StreamRuneRequestFilterTest {

  /** A W3C baggage key a client may send to name a causing event; the framework reads none. */
  private static final String CAUSATION_KEY = "streamrune.causation-id";

  /** A W3C baggage key a client may send to name a saga; the framework reads none. */
  private static final String ORIGIN_KEY = "sagaOrigin";

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

  // The filter answers a refusal of the door itself: 400 Bad Request with one fixed body, before
  // the resource method runs.

  private static StreamRuneRequestFilter anonymousFilter(StreamRuneRequestContextHolder holder) {
    return new StreamRuneRequestFilter(
        holder, Set.of(), RequestIdentityPolicy.of(Optional::empty, false), null);
  }

  /**
   * A request that carries {@code header} once per value, answered as Quarkus REST answers it:
   * {@code getHeaders()} keeps every value as received, {@code getHeaderString} joins them with a
   * comma.
   */
  private static ContainerRequestContext requestWith(String header, String... values) {
    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    for (String value : values) {
      headers.add(header, value);
    }
    ContainerRequestContext request = mock(ContainerRequestContext.class);
    when(request.getHeaders()).thenReturn(headers);
    when(request.getHeaderString(header)).thenReturn(String.join(",", values));
    return request;
  }

  /**
   * Of a repeated {@code X-Correlation-Id} or {@code X-Trace-Id} header the first value is the
   * header, as on the servlet and Micronaut filters — not the comma-joined list {@code
   * getHeaderString} returns, which would bind a value no client sent as one and would refuse a
   * request whose first value is empty.
   */
  @Test
  void ofARepeatedHeader_theFirstValueIsTaken() {
    var holder = new StreamRuneRequestContextHolder();
    anonymousFilter(holder)
        .filter(requestWith("X-Correlation-Id", "ok", "streamrune:saga:order-saga-42"));
    assertEquals("ok", holder.context().correlationId().value());

    holder = new StreamRuneRequestContextHolder();
    anonymousFilter(holder).filter(requestWith("X-Trace-Id", "trace-1", "t".repeat(256)));
    assertEquals("trace-1", holder.context().traceId().value());

    holder = new StreamRuneRequestContextHolder();
    assertAnswered400(
        anonymousFilter(holder),
        holder,
        requestWith("X-Correlation-Id", "streamrune:saga:order-saga-42", "ok"),
        "streamrune:saga:order-saga-42");
  }

  @Test
  void anEmptyFirstValue_isAnAbsentHeader_whateverFollows() {
    var holder = new StreamRuneRequestContextHolder();
    ContainerRequestContext request =
        requestWith("X-Correlation-Id", "", "streamrune:saga:order-saga-42");

    anonymousFilter(holder).filter(request);

    verify(request, never()).abortWith(any());
    assertFalse(holder.context().correlationId().value().isBlank());
    assertFalse(holder.context().correlationId().value().startsWith("streamrune:"));
  }

  /**
   * {@code filter} aborts {@code request} with {@code 400} and the shared reason as a plain-text
   * body, and publishes no context: the holder and the request property stay unset. The answer
   * repeats nothing of {@code refusedValue}.
   */
  private static void assertAnswered400(
      StreamRuneRequestFilter filter,
      StreamRuneRequestContextHolder holder,
      ContainerRequestContext request,
      String refusedValue) {
    filter.filter(request);

    ArgumentCaptor<Response> answer = ArgumentCaptor.forClass(Response.class);
    verify(request).abortWith(answer.capture());
    assertEquals(400, answer.getValue().getStatus());
    assertEquals(RequestIdentityPolicy.REFUSED_REQUEST_CONTEXT, answer.getValue().getEntity());
    assertEquals("text/plain;charset=UTF-8", answer.getValue().getMediaType().toString());
    assertFalse(
        answer.getValue().getEntity().toString().contains(refusedValue), "the value is not echoed");
    verify(request, never()).setProperty(anyString(), any());
    assertNull(holder.context(), "no context is published");
  }

  @Test
  void aReservedCorrelationId_isAnswered400() {
    for (String reserved :
        List.of("streamrune:saga:order-saga-42", "streamrune:", "streamrune:anything")) {
      var holder = new StreamRuneRequestContextHolder();
      assertAnswered400(
          anonymousFilter(holder), holder, requestWith("X-Correlation-Id", reserved), reserved);
      assertAnswered400(
          new StreamRuneRequestFilter(holder, Set.of(), RequestIdentityPolicy.of(null, true), null),
          holder,
          requestWith("X-Correlation-Id", reserved),
          reserved);
      assertAnswered400(
          new StreamRuneRequestFilter(
              holder,
              Set.of(),
              RequestIdentityPolicy.of(() -> Optional.of(UserId.of("bob")), false),
              null),
          holder,
          requestWith("X-Correlation-Id", reserved),
          reserved);
    }
  }

  @Test
  void aCorrelationIdThatIsNotInTheReservedForm_isKeptAsSent() {
    for (String ordinary :
        List.of(
            "order-saga-42",
            "saga:order-saga-42",
            "Streamrune:saga:order-saga-42",
            "streamrune",
            "upstream/streamrune:saga:order-saga-42",
            "c".repeat(255))) {
      var holder = new StreamRuneRequestContextHolder();
      ContainerRequestContext request = requestWith("X-Correlation-Id", ordinary);

      anonymousFilter(holder).filter(request);

      verify(request, never()).abortWith(any());
      assertEquals(ordinary, holder.context().correlationId().value());
    }
  }

  @Test
  void aTraceIdWithTheReservedPrefix_isKeptAsSent() {
    var holder = new StreamRuneRequestContextHolder();

    anonymousFilter(holder).filter(requestWith("X-Trace-Id", "streamrune:saga:order-saga-42"));

    assertEquals("streamrune:saga:order-saga-42", holder.context().traceId().value());
  }

  @Test
  void anOverlongCorrelationId_isAnswered400() {
    String overlong = "c".repeat(256);
    var holder = new StreamRuneRequestContextHolder();

    assertAnswered400(
        anonymousFilter(holder), holder, requestWith("X-Correlation-Id", overlong), overlong);
    assertAnswered400(
        anonymousFilter(holder), holder, requestWith("X-Trace-Id", overlong), overlong);
  }

  @Test
  void aControlCharacterInTheTraceId_isAnswered400() {
    var holder = new StreamRuneRequestContextHolder();

    assertAnswered400(
        anonymousFilter(holder),
        holder,
        requestWith("X-Trace-Id", "trace" + (char) 0x01),
        "trace" + (char) 0x01);
    assertAnswered400(
        anonymousFilter(holder),
        holder,
        requestWith("X-Correlation-Id", "corr\r\nX-Injected: 1"),
        "X-Injected");
  }

  /**
   * The identity crosses the same door. Both arms are covered — the header arm ({@code
   * trust-user-id-header=true}, the trusted-gateway mode) and the authenticated-principal arm,
   * whose value is whatever the IdP asserted and is therefore neither automatically bounded nor
   * charset-safe. An identity over 255 characters cannot be written to {@code
   * dead_letter_queue.user_id VARCHAR(255)} at all, and the bus catches and logs that INSERT
   * failure — so accepting the request would leave an accepted command with no recovery channel and
   * no signal. Identical on Spring and Micronaut.
   */
  @Test
  void anOverlongUserId_isAnswered400() {
    var holder = new StreamRuneRequestContextHolder();

    // Header-identity arm: the trusted-gateway mode, the only one that reads the header.
    var headerFilter =
        new StreamRuneRequestFilter(holder, Set.of(), RequestIdentityPolicy.of(null, true), null);
    ContainerRequestContext oversized = mock(ContainerRequestContext.class);
    when(oversized.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader("u".repeat(256)));
    assertAnswered400(headerFilter, holder, oversized, "u".repeat(256));

    ContainerRequestContext forged = mock(ContainerRequestContext.class);
    when(forged.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader("alice\r\nadmin"));
    assertAnswered400(headerFilter, holder, forged, "admin");

    // Authenticated-principal arm: "already authenticated" is not "bounded".
    AuthenticatedUserResolver oversizedPrincipal = () -> Optional.of(UserId.of("p".repeat(256)));
    var principalFilter =
        new StreamRuneRequestFilter(
            holder, Set.of(), RequestIdentityPolicy.of(oversizedPrincipal, false), null);
    assertAnswered400(
        principalFilter, holder, mock(ContainerRequestContext.class), "p".repeat(256));

    // At the bound it is accepted — the ceiling is the DLQ column width, not a style preference.
    ContainerRequestContext atBound = mock(ContainerRequestContext.class);
    when(atBound.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader("u".repeat(255)));
    headerFilter.filter(atBound);
    assertEquals(255, holder.context().userId().value().length());
  }

  /**
   * A principal the identity provider asserted and the door refuses is refused on every request of
   * that user. The filter asks the policy for its one report (logged at WARN); a refused header of
   * a client uses up no report.
   */
  @Test
  void anUnacceptableAuthenticatedPrincipal_isReportedByTheFilter_once() {
    var holder = new StreamRuneRequestContextHolder();
    AuthenticatedUserResolver oversizedPrincipal = () -> Optional.of(UserId.of("p".repeat(256)));
    RequestIdentityPolicy policy = RequestIdentityPolicy.of(oversizedPrincipal, false);
    RequestIdentityPolicy bobs =
        RequestIdentityPolicy.of(() -> Optional.of(UserId.of("bob")), false);

    assertAnswered400(
        new StreamRuneRequestFilter(holder, Set.of(), bobs, null),
        holder,
        requestWith("X-Correlation-Id", "c".repeat(256)),
        "ccc");
    assertTrue(
        bobs.unacceptablePrincipalWarning(UserId.of("p".repeat(256))).isPresent(),
        "a client's refused header used up no report");

    assertAnswered400(
        new StreamRuneRequestFilter(holder, Set.of(), policy, null),
        holder,
        mock(ContainerRequestContext.class),
        "ppp");
    assertEquals(
        Optional.empty(),
        policy.unacceptablePrincipalWarning(UserId.of("p".repeat(256))),
        "the filter took the one report");
  }

  /**
   * Only the door's refusal is answered {@code 400}. An {@link IllegalArgumentException} of the
   * application's identity resolver is not a refused header and leaves the filter as it is.
   */
  @Test
  void anIllegalArgumentExceptionOfTheIdentityResolver_isNotAnswered400() {
    var resolverFailure = new IllegalArgumentException("the resolver's own failure");
    AuthenticatedUserResolver failing =
        () -> {
          throw resolverFailure;
        };
    var holder = new StreamRuneRequestContextHolder();
    var filter =
        new StreamRuneRequestFilter(
            holder, Set.of(), RequestIdentityPolicy.of(failing, false), null);
    ContainerRequestContext request = mock(ContainerRequestContext.class);

    assertSame(
        resolverFailure,
        assertThrows(IllegalArgumentException.class, () -> filter.filter(request)));
    verify(request, never()).abortWith(any());
  }

  @Test
  void theStaticBuildContext_throwsForADirectCaller() {
    // The package-private helpers build a context for a caller that is not the filter; a refusal
    // reaches that caller as the exception.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StreamRuneRequestFilter.buildContext(
                headers(Map.of("X-Correlation-Id", "streamrune:saga:order-saga-42"))));
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
    // The gateway's role survives; every W3C baggage entry is dropped, a causation entry included.
    // The role is the X-User-Role header, so the filter runs behind a trusted gateway.
    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder()
            .put(CAUSATION_KEY, "evt-1")
            .put("email", "pii@example.com")
            .put("tracking", "leak")
            .build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    StreamRuneContext.RequestContext ctx;
    try (var _ = otelCtx.makeCurrent()) {
      ctx = boundBy(GATEWAY, Set.of(), "admin");
    }

    assertEquals(Map.of("role", "admin"), ctx.baggage());
    assertFalse(ctx.baggage().containsKey(CAUSATION_KEY), "no baggage key is kept by default");
    assertFalse(ctx.baggage().containsKey("email"));
    assertFalse(ctx.baggage().containsKey("tracking"));
  }

  @Test
  void configuredAllowlistKeepsTrackingButStillDropsEmail() {
    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder()
            .put(CAUSATION_KEY, "evt-1")
            .put("email", "pii@example.com")
            .put("tracking", "leak")
            .build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    StreamRuneContext.RequestContext ctx;
    try (var _ = otelCtx.makeCurrent()) {
      ctx = boundBy(GATEWAY, Set.of("tracking"), "admin");
    }

    assertFalse(ctx.baggage().containsKey(CAUSATION_KEY), "only the configured key is kept");
    assertEquals("admin", ctx.baggage().get("role"));
    assertEquals("leak", ctx.baggage().get("tracking"));
    assertFalse(ctx.baggage().containsKey("email"));
  }

  @Test
  void aCausationBaggageEntryIsDroppedUnlessTheApplicationAllowlistsIt() {
    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder().put(CAUSATION_KEY, "evt-trigger-1").build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    try (var _ = otelCtx.makeCurrent()) {
      assertFalse(
          StreamRuneRequestFilter.buildContext(headers(Map.of()))
              .baggage()
              .containsKey(CAUSATION_KEY),
          "the default allowlist is empty");
      // Allowlisted, it is an ordinary baggage entry; nothing reads it as a causation id.
      assertEquals(
          "evt-trigger-1",
          StreamRuneRequestFilter.buildContext(headers(Map.of()), Set.of(CAUSATION_KEY))
              .baggage()
              .get(CAUSATION_KEY));
    }
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
      String mode, RequestIdentityPolicy policy, String correlationId, Set<String> allowlist) {
    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    headers.add("X-Correlation-Id", correlationId);
    headers.add("X-Trace-Id", "trace-1");
    headers.add("X-User-Id", "mallory");
    headers.add("X-User-Role", "ADMIN");
    headers.add("X-Causation-Id", "evt-1");
    headers.add("X-Saga-Origin", "{\"sagaType\":\"x.S\",\"sagaId\":\"order-saga-42\",\"hop\":0}");
    ContainerRequestContext rc = mock(ContainerRequestContext.class);
    when(rc.getHeaders()).thenReturn(headers);
    when(rc.getHeaderString(anyString()))
        .thenAnswer(invocation -> headers.getFirst(invocation.getArgument(0)));
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current()
            .with(
                io.opentelemetry.api.baggage.Baggage.builder()
                    .put(CAUSATION_KEY, "evt-1")
                    .put(ORIGIN_KEY, "order-saga-42")
                    .build());

    var holder = new StreamRuneRequestContextHolder();
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
      try (var _ = otelCtx.makeCurrent()) {
        new StreamRuneRequestFilter(holder, allowlist, policy, null).filter(rc);
      }
      // The resource method: it binds what the filter stored and executes its command.
      StreamRuneRequestFilter.withContext(
          holder.context(),
          () -> {
            assertNull(StreamRuneContext.cause(), "a request filter binds no dispatch cause");
            assertFalse(StreamRuneContext.isSagaDispatch());
            bus.execute(new CommandBusProvenanceContract.Mark("marked-1"));
          });
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

  /**
   * The no-argument-policy filter is anonymous: the X-User-Id and X-User-Role headers are ignored.
   */
  @Test
  void filterStoresContextInHolderAndRequestProperty() {
    var holder = new StreamRuneRequestContextHolder();
    var filter = new StreamRuneRequestFilter(holder);

    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    var all = TestRequestHeaders.userIdHeader("user-1");
    all.add("X-User-Role", "admin");
    all.add("X-Correlation-Id", "corr-1");
    all.add("X-Trace-Id", "trace-1");
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
