package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.filter.FilterContinuation;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;

/** Tests for {@link StreamRuneContextFilter}. */
class StreamRuneContextFilterTest {

  @AfterEach
  void cleanup() {
    StreamRuneContextHelper.remove();
  }

  // The authorization identity must come from the authenticated principal, never the
  // client-supplied X-User-Id header.

  @Test
  void authenticatedPrincipalOverridesSpoofedUserIdHeader() {
    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var filter = filterWith(RequestIdentityPolicy.of(resolver, false));

    var ctx = filter.buildContext(request("trace-1", "alice", "corr-1", null));

    assertEquals("bob", ctx.userId().value());
  }

  @Test
  void unauthenticatedRequestIgnoresUserIdHeader() {
    AuthenticatedUserResolver resolver = Optional::empty;
    var filter = filterWith(RequestIdentityPolicy.of(resolver, false));

    var ctx = filter.buildContext(request("trace-1", "alice", "corr-1", null));

    assertNull(ctx.userId());
  }

  @Test
  void trustUserIdHeaderOptInUsesHeaderIdentity() {
    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var filter = filterWith(RequestIdentityPolicy.of(resolver, true));

    var ctx = filter.buildContext(request("trace-1", "alice", "corr-1", null));

    assertEquals("alice", ctx.userId().value());
  }

  /**
   * Without an authenticated-principal resolver and without the trust flag the header is NOT an
   * identity — this filter used to bind it whatever the flag said.
   */
  @Test
  void withoutAResolverOrTheTrustFlagTheHeaderIsIgnored() {
    var filter = filterWith(RequestIdentityPolicy.of(null, false));

    var ctx = filter.buildContext(request("trace-1", "alice", "corr-1", null));

    assertNull(ctx.userId());
  }

  @Test
  void theTrustedGatewayWithoutAResolverTakesTheHeader() {
    var filter = filterWith(RequestIdentityPolicy.of(null, true));

    var ctx = filter.buildContext(request("trace-1", "alice", "corr-1", null));

    assertEquals("alice", ctx.userId().value());
  }

  /** The no-argument filter is anonymous: the X-User-Id header is ignored. */
  @Test
  void theNoArgumentFilterIsAnonymous() {
    var ctx = new StreamRuneContextFilter().buildContext(request("trace-1", "alice", "c", null));

    assertNull(ctx.userId());
  }

  @Test
  void aNullIdentityPolicyIsRejected() {
    var properties = StreamRuneMicronautProperties.withDefaults();
    assertThrows(
        IllegalArgumentException.class, () -> new StreamRuneContextFilter(properties, null, null));
  }

  private static StreamRuneContextFilter filterWith(RequestIdentityPolicy policy) {
    return new StreamRuneContextFilter(StreamRuneMicronautProperties.withDefaults(), policy, null);
  }

  @Test
  void runsAfterMicronautSecurityFilter() {
    // The filter must run after Micronaut Security so the authenticated principal is available.
    assertTrue(
        new StreamRuneContextFilter().getOrder()
            > io.micronaut.http.filter.ServerFilterPhase.SECURITY.order());
  }

  @SuppressWarnings("unchecked")
  private HttpRequest<Object> request(
      String traceId, String userId, String correlationId, String role) {
    HttpRequest<Object> request = mock(HttpRequest.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(request.getHeaders()).thenReturn(headers);
    when(headers.get("X-Trace-Id")).thenReturn(traceId);
    when(headers.getAll("X-User-Id")).thenReturn(userId == null ? List.of() : List.of(userId));
    when(headers.get("X-Correlation-Id")).thenReturn(correlationId);
    when(headers.getAll("X-User-Role")).thenReturn(role == null ? List.of() : List.of(role));
    return request;
  }

  // The id bound is enforced at the INGRESS DOOR the
  // filter takes (RequestContext.fromRequest), not in the RequestContext constructor — which is
  // also the DLQ-replay / saga-dispatch reconstruction path. This test pins the door: were the
  // filter to fall back to the plain constructor, an unauthenticated header would reach
  // event_stream.metadata unbounded again.

  @Test
  void buildContext_rejectsAControlCharacterInAnUnauthenticatedIdHeader() {
    var filter = new StreamRuneContextFilter();

    assertThrows(
        IllegalArgumentException.class,
        () -> filter.buildContext(request("trace-1", "user-1", "corr\r\nX-Injected: 1", null)));
    assertThrows(
        IllegalArgumentException.class,
        () -> filter.buildContext(request("trace" + (char) 0x01, "user-1", "corr-1", null)));
  }

  @Test
  void buildContext_rejectsAnOversizedIdHeader() {
    var filter = new StreamRuneContextFilter();

    assertThrows(
        IllegalArgumentException.class,
        () -> filter.buildContext(request("trace-1", "user-1", "c".repeat(256), null)));
  }

  /**
   * The identity crosses the same door and is now bounded there too. The header arm is the
   * trusted-gateway mode (the only mode that reads the header); the authenticated-principal arm
   * reaches the same {@code fromRequest} call with whatever value the IdP asserted, which is
   * neither automatically bounded nor charset-safe. An identity over 255 characters cannot be
   * written to {@code dead_letter_queue.user_id VARCHAR(255)} at all, and the bus catches and logs
   * that INSERT failure — so accepting the request would leave an accepted command with no recovery
   * channel and no signal. Identical rejection on Spring and Quarkus.
   */
  @Test
  void buildContext_rejectsAnOversizedOrControlBearingUserId() {
    var filter = filterWith(RequestIdentityPolicy.of(null, true));

    assertThrows(
        IllegalArgumentException.class,
        () -> filter.buildContext(request("trace-1", "u".repeat(256), "corr-1", null)));
    assertThrows(
        IllegalArgumentException.class,
        () -> filter.buildContext(request("trace-1", "alice\r\nadmin", "corr-1", null)));

    // At the bound it is accepted — the ceiling is the DLQ column width, not a style preference.
    var ctx = filter.buildContext(request("trace-1", "u".repeat(255), "corr-1", null));
    assertEquals(255, ctx.userId().value().length());

    // Authenticated-principal arm: "already authenticated" is not "bounded".
    AuthenticatedUserResolver oversizedPrincipal = () -> Optional.of(UserId.of("p".repeat(256)));
    var principalFilter = filterWith(RequestIdentityPolicy.of(oversizedPrincipal, false));
    assertThrows(
        IllegalArgumentException.class,
        () -> principalFilter.buildContext(request("trace-1", null, "corr-1", null)));
  }

  @Test
  void buildContext_usesHeaderValues() {
    // The trusted-gateway mode, the only one in which X-User-Id is the user.
    var filter = filterWith(RequestIdentityPolicy.of(null, true));
    var ctx = filter.buildContext(request("trace-1", "user-1", "corr-1", null));

    assertEquals("trace-1", ctx.traceId().value());
    assertEquals("user-1", ctx.userId().value());
    assertEquals("corr-1", ctx.correlationId().value());
    assertNotNull(ctx.timestamp());
    assertTrue(ctx.baggage().isEmpty());
  }

  @Test
  void buildContext_defaultsMissingHeaders() {
    var filter = new StreamRuneContextFilter();
    var ctx = filter.buildContext(request(null, null, null, null));

    assertNotNull(ctx.traceId(), "missing X-Trace-Id must yield a generated trace ID");
    assertFalse(ctx.traceId().value().isBlank());
    assertNull(ctx.userId(), "missing X-User-Id must yield an anonymous user");
    assertNotNull(ctx.correlationId());
    assertFalse(ctx.correlationId().value().isBlank());
  }

  @Test
  void buildContext_defaultsBlankHeaders() {
    var filter = new StreamRuneContextFilter();
    var ctx = filter.buildContext(request("  ", "  ", "  ", "  "));

    assertFalse(ctx.traceId().value().isBlank());
    assertNull(ctx.userId());
    assertFalse(ctx.correlationId().value().isBlank());
    assertFalse(ctx.baggage().containsKey("role"));
  }

  /** A trusted-gateway filter: the only mode in which X-User-Role becomes baggage role. */
  private static StreamRuneContextFilter gatewayFilter() {
    return filterWith(RequestIdentityPolicy.of(null, true));
  }

  @Test
  void buildContext_theTrustedGatewayPropagatesRoleHeaderIntoBaggage() {
    var ctx = gatewayFilter().buildContext(request("trace-1", "user-1", "corr-1", "ADMIN"));

    assertEquals("ADMIN", ctx.baggage().get("role"));
  }

  @Test
  void buildContext_theRoleHeaderIsIgnoredOutsideTheTrustedGateway() {
    var anonymous =
        new StreamRuneContextFilter().buildContext(request("trace-1", "user-1", "corr-1", "ADMIN"));
    assertFalse(anonymous.baggage().containsKey("role"));

    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var principal =
        filterWith(RequestIdentityPolicy.of(resolver, false))
            .buildContext(request("trace-1", "alice", "corr-1", "ADMIN"));
    assertEquals("bob", principal.userId().value());
    assertFalse(principal.baggage().containsKey("role"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void buildContext_aRepeatedRoleHeaderIsNoRoleBehindATrustedGateway() {
    HttpRequest<Object> request = request("trace-1", "user-1", "corr-1", null);
    when(request.getHeaders().getAll("X-User-Role")).thenReturn(List.of("CUSTOMER", "ADMIN"));

    assertFalse(gatewayFilter().buildContext(request).baggage().containsKey("role"));
  }

  @Test
  void buildContext_aW3cBaggageRoleEntryIsDroppedEvenWhenAllowlisted() {
    // `role` is not a default allow-listed key, and an application listing it does not
    // reopen the door — the role rule owns the key.
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current()
            .with(io.opentelemetry.api.baggage.Baggage.builder().put("role", "ADMIN").build());
    var allowlistingGateway =
        new StreamRuneContextFilter(
            withBaggageAllowlist(java.util.List.of("role")),
            RequestIdentityPolicy.of(null, true),
            null);
    try (var _ = otelCtx.makeCurrent()) {
      assertFalse(
          new StreamRuneContextFilter()
              .buildContext(request("trace-1", "user-1", "corr-1", null))
              .baggage()
              .containsKey("role"));
      assertFalse(
          allowlistingGateway
              .buildContext(request("trace-1", "user-1", "corr-1", null))
              .baggage()
              .containsKey("role"));
      assertEquals(
          "CUSTOMER",
          allowlistingGateway
              .buildContext(request("trace-1", "user-1", "corr-1", "CUSTOMER"))
              .baggage()
              .get("role"));
    }
  }

  @Test
  void buildContext_dropsNonAllowlistedOtelBaggageByDefault() {
    // A non-allowlisted OTel baggage key (e.g. from an unauthenticated client-supplied
    // `baggage:` header) must never reach RequestContext/EventMetadata baggage.
    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder().put("tenant", "acme").build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    var filter = gatewayFilter();
    try (var _ = otelCtx.makeCurrent()) {
      var ctx = filter.buildContext(request("trace-1", "user-1", "corr-1", "ADMIN"));
      assertFalse(ctx.baggage().containsKey("tenant"));
      assertEquals("ADMIN", ctx.baggage().get("role"));
    }
  }

  @Test
  void buildContext_configuredAllowlistKeepsExtraKey() {
    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder().put("tenant", "acme").build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    var filter =
        new StreamRuneContextFilter(
            withBaggageAllowlist(java.util.List.of("tenant")),
            RequestIdentityPolicy.of(null, true),
            null);
    try (var _ = otelCtx.makeCurrent()) {
      var ctx = filter.buildContext(request("trace-1", "user-1", "corr-1", "ADMIN"));
      assertEquals("acme", ctx.baggage().get("tenant"));
      assertEquals("ADMIN", ctx.baggage().get("role"));
    }
  }

  @Test
  void buildContext_onlyAllowlistedKeysSurvive_defaultAllowlistDropsPiiAndTracking() {
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

    var filter = gatewayFilter();
    try (var _ = otelCtx.makeCurrent()) {
      var ctx = filter.buildContext(request("trace-1", "user-1", "corr-1", "admin"));
      assertEquals(
          "evt-1",
          ctx.baggage().get(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY));
      assertEquals("admin", ctx.baggage().get("role"));
      assertFalse(ctx.baggage().containsKey("email"));
      assertFalse(ctx.baggage().containsKey("tracking"));
    }
  }

  @Test
  void buildContext_configuredAllowlistKeepsTrackingButStillDropsEmail() {
    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder()
            .put(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY, "evt-1")
            .put("email", "pii@example.com")
            .put("tracking", "leak")
            .build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    var filter =
        new StreamRuneContextFilter(
            withBaggageAllowlist(java.util.List.of("tracking")),
            RequestIdentityPolicy.of(null, true),
            null);
    try (var _ = otelCtx.makeCurrent()) {
      var ctx = filter.buildContext(request("trace-1", "user-1", "corr-1", "admin"));
      assertEquals(
          "evt-1",
          ctx.baggage().get(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY));
      assertEquals("admin", ctx.baggage().get("role"));
      assertEquals("leak", ctx.baggage().get("tracking"));
      assertFalse(ctx.baggage().containsKey("email"));
    }
  }

  @Test
  void buildContext_causationIdSurvivesAllowlistAndPropagatesUnfiltered() {
    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder()
            .put(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY, "evt-trigger-1")
            .build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    var filter = new StreamRuneContextFilter();
    try (var _ = otelCtx.makeCurrent()) {
      var ctx = filter.buildContext(request("trace-1", "user-1", "corr-1", null));
      assertEquals(
          "evt-trigger-1",
          ctx.baggage().get(org.streamrune.core.BaggageAllowlist.CAUSATION_ID_BAGGAGE_KEY));
    }
  }

  private static StreamRuneMicronautProperties withBaggageAllowlist(java.util.List<String> keys) {
    var d = StreamRuneMicronautProperties.withDefaults();
    return new StreamRuneMicronautProperties(
        d.snapshotEveryNEvents(),
        d.retryMaxAttempts(),
        d.retryInitialDelayMs(),
        d.retryBackoffMultiplier(),
        d.pollingIntervalMs(),
        d.pollingJitterMs(),
        d.lockTimeout(),
        d.stripeCount(),
        d.maxInFlightAsyncCommands(),
        d.circuitBreakerFailureThreshold(),
        d.circuitBreakerCooldown(),
        d.eventAuditEnabled(),
        d.projectionDlqEnabled(),
        d.sagaEnabled(),
        d.sagaDeadLetterRetentionMaxAge(),
        d.outboxEnabled(),
        d.outboxBatchSize(),
        d.outboxFlushIntervalMs(),
        d.deadLetterEnabled(),
        d.deadLetterMaxRetries(),
        d.deadLetterRetryIntervalMs(),
        d.deadLetterRetentionMaxAge(),
        d.metricsEnabled(),
        d.metricsPrefix(),
        d.validationEnabled(),
        d.startupLog(),
        d.autoInitializeSchema(),
        d.outboxRetentionMaxAge(),
        d.inboxRetentionMaxAge(),
        keys,
        d.outboxSkippedRetentionMaxAge(),
        d.subscriptionSingleActiveConsumerEnabled(),
        d.outboxRetryMaxAttempts(),
        d.outboxRetryInitialDelayMs(),
        d.outboxRetryMultiplier(),
        d.subscriptionLeaseTtl(),
        d.sseTimeout(),
        d.sseKeepAliveInterval(),
        // event-store.statement-timeout
        java.time.Duration.ofSeconds(30));
  }

  @Test
  @SuppressWarnings("unchecked")
  void filter_bindsScopedValueAndThreadLocalAroundDownstream() {
    var filter = filterWith(RequestIdentityPolicy.of(null, true));
    var request = request("trace-1", "user-1", "corr-1", "ADMIN");

    var scoped = new AtomicReference<StreamRuneContext.RequestContext>();
    var threadLocal = new AtomicReference<StreamRuneContext.RequestContext>();
    MutableHttpResponse<?> response = mock(MutableHttpResponse.class);
    FilterContinuation<MutableHttpResponse<?>> continuation = mock(FilterContinuation.class);
    when(continuation.proceed())
        .thenAnswer(
            inv -> {
              scoped.set(
                  StreamRuneContext.CURRENT.isBound() ? StreamRuneContext.CURRENT.get() : null);
              threadLocal.set(StreamRuneContextHelper.get());
              return response;
            });

    var result = filter.filter(request, continuation);

    assertSame(response, result);
    assertNotNull(scoped.get(), "ScopedValue must be bound during downstream execution");
    assertEquals("user-1", scoped.get().userId().value());
    assertEquals("ADMIN", scoped.get().baggage().get("role"));
    assertNotNull(threadLocal.get(), "ThreadLocal helper must be populated downstream");
    assertSame(scoped.get(), threadLocal.get());
    assertNull(StreamRuneContextHelper.get(), "ThreadLocal must be restored after the filter");
    assertFalse(StreamRuneContext.CURRENT.isBound(), "ScopedValue must not leak");
  }

  @Test
  void requestContextElement_setsAndRestoresThreadLocal() {
    var filter = new StreamRuneContextFilter();
    var ctx = filter.buildContext(request("trace-1", "user-1", "corr-1", null));
    var previous = filter.buildContext(request("trace-0", "user-0", "corr-0", null));
    var element = new StreamRuneContextFilter.RequestContextElement(ctx);

    StreamRuneContextHelper.set(previous);
    var old = element.updateThreadContext();
    assertSame(previous, old);
    assertSame(ctx, StreamRuneContextHelper.get());

    element.restoreThreadContext(old);
    assertSame(previous, StreamRuneContextHelper.get());

    element.restoreThreadContext(null);
    assertNull(StreamRuneContextHelper.get());
  }
}
