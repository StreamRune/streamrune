package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.filter.FilterContinuation;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
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

/** Tests for {@link StreamRuneContextFilter}. */
class StreamRuneContextFilterTest {

  /** A W3C baggage key a client may send to name a causing event; the framework reads none. */
  private static final String CAUSATION_KEY = "streamrune.causation-id";

  /** A W3C baggage key a client may send to name a saga; the framework reads none. */
  private static final String ORIGIN_KEY = "sagaOrigin";

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

    var filter = gatewayFilter();
    try (var _ = otelCtx.makeCurrent()) {
      var ctx = filter.buildContext(request("trace-1", "user-1", "corr-1", "admin"));
      assertEquals(java.util.Map.of("role", "admin"), ctx.baggage());
      assertFalse(ctx.baggage().containsKey(CAUSATION_KEY), "no baggage key is kept by default");
      assertFalse(ctx.baggage().containsKey("email"));
      assertFalse(ctx.baggage().containsKey("tracking"));
    }
  }

  @Test
  void buildContext_configuredAllowlistKeepsTrackingButStillDropsEmail() {
    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder()
            .put(CAUSATION_KEY, "evt-1")
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
      assertFalse(ctx.baggage().containsKey(CAUSATION_KEY), "only the configured key is kept");
      assertEquals("admin", ctx.baggage().get("role"));
      assertEquals("leak", ctx.baggage().get("tracking"));
      assertFalse(ctx.baggage().containsKey("email"));
    }
  }

  @Test
  void buildContext_aCausationBaggageEntryIsDroppedUnlessTheApplicationAllowlistsIt() {
    io.opentelemetry.api.baggage.Baggage baggage =
        io.opentelemetry.api.baggage.Baggage.builder().put(CAUSATION_KEY, "evt-trigger-1").build();
    io.opentelemetry.context.Context otelCtx =
        io.opentelemetry.context.Context.current().with(baggage);

    try (var _ = otelCtx.makeCurrent()) {
      assertFalse(
          new StreamRuneContextFilter()
              .buildContext(request("trace-1", "user-1", "corr-1", null))
              .baggage()
              .containsKey(CAUSATION_KEY),
          "the default allowlist is empty");
      // Allowlisted, it is an ordinary baggage entry; nothing reads it as a causation id.
      assertEquals(
          "evt-trigger-1",
          new StreamRuneContextFilter(
                  withBaggageAllowlist(List.of(CAUSATION_KEY)),
                  RequestIdentityPolicy.of(null, false),
                  null)
              .buildContext(request("trace-1", "user-1", "corr-1", null))
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

  @SuppressWarnings("unchecked")
  private static void assertNoHeaderSetsProvenance(
      String mode, RequestIdentityPolicy policy, String correlationId, Set<String> allowlist) {
    Map<String, String> sent = new LinkedHashMap<>();
    sent.put("X-Correlation-Id", correlationId);
    sent.put("X-Trace-Id", "trace-1");
    sent.put("X-User-Id", "mallory");
    sent.put("X-User-Role", "ADMIN");
    sent.put("X-Causation-Id", "evt-1");
    sent.put("X-Saga-Origin", "{\"sagaType\":\"x.S\",\"sagaId\":\"order-saga-42\",\"hop\":0}");
    HttpRequest<Object> request = mock(HttpRequest.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(request.getHeaders()).thenReturn(headers);
    when(headers.get(any(CharSequence.class)))
        .thenAnswer(invocation -> sent.get(invocation.getArgument(0).toString()));
    when(headers.getAll(any(CharSequence.class)))
        .thenAnswer(
            invocation -> {
              String value = sent.get(invocation.getArgument(0).toString());
              return value == null ? List.of() : List.of(value);
            });
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
      MutableHttpResponse<?> response = mock(MutableHttpResponse.class);
      FilterContinuation<MutableHttpResponse<?>> continuation = mock(FilterContinuation.class);
      when(continuation.proceed())
          .thenAnswer(
              invocation -> {
                assertNull(StreamRuneContext.cause(), "a request filter binds no dispatch cause");
                assertFalse(StreamRuneContext.isSagaDispatch());
                bus.execute(new CommandBusProvenanceContract.Mark("marked-1"));
                return response;
              });
      try (var _ = otelCtx.makeCurrent()) {
        new StreamRuneContextFilter(withBaggageAllowlist(List.copyOf(allowlist)), policy, null)
            .filter(request, continuation);
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
        java.time.Duration.ofSeconds(30),
        // sse.polling-interval
        java.time.Duration.ofSeconds(1));
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
