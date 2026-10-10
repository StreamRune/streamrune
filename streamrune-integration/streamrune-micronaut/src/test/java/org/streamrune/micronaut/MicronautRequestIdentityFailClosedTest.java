package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.filters.AuthenticationFetcher;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import org.streamrune.core.CommandAuthorizationPolicy;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.AuthorizationCommandInterceptor;
import org.streamrune.runtime.VirtualThreadCommandBus;
import reactor.core.publisher.Mono;

/**
 * The HTTP identity fails CLOSED on Micronaut, exactly as it does on Spring and Quarkus. Regression
 * matrix driven through a REAL embedded Netty server and HTTP client over the real {@link
 * StreamRuneMicronautModule}: the {@code RequestIdentityPolicy} bean is the one the module builds
 * from the real {@code AuthenticatedUserResolver} wiring (Micronaut Security enabled = {@link
 * MicronautAuthenticatedUserResolver} over the real {@code SecurityService}, or an application
 * resolver bean) and the real {@code streamrune.security.trust-user-id-header} binding; {@link
 * StreamRuneContextFilter} and {@link SseController} are the container-built beans serving real
 * requests; the startup rule runs as a real {@code StartupEvent} dispatch during context start. A
 * wiring regression — the flag no longer reaching the policy, the resolver no longer wired, the
 * validator no longer listening — turns a row red.
 *
 * <ul>
 *   <li>no Micronaut Security, no trust flag, {@code X-User-Id} sent ⇒ ANONYMOUS (the header is
 *       ignored);
 *   <li>authenticated principal, mismatching {@code X-User-Id} ⇒ the principal;
 *   <li>trusted gateway ({@code streamrune.security.trust-user-id-header=true}) ⇒ the header;
 *   <li>command authorization configured, but neither a resolver nor the trust flag ⇒ startup is
 *       refused;
 *   <li>the same caller rows for the SSE endpoint, which resolves its caller through the same
 *       policy bean;
 *   <li>the {@code X-User-Role} header becomes baggage {@code role} only behind a trusted gateway:
 *       anonymous + header ⇒ no role; authenticated principal + header ⇒ no role (and not the
 *       resolver's roles either — those are the captured {@code authority}); trusted gateway +
 *       header ⇒ the header's role. (A W3C {@code baggage: role=} entry is pinned by {@code
 *       StreamRuneContextFilterTest}: no OpenTelemetry propagator parses the header here.)
 * </ul>
 *
 * <p>Previously the first row bound the header as the user — any client could impersonate an owner
 * and forge the audit attribution — the refusal rows started cleanly, and the SSE endpoint ignored
 * the header even behind a trusted gateway. Previously every mode copied a client's {@code
 * X-User-Role} into baggage {@code role}, and from there into every produced event's metadata.
 */
class MicronautRequestIdentityFailClosedTest {

  static final String SPEC = "MicronautRequestIdentityFailClosedTest";

  /** Selects an application-built command bus fixture: {@code authorizing} or {@code plain}. */
  static final String APPLICATION_BUS = "identity-test.application-bus";

  private static final Argument<Map<String, String>> MAP =
      Argument.mapOf(String.class, String.class);

  private static final String VALIDATOR_LOGGER =
      "org.streamrune.micronaut.StreamRuneRequestIdentityValidator";

  @AfterEach
  void clearRecordedCallers() {
    RecordingSseAuthorizerFixture.CALLERS.clear();
  }

  // ---- the request filter -----------------------------------------------------------------------

  @Test
  void withoutMicronautSecurityOrTheTrustFlagTheUserIdHeaderIsIgnoredAndTheRequestIsAnonymous() {
    try (var server = boot(noSecurity())) {
      assertThat(boundUserId(server, List.of("alice"), null)).isEmpty();
    }
  }

  @Test
  void anAuthenticatedMicronautSecurityPrincipalWinsOverAMismatchingUserIdHeader() {
    try (var server = boot(withMicronautSecurity())) {
      assertThat(boundUserId(server, List.of("alice"), "bob")).isEqualTo("bob");
    }
  }

  @Test
  void anUnauthenticatedRequestWithMicronautSecurityIsAnonymousWhateverTheHeaderSays() {
    try (var server = boot(withMicronautSecurity())) {
      assertThat(boundUserId(server, List.of("alice"), null)).isEmpty();
    }
  }

  @Test
  void anApplicationAuthenticatedUserResolverWinsOverAMismatchingUserIdHeader() {
    Map<String, Object> props = noSecurity();
    props.put("identity-test.resolver", "true");
    try (var server = boot(props)) {
      assertThat(boundUserId(server, List.of("alice"), null)).isEqualTo("bob");
    }
  }

  @Test
  void theTrustedGatewayModeTakesTheIdentityFromTheHeader() {
    Map<String, Object> props = noSecurity();
    props.put("streamrune.security.trust-user-id-header", "true");
    try (var server = boot(props)) {
      assertThat(boundUserId(server, List.of("alice"), null)).isEqualTo("alice");
    }
  }

  @Test
  void theTrustedGatewayModeTakesTheHeaderEvenWithMicronautSecurity() {
    Map<String, Object> props = withMicronautSecurity();
    props.put("streamrune.security.trust-user-id-header", "true");
    try (var server = boot(props)) {
      assertThat(boundUserId(server, List.of("alice"), "bob")).isEqualTo("alice");
    }
  }

  @Test
  void aRepeatedUserIdHeaderIsAnonymousBehindATrustedGateway() {
    // A gateway that appends its X-User-Id after the client's instead of replacing it: the filter
    // must not pick either value (Micronaut's single-valued get would take the client's first one).
    Map<String, Object> props = noSecurity();
    props.put("streamrune.security.trust-user-id-header", "true");
    try (var server = boot(props)) {
      assertThat(boundUserId(server, List.of("victim", "real"), null)).isEmpty();
    }
  }

  @Test
  void aRepeatedUserIdHeaderDoesNotDisplaceTheAuthenticatedPrincipal() {
    try (var server = boot(withMicronautSecurity())) {
      assertThat(boundUserId(server, List.of("alice", "bob"), "bob")).isEqualTo("bob");
    }
  }

  // ---- X-User-Role is read only behind a trusted gateway --------------------------------

  @Test
  void withoutMicronautSecurityOrTheTrustFlagTheRoleHeaderIsIgnored() {
    try (var server = boot(noSecurity())) {
      var bound = bound(server, List.of("alice"), List.of("ADMIN"), null);
      assertThat(bound.get("userId")).isEmpty();
      assertThat(bound.get("role")).isEmpty();
    }
  }

  @Test
  void anAuthenticatedPrincipalNeverTakesTheRoleHeaderNorLendsItsRolesToTheBaggage() {
    // Micronaut Security enabled: the principal is the identity and the shipped
    // MicronautSecurityUserRoleResolver is wired. Its verified roles are captured into the
    // context's authority; baggage `role` is neither the client's header nor the resolver's answer.
    try (var server = boot(withMicronautSecurity())) {
      var bound = bound(server, List.of("alice"), List.of("CUSTOMER"), "bob");
      assertThat(bound).containsEntry("userId", "bob");
      assertThat(bound.get("role")).isEmpty();
      assertThat(bound).containsEntry("authorityRoles", "ADMIN");
    }
  }

  @Test
  void anUnauthenticatedRequestWithMicronautSecurityIgnoresTheRoleHeader() {
    try (var server = boot(withMicronautSecurity())) {
      var bound = bound(server, List.of("alice"), List.of("ADMIN"), null);
      assertThat(bound.get("userId")).isEmpty();
      assertThat(bound.get("role")).isEmpty();
    }
  }

  @Test
  void theTrustedGatewayModeTakesTheRoleFromTheHeader() {
    Map<String, Object> props = noSecurity();
    props.put("streamrune.security.trust-user-id-header", "true");
    try (var server = boot(props)) {
      var bound = bound(server, List.of("alice"), List.of("ADMIN"), null);
      assertThat(bound).containsEntry("userId", "alice");
      assertThat(bound).containsEntry("role", "ADMIN");
    }
  }

  @Test
  void aRepeatedRoleHeaderIsNoRoleBehindATrustedGateway() {
    // A gateway that appends its X-User-Role after the client's instead of replacing it: neither
    // value is the role (Micronaut's single-valued get would take the client's first one).
    Map<String, Object> props = noSecurity();
    props.put("streamrune.security.trust-user-id-header", "true");
    try (var server = boot(props)) {
      var bound = bound(server, List.of("alice"), List.of("ADMIN", "CUSTOMER"), null);
      assertThat(bound).containsEntry("userId", "alice");
      assertThat(bound.get("role")).isEmpty();
    }
  }

  // ---- the startup rule -------------------------------------------------------------------------

  @Test
  void aCommandAuthorizationPolicyWithoutMicronautSecurityOrTheTrustFlagRefusesStartup() {
    Map<String, Object> props = noSecurity();
    props.put("identity-test.policy", "true");

    IllegalStateException refused = refusal(props);

    assertThat(refused)
        .hasMessageContaining("AuthorizationCommandInterceptor")
        .hasMessageContaining("AuthenticatedUserResolver")
        .hasMessageContaining("streamrune.security.trust-user-id-header=true");
  }

  @Test
  void annotationAuthorizationWithoutMicronautSecurityOrTheTrustFlagRefusesStartup() {
    // An application UserRoleResolver (no Micronaut Security) gets the
    // AnnotationAuthorizationInterceptor produced — which resolves authorities FOR
    // RequestContext.userId.
    Map<String, Object> props = noSecurity();
    props.put("identity-test.role-resolver", "true");

    assertThat(refusal(props)).hasMessageContaining("AnnotationAuthorizationInterceptor");
  }

  @Test
  void anAuthorizationInterceptorInsideAnApplicationBusRefusesStartupWithoutAnIdentitySource() {
    // The application's own bus carries the interceptor inline — it is not a bean. The rule reads
    // the chain the bus runs, so the bus is refused exactly like the module-built one.
    Map<String, Object> props = noSecurity();
    props.put(APPLICATION_BUS, "authorizing");

    assertThat(refusal(props))
        .hasMessageContaining("(AuthorizationCommandInterceptor)")
        .hasMessageContaining("AuthenticatedUserResolver");
  }

  @Test
  void anAuthorizationInterceptorLeftOutOfAnApplicationBusDoesNotRefuseStartup() {
    // A CommandAuthorizationPolicy bean makes the module add an AuthorizationCommandInterceptor,
    // but the application's own bus does not run it: no command consults the request identity.
    Map<String, Object> props = noSecurity();
    props.put("identity-test.policy", "true");
    props.put(APPLICATION_BUS, "plain");

    try (var server = boot(props)) {
      assertThat(server.isRunning()).isTrue();
    }
  }

  @Test
  void aCommandAuthorizationPolicyBehindTheTrustedGatewayStartsAndLogsTheModeAsAWarning() {
    Map<String, Object> props = noSecurity();
    props.put("identity-test.policy", "true");
    props.put("streamrune.security.trust-user-id-header", "true");
    ListAppender<ILoggingEvent> appender = captureValidatorLog();
    try (var server = boot(props)) {
      assertThat(boundUserId(server, List.of("alice"), null)).isEqualTo("alice");
    } finally {
      detach(appender);
    }
    assertThat(appender.list)
        .anySatisfy(
            e -> {
              assertThat(e.getLevel()).isEqualTo(Level.WARN);
              assertThat(e.getFormattedMessage()).startsWith("Request identity: TRUSTED_GATEWAY");
            });
  }

  @Test
  void aCommandAuthorizationPolicyWithMicronautSecurityStarts() {
    Map<String, Object> props = withMicronautSecurity();
    props.put("identity-test.policy", "true");
    ListAppender<ILoggingEvent> appender = captureValidatorLog();
    try (var server = boot(props)) {
      assertThat(boundUserId(server, List.of("alice"), "bob")).isEqualTo("bob");
    } finally {
      detach(appender);
    }
    assertThat(appender.list)
        .anySatisfy(
            e -> {
              assertThat(e.getLevel()).isEqualTo(Level.INFO);
              assertThat(e.getFormattedMessage())
                  .startsWith("Request identity: AUTHENTICATED_PRINCIPAL");
            });
  }

  @Test
  void anAnonymousApplicationWithoutCommandAuthorizationStarts() {
    ListAppender<ILoggingEvent> appender = captureValidatorLog();
    try (var server = boot(noSecurity())) {
      assertThat(boundUserId(server, List.of("alice"), null)).isEmpty();
    } finally {
      detach(appender);
    }
    assertThat(appender.list)
        .anySatisfy(
            e -> {
              assertThat(e.getLevel()).isEqualTo(Level.INFO);
              assertThat(e.getFormattedMessage()).startsWith("Request identity: ANONYMOUS");
            });
  }

  // ---- the SSE endpoint -------------------------------------------------------------------------

  @Test
  void theSseEndpointAuthorizesAnAnonymousCallerWithoutSecurityOrTheTrustFlag() {
    Map<String, Object> props = noSecurity();
    props.put("streamrune.sse.enabled", "true");
    try (var server = boot(props)) {
      assertThat(sseCaller(server, List.of("alice"), null)).isNull();
    }
  }

  @Test
  void theSseEndpointServesARequestWithoutTheHeaderBehindTheTrustedGateway() {
    // The header parameter is optional: a request without it reaches the authorizer (no 400).
    Map<String, Object> props = noSecurity();
    props.put("streamrune.sse.enabled", "true");
    props.put("streamrune.security.trust-user-id-header", "true");
    try (var server = boot(props)) {
      assertThat(sseCaller(server, List.of(), null)).isNull();
    }
  }

  @Test
  void theSseEndpointAuthorizesTheHeaderCallerBehindTheTrustedGateway() {
    Map<String, Object> props = noSecurity();
    props.put("streamrune.sse.enabled", "true");
    props.put("streamrune.security.trust-user-id-header", "true");
    try (var server = boot(props)) {
      assertThat(sseCaller(server, List.of("alice"), null)).isEqualTo(UserId.of("alice"));
    }
  }

  @Test
  void theSseEndpointAuthorizesTheAuthenticatedPrincipalNotAMismatchingHeader() {
    Map<String, Object> props = withMicronautSecurity();
    props.put("streamrune.sse.enabled", "true");
    try (var server = boot(props)) {
      assertThat(sseCaller(server, List.of("alice"), "bob")).isEqualTo(UserId.of("bob"));
    }
  }

  @Test
  void theSseEndpointTreatsARepeatedUserIdHeaderAsAnonymousBehindATrustedGateway() {
    Map<String, Object> props = noSecurity();
    props.put("streamrune.sse.enabled", "true");
    props.put("streamrune.security.trust-user-id-header", "true");
    try (var server = boot(props)) {
      assertThat(sseCaller(server, List.of("victim", "real"), null)).isNull();
    }
  }

  @Test
  void theSseEndpointAnswersAnInvalidAggregateTypeWithA400BeforeTheAuthorizer() {
    Map<String, Object> props = noSecurity();
    props.put("streamrune.sse.enabled", "true");
    try (var server = boot(props);
        HttpClient client = HttpClient.create(server.getURL())) {
      HttpClientResponseException rejected =
          assertThrows(
              HttpClientResponseException.class,
              () ->
                  client
                      .toBlocking()
                      .exchange(HttpRequest.GET("/api/sse/Order/o-1"), String.class));
      assertThat(rejected.getStatus().getCode()).isEqualTo(400);
    }
    assertThat(RecordingSseAuthorizerFixture.CALLERS).isEmpty();
  }

  @Test
  void aSingleCommaBearingHeaderValueIsOneValueAtBothEntryPoints() {
    // One header value containing a comma stays one value — no List-typed binding splits it.
    Map<String, Object> props = noSecurity();
    props.put("streamrune.sse.enabled", "true");
    props.put("streamrune.security.trust-user-id-header", "true");
    try (var server = boot(props)) {
      assertThat(boundUserId(server, List.of("a,b"), null)).isEqualTo("a,b");
      assertThat(sseCaller(server, List.of("a,b"), null)).isEqualTo(UserId.of("a,b"));
    }
  }

  // ---- harness ----------------------------------------------------------------------------------

  private static Map<String, Object> noSecurity() {
    Map<String, Object> props = new HashMap<>();
    props.put("spec.name", SPEC);
    props.put("micronaut.server.port", "-1");
    props.put("micronaut.security.enabled", "false");
    return props;
  }

  /**
   * Micronaut Security enabled: its {@code SecurityService} exists, so the module wires {@link
   * MicronautAuthenticatedUserResolver}. Every route is open to anonymous callers (the identity
   * rule, not route security, is under test) and {@link HeaderAuthenticationFixture} authenticates
   * a request that carries {@code X-Test-Authenticated-As}.
   */
  private static Map<String, Object> withMicronautSecurity() {
    Map<String, Object> props = new HashMap<>();
    props.put("spec.name", SPEC);
    props.put("micronaut.server.port", "-1");
    props.put("micronaut.security.enabled", "true");
    props.put("identity-test.authentication", "true");
    props.put(
        "micronaut.security.intercept-url-map",
        List.of(Map.of("pattern", "/**", "access", List.of("isAnonymous()"))));
    return props;
  }

  private static EmbeddedServer boot(Map<String, Object> props) {
    return ApplicationContext.run(EmbeddedServer.class, props);
  }

  /** The user id the request filter bound for one real request; empty when anonymous. */
  private static String boundUserId(
      EmbeddedServer server, List<String> userIdHeaders, String authenticatedAs) {
    return bound(server, userIdHeaders, List.of(), authenticatedAs).get("userId");
  }

  /**
   * What the request filter bound for one real request carrying one {@code X-User-Id} line per
   * element of {@code userIdHeaders} and one {@code X-User-Role} line per element of {@code
   * roleHeaders}: {@code userId} and baggage {@code role} (empty when absent), and the captured
   * {@code authorityRoles} (comma-joined, sorted; empty when nothing was captured).
   */
  private static Map<String, String> bound(
      EmbeddedServer server,
      List<String> userIdHeaders,
      List<String> roleHeaders,
      String authenticatedAs) {
    try (HttpClient client = HttpClient.create(server.getURL())) {
      MutableHttpRequest<?> request = HttpRequest.GET("/identity-test/identity-probe");
      userIdHeaders.forEach(value -> request.getHeaders().add("X-User-Id", value));
      roleHeaders.forEach(value -> request.getHeaders().add("X-User-Role", value));
      if (authenticatedAs != null) {
        request.header(HeaderAuthenticationFixture.HEADER, authenticatedAs);
      }
      return client.toBlocking().retrieve(request, MAP);
    }
  }

  /** The caller the SSE endpoint handed to the application's {@link SseAuthorizer}. */
  private static UserId sseCaller(
      EmbeddedServer server, List<String> userIdHeaders, String authenticatedAs) {
    try (HttpClient client = HttpClient.create(server.getURL())) {
      MutableHttpRequest<?> request = HttpRequest.GET("/api/sse/order/order-1");
      userIdHeaders.forEach(value -> request.getHeaders().add("X-User-Id", value));
      if (authenticatedAs != null) {
        request.header(HeaderAuthenticationFixture.HEADER, authenticatedAs);
      }
      // The recording authorizer denies, so the endpoint answers 403 without opening a stream.
      HttpClientResponseException denied =
          assertThrows(
              HttpClientResponseException.class,
              () -> client.toBlocking().exchange(request, String.class));
      assertThat(denied.getStatus().getCode()).isEqualTo(403);
    }
    assertThat(RecordingSseAuthorizerFixture.CALLERS).hasSize(1);
    return RecordingSseAuthorizerFixture.CALLERS.getFirst().orElse(null);
  }

  private static IllegalStateException refusal(Map<String, Object> props) {
    RuntimeException failed =
        assertThrows(RuntimeException.class, () -> boot(props).close(), "startup must be refused");
    List<Throwable> chain = new ArrayList<>();
    for (Throwable t = failed; t != null && !chain.contains(t); t = t.getCause()) {
      chain.add(t);
    }
    return chain.stream()
        .filter(IllegalStateException.class::isInstance)
        .map(IllegalStateException.class::cast)
        .filter(e -> e.getMessage() != null && e.getMessage().contains("no identity source"))
        .findFirst()
        .orElseThrow(
            () -> new AssertionError("startup failed, but not with the identity refusal", failed));
  }

  private static ListAppender<ILoggingEvent> captureValidatorLog() {
    Logger logger = (Logger) LoggerFactory.getLogger(VALIDATOR_LOGGER);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    return appender;
  }

  private static void detach(ListAppender<ILoggingEvent> appender) {
    Logger logger = (Logger) LoggerFactory.getLogger(VALIDATOR_LOGGER);
    logger.detachAppender(appender);
    logger.setLevel(null);
  }

  // ---- fixtures ---------------------------------------------------------------------------------

  /**
   * Reports the user id, the baggage {@code role} and the captured authority's roles {@link
   * StreamRuneContextFilter} bound for the request.
   */
  @Controller("/identity-test/identity-probe")
  @Requires(property = "spec.name", value = SPEC)
  static class IdentityProbeController {

    @Get
    Map<String, String> probe() {
      var scoped = StreamRuneContext.CURRENT.isBound() ? StreamRuneContext.CURRENT.get() : null;
      var ctx = scoped != null ? scoped : StreamRuneContextHelper.get();
      Map<String, String> result = new HashMap<>();
      result.put("userId", ctx != null && ctx.userId() != null ? ctx.userId().value() : "");
      result.put("role", ctx != null ? ctx.baggage().getOrDefault("role", "") : "");
      result.put(
          "authorityRoles",
          ctx != null && ctx.authority() != null
              ? String.join(",", new java.util.TreeSet<>(ctx.authority().roles()))
              : "");
      return result;
    }
  }

  /** An application {@link AuthenticatedUserResolver}: every request is authenticated as bob. */
  @Factory
  @Requires(property = "spec.name", value = SPEC)
  @Requires(property = "identity-test.resolver", value = "true")
  static class ApplicationResolverFixture {
    @Singleton
    AuthenticatedUserResolver authenticatedUserResolver() {
      return () -> Optional.of(UserId.of("bob"));
    }
  }

  /** Command authorization through a policy: the module adds an AuthorizationCommandInterceptor. */
  @Factory
  @Requires(property = "spec.name", value = SPEC)
  @Requires(property = "identity-test.policy", value = "true")
  static class PolicyFixture {
    @Singleton
    CommandAuthorizationPolicy commandAuthorizationPolicy() {
      return CommandAuthorizationPolicy.allowAll();
    }
  }

  /** Annotation authorization: the module adds an AnnotationAuthorizationInterceptor. */
  @Factory
  @Requires(property = "spec.name", value = SPEC)
  @Requires(property = "identity-test.role-resolver", value = "true")
  static class RoleResolverFixture {
    @Singleton
    UserRoleResolver userRoleResolver() {
      return userId -> UserAuthority.EMPTY;
    }
  }

  /**
   * An {@link EventStore} bean, so the module builds its command bus — the bus whose chain the
   * startup rule reads — without a database. An application bus fixture replaces that bus.
   */
  @Factory
  @Requires(property = "spec.name", value = SPEC)
  static class EventStoreFixture {
    @Singleton
    EventStore eventStore() {
      // The head of an empty global stream: the SSE event feed of the tests that enable the
      // endpoint starts from it.
      EventStore store = mock(EventStore.class);
      when(store.lastGlobalOffset()).thenReturn(GlobalOffset.initial());
      return store;
    }
  }

  /** The application's own bus, authorizing through an interceptor that is not a bean. */
  @Factory
  @Requires(property = "spec.name", value = SPEC)
  @Requires(property = APPLICATION_BUS, value = "authorizing")
  static class ApplicationAuthorizingBusFixture {
    @Singleton
    VirtualThreadCommandBus commandBus() {
      return VirtualThreadCommandBus.builder()
          .eventStore(mock(EventStore.class))
          .interceptors(new AuthorizationCommandInterceptor(CommandAuthorizationPolicy.allowAll()))
          .build();
    }
  }

  /** The application's own bus, with no interceptor at all. */
  @Factory
  @Requires(property = "spec.name", value = SPEC)
  @Requires(property = APPLICATION_BUS, value = "plain")
  static class ApplicationPlainBusFixture {
    @Singleton
    VirtualThreadCommandBus commandBus() {
      return VirtualThreadCommandBus.builder().eventStore(mock(EventStore.class)).build();
    }
  }

  /** Records each SSE caller and denies, so no stream is ever opened. */
  @Factory
  @Requires(property = "spec.name", value = SPEC)
  static class RecordingSseAuthorizerFixture {
    static final List<Optional<UserId>> CALLERS = new CopyOnWriteArrayList<>();

    @Singleton
    SseAuthorizer recordingSseAuthorizer() {
      return (UserId principal, StreamId streamId) -> {
        CALLERS.add(Optional.ofNullable(principal));
        return false;
      };
    }
  }

  /**
   * Real Micronaut Security authentication: a request carrying {@value #HEADER} is authenticated as
   * that name, with the role {@code ADMIN}, which {@code SecurityService} then reports to {@link
   * MicronautAuthenticatedUserResolver} and {@link MicronautSecurityUserRoleResolver}.
   */
  @Singleton
  @Requires(property = "spec.name", value = SPEC)
  @Requires(property = "identity-test.authentication", value = "true")
  static class HeaderAuthenticationFixture implements AuthenticationFetcher<HttpRequest<?>> {
    static final String HEADER = "X-Test-Authenticated-As";

    @Override
    public Publisher<Authentication> fetchAuthentication(HttpRequest<?> request) {
      String name = request.getHeaders().get(HEADER);
      return name == null ? Mono.empty() : Mono.just(Authentication.build(name, List.of("ADMIN")));
    }
  }
}
