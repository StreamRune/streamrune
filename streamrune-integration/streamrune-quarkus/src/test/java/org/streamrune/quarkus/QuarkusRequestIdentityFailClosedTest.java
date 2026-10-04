package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.streamrune.core.CommandAuthorizationPolicy;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.AuthorizationCommandInterceptor;
import org.streamrune.runtime.SseEventPublisher;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The HTTP identity fails CLOSED on Quarkus, exactly as it does on Spring. Regression matrix driven
 * through a REAL Arc container ({@link RealArcTestContainer}) over the real {@link
 * StreamRuneProducers}: the {@link RequestIdentityPolicy} bean is the one the producers build from
 * the real {@code AuthenticatedUserResolver} producer (Quarkus Security present = a {@link
 * SecurityIdentity} bean exists) and the real {@code streamrune.security.trust-user-id-header}
 * binding; the {@link StreamRuneRequestFilter} is constructed by Arc through its {@code @Inject}
 * constructor, registered the way Quarkus REST registers a {@code @Provider}; and the startup rule
 * runs as a real {@code @Observes StartupEvent} dispatch. A wiring regression — the flag no longer
 * reaching the policy, the resolver no longer wired, the validator no longer observing — turns a
 * row red.
 *
 * <ul>
 *   <li>no Quarkus Security, no trust flag, {@code X-User-Id} sent ⇒ ANONYMOUS (the header is
 *       ignored);
 *   <li>authenticated principal, mismatching {@code X-User-Id} ⇒ the principal;
 *   <li>trusted gateway ({@code streamrune.security.trust-user-id-header=true}) ⇒ the header;
 *   <li>command authorization configured, but neither a resolver nor the trust flag ⇒ startup is
 *       refused;
 *   <li>the same rows for the SSE endpoint, which resolves its caller through the same policy bean;
 *   <li>the {@code X-User-Role} header becomes baggage {@code role} only behind a trusted gateway:
 *       anonymous + header ⇒ no role; authenticated principal + header ⇒ no role (and not the
 *       resolver's roles either — those are the captured {@code authority}); trusted gateway +
 *       header ⇒ the header's role; a W3C {@code baggage: role=} entry never reaches the key.
 * </ul>
 *
 * <p>Previously the first row bound the header as the user — any client could impersonate an owner
 * and forge the audit attribution — the refusal rows started cleanly, and the SSE endpoint ignored
 * the header even behind a trusted gateway. Previously every mode copied a client's {@code
 * X-User-Role} into baggage {@code role}, and from there into every produced event's metadata.
 */
class QuarkusRequestIdentityFailClosedTest {

  // ---- the request filter -----------------------------------------------------------------------

  @Test
  void withoutQuarkusSecurityOrTheTrustFlagTheUserIdHeaderIsIgnoredAndTheRequestIsAnonymous()
      throws Exception {
    try (var arc = bootWeb(NoTrustFlag.class)) {
      startUp(arc);
      assertThat(boundUserId(arc, "alice")).isNull();
    }
  }

  @Test
  void anAuthenticatedPrincipalWinsOverAMismatchingUserIdHeader() throws Exception {
    try (var arc = bootWeb(NoTrustFlag.class, AuthenticatedAsBob.class)) {
      startUp(arc);
      assertThat(boundUserId(arc, "alice")).isEqualTo(UserId.of("bob"));
    }
  }

  @Test
  void anUnauthenticatedRequestWithQuarkusSecurityIsAnonymousWhateverTheHeaderSays()
      throws Exception {
    try (var arc = bootWeb(NoTrustFlag.class, Unauthenticated.class)) {
      startUp(arc);
      assertThat(boundUserId(arc, "alice")).isNull();
    }
  }

  @Test
  void theTrustedGatewayModeTakesTheIdentityFromTheHeader() throws Exception {
    try (var arc = bootWeb(TrustFlag.class)) {
      startUp(arc);
      assertThat(boundUserId(arc, "alice")).isEqualTo(UserId.of("alice"));
    }
  }

  @Test
  void theTrustedGatewayModeTakesTheHeaderEvenWithQuarkusSecurity() throws Exception {
    try (var arc = bootWeb(TrustFlag.class, AuthenticatedAsBob.class)) {
      startUp(arc);
      assertThat(boundUserId(arc, "alice")).isEqualTo(UserId.of("alice"));
    }
  }

  @Test
  void aRepeatedUserIdHeaderIsAnonymousBehindATrustedGateway() throws Exception {
    // A gateway that appends its X-User-Id after the client's instead of replacing it: the filter
    // must not pick either value, nor the comma-joined "victim,real" getHeaderString returns.
    try (var arc = bootWeb(TrustFlag.class)) {
      startUp(arc);
      assertThat(boundUserId(arc, "victim", "real")).isNull();
    }
  }

  @Test
  void aRepeatedUserIdHeaderDoesNotDisplaceTheAuthenticatedPrincipal() throws Exception {
    try (var arc = bootWeb(NoTrustFlag.class, AuthenticatedAsBob.class)) {
      startUp(arc);
      assertThat(boundUserId(arc, "alice", "bob")).isEqualTo(UserId.of("bob"));
    }
  }

  // ---- X-User-Role is read only behind a trusted gateway --------------------------------

  @Test
  void withoutQuarkusSecurityOrTheTrustFlagTheRoleHeaderIsIgnored() throws Exception {
    try (var arc = bootWeb(NoTrustFlag.class)) {
      startUp(arc);
      var bound = boundContext(arc, List.of("alice"), List.of("ADMIN"), Map.of());
      assertThat(bound.userId()).isNull();
      assertThat(bound.baggage()).doesNotContainKey("role");
    }
  }

  @Test
  void anAuthenticatedPrincipalNeverTakesTheRoleHeaderNorLendsItsRolesToTheBaggage()
      throws Exception {
    // Quarkus Security present: the principal is the identity and the shipped
    // QuarkusSecurityUserRoleResolver is produced. Its verified roles are captured into the
    // context's authority; baggage `role` is neither the client's header nor the resolver's answer.
    try (var arc = bootWeb(NoTrustFlag.class, AuthenticatedAsBob.class)) {
      startUp(arc);
      var bound = boundContext(arc, List.of("alice"), List.of("CUSTOMER"), Map.of());
      assertThat(bound.userId()).isEqualTo(UserId.of("bob"));
      assertThat(bound.baggage()).doesNotContainKey("role");
      assertThat(bound.authority()).isNotNull();
      assertThat(bound.authority().roles()).containsExactly("ADMIN");
    }
  }

  @Test
  void anUnauthenticatedRequestWithQuarkusSecurityIgnoresTheRoleHeader() throws Exception {
    try (var arc = bootWeb(NoTrustFlag.class, Unauthenticated.class)) {
      startUp(arc);
      var bound = boundContext(arc, List.of("alice"), List.of("ADMIN"), Map.of());
      assertThat(bound.userId()).isNull();
      assertThat(bound.baggage()).doesNotContainKey("role");
    }
  }

  @Test
  void theTrustedGatewayModeTakesTheRoleFromTheHeader() throws Exception {
    try (var arc = bootWeb(TrustFlag.class)) {
      startUp(arc);
      var bound = boundContext(arc, List.of("alice"), List.of("ADMIN"), Map.of());
      assertThat(bound.userId()).isEqualTo(UserId.of("alice"));
      assertThat(bound.baggage()).containsEntry("role", "ADMIN");
    }
  }

  @Test
  void aRepeatedRoleHeaderIsNoRoleBehindATrustedGateway() throws Exception {
    // Neither value, nor the comma-joined "ADMIN,CUSTOMER" getHeaderString would return.
    try (var arc = bootWeb(TrustFlag.class)) {
      startUp(arc);
      var bound = boundContext(arc, List.of("alice"), List.of("ADMIN", "CUSTOMER"), Map.of());
      assertThat(bound.baggage()).doesNotContainKey("role");
    }
  }

  @Test
  void aW3cBaggageRoleEntryNeverReachesTheRoleKeyInAnyMode() throws Exception {
    Map<String, String> smuggled = Map.of("role", "ADMIN");
    try (var arc = bootWeb(NoTrustFlag.class)) {
      startUp(arc);
      assertThat(boundContext(arc, List.of(), List.of(), smuggled).baggage())
          .doesNotContainKey("role");
    }
    // `role` is reserved for the role rule: allow-listing it changes nothing.
    try (var arc = bootWeb(TrustFlagAllowlistingRole.class)) {
      startUp(arc);
      assertThat(boundContext(arc, List.of("alice"), List.of(), smuggled).baggage())
          .doesNotContainKey("role");
      assertThat(boundContext(arc, List.of("alice"), List.of("CUSTOMER"), smuggled).baggage())
          .containsEntry("role", "CUSTOMER");
    }
  }

  // ---- the startup rule -------------------------------------------------------------------------

  @Test
  void aCommandAuthorizationPolicyWithoutQuarkusSecurityOrTheTrustFlagRefusesStartup()
      throws Exception {
    try (var arc = bootWeb(NoTrustFlag.class, PolicyConfig.class)) {
      IllegalStateException refused = assertThrows(IllegalStateException.class, () -> startUp(arc));
      assertThat(refused)
          .hasMessageContaining("AuthorizationCommandInterceptor")
          .hasMessageContaining("AuthenticatedUserResolver")
          .hasMessageContaining("streamrune.security.trust-user-id-header=true");
    }
  }

  @Test
  void annotationAuthorizationWithoutQuarkusSecurityOrTheTrustFlagRefusesStartup()
      throws Exception {
    // An application UserRoleResolver (no Quarkus Security) gets the
    // AnnotationAuthorizationInterceptor produced — which resolves authorities FOR
    // RequestContext.userId.
    try (var arc = bootWeb(NoTrustFlag.class, RoleResolverConfig.class)) {
      IllegalStateException refused = assertThrows(IllegalStateException.class, () -> startUp(arc));
      assertThat(refused).hasMessageContaining("AnnotationAuthorizationInterceptor");
    }
  }

  @Test
  void anAuthorizationInterceptorInsideAnApplicationBusRefusesStartupWithoutAnIdentitySource()
      throws Exception {
    // The application's own bus carries the interceptor inline — it is not a bean. The rule reads
    // the chain the bus runs, so the bus is refused exactly like the framework-produced one.
    try (var arc = bootWeb(NoTrustFlag.class, ApplicationAuthorizingBus.class)) {
      IllegalStateException refused = assertThrows(IllegalStateException.class, () -> startUp(arc));
      assertThat(refused)
          .hasMessageContaining("(AuthorizationCommandInterceptor)")
          .hasMessageContaining("AuthenticatedUserResolver");
    }
  }

  @Test
  void anAuthorizationInterceptorLeftOutOfAnApplicationBusDoesNotRefuseStartup() throws Exception {
    // A CommandAuthorizationPolicy bean makes the producers build an
    // AuthorizationCommandInterceptor, but the application's own bus does not run it: no command
    // consults the request identity.
    try (var arc = bootWeb(NoTrustFlag.class, PolicyConfig.class, ApplicationPlainBus.class)) {
      assertDoesNotThrow(() -> startUp(arc));
    }
  }

  @Test
  void commandAuthorizationStartsBehindATrustedGateway() throws Exception {
    try (var arc = bootWeb(TrustFlag.class, PolicyConfig.class, RoleResolverConfig.class)) {
      assertDoesNotThrow(() -> startUp(arc));
    }
  }

  @Test
  void commandAuthorizationStartsWithQuarkusSecurity() throws Exception {
    // A SecurityIdentity bean makes the producers wire QuarkusAuthenticatedUserResolver (and the
    // QuarkusSecurityUserRoleResolver, hence the AnnotationAuthorizationInterceptor too).
    try (var arc = bootWeb(NoTrustFlag.class, Unauthenticated.class, PolicyConfig.class)) {
      assertDoesNotThrow(() -> startUp(arc));
    }
  }

  @Test
  void anAnonymousApplicationWithoutCommandAuthorizationStarts() throws Exception {
    try (var arc = bootWeb(NoTrustFlag.class)) {
      assertDoesNotThrow(() -> startUp(arc));
    }
  }

  @Test
  void aHeadlessApplicationWithoutQuarkusRestIsNotChecked() throws Exception {
    // No Quarkus REST, so no request filter bean: nothing derives identities from HTTP requests.
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                StreamRuneRequestIdentityValidator.class,
                Infrastructure.class,
                NoTrustFlag.class,
                PolicyConfig.class))) {
      assertDoesNotThrow(() -> startUp(arc));
    }
  }

  @Test
  void twoAuthenticatedUserResolverBeansAreRefusedInsteadOfSilentlyRunningAnonymously()
      throws Exception {
    // CDI's Instance.isResolvable() is false for an AMBIGUOUS instance as well as an unsatisfied
    // one, so two resolvers used to fall back to ANONYMOUS without a word. Spring and Micronaut
    // fail on the same configuration; Quarkus must too.
    try (var arc = bootWeb(NoTrustFlag.class, ResolverA.class, ResolverB.class)) {
      Throwable failure =
          org.assertj.core.api.Assertions.catchThrowable(
              () -> arc.container().instance(RequestIdentityPolicy.class).get());
      assertThat(failure).as("an ambiguous resolver must be refused").isNotNull();
      Throwable root = failure;
      while (root.getCause() != null) {
        root = root.getCause();
      }
      assertThat(root)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("more than one AuthenticatedUserResolver");
    }
  }

  // ---- the SSE endpoint resolves the caller exactly like the request filter -------------------

  @Test
  void theSseEndpointIgnoresTheHeaderWithoutQuarkusSecurityOrTheTrustFlag() throws Exception {
    try (var arc = bootWeb(NoTrustFlag.class)) {
      assertThat(sseCaller(arc, "alice")).isEmpty();
    }
  }

  @Test
  void theSseEndpointTakesTheHeaderBehindATrustedGateway() throws Exception {
    try (var arc = bootWeb(TrustFlag.class)) {
      assertThat(sseCaller(arc, "alice")).contains(UserId.of("alice"));
    }
  }

  @Test
  void theSseEndpointAuthorizesTheAuthenticatedPrincipalOverAMismatchingHeader() throws Exception {
    try (var arc = bootWeb(NoTrustFlag.class, AuthenticatedAsBob.class)) {
      assertThat(sseCaller(arc, "alice")).contains(UserId.of("bob"));
    }
  }

  @Test
  void theSseEndpointTreatsARepeatedUserIdHeaderAsAnonymousBehindATrustedGateway()
      throws Exception {
    // Before, the endpoint's single-valued @HeaderParam took the first value ("victim") while the
    // filter's getHeaderString saw "victim,real": two different callers for one request.
    try (var arc = bootWeb(TrustFlag.class)) {
      assertThat(sseCaller(arc, "victim", "real")).isEmpty();
    }
  }

  @Test
  void aSingleCommaBearingHeaderValueIsOneValueAtBothEntryPoints() throws Exception {
    try (var arc = bootWeb(TrustFlag.class)) {
      startUp(arc);
      assertThat(boundUserId(arc, "a,b")).isEqualTo(UserId.of("a,b"));
      assertThat(sseCaller(arc, "a,b")).contains(UserId.of("a,b"));
    }
  }

  @Test
  void theSseEndpointReceivesTheRequestHeadersAndThePolicyBean() throws Exception {
    // Quarkus REST cannot boot in this module, so the JAX-RS binding is pinned structurally: the
    // stream endpoint's third parameter IS the injected HttpHeaders (every X-User-Id value, the
    // same multivalued view the filter reads), not a single-valued @HeaderParam, and the injecting
    // constructor takes the RequestIdentityPolicy bean the filter takes (not an
    // AuthenticatedUserResolver).
    Method stream =
        SseController.class.getMethod("stream", String.class, String.class, HttpHeaders.class);
    assertThat(stream.isAnnotationPresent(jakarta.ws.rs.GET.class)).isTrue();
    Parameter headers = stream.getParameters()[2];
    assertThat(headers.isAnnotationPresent(Context.class)).isTrue();
    assertThat(headers.getAnnotation(HeaderParam.class)).isNull();
    var injecting =
        SseController.class.getConstructor(
            SseEventPublisher.class,
            SseAuthorizer.class,
            RequestIdentityPolicy.class,
            jakarta.inject.Provider.class,
            StreamRuneQuarkusProperties.class);
    assertThat(injecting.isAnnotationPresent(jakarta.inject.Inject.class)).isTrue();
  }

  // ---- harness ----------------------------------------------------------------------------------

  /** Boots the producers, the validator and the Quarkus-REST-registered request filter. */
  private static RealArcTestContainer bootWeb(Class<?>... fixtures) throws Exception {
    List<Class<?>> beans = new ArrayList<>();
    beans.add(StreamRuneProducers.class);
    beans.add(StreamRuneRequestIdentityValidator.class);
    beans.add(StreamRuneRequestContextHolder.class);
    beans.add(Infrastructure.class);
    beans.add(ApplicationEventStore.class);
    beans.addAll(List.of(fixtures));
    return RealArcTestContainer.bootWithRestProviders(
        beans, List.of(StreamRuneRequestFilter.class));
  }

  /** A real, synchronous {@code Event#fire} of the Quarkus startup event. */
  private static void startUp(RealArcTestContainer arc) {
    arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());
  }

  /**
   * Sends one request carrying one {@code X-User-Id} value per element of {@code headers} through
   * the Arc-built filter and returns the {@code RequestContext.userId} it bound.
   */
  private static UserId boundUserId(RealArcTestContainer arc, String... headers) {
    return boundContext(arc, List.of(headers), List.of(), Map.of()).userId();
  }

  /**
   * Sends one request carrying one {@code X-User-Id} value per element of {@code userIds} and one
   * {@code X-User-Role} value per element of {@code roles} through the Arc-built filter, with
   * {@code w3cBaggage} as the current OpenTelemetry baggage (what a client's W3C {@code baggage:}
   * header becomes), and returns the {@code RequestContext} it bound.
   */
  private static StreamRuneContext.RequestContext boundContext(
      RealArcTestContainer arc,
      List<String> userIds,
      List<String> roles,
      Map<String, String> w3cBaggage) {
    StreamRuneRequestFilter filter = arc.container().instance(StreamRuneRequestFilter.class).get();
    ContainerRequestContext request = mock(ContainerRequestContext.class);
    // Stubbed exactly as Quarkus REST answers: getHeaderString joins repeated values with ",",
    // getHeaders() keeps every value as received.
    when(request.getHeaderString("X-User-Id")).thenReturn(String.join(",", userIds));
    when(request.getHeaderString("X-User-Role"))
        .thenReturn(roles.isEmpty() ? null : String.join(",", roles));
    MultivaluedMap<String, String> all = new MultivaluedHashMap<>();
    if (!userIds.isEmpty()) {
      all.put("X-User-Id", List.copyOf(userIds));
    }
    if (!roles.isEmpty()) {
      all.put("X-User-Role", List.copyOf(roles));
    }
    when(request.getHeaders()).thenReturn(all);
    var baggage = io.opentelemetry.api.baggage.Baggage.builder();
    w3cBaggage.forEach(baggage::put);
    var requestScope = arc.container().requestContext();
    requestScope.activate();
    try (var _ = io.opentelemetry.context.Context.current().with(baggage.build()).makeCurrent()) {
      filter.filter(request);
      var bound = ArgumentCaptor.forClass(Object.class);
      verify(request).setProperty(eq(StreamRuneRequestFilter.CONTEXT_PROPERTY), bound.capture());
      assertThat(bound.getValue())
          .as("the filter must bind a request context")
          .isInstanceOf(StreamRuneContext.RequestContext.class);
      return (StreamRuneContext.RequestContext) bound.getValue();
    } finally {
      requestScope.terminate();
    }
  }

  /**
   * Opens {@code /api/sse/stream-1} with one {@code X-User-Id} value per element of {@code headers}
   * on an {@link SseController} built through its injecting constructor from the container's own
   * policy bean (the capturing authorizer denies, so no subscription is opened) and returns the
   * caller the {@link SseAuthorizer} was asked about.
   */
  private static Optional<UserId> sseCaller(RealArcTestContainer arc, String... headers) {
    RequestIdentityPolicy policy = arc.container().instance(RequestIdentityPolicy.class).get();
    assertSame(
        policy,
        arc.container().instance(RequestIdentityPolicy.class).get(),
        "one policy decision for the whole application");
    var authorizer = new CapturingSseAuthorizer();
    var controller =
        new SseController(
            mock(SseEventPublisher.class),
            authorizer,
            policy,
            () -> Boolean.TRUE,
            TestProperties.defaults());
    HttpHeaders requestHeaders = mock(HttpHeaders.class);
    when(requestHeaders.getRequestHeader("X-User-Id")).thenReturn(List.of(headers));
    try {
      assertThrows(
          ForbiddenException.class, () -> controller.stream("order", "order-1", requestHeaders));
    } finally {
      controller.shutdown();
    }
    assertThat(authorizer.asked).as("the SSE authorizer must be consulted").hasSize(1);
    return authorizer.asked.getFirst();
  }

  /** Records every caller it is asked about, and denies. */
  static final class CapturingSseAuthorizer implements SseAuthorizer {
    final List<Optional<UserId>> asked = new CopyOnWriteArrayList<>();

    @Override
    public boolean isAuthorized(UserId principal, StreamId streamId) {
      asked.add(Optional.ofNullable(principal));
      return false;
    }
  }

  private static StreamRuneQuarkusProperties bind(Map<String, String> overrides) {
    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .withMapping(StreamRuneQuarkusProperties.class)
            .withConverter(
                Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
            .withDefaultValues(overrides)
            .build();
    return config.getConfigMapping(StreamRuneQuarkusProperties.class);
  }

  /** The bean a real Quarkus application supplies that raw Arc cannot: the Agroal DataSource. */
  @ApplicationScoped
  public static class Infrastructure {
    @Produces
    @Singleton
    public DataSource dataSource() {
      return mock(DataSource.class);
    }
  }

  /**
   * An application {@link EventStore}: the startup rule resolves the produced command bus, which
   * this lets the producers build without a database.
   */
  @ApplicationScoped
  public static class ApplicationEventStore {
    @Produces
    @Singleton
    public EventStore eventStore() {
      return mock(EventStore.class);
    }
  }

  /** {@code streamrune.security.trust-user-id-header} left at its default ({@code false}). */
  @ApplicationScoped
  public static class NoTrustFlag {
    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return bind(Map.of());
    }
  }

  /** {@code streamrune.security.trust-user-id-header=true}: the trusted-gateway mode. */
  @ApplicationScoped
  public static class TrustFlag {
    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return bind(Map.of(RequestIdentityPolicy.TRUST_USER_ID_HEADER_PROPERTY, "true"));
    }
  }

  /** Quarkus Security present; the request is authenticated as {@code bob}, role {@code ADMIN}. */
  @ApplicationScoped
  public static class AuthenticatedAsBob {
    @Produces
    @Singleton
    public SecurityIdentity securityIdentity() {
      SecurityIdentity identity = mock(SecurityIdentity.class);
      when(identity.isAnonymous()).thenReturn(false);
      when(identity.getPrincipal()).thenReturn(() -> "bob");
      when(identity.getRoles()).thenReturn(Set.of("ADMIN"));
      return identity;
    }
  }

  /** The trusted-gateway mode, with an application that (uselessly) allow-lists {@code role}. */
  @ApplicationScoped
  public static class TrustFlagAllowlistingRole {
    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return bind(
          Map.of(
              RequestIdentityPolicy.TRUST_USER_ID_HEADER_PROPERTY,
              "true",
              "streamrune.metadata.baggage-allowlist",
              "role"));
    }
  }

  /** An application {@link org.streamrune.integration.AuthenticatedUserResolver} bean. */
  @ApplicationScoped
  public static class ResolverA {
    @Produces
    @Singleton
    public org.streamrune.integration.AuthenticatedUserResolver resolverA() {
      return () -> Optional.of(UserId.of("a"));
    }
  }

  /** A second application resolver: together with {@link ResolverA} the choice is ambiguous. */
  @ApplicationScoped
  public static class ResolverB {
    @Produces
    @Singleton
    public org.streamrune.integration.AuthenticatedUserResolver resolverB() {
      return () -> Optional.of(UserId.of("b"));
    }
  }

  /** Quarkus Security present; the request is not authenticated. */
  @ApplicationScoped
  public static class Unauthenticated {
    @Produces
    @Singleton
    public SecurityIdentity securityIdentity() {
      SecurityIdentity identity = mock(SecurityIdentity.class);
      when(identity.isAnonymous()).thenReturn(true);
      return identity;
    }
  }

  /** The application's own bus, authorizing through an interceptor that is not a bean. */
  @ApplicationScoped
  public static class ApplicationAuthorizingBus {
    @Produces
    @Singleton
    public VirtualThreadCommandBus commandBus() {
      return VirtualThreadCommandBus.builder()
          .eventStore(mock(EventStore.class))
          .interceptors(new AuthorizationCommandInterceptor(CommandAuthorizationPolicy.allowAll()))
          .build();
    }
  }

  /** The application's own bus, with no interceptor at all. */
  @ApplicationScoped
  public static class ApplicationPlainBus {
    @Produces
    @Singleton
    public VirtualThreadCommandBus commandBus() {
      return VirtualThreadCommandBus.builder().eventStore(mock(EventStore.class)).build();
    }
  }

  @ApplicationScoped
  public static class PolicyConfig {
    @Produces
    @Singleton
    public CommandAuthorizationPolicy commandAuthorizationPolicy() {
      return CommandAuthorizationPolicy.allowAll();
    }
  }

  @ApplicationScoped
  public static class RoleResolverConfig {
    @Produces
    @Singleton
    public UserRoleResolver userRoleResolver() {
      return userId -> new UserAuthority(Set.of(), Set.of());
    }
  }
}
