package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import jakarta.servlet.FilterChain;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.streamrune.core.CommandAuthorizationPolicy;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.AuthorizationCommandInterceptor;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The HTTP identity fails CLOSED. Regression matrix for the Spring integration, driven through the
 * REAL auto-configured {@link ScopedValueFilter} (and its startup validation) rather than a
 * hand-built filter, so a wiring regression — the trust flag no longer reaching the filter, the
 * resolver no longer wired, the validator no longer registered — turns a row red.
 *
 * <ul>
 *   <li>no resolver, no trust flag, {@code X-User-Id} sent ⇒ ANONYMOUS (the header is ignored);
 *   <li>authenticated principal, mismatching {@code X-User-Id} ⇒ the principal;
 *   <li>trusted gateway ({@code streamrune.security.trust-user-id-header=true}) ⇒ the header;
 *   <li>command authorization configured, but neither a resolver nor the trust flag ⇒ startup is
 *       refused;
 *   <li>the {@code X-User-Role} header becomes baggage {@code role} only behind a trusted gateway:
 *       anonymous + header ⇒ no role; authenticated principal + header ⇒ no role (and not the
 *       resolver's roles either — those are the captured {@code authority}); trusted gateway +
 *       header ⇒ the header's role; a W3C {@code baggage: role=} entry never reaches the key.
 * </ul>
 *
 * <p>Previously the first row bound the header as the user — any client could impersonate an owner
 * and forge the audit attribution — and the refusal rows started cleanly. Previously every mode
 * copied a client's {@code X-User-Role} into baggage {@code role}, and from there into every
 * produced event's metadata.
 */
class RequestIdentityFailClosedTest {

  /** Hides Spring Security, so no {@code AuthenticatedUserResolver} is auto-configured. */
  private static final FilteredClassLoader NO_SPRING_SECURITY =
      new FilteredClassLoader(SecurityContextHolder.class);

  private final WebApplicationContextRunner web =
      new WebApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withUserConfiguration(StubInfrastructure.class);

  @AfterEach
  void clearSecurityContext() {
    SecurityContextHolder.clearContext();
  }

  @Test
  void withoutAResolverOrTheTrustFlagTheUserIdHeaderIsIgnoredAndTheRequestIsAnonymous() {
    web.withClassLoader(NO_SPRING_SECURITY)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(boundUserId(ctx, "alice")).isNull();
            });
  }

  @Test
  void anAuthenticatedPrincipalWinsOverAMismatchingUserIdHeader() {
    web.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          SecurityContextHolder.getContext()
              .setAuthentication(
                  UsernamePasswordAuthenticationToken.authenticated("bob", "n/a", List.of()));
          assertThat(boundUserId(ctx, "alice")).isEqualTo(UserId.of("bob"));
        });
  }

  @Test
  void anUnauthenticatedRequestWithAResolverIsAnonymousWhateverTheHeaderSays() {
    web.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          assertThat(boundUserId(ctx, "alice")).isNull();
        });
  }

  @Test
  void theTrustedGatewayModeTakesTheIdentityFromTheHeader() {
    web.withClassLoader(NO_SPRING_SECURITY)
        .withPropertyValues("streamrune.security.trust-user-id-header=true")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(boundUserId(ctx, "alice")).isEqualTo(UserId.of("alice"));
            });
  }

  @Test
  void aRepeatedUserIdHeaderIsAnonymousBehindATrustedGateway() {
    // A gateway that appends its X-User-Id after the client's instead of replacing it: the filter
    // must not pick either value (Spring's getHeader would take the client's first one).
    web.withClassLoader(NO_SPRING_SECURITY)
        .withPropertyValues("streamrune.security.trust-user-id-header=true")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(boundUserId(ctx, "victim", "real")).isNull();
            });
  }

  @Test
  void aRepeatedUserIdHeaderDoesNotDisplaceTheAuthenticatedPrincipal() {
    web.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          SecurityContextHolder.getContext()
              .setAuthentication(
                  UsernamePasswordAuthenticationToken.authenticated("bob", "n/a", List.of()));
          assertThat(boundUserId(ctx, "alice", "bob")).isEqualTo(UserId.of("bob"));
        });
  }

  @Test
  void aCommandAuthorizationPolicyWithoutAResolverOrTheTrustFlagRefusesStartup() {
    web.withClassLoader(NO_SPRING_SECURITY)
        .withUserConfiguration(PolicyConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessageContaining("AuthorizationCommandInterceptor")
                  .hasMessageContaining("AuthenticatedUserResolver")
                  .hasMessageContaining("streamrune.security.trust-user-id-header=true");
            });
  }

  @Test
  void annotationAuthorizationWithoutAResolverOrTheTrustFlagRefusesStartup() {
    // A UserRoleResolver of the application's own (no Spring Security) registers the
    // AnnotationAuthorizationInterceptor — which resolves authorities FOR RequestContext.userId.
    web.withClassLoader(NO_SPRING_SECURITY)
        .withUserConfiguration(RoleResolverConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessageContaining("AnnotationAuthorizationInterceptor");
            });
  }

  @Test
  void anAuthorizationInterceptorInsideAHandBuiltBusRefusesStartupWithoutAnIdentitySource() {
    // The application's own bus carries the interceptor inline — it is not a bean. The rule reads
    // the chain the bus runs, so the bus is refused exactly like the auto-configured one.
    web.withClassLoader(NO_SPRING_SECURITY)
        .withUserConfiguration(HandBuiltAuthorizingBusConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessageContaining("(AuthorizationCommandInterceptor)")
                  .hasMessageContaining("AuthenticatedUserResolver");
            });
  }

  @Test
  void anAuthorizationInterceptorBeanLeftOutOfAHandBuiltBusDoesNotRefuseStartup() {
    // A CommandAuthorizationPolicy bean registers an AuthorizationCommandInterceptor bean, but the
    // application's own bus does not run it: no command consults the request identity.
    web.withClassLoader(NO_SPRING_SECURITY)
        .withUserConfiguration(PolicyConfig.class, HandBuiltPlainBusConfig.class)
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }

  @Test
  void commandAuthorizationStartsBehindATrustedGateway() {
    web.withClassLoader(NO_SPRING_SECURITY)
        .withUserConfiguration(PolicyConfig.class, RoleResolverConfig.class)
        .withPropertyValues("streamrune.security.trust-user-id-header=true")
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }

  @Test
  void commandAuthorizationStartsWithAnAuthenticatedUserResolver() {
    // Spring Security on the classpath auto-configures SpringSecurityAuthenticatedUserResolver.
    web.withUserConfiguration(PolicyConfig.class).run(ctx -> assertThat(ctx).hasNotFailed());
  }

  @Test
  void anAnonymousApplicationWithoutCommandAuthorizationStarts() {
    web.withClassLoader(NO_SPRING_SECURITY).run(ctx -> assertThat(ctx).hasNotFailed());
  }

  // ---- X-User-Role is read only behind a trusted gateway --------------------------------

  @Test
  void withoutAResolverOrTheTrustFlagTheRoleHeaderIsIgnored() {
    web.withClassLoader(NO_SPRING_SECURITY)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var bound = boundContext(ctx, List.of("alice"), List.of("ADMIN"), Map.of());
              assertThat(bound.userId()).isNull();
              assertThat(bound.baggage()).doesNotContainKey("role");
            });
  }

  @Test
  void anAuthenticatedPrincipalNeverTakesTheRoleHeaderNorLendsItsRolesToTheBaggage() {
    // Spring Security on the classpath: the principal is the identity and the shipped
    // SpringSecurityUserRoleResolver is wired. Its verified roles are captured into the context's
    // authority; baggage `role` is neither the client's header nor the resolver's answer.
    web.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          SecurityContextHolder.getContext()
              .setAuthentication(
                  UsernamePasswordAuthenticationToken.authenticated(
                      "bob", "n/a", List.of(new SimpleGrantedAuthority("ROLE_ADMIN"))));
          var bound = boundContext(ctx, List.of("alice"), List.of("CUSTOMER"), Map.of());
          assertThat(bound.userId()).isEqualTo(UserId.of("bob"));
          assertThat(bound.baggage()).doesNotContainKey("role");
          assertThat(bound.authority()).isNotNull();
          assertThat(bound.authority().roles()).containsExactly("ADMIN");
        });
  }

  @Test
  void anUnauthenticatedRequestWithAResolverIgnoresTheRoleHeader() {
    web.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          var bound = boundContext(ctx, List.of("alice"), List.of("ADMIN"), Map.of());
          assertThat(bound.userId()).isNull();
          assertThat(bound.baggage()).doesNotContainKey("role");
        });
  }

  @Test
  void theTrustedGatewayModeTakesTheRoleFromTheHeader() {
    web.withClassLoader(NO_SPRING_SECURITY)
        .withPropertyValues("streamrune.security.trust-user-id-header=true")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var bound = boundContext(ctx, List.of("alice"), List.of("ADMIN"), Map.of());
              assertThat(bound.userId()).isEqualTo(UserId.of("alice"));
              assertThat(bound.baggage()).containsEntry("role", "ADMIN");
            });
  }

  @Test
  void aRepeatedRoleHeaderIsNoRoleBehindATrustedGateway() {
    // A gateway that appends its X-User-Role after the client's instead of replacing it: neither
    // value is the role (Spring's getHeader would take the client's first one).
    web.withClassLoader(NO_SPRING_SECURITY)
        .withPropertyValues("streamrune.security.trust-user-id-header=true")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var bound =
                  boundContext(ctx, List.of("alice"), List.of("ADMIN", "CUSTOMER"), Map.of());
              assertThat(bound.baggage()).doesNotContainKey("role");
            });
  }

  @Test
  void aW3cBaggageRoleEntryNeverReachesTheRoleKeyInAnyMode() {
    Map<String, String> smuggled = Map.of("role", "ADMIN");
    web.withClassLoader(NO_SPRING_SECURITY)
        .run(
            ctx ->
                assertThat(boundContext(ctx, List.of(), List.of(), smuggled).baggage())
                    .doesNotContainKey("role"));
    web.withClassLoader(NO_SPRING_SECURITY)
        .withPropertyValues(
            "streamrune.security.trust-user-id-header=true",
            // `role` is reserved for the role rule: allow-listing it changes nothing.
            "streamrune.metadata.baggage-allowlist=role")
        .run(
            ctx -> {
              assertThat(boundContext(ctx, List.of("alice"), List.of(), smuggled).baggage())
                  .doesNotContainKey("role");
              assertThat(
                      boundContext(ctx, List.of("alice"), List.of("CUSTOMER"), smuggled).baggage())
                  .containsEntry("role", "CUSTOMER");
            });
  }

  // ---- the SSE endpoint resolves the caller exactly like the request filter -------------------

  @Test
  void theSseEndpointIgnoresTheHeaderWithoutAResolverOrTheTrustFlag() {
    web.withClassLoader(NO_SPRING_SECURITY)
        .withUserConfiguration(CapturingSseAuthorizerConfig.class)
        .withPropertyValues("streamrune.sse.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(sseCaller(ctx, "alice")).isEmpty();
            });
  }

  @Test
  void theSseEndpointTakesTheHeaderBehindATrustedGateway() {
    web.withClassLoader(NO_SPRING_SECURITY)
        .withUserConfiguration(CapturingSseAuthorizerConfig.class)
        .withPropertyValues(
            "streamrune.sse.enabled=true", "streamrune.security.trust-user-id-header=true")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(sseCaller(ctx, "alice")).contains(UserId.of("alice"));
            });
  }

  @Test
  void theSseEndpointAuthorizesTheAuthenticatedPrincipalOverAMismatchingHeader() {
    web.withUserConfiguration(CapturingSseAuthorizerConfig.class)
        .withPropertyValues("streamrune.sse.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              SecurityContextHolder.getContext()
                  .setAuthentication(
                      UsernamePasswordAuthenticationToken.authenticated("bob", "n/a", List.of()));
              assertThat(sseCaller(ctx, "alice")).contains(UserId.of("bob"));
            });
  }

  @Test
  void theSseEndpointTreatsARepeatedUserIdHeaderAsAnonymousBehindATrustedGateway() {
    // The filter and the SSE endpoint must read a repeated header the same way: before, the
    // endpoint's @RequestHeader String bound the comma-joined "victim,real" as the caller.
    web.withClassLoader(NO_SPRING_SECURITY)
        .withUserConfiguration(CapturingSseAuthorizerConfig.class)
        .withPropertyValues(
            "streamrune.sse.enabled=true", "streamrune.security.trust-user-id-header=true")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(sseCaller(ctx, "victim", "real")).isEmpty();
            });
  }

  @Test
  void theSseEndpointTakesASingleCommaBearingHeaderValueVerbatimBehindATrustedGateway() {
    // One header value that happens to contain a comma is one value, exactly as the filter reads
    // it — it is not split into two (which a List-typed @RequestHeader would do).
    web.withClassLoader(NO_SPRING_SECURITY)
        .withUserConfiguration(CapturingSseAuthorizerConfig.class)
        .withPropertyValues(
            "streamrune.sse.enabled=true", "streamrune.security.trust-user-id-header=true")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(sseCaller(ctx, "a,b")).contains(UserId.of("a,b"));
              assertThat(boundUserId(ctx, "a,b")).isEqualTo(UserId.of("a,b"));
            });
  }

  /**
   * Sends {@code GET /api/sse/order/order-1} with one {@code X-User-Id} line per element of {@code
   * headers} through the auto-configured filter and SSE controller (the capturing authorizer
   * denies, so no subscription is opened) and returns the caller the {@link SseAuthorizer} was
   * asked about.
   */
  private static Optional<UserId> sseCaller(ApplicationContext ctx, String... headers)
      throws Exception {
    var authorizer = ctx.getBean(CapturingSseAuthorizer.class);
    MockMvc mvc =
        MockMvcBuilders.standaloneSetup(ctx.getBean(SseController.class))
            .addFilters(ctx.getBean(ScopedValueFilter.class))
            .build();
    mvc.perform(get("/api/sse/order/order-1").header("X-User-Id", (Object[]) headers))
        .andExpect(status().isForbidden());
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

  @Configuration(proxyBeanMethods = false)
  static class CapturingSseAuthorizerConfig {
    @Bean
    CapturingSseAuthorizer capturingSseAuthorizer() {
      return new CapturingSseAuthorizer();
    }
  }

  /**
   * Sends one request carrying one {@code X-User-Id} line per element of {@code headers} through
   * the auto-configured filter and returns the {@code RequestContext.userId} the chain observed.
   */
  private static UserId boundUserId(ApplicationContext ctx, String... headers) throws Exception {
    return boundContext(ctx, List.of(headers), List.of(), Map.of()).userId();
  }

  /**
   * Sends one request through the auto-configured filter carrying one {@code X-User-Id} line per
   * element of {@code userIds} and one {@code X-User-Role} line per element of {@code roles}, with
   * {@code w3cBaggage} as the current OpenTelemetry baggage (what a client's W3C {@code baggage:}
   * header becomes), and returns the {@code RequestContext} the chain observed.
   */
  private static StreamRuneContext.RequestContext boundContext(
      ApplicationContext ctx,
      List<String> userIds,
      List<String> roles,
      Map<String, String> w3cBaggage)
      throws Exception {
    var filter = ctx.getBean(ScopedValueFilter.class);
    var request = new MockHttpServletRequest("POST", "/orders");
    userIds.forEach(value -> request.addHeader("X-User-Id", value));
    roles.forEach(value -> request.addHeader("X-User-Role", value));
    var observed = new AtomicReference<StreamRuneContext.RequestContext>();
    FilterChain chain = (req, res) -> observed.set(StreamRuneContext.CURRENT.get());
    var baggage = io.opentelemetry.api.baggage.Baggage.builder();
    w3cBaggage.forEach(baggage::put);
    try (var _ = io.opentelemetry.context.Context.current().with(baggage.build()).makeCurrent()) {
      filter.doFilter(request, new MockHttpServletResponse(), chain);
    }
    assertThat(observed.get()).as("the filter must bind a request context").isNotNull();
    return observed.get();
  }

  @Configuration(proxyBeanMethods = false)
  static class StubInfrastructure {
    @Bean
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Bean
    EventStoreFactory eventStoreFactory() {
      return () -> mock(EventStore.class);
    }
  }

  /** The application's own bus, authorizing through an interceptor that is not a bean. */
  @Configuration(proxyBeanMethods = false)
  static class HandBuiltAuthorizingBusConfig {
    @Bean
    VirtualThreadCommandBus commandBus() {
      return VirtualThreadCommandBus.builder()
          .eventStore(mock(EventStore.class))
          .interceptors(new AuthorizationCommandInterceptor(CommandAuthorizationPolicy.allowAll()))
          .build();
    }
  }

  /** The application's own bus, with no interceptor at all. */
  @Configuration(proxyBeanMethods = false)
  static class HandBuiltPlainBusConfig {
    @Bean
    VirtualThreadCommandBus commandBus() {
      return VirtualThreadCommandBus.builder().eventStore(mock(EventStore.class)).build();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class PolicyConfig {
    @Bean
    CommandAuthorizationPolicy commandAuthorizationPolicy() {
      return CommandAuthorizationPolicy.allowAll();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class RoleResolverConfig {
    @Bean
    UserRoleResolver userRoleResolver() {
      return userId -> new UserAuthority(Set.of(), Set.of());
    }
  }
}
