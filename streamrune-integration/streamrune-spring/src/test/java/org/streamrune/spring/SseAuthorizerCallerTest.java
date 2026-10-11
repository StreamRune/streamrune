package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.context.LifecycleAutoConfiguration;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.http.converter.autoconfigure.HttpMessageConvertersAutoConfiguration;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.tomcat.autoconfigure.servlet.TomcatServletWebServerAutoConfiguration;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.webmvc.autoconfigure.DispatcherServletAutoConfiguration;
import org.springframework.boot.webmvc.autoconfigure.WebMvcAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.SseAuthorizer;

/**
 * What the {@link SseAuthorizer} of the Server-Sent Events endpoint is handed: the request context
 * {@link ScopedValueFilter} bound for the request, with the caller and the roles the configured
 * {@link UserRoleResolver} resolved for that caller at the request edge.
 *
 * <p>Boots a real embedded Tomcat with the auto-configuration behind a trusted gateway ({@code
 * X-User-Id} is the caller), with a role resolver that knows one user with the role the authorizer
 * of the fixture asks for, and opens {@code /api/sse/order/o-1} over HTTP.
 */
class SseAuthorizerCallerTest {

  private static final String SUPPORT = "SUPPORT";

  /** One call of the fixture's authorizer. */
  record Asked(UserId userId, UserAuthority authority, boolean isTheBoundContext) {}

  @Test
  void aCallerWithTheRoleIsAdmittedAndTheAuthorizerSeesTheRoleInTheRequestContext()
      throws Exception {
    try (var application = SseApplicationUnderTest.start(RoleGatedApplication.class)) {
      assertThat(application.statusOfAStreamRequestOf("alice")).isEqualTo(200);

      assertThat(application.asked())
          .singleElement()
          .satisfies(
              asked -> {
                assertThat(asked.userId()).isEqualTo(UserId.of("alice"));
                assertThat(asked.authority()).isNotNull();
                assertThat(asked.authority().roles()).containsExactly(SUPPORT);
                assertThat(asked.isTheBoundContext())
                    .as("the authorizer is handed the context the filter bound")
                    .isTrue();
              });
    }
  }

  @Test
  void aCallerWithoutTheRoleIsRefusedAndTheAuthorizerSeesNoRole() throws Exception {
    try (var application = SseApplicationUnderTest.start(RoleGatedApplication.class)) {
      assertThat(application.statusOfAStreamRequestOf("bob")).isEqualTo(403);

      assertThat(application.asked())
          .singleElement()
          .satisfies(
              asked -> {
                assertThat(asked.userId()).isEqualTo(UserId.of("bob"));
                assertThat(asked.authority()).isEqualTo(UserAuthority.EMPTY);
              });
    }
  }

  @Test
  void anUnauthenticatedCallerIsRefusedAndTheAuthorizerSeesNoUserAndNoAuthority() throws Exception {
    try (var application = SseApplicationUnderTest.start(RoleGatedApplication.class)) {
      assertThat(application.statusOfAStreamRequestOf(null)).isEqualTo(403);

      assertThat(application.asked())
          .singleElement()
          .satisfies(
              asked -> {
                assertThat(asked.userId()).isNull();
                assertThat(asked.authority()).isNull();
              });
    }
  }

  @Test
  void aRequestTheFilterDoesNotCoverIsRefusedAndTheAuthorizerIsNotCalled() throws Exception {
    try (var application = SseApplicationUnderTest.start(FilterElsewhereApplication.class)) {
      assertThat(application.statusOfAStreamRequestOf("alice")).isEqualTo(403);

      assertThat(application.asked())
          .as("no request context is bound, so the authorizer is not asked")
          .isEmpty();
    }
  }

  /** A started application and an HTTP client for it. */
  private record SseApplicationUnderTest(ConfigurableApplicationContext context, HttpClient client)
      implements AutoCloseable {

    static SseApplicationUnderTest start(Class<?> application) {
      ConfigurableApplicationContext context =
          new SpringApplicationBuilder(application)
              .web(WebApplicationType.SERVLET)
              .registerShutdownHook(false)
              .properties(
                  "server.port=0",
                  "streamrune.sse.enabled=true",
                  "streamrune.security.trust-user-id-header=true")
              .run();
      return new SseApplicationUnderTest(
          context, HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build());
    }

    /** Opens the stream as {@code userId} ({@code null}: no {@code X-User-Id}) and closes it. */
    int statusOfAStreamRequestOf(String userId) throws Exception {
      int port = ((WebServerApplicationContext) context).getWebServer().getPort();
      HttpRequest.Builder request =
          HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/sse/order/o-1"))
              .header("Accept", "text/event-stream")
              .timeout(Duration.ofSeconds(10));
      if (userId != null) {
        request.header("X-User-Id", userId);
      }
      HttpResponse<InputStream> response =
          client.send(request.build(), HttpResponse.BodyHandlers.ofInputStream());
      response.body().close();
      return response.statusCode();
    }

    List<Asked> asked() {
      return context.getBean(RecordingRoleAuthorizer.class).asked;
    }

    @Override
    public void close() {
      client.shutdownNow();
      context.close();
    }
  }

  /** Admits only a caller whose captured authority has the support role; records every call. */
  static final class RecordingRoleAuthorizer implements SseAuthorizer {

    final List<Asked> asked = new CopyOnWriteArrayList<>();

    @Override
    public boolean isAuthorized(StreamRuneContext.RequestContext caller, StreamId streamId) {
      asked.add(
          new Asked(caller.userId(), caller.authority(), caller == StreamRuneContext.capture()));
      return caller.authority() != null && caller.authority().hasRole(SUPPORT);
    }
  }

  /**
   * Knows the roles of one user. It declares {@link #requiresRequestContext()}, as the resolver of
   * a security module does, so the request filter captures its answer at the request edge.
   */
  static final class OneSupportUser implements UserRoleResolver {

    @Override
    public UserAuthority resolve(UserId userId) {
      return "alice".equals(userId.value())
          ? new UserAuthority(Set.of(SUPPORT), Set.of())
          : UserAuthority.EMPTY;
    }

    @Override
    public boolean requiresRequestContext() {
      return true;
    }
  }

  /** The servlet web stack plus the StreamRune auto-configuration, on stub persistence. */
  @Configuration(proxyBeanMethods = false)
  @ImportAutoConfiguration({
    PropertyPlaceholderAutoConfiguration.class,
    LifecycleAutoConfiguration.class,
    TomcatServletWebServerAutoConfiguration.class,
    DispatcherServletAutoConfiguration.class,
    WebMvcAutoConfiguration.class,
    HttpMessageConvertersAutoConfiguration.class,
    JacksonAutoConfiguration.class,
    StreamRuneAutoConfiguration.class
  })
  static class RoleGatedApplication {

    @Bean
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Bean
    EventStoreFactory eventStoreFactory() {
      return SpringTestMocks::emptyEventStore;
    }

    @Bean
    UserRoleResolver userRoleResolver() {
      return new OneSupportUser();
    }

    @Bean
    RecordingRoleAuthorizer sseAuthorizer() {
      return new RecordingRoleAuthorizer();
    }
  }

  /**
   * The same application with the request filter registered for another path only: the request for
   * a stream passes no {@link ScopedValueFilter}.
   */
  @Configuration(proxyBeanMethods = false)
  static class FilterElsewhereApplication extends RoleGatedApplication {

    @Bean
    FilterRegistrationBean<ScopedValueFilter> scopedValueFilterRegistration(
        ScopedValueFilter filter) {
      var registration = new FilterRegistrationBean<>(filter);
      registration.addUrlPatterns("/api/commands/*");
      return registration;
    }
  }
}
