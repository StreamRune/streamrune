package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.Filter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
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
import org.springframework.boot.webmvc.autoconfigure.error.ErrorMvcAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.test.InMemoryEventStore;

/**
 * The auto-configured {@link ScopedValueFilter} in a Spring Boot servlet application with Boot's
 * error handling and a Spring Security filter chain, over a socket.
 *
 * <p>The security chain is registered as Spring Boot registers it — ahead of the request filter,
 * for request, async and error dispatches — and its rules admit {@code /commands} and deny every
 * other path, {@code /error} included. An answer that went through an error dispatch would
 * therefore come back as the security chain's refusal, not as {@code 400}. The filter writes its
 * answer to a refused request itself: {@code 400 Bad Request}, {@code text/plain}, the one fixed
 * reason. No error dispatch is started, Boot's error controller is not asked, and the controller
 * behind the filter is not called.
 */
class ScopedValueFilterSpringBootTest {

  private static final AtomicInteger CONTROLLER_CALLS = new AtomicInteger();
  private static final AtomicInteger ERROR_DISPATCHES = new AtomicInteger();

  private final HttpClient client =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
  private ConfigurableApplicationContext context;
  private int port;

  /** Answers with the correlation id the filter bound. */
  @RestController
  static class EchoController {
    @PostMapping("/commands")
    String correlationId() {
      CONTROLLER_CALLS.incrementAndGet();
      return StreamRuneContext.CURRENT.get().correlationId().value();
    }
  }

  /**
   * The servlet web stack with Boot's error handling, a security chain that denies {@code /error},
   * and the StreamRune auto-configuration.
   */
  @Configuration(proxyBeanMethods = false)
  @EnableWebSecurity
  @ImportAutoConfiguration({
    PropertyPlaceholderAutoConfiguration.class,
    LifecycleAutoConfiguration.class,
    TomcatServletWebServerAutoConfiguration.class,
    DispatcherServletAutoConfiguration.class,
    WebMvcAutoConfiguration.class,
    ErrorMvcAutoConfiguration.class,
    HttpMessageConvertersAutoConfiguration.class,
    JacksonAutoConfiguration.class,
    StreamRuneAutoConfiguration.class
  })
  static class Application {

    @Bean
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Bean
    AuditStore auditStore() {
      return mock(AuditStore.class);
    }

    @Bean
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Bean
    EchoController echoController() {
      return new EchoController();
    }

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
      return http.csrf(csrf -> csrf.disable())
          .authorizeHttpRequests(
              rules -> rules.requestMatchers("/commands").permitAll().anyRequest().denyAll())
          .build();
    }

    /**
     * The security chain where Spring Boot's security auto-configuration puts it: order {@code
     * -100}, ahead of the request filter, for request, async and error dispatches.
     */
    @Bean
    FilterRegistrationBean<Filter> securityFilterChainRegistration(
        @Qualifier("springSecurityFilterChain") Filter springSecurityFilterChain) {
      var registration = new FilterRegistrationBean<>(springSecurityFilterChain);
      registration.setOrder(-100);
      registration.setDispatcherTypes(
          DispatcherType.REQUEST, DispatcherType.ASYNC, DispatcherType.ERROR);
      return registration;
    }

    /** Counts the error dispatches of the container, ahead of every other filter. */
    @Bean
    FilterRegistrationBean<Filter> errorDispatchCounter() {
      Filter counter =
          (request, response, chain) -> {
            ERROR_DISPATCHES.incrementAndGet();
            chain.doFilter(request, response);
          };
      var registration = new FilterRegistrationBean<>(counter);
      registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
      registration.setDispatcherTypes(DispatcherType.ERROR);
      return registration;
    }
  }

  @BeforeEach
  void start() {
    CONTROLLER_CALLS.set(0);
    ERROR_DISPATCHES.set(0);
    context =
        new SpringApplicationBuilder(Application.class)
            .web(WebApplicationType.SERVLET)
            .registerShutdownHook(false)
            .properties("server.port=0")
            .run();
    port = ((WebServerApplicationContext) context).getWebServer().getPort();
  }

  @AfterEach
  void stop() {
    client.shutdownNow();
    context.close();
  }

  private HttpResponse<String> post(Map<String, String> headers) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/commands"))
            .timeout(Duration.ofSeconds(10))
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.noBody());
    headers.forEach(request::header);
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  private void assertRefused(String header, String value) throws Exception {
    HttpResponse<String> response = post(Map.of(header, value));

    assertThat(response.statusCode()).as("status for a refused %s", header).isEqualTo(400);
    assertThat(response.body())
        .as("the one fixed reason, and nothing of the refused value")
        .isEqualTo(RequestIdentityPolicy.REFUSED_REQUEST_CONTEXT);
    assertThat(response.headers().firstValue("Content-Type").orElse("").replace(" ", ""))
        .isEqualToIgnoringCase("text/plain;charset=UTF-8");
    assertThat(CONTROLLER_CALLS).as("the controller is not called").hasValue(0);
    assertThat(ERROR_DISPATCHES).as("no error dispatch is started").hasValue(0);
  }

  @Test
  void theSecurityChainDeniesEveryOtherPath_theErrorPathIncluded() throws Exception {
    // What an answer that relied on an error dispatch would meet.
    for (String path : new String[] {"/error", "/anything-else"}) {
      HttpResponse<String> response =
          client.send(
              HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                  .timeout(Duration.ofSeconds(10))
                  .POST(HttpRequest.BodyPublishers.noBody())
                  .build(),
              HttpResponse.BodyHandlers.ofString());

      assertThat(response.statusCode()).as("status of %s", path).isIn(401, 403);
    }
    assertThat(CONTROLLER_CALLS).hasValue(0);
  }

  @Test
  void a256CharacterCorrelationId_isAnswered400() throws Exception {
    assertRefused("X-Correlation-Id", "a".repeat(256));
  }

  @Test
  void aCorrelationIdWithTheReservedPrefix_isAnswered400() throws Exception {
    assertRefused("X-Correlation-Id", "streamrune:saga:order-saga-42");
  }

  @Test
  void aControlCharacterInTheTraceId_isAnswered400() throws Exception {
    // A horizontal tab is the control character an HTTP header value may carry.
    assertRefused("X-Trace-Id", "trace\tid");
  }

  @Test
  void anOrdinaryCorrelationId_reachesTheControllerAsSent() throws Exception {
    HttpResponse<String> response = post(Map.of("X-Correlation-Id", "upstream-request-7"));

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEqualTo("upstream-request-7");
    assertThat(CONTROLLER_CALLS).hasValue(1);
  }
}
