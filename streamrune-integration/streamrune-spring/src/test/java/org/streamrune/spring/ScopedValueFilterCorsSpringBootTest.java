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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
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
import org.springframework.context.annotation.Import;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.test.InMemoryEventStore;

/**
 * Whether a browser of another origin can read the request filter's {@code 400}: the CORS response
 * header on the refusal, for each of the usual ways an application configures CORS, over a socket.
 *
 * <p>The request filter sits at order {@code -90} and answers a refused request itself, so only
 * what ran before it has touched the response. CORS that runs ahead of it — Spring Security's
 * {@code cors()}, whose chain sits at {@code -100}, or a {@code CorsFilter} registered with a lower
 * order value — has written {@code Access-Control-Allow-Origin} by then, and the refusal carries
 * it. CORS that runs behind it — Spring MVC's own ({@code addCorsMappings}, {@code @CrossOrigin}),
 * which is applied inside the {@code DispatcherServlet}, and a {@code CorsFilter} bean at its
 * default order — never sees the refused request: the {@code 400} carries no CORS header, and a
 * browser reports a network error to the page instead of the status.
 *
 * <p>A preflight carries no {@code X-Correlation-Id} value and is not refused; the cases here are
 * the actual request.
 */
class ScopedValueFilterCorsSpringBootTest {

  private static final String PAGE_ORIGIN = "https://app.example";
  private static final String ALLOW_ORIGIN = "Access-Control-Allow-Origin";

  private final HttpClient client =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
  private ConfigurableApplicationContext context;
  private int port;

  @RestController
  static class EchoController {
    @PostMapping("/commands")
    String correlationId() {
      return StreamRuneContext.CURRENT.get().correlationId().value();
    }
  }

  /** The servlet web stack and the StreamRune auto-configuration, without any CORS. */
  @Configuration(proxyBeanMethods = false)
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
  }

  private static CorsConfigurationSource allowingThePage() {
    CorsConfiguration cors = new CorsConfiguration();
    cors.setAllowedOrigins(List.of(PAGE_ORIGIN));
    cors.setAllowedMethods(List.of("POST"));
    cors.setAllowedHeaders(List.of("*"));
    UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    source.registerCorsConfiguration("/**", cors);
    return source;
  }

  /** CORS in Spring MVC: applied by the handler mapping, inside the dispatcher servlet. */
  @Configuration(proxyBeanMethods = false)
  @Import(Application.class)
  static class CorsInSpringMvc implements WebMvcConfigurer {
    @Override
    public void addCorsMappings(CorsRegistry registry) {
      registry.addMapping("/**").allowedOrigins(PAGE_ORIGIN).allowedMethods("POST");
    }
  }

  /** A {@code CorsFilter} bean with no order of its own: Spring Boot registers it last. */
  @Configuration(proxyBeanMethods = false)
  @Import(Application.class)
  static class CorsFilterBeanAtItsDefaultOrder {
    @Bean
    CorsFilter corsFilter() {
      return new CorsFilter(allowingThePage());
    }
  }

  /** A {@code CorsFilter} registered ahead of the request filter. */
  @Configuration(proxyBeanMethods = false)
  @Import(Application.class)
  static class CorsFilterAheadOfTheRequestFilter {
    @Bean
    FilterRegistrationBean<CorsFilter> corsFilterRegistration() {
      var registration = new FilterRegistrationBean<>(new CorsFilter(allowingThePage()));
      registration.setOrder(StreamRuneAutoConfiguration.SCOPED_VALUE_FILTER_ORDER - 1);
      return registration;
    }
  }

  /** Spring Security's {@code cors()}, the chain where Spring Boot puts it: order {@code -100}. */
  @Configuration(proxyBeanMethods = false)
  @EnableWebSecurity
  @Import(Application.class)
  static class CorsInSpringSecurity {
    @Bean
    CorsConfigurationSource corsConfigurationSource() {
      return allowingThePage();
    }

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
      return http.csrf(csrf -> csrf.disable())
          .cors(Customizer.withDefaults())
          .authorizeHttpRequests(rules -> rules.anyRequest().permitAll())
          .build();
    }

    @Bean
    FilterRegistrationBean<Filter> securityFilterChainRegistration(
        @Qualifier("springSecurityFilterChain") Filter springSecurityFilterChain) {
      var registration = new FilterRegistrationBean<>(springSecurityFilterChain);
      registration.setOrder(-100);
      registration.setDispatcherTypes(
          DispatcherType.REQUEST, DispatcherType.ASYNC, DispatcherType.ERROR);
      return registration;
    }
  }

  private void start(Class<?> application) {
    context =
        new SpringApplicationBuilder(application)
            .web(WebApplicationType.SERVLET)
            .registerShutdownHook(false)
            .properties("server.port=0")
            .run();
    port = ((WebServerApplicationContext) context).getWebServer().getPort();
  }

  @AfterEach
  void stop() {
    client.shutdownNow();
    if (context != null) {
      context.close();
    }
  }

  /** The actual cross-origin request of a page, after its preflight. */
  private HttpResponse<String> postFromThePage(Map<String, String> headers) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/commands"))
            .timeout(Duration.ofSeconds(10))
            .header("Origin", PAGE_ORIGIN)
            .POST(HttpRequest.BodyPublishers.noBody());
    headers.forEach(request::header);
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  /** The CORS header of the refusal, after showing that an accepted request carries it. */
  private Optional<String> allowOriginOfTheRefusal(Class<?> application) throws Exception {
    start(application);

    HttpResponse<String> accepted = postFromThePage(Map.of("X-Correlation-Id", "page-request-7"));
    assertThat(accepted.statusCode()).isEqualTo(200);
    assertThat(accepted.body()).isEqualTo("page-request-7");
    assertThat(accepted.headers().firstValue(ALLOW_ORIGIN))
        .as("CORS is configured: an accepted request is readable by the page")
        .contains(PAGE_ORIGIN);

    HttpResponse<String> refused =
        postFromThePage(Map.of("X-Correlation-Id", "streamrune:saga:order-saga-42"));
    assertThat(refused.statusCode()).isEqualTo(400);
    assertThat(refused.body()).isEqualTo(RequestIdentityPolicy.REFUSED_REQUEST_CONTEXT);
    return refused.headers().firstValue(ALLOW_ORIGIN);
  }

  @Test
  void corsInSpringMvc_runsBehindTheFilter_theRefusalCarriesNoCorsHeader() throws Exception {
    assertThat(allowOriginOfTheRefusal(CorsInSpringMvc.class)).isEmpty();
  }

  @Test
  void aCorsFilterBeanAtItsDefaultOrder_runsBehindTheFilter_theRefusalCarriesNoCorsHeader()
      throws Exception {
    assertThat(allowOriginOfTheRefusal(CorsFilterBeanAtItsDefaultOrder.class)).isEmpty();
  }

  @Test
  void aCorsFilterRegisteredAheadOfTheFilter_theRefusalIsReadableByThePage() throws Exception {
    assertThat(allowOriginOfTheRefusal(CorsFilterAheadOfTheRequestFilter.class))
        .contains(PAGE_ORIGIN);
  }

  @Test
  void corsInSpringSecurity_runsAheadOfTheFilter_theRefusalIsReadableByThePage() throws Exception {
    assertThat(allowOriginOfTheRefusal(CorsInSpringSecurity.class)).contains(PAGE_ORIGIN);
  }
}
