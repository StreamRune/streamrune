package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.catalina.Context;
import org.apache.catalina.startup.Tomcat;
import org.apache.tomcat.util.descriptor.web.FilterDef;
import org.apache.tomcat.util.descriptor.web.FilterMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.integration.RequestIdentityPolicy;

/**
 * {@link ScopedValueFilter} in front of a servlet on an embedded Tomcat, over a socket: what a
 * client receives for a correlation id or a trace id the request edge refuses, and which value the
 * filter binds when a header is sent more than once.
 *
 * <p>The filter writes the answer itself — {@code 400 Bad Request}, {@code text/plain}, the one
 * fixed reason — and starts no error dispatch, so neither the container's error page nor an error
 * handler of the application takes part; the servlet behind the filter is not called.
 */
class ScopedValueFilterOverTomcatTest {

  private final AtomicInteger servletCalls = new AtomicInteger();
  private final AtomicInteger errorDispatches = new AtomicInteger();
  private final HttpClient client =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
  private Tomcat tomcat;
  private int port;

  /** Answers with the correlation id the filter bound; {@code /trace} with the trace id. */
  private final class EchoServlet extends HttpServlet {
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
      servletCalls.incrementAndGet();
      StreamRuneContext.RequestContext bound = StreamRuneContext.CURRENT.get();
      response.setContentType("text/plain");
      response
          .getWriter()
          .write(
              request.getRequestURI().endsWith("/trace")
                  ? bound.traceId().value()
                  : bound.correlationId().value());
    }
  }

  /** Counts the error dispatches of the container: a filter mapped to them alone. */
  private final class ErrorDispatchCounter implements jakarta.servlet.Filter {
    @Override
    public void doFilter(
        jakarta.servlet.ServletRequest request,
        jakarta.servlet.ServletResponse response,
        jakarta.servlet.FilterChain chain)
        throws IOException, jakarta.servlet.ServletException {
      errorDispatches.incrementAndGet();
      chain.doFilter(request, response);
    }
  }

  @BeforeEach
  void start(@TempDir Path baseDir) throws Exception {
    tomcat = new Tomcat();
    tomcat.setBaseDir(baseDir.toString());
    tomcat.setPort(0);
    Context context = tomcat.addContext("", baseDir.toString());
    FilterDef filter = new FilterDef();
    filter.setFilterName("streamrune");
    filter.setFilter(new ScopedValueFilter());
    context.addFilterDef(filter);
    FilterMap mapping = new FilterMap();
    mapping.setFilterName("streamrune");
    mapping.addURLPattern("/*");
    context.addFilterMap(mapping);
    FilterDef errors = new FilterDef();
    errors.setFilterName("error-dispatches");
    errors.setFilter(new ErrorDispatchCounter());
    context.addFilterDef(errors);
    FilterMap errorMapping = new FilterMap();
    errorMapping.setFilterName("error-dispatches");
    errorMapping.addURLPattern("/*");
    errorMapping.setDispatcher("ERROR");
    context.addFilterMap(errorMapping);
    Tomcat.addServlet(context, "echo", new EchoServlet());
    context.addServletMappingDecoded("/*", "echo");
    tomcat.getConnector();
    tomcat.start();
    port = tomcat.getConnector().getLocalPort();
  }

  @AfterEach
  void stop() throws Exception {
    client.shutdownNow();
    tomcat.stop();
    tomcat.destroy();
  }

  private HttpResponse<String> post(Map<String, String> headers) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/commands"))
            .timeout(Duration.ofSeconds(10))
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
    assertThat(servletCalls).as("the servlet is not called").hasValue(0);
    assertThat(errorDispatches).as("no error dispatch is started").hasValue(0);
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

  // ==== A header sent more than once ====

  private static final String RESERVED = "streamrune:saga:order-saga-42";

  @Test
  void ofARepeatedCorrelationIdHeader_theFirstValueIsTaken() throws Exception {
    RawHttp.Answer okFirst =
        RawHttp.post(
            port, "/commands", List.of("X-Correlation-Id: ok", "X-Correlation-Id: " + RESERVED));
    assertThat(okFirst.status()).isEqualTo(200);
    assertThat(okFirst.body()).isEqualTo("ok");

    RawHttp.Answer reservedFirst =
        RawHttp.post(
            port, "/commands", List.of("X-Correlation-Id: " + RESERVED, "X-Correlation-Id: ok"));
    assertThat(reservedFirst.status()).isEqualTo(400);
    assertThat(reservedFirst.body()).isEqualTo(RequestIdentityPolicy.REFUSED_REQUEST_CONTEXT);
    assertThat(reservedFirst.contentType()).isEqualTo("text/plain;charset=utf-8");
    assertThat(servletCalls).hasValue(1);
  }

  @Test
  void anEmptyFirstCorrelationIdHeader_isAnAbsentHeader_whateverFollows() throws Exception {
    RawHttp.Answer answer =
        RawHttp.post(
            port, "/commands", List.of("X-Correlation-Id:", "X-Correlation-Id: " + RESERVED));

    assertThat(answer.status()).isEqualTo(200);
    assertThat(answer.body()).as("a generated id").isNotBlank().doesNotContain("streamrune:");
  }

  @Test
  void aCommaJoinedCorrelationId_isOneValue() throws Exception {
    RawHttp.Answer okFirst =
        RawHttp.post(port, "/commands", List.of("X-Correlation-Id: ok," + RESERVED));
    assertThat(okFirst.status()).isEqualTo(200);
    assertThat(okFirst.body()).isEqualTo("ok," + RESERVED);

    assertThat(
            RawHttp.post(port, "/commands", List.of("X-Correlation-Id: " + RESERVED + ",ok"))
                .status())
        .isEqualTo(400);
  }

  @Test
  void ofARepeatedTraceIdHeader_theFirstValueIsTaken() throws Exception {
    String overlong = "a".repeat(256);
    RawHttp.Answer okFirst =
        RawHttp.post(port, "/trace", List.of("X-Trace-Id: trace-1", "X-Trace-Id: " + overlong));
    assertThat(okFirst.status()).isEqualTo(200);
    assertThat(okFirst.body()).isEqualTo("trace-1");

    assertThat(
            RawHttp.post(port, "/trace", List.of("X-Trace-Id: " + overlong, "X-Trace-Id: trace-1"))
                .status())
        .isEqualTo(400);
  }

  @Test
  void anOrdinaryCorrelationId_reachesTheServletAsSent() throws Exception {
    HttpResponse<String> response = post(Map.of("X-Correlation-Id", "upstream-request-7"));

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEqualTo("upstream-request-7");
    assertThat(servletCalls).hasValue(1);
  }
}
