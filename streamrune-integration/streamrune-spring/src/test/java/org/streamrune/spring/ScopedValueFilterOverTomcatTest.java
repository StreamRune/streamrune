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
 * {@link ScopedValueFilter} in front of a servlet on an embedded Tomcat, over a socket: the status
 * a client receives for a correlation id or a trace id the request edge refuses. The filter answers
 * {@code 400 Bad Request} through {@code sendError}, so the body is the container's error page; the
 * servlet behind the filter is not called.
 */
class ScopedValueFilterOverTomcatTest {

  private final AtomicInteger servletCalls = new AtomicInteger();
  private final HttpClient client =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
  private Tomcat tomcat;
  private int port;

  /** Answers with the correlation id the filter bound. */
  private final class EchoServlet extends HttpServlet {
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
      servletCalls.incrementAndGet();
      response.setContentType("text/plain");
      response.getWriter().write(StreamRuneContext.CURRENT.get().correlationId().value());
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
    assertThat(response.body()).as("the refused value is not echoed").doesNotContain(value);
    assertThat(servletCalls).as("the servlet is not called").hasValue(0);
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
  void theReasonIsTheContainersErrorMessage() throws Exception {
    HttpResponse<String> response = post(Map.of("X-Correlation-Id", "a".repeat(256)));

    assertThat(response.statusCode()).isEqualTo(400);
    // Tomcat's error page prints the sendError message, HTML-escaped.
    assertThat(response.body())
        .contains(RequestIdentityPolicy.REFUSED_REQUEST_CONTEXT.replace("'", "&#39;"));
  }

  @Test
  void anOrdinaryCorrelationId_reachesTheServletAsSent() throws Exception {
    HttpResponse<String> response = post(Map.of("X-Correlation-Id", "upstream-request-7"));

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEqualTo("upstream-request-7");
    assertThat(servletCalls).hasValue(1);
  }
}
