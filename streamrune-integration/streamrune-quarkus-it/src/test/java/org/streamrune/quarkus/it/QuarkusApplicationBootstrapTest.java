package org.streamrune.quarkus.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.streamrune.runtime.MultiProjectionRunner;

/**
 * Starts a Quarkus application on streamrune-quarkus with Quarkus REST and an Agroal PostgreSQL
 * datasource, configured through {@code application.properties} and a system property, and uses it.
 *
 * <p>The application starts at all only when SmallRye accepts the projection keys the integration
 * reads by name ({@code streamrune.projections.<name>.enabled}, {@code
 * streamrune.projections.<name>.error-strategy}, {@code
 * streamrune.projections.auto-discovery.enabled}) from a file and a system property, and when the
 * StreamRune request filter, which Quarkus REST creates during static initialization, needs no
 * runtime configuration to be created.
 */
@QuarkusTest
@QuarkusTestResource(PostgresResource.class)
class QuarkusApplicationBootstrapTest {

  @Inject
  @Named("streamRuneMultiProjectionRunner")
  MultiProjectionRunner runner;

  @TestHTTPResource("/caller")
  URI caller;

  @Test
  void theProjectionKeysOfTheFileAndOfASystemPropertyApply() {
    Set<String> running =
        runner.status().keySet().stream().map(name -> name.value()).collect(Collectors.toSet());

    assertThat(running).containsExactlyInAnyOrder("orders", "order-totals");
  }

  @Test
  void theRequestFilterBuildsTheContextWithTheRuntimeIdentityMode() throws Exception {
    HttpResponse<String> response =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(caller)
                    .timeout(Duration.ofSeconds(30))
                    .header("X-Correlation-Id", "bootstrap-correlation")
                    .header("X-User-Id", "gateway-user")
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);
    // streamrune.security.trust-user-id-header=true, read from the runtime configuration: the
    // trusted-gateway mode takes the user from X-User-Id.
    assertThat(response.body()).isEqualTo("bootstrap-correlation\ngateway-user");
  }
}
