package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Singleton;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;

/**
 * {@link SagaOriginHttpRoutingTest}, every case of it, in the authenticated-principal identity
 * mode: what the request edge refuses and what reaches a saga do not depend on where the user comes
 * from.
 */
@MicronautTest
@Property(name = "spec.name", value = "SagaOriginHttpRoutingTest")
@Property(name = "micronaut.security.enabled", value = "false")
@Property(name = "test.identity", value = "authenticated-principal")
class SagaOriginHttpRoutingAuthenticatedPrincipalTest extends SagaOriginHttpRoutingTest {

  /** The application's own resolver: every request is the authenticated user {@code mallory}. */
  @Factory
  @Requires(property = "test.identity", value = "authenticated-principal")
  static class Principal {
    @Singleton
    AuthenticatedUserResolver authenticatedUserResolver() {
      return () -> Optional.of(UserId.of("mallory"));
    }
  }

  @Test
  void theApplicationRunsInTheAuthenticatedPrincipalMode() {
    assertThat(identityPolicy.mode()).isEqualTo(RequestIdentityPolicy.Mode.AUTHENTICATED_PRINCIPAL);
  }
}
