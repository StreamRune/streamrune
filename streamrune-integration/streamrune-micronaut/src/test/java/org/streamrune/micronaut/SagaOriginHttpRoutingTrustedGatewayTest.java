package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.streamrune.integration.RequestIdentityPolicy;

/**
 * {@link SagaOriginHttpRoutingTest}, every case of it, in the trusted-gateway identity mode: what
 * the request edge refuses and what reaches a saga do not depend on where the user comes from.
 */
@MicronautTest
@Property(name = "spec.name", value = "SagaOriginHttpRoutingTest")
@Property(name = "micronaut.security.enabled", value = "false")
@Property(name = "streamrune.security.trust-user-id-header", value = "true")
class SagaOriginHttpRoutingTrustedGatewayTest extends SagaOriginHttpRoutingTest {

  @Test
  void theApplicationRunsInTheTrustedGatewayMode() {
    assertThat(identityPolicy.mode()).isEqualTo(RequestIdentityPolicy.Mode.TRUSTED_GATEWAY);
  }
}
