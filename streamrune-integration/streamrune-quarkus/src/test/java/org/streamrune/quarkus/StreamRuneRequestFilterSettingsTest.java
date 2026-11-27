package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.container.ContainerRequestContext;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.RequestIdentityPolicy;

/**
 * The filter Quarkus/Arc builds reads its settings — the baggage allowlist, the identity policy,
 * the role resolver — when the first request arrives, never while it is created: Quarkus REST
 * creates its filters during static initialization, before the runtime configuration exists.
 */
class StreamRuneRequestFilterSettingsTest {

  @SuppressWarnings("unchecked")
  private final Instance<StreamRuneQuarkusProperties> properties = mock(Instance.class);

  @SuppressWarnings("unchecked")
  private final Instance<RequestIdentityPolicy> policy = mock(Instance.class);

  @SuppressWarnings("unchecked")
  private final Instance<UserRoleResolver> roles = mock(Instance.class);

  private final StreamRuneRequestContextHolder holder = new StreamRuneRequestContextHolder();

  @Test
  void creatingTheFilterReadsNothing() {
    new StreamRuneRequestFilter(holder, properties, policy, roles);

    verifyNoInteractions(properties, policy, roles);
  }

  @Test
  void theFirstRequestReadsTheSettingsAndEveryLaterRequestKeepsThem() {
    when(properties.get())
        .thenReturn(TestProperties.of(Map.of("streamrune.metadata.baggage-allowlist", "tenant")));
    when(policy.get()).thenReturn(RequestIdentityPolicy.of(null, true));
    when(roles.isResolvable()).thenReturn(false);
    var filter = new StreamRuneRequestFilter(holder, properties, policy, roles);

    filter.filter(requestFrom("gateway-user"));
    assertEquals(UserId.of("gateway-user"), holder.context().userId());
    filter.filter(requestFrom("another-user"));
    assertEquals(UserId.of("another-user"), holder.context().userId());

    verify(properties, times(1)).get();
    verify(policy, times(1)).get();
    verify(roles, times(1)).isResolvable();
    verify(roles, times(0)).get();
  }

  @Test
  void aResolvableRoleResolverIsTheOneTheRequestEdgeUses() {
    UserRoleResolver resolver = mock(UserRoleResolver.class);
    when(properties.get()).thenReturn(TestProperties.defaults());
    when(policy.get()).thenReturn(RequestIdentityPolicy.of(null, true));
    when(roles.isResolvable()).thenReturn(true);
    when(roles.get()).thenReturn(resolver);
    var filter = new StreamRuneRequestFilter(holder, properties, policy, roles);

    filter.filter(requestFrom("gateway-user"));

    // The request has a user, so the resolver is asked whether it needs the request context.
    assertEquals(UserId.of("gateway-user"), holder.context().userId());
    verify(roles, times(1)).get();
    verify(resolver).requiresRequestContext();
  }

  @Test
  void aReadThatFailsIsNotKeptSoTheNextRequestReadsAgain() {
    RequestIdentityPolicy trusted = RequestIdentityPolicy.of(null, true);
    when(properties.get()).thenReturn(TestProperties.defaults());
    IllegalStateException failure = new IllegalStateException("not yet");
    when(policy.get()).thenThrow(failure).thenReturn(trusted);
    when(roles.isResolvable()).thenReturn(false);
    var filter = new StreamRuneRequestFilter(holder, properties, policy, roles);

    assertSame(failure, assertThrows(IllegalStateException.class, () -> filter.filter(request())));
    assertNull(holder.context());

    filter.filter(requestFrom("gateway-user"));
    assertEquals(UserId.of("gateway-user"), holder.context().userId());
    verify(policy, times(2)).get();
  }

  @Test
  void aNullRoleResolverInstanceCapturesNoAuthority() {
    when(properties.get()).thenReturn(TestProperties.defaults());
    when(policy.get()).thenReturn(RequestIdentityPolicy.of(null, true));
    var filter = new StreamRuneRequestFilter(holder, properties, policy, null);

    filter.filter(requestFrom("gateway-user"));

    assertEquals(UserId.of("gateway-user"), holder.context().userId());
    assertNull(holder.context().authority());
  }

  private static ContainerRequestContext request() {
    ContainerRequestContext request = mock(ContainerRequestContext.class);
    when(request.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader());
    return request;
  }

  private static ContainerRequestContext requestFrom(String userId) {
    ContainerRequestContext request = mock(ContainerRequestContext.class);
    when(request.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader(userId));
    return request;
  }
}
