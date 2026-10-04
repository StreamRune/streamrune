package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micronaut.context.BeanProvider;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandAuthorizationPolicy;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.EventStore;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.runtime.AnnotationAuthorizationInterceptor;
import org.streamrune.runtime.AuthorizationCommandInterceptor;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Unit rows of the startup rule that a booted Micronaut application cannot reach: the
 * replaced-filter exemption, the interceptor filter, and the listener contract. The booted rows
 * (refusal, start, mode log) live in {@link MicronautRequestIdentityFailClosedTest}.
 */
class StreamRuneRequestIdentityValidatorTest {

  private static final RequestIdentityPolicy ANONYMOUS = RequestIdentityPolicy.of(null, false);

  @SuppressWarnings("unchecked")
  private static BeanProvider<StreamRuneContextFilter> filter(boolean present) {
    BeanProvider<StreamRuneContextFilter> provider = mock(BeanProvider.class);
    when(provider.isPresent()).thenReturn(present);
    return provider;
  }

  /** One command bus running {@code interceptors}, as the validator resolves it. */
  @SuppressWarnings("unchecked")
  private static BeanProvider<VirtualThreadCommandBus> busRunning(
      CommandInterceptor... interceptors) {
    BeanProvider<VirtualThreadCommandBus> provider = mock(BeanProvider.class);
    when(provider.iterator())
        .thenAnswer(
            invocation ->
                List.of(
                        VirtualThreadCommandBus.builder()
                            .eventStore(mock(EventStore.class))
                            .interceptors(interceptors)
                            .build())
                    .iterator());
    return provider;
  }

  private static CommandInterceptor policyInterceptor() {
    return new AuthorizationCommandInterceptor(CommandAuthorizationPolicy.allowAll());
  }

  @Test
  void anAnonymousApplicationWhoseRequestFilterWasReplacedIsNotChecked() {
    var validator =
        new StreamRuneRequestIdentityValidator(
            ANONYMOUS, busRunning(policyInterceptor()), filter(false));

    assertDoesNotThrow(() -> validator.onApplicationEvent(mock(StartupEvent.class)));
  }

  @Test
  void bothIdentityConsumingInterceptorsAreNamedAndOthersIgnored() {
    CommandInterceptor unrelated = CommandInterceptor.noop();
    var validator =
        new StreamRuneRequestIdentityValidator(
            ANONYMOUS,
            busRunning(
                unrelated,
                policyInterceptor(),
                new AnnotationAuthorizationInterceptor(userId -> UserAuthority.EMPTY)),
            filter(true));

    var refused =
        assertThrows(
            IllegalStateException.class,
            () -> validator.onApplicationEvent(mock(StartupEvent.class)));

    assertTrue(
        refused
            .getMessage()
            .contains("(AuthorizationCommandInterceptor, AnnotationAuthorizationInterceptor)"),
        refused.getMessage());
  }

  @Test
  void aResolverSatisfiesTheRuleWithAuthorizationConfigured() {
    var validator =
        new StreamRuneRequestIdentityValidator(
            RequestIdentityPolicy.of(() -> Optional.of(UserId.of("bob")), false),
            busRunning(policyInterceptor()),
            filter(true));

    assertDoesNotThrow(() -> validator.onApplicationEvent(mock(StartupEvent.class)));
  }

  /**
   * Startup-time validation runs on the context-level {@link StartupEvent}, which fires before the
   * HTTP server binds and before {@link StreamRuneLifecycle} starts any runner (order 1000 &lt;
   * 1100).
   */
  @Test
  void listensOnTheContextLevelStartupEventInTheValidatorTier() {
    ParameterizedType listener =
        (ParameterizedType)
            Arrays.stream(StreamRuneRequestIdentityValidator.class.getGenericInterfaces())
                .filter(
                    t ->
                        t instanceof ParameterizedType p
                            && p.getRawType() == ApplicationEventListener.class)
                .findFirst()
                .orElseThrow();
    assertEquals(StartupEvent.class, listener.getActualTypeArguments()[0]);
    assertEquals(1000, new StreamRuneRequestIdentityValidator(null, null, null).getOrder());
  }
}
