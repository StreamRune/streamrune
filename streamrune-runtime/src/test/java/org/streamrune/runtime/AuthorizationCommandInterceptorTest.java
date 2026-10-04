package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.Command;
import org.streamrune.core.CommandAuthorizationPolicy;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

class AuthorizationCommandInterceptorTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  record TestCommand() implements Command {}

  private static final CommandContext CTX =
      new CommandContext(
          new TestCommand(),
          "TestCommand",
          CommandId.of("test-id"),
          TYPE,
          AggregateId.of("agg-1"),
          null,
          Instant.now());

  @Test
  void allows_command_when_policy_permits() {
    var interceptor = new AuthorizationCommandInterceptor(CommandAuthorizationPolicy.allowAll());
    assertTrue(interceptor.before(CTX));
  }

  @Test
  void throws_AuthorizationException_when_policy_denies() {
    var interceptor =
        new AuthorizationCommandInterceptor(CommandAuthorizationPolicy.denyAll("no access"));
    assertThrows(AuthorizationException.class, () -> interceptor.before(CTX));
  }

  @Test
  void passes_userId_from_scoped_context_to_policy() {
    UserId expectedUser = UserId.of("user-42");
    CommandAuthorizationPolicy policy = mock(CommandAuthorizationPolicy.class);
    var interceptor = new AuthorizationCommandInterceptor(policy);

    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, expectedUser, CorrelationId.of("corr-1"), Instant.now(), null))
        .run(() -> interceptor.before(CTX));

    verify(policy).authorize(expectedUser, CTX.command());
  }

  @Test
  void passes_null_userId_when_scoped_value_not_bound() {
    CommandAuthorizationPolicy policy = mock(CommandAuthorizationPolicy.class);
    var interceptor = new AuthorizationCommandInterceptor(policy);

    interceptor.before(CTX); // no ScopedValue bound

    verify(policy).authorize(null, CTX.command());
  }

  @Test
  void passes_null_userId_when_user_not_set_in_context() {
    CommandAuthorizationPolicy policy = mock(CommandAuthorizationPolicy.class);
    var interceptor = new AuthorizationCommandInterceptor(policy);

    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, null, CorrelationId.of("corr-1"), Instant.now(), null))
        .run(() -> interceptor.before(CTX));

    verify(policy).authorize(null, CTX.command());
  }

  @Test
  void constructor_rejects_null_policy() {
    assertThrows(IllegalArgumentException.class, () -> new AuthorizationCommandInterceptor(null));
  }

  // ===================== Saga-system principal on the policy-based sibling =====================
  //
  // AnnotationAuthorizationInterceptor honors StreamRuneContext.SAGA_OWNED
  // as a trusted system principal, because saga-dispatched commands — forward and,
  // critically, COMPENSATION commands like RefundPayment — run on a subscription poll thread, a
  // SagaTimeoutRunner thread or the SagaCompensationRetrySweeper thread, where no
  // StreamRuneContext user is bound. Its policy-based twin was left out: a policy saw userId ==
  // null on every saga path, and the documented shape of such a policy ("if (userId == null) throw
  // new AuthorizationException(...)") therefore rejected every saga-dispatched command.

  private static void runSagaOwned(Runnable action) {
    ScopedValue.where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE).run(action);
  }

  @Test
  void sagaOwnedDispatch_runsAsTheSagaSystemPrincipal_policyNotConsulted() {
    CommandAuthorizationPolicy policy = mock(CommandAuthorizationPolicy.class);
    doThrow(new AuthorizationException("Authentication required"))
        .when(policy)
        .authorize(isNull(), any());
    var interceptor = new AuthorizationCommandInterceptor(policy);

    runSagaOwned(() -> assertTrue(interceptor.before(CTX)));
    verify(policy, never()).authorize(any(), any());
  }

  @Test
  void sagaOwnedDispatch_withOptOut_stillConsultsThePolicy() {
    CommandAuthorizationPolicy policy = mock(CommandAuthorizationPolicy.class);
    doThrow(new AuthorizationException("Authentication required"))
        .when(policy)
        .authorize(isNull(), any());
    var interceptor =
        new AuthorizationCommandInterceptor(policy, /* honorSagaSystemPrincipal= */ false);

    assertThrows(AuthorizationException.class, () -> runSagaOwned(() -> interceptor.before(CTX)));
    verify(policy).authorize(null, CTX.command());
  }

  @Test
  void ordinaryDispatch_isUnaffectedByTheSagaMarker() {
    // The bypass is scoped strictly to a SAGA_OWNED dispatch: an ordinary unauthenticated execution
    // still reaches the policy and still fails closed.
    CommandAuthorizationPolicy policy = mock(CommandAuthorizationPolicy.class);
    doThrow(new AuthorizationException("Authentication required"))
        .when(policy)
        .authorize(isNull(), any());
    var interceptor = new AuthorizationCommandInterceptor(policy);

    assertThrows(AuthorizationException.class, () -> interceptor.before(CTX));
    verify(policy).authorize(null, CTX.command());
  }
}
