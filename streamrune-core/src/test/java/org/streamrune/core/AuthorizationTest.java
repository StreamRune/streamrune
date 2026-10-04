package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

class AuthorizationTest {

  @Test
  void currentUserId_returns_null_when_no_context() {
    assertNull(Authorization.currentUserId());
  }

  @Test
  void currentUserId_returns_null_when_context_has_no_user() {
    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, null, CorrelationId.of("corr-1"), Instant.now(), null))
        .run(() -> assertNull(Authorization.currentUserId()));
  }

  @Test
  void currentUserId_returns_userId_from_context() {
    UserId expected = UserId.of("user-42");
    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, expected, CorrelationId.of("corr-1"), Instant.now(), null))
        .run(() -> assertEquals(expected, Authorization.currentUserId()));
  }

  @Test
  void requireOwner_passes_when_caller_is_owner() {
    UserId owner = UserId.of("user-42");
    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, owner, CorrelationId.of("corr-1"), Instant.now(), null))
        .run(() -> assertDoesNotThrow(() -> Authorization.requireOwner(owner)));
  }

  @Test
  void requireOwner_throws_when_caller_is_not_owner() {
    UserId caller = UserId.of("user-42");
    UserId owner = UserId.of("user-99");
    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, caller, CorrelationId.of("corr-1"), Instant.now(), null))
        .run(
            () ->
                assertThrows(
                    AuthorizationException.class, () -> Authorization.requireOwner(owner)));
  }

  @Test
  void requireOwner_throws_when_no_authenticated_user() {
    UserId owner = UserId.of("user-99");
    assertThrows(AuthorizationException.class, () -> Authorization.requireOwner(owner));
  }

  @Test
  void requireOwner_throws_when_owner_is_null_even_for_authenticated_caller() {
    // initialState() of a not-yet-created aggregate has no owner: an unconditional
    // requireOwner(state.ownerId()) therefore denies every creation command.
    UserId caller = UserId.of("user-42");
    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, caller, CorrelationId.of("corr-1"), Instant.now(), null))
        .run(
            () ->
                assertThrows(AuthorizationException.class, () -> Authorization.requireOwner(null)));
  }

  @Test
  void requireOwner_throws_when_context_has_null_user() {
    UserId owner = UserId.of("user-99");
    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, null, CorrelationId.of("corr-1"), Instant.now(), null))
        .run(
            () ->
                assertThrows(
                    AuthorizationException.class, () -> Authorization.requireOwner(owner)));
  }

  // --- saga-dispatched commands ---

  private static StreamRuneContext.RequestContext contextFor(UserId user) {
    return new StreamRuneContext.RequestContext(
        null, user, CorrelationId.of("corr-1"), Instant.now(), null);
  }

  /** Runs {@code body} the way a saga-issued command runs: under the saga marker, no end user. */
  private static void runAsSaga(Runnable body) {
    ScopedValue.where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE).run(body);
  }

  @Test
  void isSagaDispatch_is_false_when_marker_is_unbound() {
    assertFalse(StreamRuneContext.isSagaDispatch());
  }

  @Test
  void isSagaDispatch_is_true_only_when_marker_is_bound_true() {
    runAsSaga(() -> assertTrue(StreamRuneContext.isSagaDispatch()));
    ScopedValue.where(StreamRuneContext.SAGA_OWNED, Boolean.FALSE)
        .run(() -> assertFalse(StreamRuneContext.isSagaDispatch()));
  }

  @Test
  void requireOwnerOrSaga_passes_for_a_saga_dispatch_with_no_user() {
    UserId owner = UserId.of("user-99");
    runAsSaga(() -> assertDoesNotThrow(() -> Authorization.requireOwnerOrSaga(owner)));
  }

  @Test
  void requireOwnerOrSaga_passes_for_a_saga_dispatch_on_a_thread_bound_to_another_user() {
    // An operator replaying a saga from a request thread: the saga acts, not the operator.
    UserId owner = UserId.of("user-99");
    ScopedValue.where(StreamRuneContext.CURRENT, contextFor(UserId.of("operator-1")))
        .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
        .run(() -> assertDoesNotThrow(() -> Authorization.requireOwnerOrSaga(owner)));
  }

  @Test
  void requireOwnerOrSaga_passes_when_caller_is_owner() {
    UserId owner = UserId.of("user-42");
    ScopedValue.where(StreamRuneContext.CURRENT, contextFor(owner))
        .run(() -> assertDoesNotThrow(() -> Authorization.requireOwnerOrSaga(owner)));
  }

  @Test
  void requireOwnerOrSaga_throws_when_caller_is_not_owner_outside_a_saga() {
    ScopedValue.where(StreamRuneContext.CURRENT, contextFor(UserId.of("user-42")))
        .run(
            () ->
                assertThrows(
                    AuthorizationException.class,
                    () -> Authorization.requireOwnerOrSaga(UserId.of("user-99"))));
  }

  @Test
  void requireOwnerOrSaga_throws_when_unauthenticated_outside_a_saga() {
    assertThrows(
        AuthorizationException.class, () -> Authorization.requireOwnerOrSaga(UserId.of("u-1")));
    ScopedValue.where(StreamRuneContext.CURRENT, contextFor(null))
        .run(
            () ->
                assertThrows(
                    AuthorizationException.class,
                    () -> Authorization.requireOwnerOrSaga(UserId.of("u-1"))));
  }

  @Test
  void requireOwnerOrSaga_throws_when_owner_is_null_outside_a_saga() {
    ScopedValue.where(StreamRuneContext.CURRENT, contextFor(UserId.of("user-42")))
        .run(
            () ->
                assertThrows(
                    AuthorizationException.class, () -> Authorization.requireOwnerOrSaga(null)));
  }

  @Test
  void requireOwnerOrSaga_throws_when_the_marker_is_bound_false() {
    ScopedValue.where(StreamRuneContext.SAGA_OWNED, Boolean.FALSE)
        .run(
            () ->
                assertThrows(
                    AuthorizationException.class,
                    () -> Authorization.requireOwnerOrSaga(UserId.of("user-99"))));
  }

  @Test
  void requireOwner_denies_a_saga_dispatch_and_names_the_cause() {
    // requireOwner stays strict: a saga has no end user, so it can never be the owner. The denial
    // must say so, or a saga failing on it reads like an ordinary rejection.
    runAsSaga(
        () -> {
          var denied =
              assertThrows(
                  AuthorizationException.class,
                  () -> Authorization.requireOwner(UserId.of("user-99")));
          assertTrue(
              denied.getMessage().contains("saga"),
              "the denial names the saga dispatch: " + denied.getMessage());
          assertTrue(
              denied.getMessage().contains("requireOwnerOrSaga"),
              "the denial names the helper that admits sagas: " + denied.getMessage());
        });
  }

  @Test
  void requireOwner_denial_outside_a_saga_does_not_mention_sagas() {
    var denied =
        assertThrows(
            AuthorizationException.class, () -> Authorization.requireOwner(UserId.of("u-1")));
    assertFalse(denied.getMessage().contains("saga"), denied.getMessage());
  }
}
