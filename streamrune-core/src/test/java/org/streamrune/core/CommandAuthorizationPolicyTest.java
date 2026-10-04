package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.streamrune.core.types.UserId;

class CommandAuthorizationPolicyTest {

  record TestCmd() implements Command {}

  private static final TestCmd CMD = new TestCmd();

  @Test
  void allowAll_permits_any_command() {
    var policy = CommandAuthorizationPolicy.allowAll();
    assertDoesNotThrow(() -> policy.authorize(UserId.of("user-1"), CMD));
  }

  @Test
  void allowAll_permits_null_userId() {
    var policy = CommandAuthorizationPolicy.allowAll();
    assertDoesNotThrow(() -> policy.authorize(null, CMD));
  }

  @Test
  void denyAll_throws_AuthorizationException() {
    var policy = CommandAuthorizationPolicy.denyAll("forbidden");
    AuthorizationException ex =
        assertThrows(
            AuthorizationException.class, () -> policy.authorize(UserId.of("user-1"), CMD));
    assertEquals("forbidden", ex.getMessage());
  }

  @Test
  void denyAll_throws_for_null_userId() {
    var policy = CommandAuthorizationPolicy.denyAll("no anon");
    assertThrows(AuthorizationException.class, () -> policy.authorize(null, CMD));
  }

  @Test
  void denyAll_throws_NullPointerException_for_null_message() {
    assertThrows(NullPointerException.class, () -> CommandAuthorizationPolicy.denyAll(null));
  }

  @Test
  void authorizationException_is_a_DomainException() {
    var ex = new AuthorizationException("denied");
    assertInstanceOf(DomainException.class, ex);
    assertEquals("denied", ex.getMessage());
  }

  @Test
  void custom_lambda_policy_works() {
    CommandAuthorizationPolicy policy =
        (userId, command) -> {
          if (userId == null || !"admin".equals(userId.value())) {
            throw new AuthorizationException("admin only");
          }
        };
    assertDoesNotThrow(() -> policy.authorize(UserId.of("admin"), CMD));
    assertThrows(AuthorizationException.class, () -> policy.authorize(UserId.of("guest"), CMD));
  }
}
