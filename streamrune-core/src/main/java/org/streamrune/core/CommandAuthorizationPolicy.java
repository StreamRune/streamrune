package org.streamrune.core;

import java.util.Objects;
import org.streamrune.core.types.UserId;

/**
 * Policy that decides whether a caller is allowed to execute a command.
 *
 * <p>Register an implementation as a bean in your framework and StreamRune will automatically
 * enforce it on every command via {@code AuthorizationCommandInterceptor}.
 *
 * <p>A null {@code userId} means the command was submitted without an authenticated user.
 * Implementations may choose to allow or deny anonymous commands.
 */
@FunctionalInterface
public interface CommandAuthorizationPolicy {

  /**
   * Checks whether the given user may execute the given command.
   *
   * @param userId the caller's identity, or {@code null} for an unauthenticated request
   * @param command the command object about to be executed
   * @throws AuthorizationException if the caller is not permitted to execute the command
   */
  void authorize(UserId userId, Command command);

  /** Returns a policy that permits every command unconditionally. Useful in tests. */
  static CommandAuthorizationPolicy allowAll() {
    return (userId, command) -> {};
  }

  /**
   * Returns a policy that denies every command with the given message.
   *
   * @param message the detail message included in {@link AuthorizationException}
   */
  static CommandAuthorizationPolicy denyAll(String message) {
    Objects.requireNonNull(message, "message must not be null");
    return (userId, command) -> {
      throw new AuthorizationException(message);
    };
  }
}
