package org.streamrune.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares that the caller must have at least one of the listed permissions to execute this
 * command.
 *
 * <p>Checked by {@code AnnotationAuthorizationInterceptor} in {@code before()}, before the command
 * reaches the Decider. When combined with {@link RequireRole}, both must be satisfied (AND).
 *
 * <p>May be placed on the command class itself or on any interface or superclass in its hierarchy
 * (e.g. a sealed command interface shared by several command records). Lookup is performed via
 * {@link AuthorizationAnnotations}; the declaration nearest to the command class wins.
 *
 * <p>Example:
 *
 * <pre>{@code
 * @RequirePermission("orders:cancel")
 * record CancelOrderCommand(String orderId) {}
 * }</pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RequirePermission {
  /** One or more permission names. The caller must have at least one (OR logic). */
  String[] value();
}
