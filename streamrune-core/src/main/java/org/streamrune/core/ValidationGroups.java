package org.streamrune.core;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies JSR-380 validation groups to apply when validating a command.
 *
 * <p>When present on a command class, the {@code BeanValidationInterceptor} validates using only
 * the specified groups. When absent, the {@code Default} group is used.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * @ValidationGroups({CreateOrder.class, Default.class})
 * public record CreateOrderCommand(@NotBlank String orderId) implements Command {}
 * }</pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ValidationGroups {
  /** The validation groups to apply. */
  Class<?>[] value();
}
