package org.streamrune.core;

import java.lang.annotation.Annotation;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

/**
 * Static lookup for {@link RequireRole} and {@link RequirePermission} on command types.
 *
 * <p>Annotations are searched on the command class itself, then on its interfaces, then on its
 * superclass, breadth-first through the whole hierarchy. The nearest declaration wins, so an
 * annotation on the command class overrides one on the sealed command interface it implements. This
 * supports the documented pattern of annotating a sealed command interface once instead of every
 * command record:
 *
 * <pre>{@code
 * @RequireRole("ADMIN")
 * sealed interface AdminCommand permits DeleteOrderCommand, RefundOrderCommand {}
 *
 * record DeleteOrderCommand(String orderId) implements AdminCommand {}
 * }</pre>
 */
public final class AuthorizationAnnotations {

  private AuthorizationAnnotations() {}

  /**
   * Returns the {@link RequireRole} annotation nearest to the given type in its class hierarchy, or
   * {@code null} if neither the type nor any of its interfaces or superclasses declares one.
   *
   * @param commandType the command class to inspect, never {@code null}
   */
  public static RequireRole findRequireRole(Class<?> commandType) {
    return find(commandType, RequireRole.class);
  }

  /**
   * Returns the {@link RequirePermission} annotation nearest to the given type in its class
   * hierarchy, or {@code null} if neither the type nor any of its interfaces or superclasses
   * declares one.
   *
   * @param commandType the command class to inspect, never {@code null}
   */
  public static RequirePermission findRequirePermission(Class<?> commandType) {
    return find(commandType, RequirePermission.class);
  }

  /**
   * Returns {@code true} if the given type carries {@link RequireRole} or {@link RequirePermission}
   * anywhere in its class hierarchy (the type itself, its interfaces, or its superclasses).
   *
   * @param commandType the command class to inspect, never {@code null}
   */
  public static boolean requiresAuthorization(Class<?> commandType) {
    return findRequireRole(commandType) != null || findRequirePermission(commandType) != null;
  }

  private static <A extends Annotation> A find(Class<?> type, Class<A> annotationType) {
    if (type == null) {
      throw new IllegalArgumentException("commandType is required");
    }
    Deque<Class<?>> queue = new ArrayDeque<>();
    Set<Class<?>> visited = new HashSet<>();
    queue.add(type);
    while (!queue.isEmpty()) {
      Class<?> current = queue.poll();
      if (current == Object.class || !visited.add(current)) {
        continue;
      }
      A annotation = current.getDeclaredAnnotation(annotationType);
      if (annotation != null) {
        return annotation;
      }
      Collections.addAll(queue, current.getInterfaces());
      Class<?> superclass = current.getSuperclass();
      if (superclass != null) {
        queue.add(superclass);
      }
    }
    return null;
  }
}
