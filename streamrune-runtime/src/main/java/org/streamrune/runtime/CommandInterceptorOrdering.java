package org.streamrune.runtime;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.streamrune.core.CommandInterceptor;

/**
 * Canonical execution order for the framework's auto-registered {@link CommandInterceptor}s, shared
 * by all three integrations so the chain is identical on Spring, Quarkus, and Micronaut.
 *
 * <p>The command bus runs {@code before()} in ascending order and {@code after()}/{@code onError()}
 * in reverse (see the {@link CommandInterceptor} lifecycle contract). The order is
 * <em>security-relevant</em>, not cosmetic:
 *
 * <ul>
 *   <li>Audit ({@value #ORDER_AUDIT}) is OUTSIDE Authorization ({@value #ORDER_AUTHORIZATION}) so a
 *       command the authorization interceptor rejects (its {@code before()} throws) still reaches
 *       {@code AuditCommandInterceptor.onError()} — the denied attempt is audited. Reverse the two
 *       and the rejection throws before audit's {@code before()} runs, so the bus never delivers
 *       {@code onError()} to audit and the denial goes UNAUDITED.
 *   <li>Bean Validation ({@value #ORDER_VALIDATION}) runs AFTER authorization so unauthorized
 *       callers cannot probe validation.
 *   <li>The circuit breaker ({@value #ORDER_CIRCUIT_BREAKER}) is innermost of the gates so it
 *       counts only command-execution failures, never client rejections.
 * </ul>
 *
 * <p>Spring achieves this order via {@code @Order} on its {@code @Bean} methods (it references the
 * same constants); Quarkus and Micronaut assemble the chain from an unordered container collection,
 * so they call {@link #sorted(List)} to impose it. Values leave gaps so a user interceptor can slot
 * between framework ones.
 *
 * <p><b>User-interceptor ordering.</b> A user interceptor declares its position with a standard
 * ordering annotation — {@code @jakarta.annotation.Priority}, Spring's {@code @Order}, or
 * Micronaut's {@code @Order} — whose {@code value()} is read into this same numeric space (lower =
 * more outer). So {@code @Priority(2500)} slots a user interceptor between {@link #ORDER_AUDIT}
 * (2000) and {@link #ORDER_AUTHORIZATION} (3000) identically on all three integrations: Spring
 * already honors it via {@code orderedStream()}, and {@link #sorted(List)} now honors it on Quarkus
 * and Micronaut too. A user interceptor with no such annotation sorts at {@link
 * #ORDER_USER_DEFAULT} (innermost), mirroring Spring's {@code Ordered.LOWEST_PRECEDENCE} default.
 */
public final class CommandInterceptorOrdering {

  private CommandInterceptorOrdering() {}

  /** Outermost interceptor: the OTel span covers the whole pipeline, including rejections. */
  public static final int ORDER_OPEN_TELEMETRY = 1000;

  /** Audits every command attempt, including ones later rejected by authorization/validation. */
  public static final int ORDER_AUDIT = 2000;

  /** Policy-based authorization gate. */
  public static final int ORDER_AUTHORIZATION = 3000;

  /** Annotation-based ({@code @RequireRole}/{@code @RequirePermission}) authorization gate. */
  public static final int ORDER_ANNOTATION_AUTHORIZATION = 3100;

  /** Bean Validation runs after authorization so unauthorized callers cannot probe validation. */
  public static final int ORDER_VALIDATION = 4000;

  /**
   * Innermost gate: the breaker counts only command execution failures, never client rejections.
   */
  public static final int ORDER_CIRCUIT_BREAKER = 5000;

  /** Innermost interceptor: inline projections are applied first once events are committed. */
  public static final int ORDER_INLINE_PROJECTION = 6000;

  /**
   * User interceptors of an unrecognized type run innermost (after every framework interceptor).
   */
  public static final int ORDER_USER_DEFAULT = Integer.MAX_VALUE;

  // Keyed by exact class (not instanceof) so a user subclass of a framework interceptor is treated
  // as a user interceptor rather than being silently slotted into a framework position. The lookup
  // looks through container-generated (synthetic) subclasses first; see frameworkLookupClass.
  private static final Map<Class<?>, Integer> ORDERS =
      Map.of(
          OpenTelemetryCommandInterceptor.class, ORDER_OPEN_TELEMETRY,
          AuditCommandInterceptor.class, ORDER_AUDIT,
          AuthorizationCommandInterceptor.class, ORDER_AUTHORIZATION,
          AnnotationAuthorizationInterceptor.class, ORDER_ANNOTATION_AUTHORIZATION,
          BeanValidationInterceptor.class, ORDER_VALIDATION,
          CircuitBreakerCommandInterceptor.class, ORDER_CIRCUIT_BREAKER,
          InlineProjectionInterceptor.class, ORDER_INLINE_PROJECTION);

  // Standard ordering annotations a user interceptor may carry, in precedence order (the first one
  // present wins). Matched by fully-qualified name via reflection so the runtime module needs no
  // compile-time dependency on jakarta.annotation, Spring, or Micronaut — whichever the user's
  // framework supplies is honored. All three define an int {@code value()} where lower = higher
  // precedence (runs first / more outer), matching this class's numeric convention.
  private static final List<String> ORDER_ANNOTATIONS =
      List.of(
          "jakarta.annotation.Priority",
          "org.springframework.core.annotation.Order",
          "io.micronaut.core.annotation.Order");

  /**
   * Returns the canonical order value for {@code interceptor}: the framework constant for a
   * recognized framework interceptor, else a user-declared order read from a standard ordering
   * annotation ({@code @jakarta.annotation.Priority} / Spring or Micronaut {@code @Order}), else
   * {@link #ORDER_USER_DEFAULT} (innermost).
   */
  public static int orderOf(CommandInterceptor interceptor) {
    Integer framework = ORDERS.get(frameworkLookupClass(interceptor.getClass()));
    if (framework != null) {
      return framework;
    }
    Integer declared = userDeclaredOrder(interceptor);
    return declared != null ? declared : ORDER_USER_DEFAULT;
  }

  /**
   * The class to look up in {@link #ORDERS}: {@code type} itself, or, for a class the compiler did
   * not write, the first class above it that it did. A container hands the bus a normal-scoped bean
   * through a generated subclass of the bean class — an Arc client proxy for an
   * {@code @ApplicationScoped} bean, an intercepted subclass — and such a class is synthetic. An
   * application bean of a framework interceptor type reached through one must still take that
   * interceptor's slot, or a replacement audit interceptor would sort innermost and miss the
   * refusals it exists to record. A subclass the application compiles is not synthetic, so it stays
   * a user interceptor.
   */
  private static Class<?> frameworkLookupClass(Class<?> type) {
    Class<?> c = type;
    while (c.isSynthetic() && c.getSuperclass() != null && c.getSuperclass() != Object.class) {
      c = c.getSuperclass();
    }
    return c;
  }

  /**
   * Reads a user interceptor's declared order from the first present {@linkplain #ORDER_ANNOTATIONS
   * standard ordering annotation}'s {@code value()}, or {@code null} if none is present. Uses
   * reflection by annotation type name so an unrecognized/absent annotation type never links.
   *
   * <p><b>Proxy unwrapping.</b> Walks the superclass chain rather than reading only {@code
   * interceptor.getClass()}. On Quarkus a normal-scoped ({@code @ApplicationScoped}) user
   * interceptor is injected as a CDI client proxy — a generated subclass of the bean class — and
   * {@code @jakarta.annotation.Priority}, Spring {@code @Order}, and Micronaut {@code @Order} are
   * all non-{@code @Inherited}, so the proxy class carries no ordering annotation and the declared
   * order would be lost. Reading each class's {@linkplain Class#getDeclaredAnnotations() declared}
   * annotations up the chain finds the annotation on the underlying bean class (closest class
   * wins), with no compile-time dependency on Arc — a generic superclass walk suffices.
   */
  private static Integer userDeclaredOrder(CommandInterceptor interceptor) {
    for (Class<?> c = interceptor.getClass();
        c != null && c != Object.class;
        c = c.getSuperclass()) {
      Annotation[] annotations = c.getDeclaredAnnotations();
      for (String annotationType : ORDER_ANNOTATIONS) {
        for (Annotation annotation : annotations) {
          if (annotation.annotationType().getName().equals(annotationType)) {
            try {
              Object value = annotation.annotationType().getMethod("value").invoke(annotation);
              if (value instanceof Integer intValue) {
                return intValue;
              }
            } catch (ReflectiveOperationException _) {
              // Annotation shape did not match the expected int value() — treat as undeclared.
            }
          }
        }
      }
    }
    return null;
  }

  /**
   * Returns a new list containing {@code interceptors} sorted into the canonical execution order.
   * The sort is stable, so interceptors sharing an order value (e.g. two user interceptors) keep
   * their input (container) order. The input list is not modified.
   */
  public static List<CommandInterceptor> sorted(List<CommandInterceptor> interceptors) {
    List<CommandInterceptor> copy = new ArrayList<>(interceptors);
    copy.sort(Comparator.comparingInt(CommandInterceptorOrdering::orderOf));
    return copy;
  }
}
