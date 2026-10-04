package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.lang.classfile.ClassFile;
import java.lang.constant.ClassDesc;
import java.lang.constant.ConstantDescs;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandAuthorizationPolicy;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.projection.Projection;

/**
 * Pins the canonical, security-relevant interceptor order enforced by {@link
 * CommandInterceptorOrdering}. This is the single source of truth the Quarkus and Micronaut
 * integrations sort their container-assembled chain by (and that Spring's {@code @Order} constants
 * reference), so all three frameworks run the identical order: OpenTelemetry → Audit →
 * Authorization → AnnotationAuthorization → BeanValidation → CircuitBreaker → InlineProjection,
 * with unrecognized user interceptors innermost.
 */
class CommandInterceptorOrderingTest {

  private static final OpenTelemetryCommandInterceptor OTEL =
      new OpenTelemetryCommandInterceptor(io.opentelemetry.api.OpenTelemetry.noop());
  private static final AuditCommandInterceptor AUDIT =
      new AuditCommandInterceptor((AuditStore) entry -> {});
  private static final AuthorizationCommandInterceptor AUTHZ =
      new AuthorizationCommandInterceptor(CommandAuthorizationPolicy.allowAll());
  private static final AnnotationAuthorizationInterceptor ANNOTATION_AUTHZ =
      new AnnotationAuthorizationInterceptor(mock(UserRoleResolver.class));
  private static final BeanValidationInterceptor VALIDATION =
      new BeanValidationInterceptor(mock(jakarta.validation.Validator.class));
  private static final CircuitBreakerCommandInterceptor CIRCUIT_BREAKER =
      new CircuitBreakerCommandInterceptor(5, Duration.ofSeconds(1));
  private static final InlineProjectionInterceptor INLINE_PROJECTION =
      InlineProjectionInterceptor.builder()
          .register("p", (Projection) (List<EventEnvelope> batch) -> {})
          .build();

  @Test
  void sortsFrameworkInterceptorsIntoCanonicalOrder() {
    // Deliberately scrambled input order.
    List<CommandInterceptor> scrambled =
        List.of(
            CIRCUIT_BREAKER, AUTHZ, INLINE_PROJECTION, OTEL, VALIDATION, AUDIT, ANNOTATION_AUTHZ);

    List<CommandInterceptor> sorted = CommandInterceptorOrdering.sorted(scrambled);

    assertThat(sorted)
        .containsExactly(
            OTEL, AUDIT, AUTHZ, ANNOTATION_AUTHZ, VALIDATION, CIRCUIT_BREAKER, INLINE_PROJECTION);
  }

  @Test
  void unrecognizedUserInterceptorsSortInnermost() {
    var user = new CommandInterceptor() {};
    List<CommandInterceptor> sorted =
        CommandInterceptorOrdering.sorted(List.of(user, AUTHZ, AUDIT));

    assertThat(sorted).containsExactly(AUDIT, AUTHZ, user);
    assertThat(sorted.getLast()).isSameAs(user);
  }

  @Test
  void sortIsStableForEqualOrderInterceptors() {
    // Two user interceptors share ORDER_USER_DEFAULT — their relative input order must be
    // preserved.
    var userA = new CommandInterceptor() {};
    var userB = new CommandInterceptor() {};
    List<CommandInterceptor> sorted =
        CommandInterceptorOrdering.sorted(List.of(userA, userB, AUDIT));

    assertThat(sorted).containsExactly(AUDIT, userA, userB);
  }

  @Test
  void auditSortsOutsideAuthorization_soDenialsAreAudited() {
    // The security-critical invariant: audit's order value is strictly less (more outer) than
    // authorization's, so a denial thrown from authz.before() still reaches audit.onError().
    assertThat(CommandInterceptorOrdering.orderOf(AUDIT))
        .isLessThan(CommandInterceptorOrdering.orderOf(AUTHZ));
    // And validation runs after authorization so unauthorized callers cannot probe validation.
    assertThat(CommandInterceptorOrdering.orderOf(AUTHZ))
        .isLessThan(CommandInterceptorOrdering.orderOf(VALIDATION));
  }

  // A user interceptor's declared order (@jakarta.annotation.Priority,
  // Spring @Order, or Micronaut @Order) is honored by sorted() — the same numeric space as the
  // framework constants — so a user interceptor lands in the SAME chain position on all three
  // integrations (Spring already honors @Order via orderedStream()). Previously every user
  // interceptor collapsed to ORDER_USER_DEFAULT (innermost) on Quarkus/Micronaut.

  @jakarta.annotation.Priority(2500)
  static final class BetweenAuditAndAuthzInterceptor implements CommandInterceptor {}

  @jakarta.annotation.Priority(3500)
  static final class BetweenAuthzAndValidationInterceptor implements CommandInterceptor {}

  @Test
  void userInterceptorPriorityIsReadIntoTheNumericSpace() {
    assertThat(CommandInterceptorOrdering.orderOf(new BetweenAuditAndAuthzInterceptor()))
        .isEqualTo(2500);
  }

  @Test
  void userInterceptorWithPrioritySlotsBetweenFrameworkInterceptors() {
    var user = new BetweenAuditAndAuthzInterceptor();
    // Scrambled: user declared @Priority(2500) must land between AUDIT (2000) and AUTHZ (3000).
    List<CommandInterceptor> sorted =
        CommandInterceptorOrdering.sorted(List.of(AUTHZ, user, AUDIT));

    assertThat(sorted).containsExactly(AUDIT, user, AUTHZ);
  }

  @Test
  void multipleUserInterceptorsOrderByTheirDeclaredPriority() {
    var inner = new BetweenAuthzAndValidationInterceptor(); // 3500
    var outer = new BetweenAuditAndAuthzInterceptor(); // 2500
    // Input order is inner-before-outer; declared order must re-sort them outer-before-inner.
    List<CommandInterceptor> sorted =
        CommandInterceptorOrdering.sorted(List.of(inner, outer, AUTHZ));

    assertThat(sorted).containsExactly(outer, AUTHZ, inner);
  }

  @Test
  void userInterceptorWithNoDeclaredOrderStillSortsInnermost() {
    // Regression guard: an unannotated user interceptor keeps the ORDER_USER_DEFAULT behavior.
    var user = new CommandInterceptor() {};
    assertThat(CommandInterceptorOrdering.orderOf(user)).isEqualTo(Integer.MAX_VALUE);
  }

  // On Quarkus a normal-scoped (@ApplicationScoped) @Priority interceptor is injected as a
  // CDI client proxy — a generated subclass of the bean class. jakarta @Priority / Spring @Order /
  // Micronaut @Order are all non-@Inherited, so getClass().getAnnotations() on the proxy finds
  // nothing and the interceptor silently collapsed to ORDER_USER_DEFAULT (innermost), breaking the
  // documented cross-framework ordering parity. orderOf() must unwrap the proxy (walk the
  // superclass chain) and read the bean class's declared ordering annotation.

  @jakarta.annotation.Priority(2500)
  static class TenantContextInterceptor implements CommandInterceptor {}

  /** Mimics a CDI client proxy: a generated subclass carrying NO ordering annotation of its own. */
  static final class TenantContextInterceptor_ClientProxy extends TenantContextInterceptor {}

  @Test
  void proxySubclassOfPriorityInterceptor_honorsSuperclassPriority() {
    var proxy = new TenantContextInterceptor_ClientProxy();
    assertThat(CommandInterceptorOrdering.orderOf(proxy)).isEqualTo(2500);
  }

  @Test
  void proxySubclassPriority_slotsBetweenFrameworkInterceptors() {
    CommandInterceptor proxy = new TenantContextInterceptor_ClientProxy();
    // The proxy's @Priority(2500) (on its bean superclass) must land between AUDIT (2000) and
    // AUTHZ (3000), identically to the non-proxied case on Spring.
    List<CommandInterceptor> sorted =
        CommandInterceptorOrdering.sorted(List.of(AUTHZ, proxy, AUDIT));
    assertThat(sorted).containsExactly(AUDIT, proxy, AUTHZ);
  }

  // A normal-scoped bean of a framework interceptor type reaches the bus as a container-generated
  // proxy: a synthetic subclass of the framework class. It stands for the framework interceptor
  // and must take that interceptor's slot. A subclass the application compiles is not synthetic
  // and stays a user interceptor.

  private static final CommandInterceptor GENERATED_VALIDATION_PROXY = generatedValidationProxy();

  /** A synthetic subclass of the validation interceptor, built the way a CDI container does. */
  private static CommandInterceptor generatedValidationProxy() {
    ClassDesc superclass = ClassDesc.of(BeanValidationInterceptor.class.getName());
    ClassDesc proxy =
        ClassDesc.of("org.streamrune.runtime.BeanValidationInterceptor_GeneratedProxy");
    MethodTypeDesc constructor =
        MethodTypeDesc.of(
            ConstantDescs.CD_void, ClassDesc.of(jakarta.validation.Validator.class.getName()));
    byte[] bytes =
        ClassFile.of()
            .build(
                proxy,
                cb ->
                    cb.withFlags(
                            ClassFile.ACC_PUBLIC | ClassFile.ACC_SUPER | ClassFile.ACC_SYNTHETIC)
                        .withSuperclass(superclass)
                        .withMethodBody(
                            ConstantDescs.INIT_NAME,
                            constructor,
                            ClassFile.ACC_PUBLIC,
                            code ->
                                code.aload(0)
                                    .aload(1)
                                    .invokespecial(superclass, ConstantDescs.INIT_NAME, constructor)
                                    .return_()));
    try {
      Class<?> type = MethodHandles.lookup().defineClass(bytes);
      return (CommandInterceptor)
          type.getConstructor(jakarta.validation.Validator.class)
              .newInstance(mock(jakarta.validation.Validator.class));
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  static final class ApplicationValidationInterceptor extends BeanValidationInterceptor {
    ApplicationValidationInterceptor() {
      super(mock(jakarta.validation.Validator.class));
    }
  }

  @Test
  void generatedProxyOfAFrameworkInterceptorTakesThatInterceptorsSlot() {
    assertThat(GENERATED_VALIDATION_PROXY.getClass().isSynthetic()).isTrue();
    assertThat(CommandInterceptorOrdering.orderOf(GENERATED_VALIDATION_PROXY))
        .isEqualTo(CommandInterceptorOrdering.ORDER_VALIDATION);
    assertThat(
            CommandInterceptorOrdering.sorted(
                List.of(CIRCUIT_BREAKER, GENERATED_VALIDATION_PROXY, AUTHZ)))
        .containsExactly(AUTHZ, GENERATED_VALIDATION_PROXY, CIRCUIT_BREAKER);
  }

  @Test
  void applicationSubclassOfAFrameworkInterceptorStaysAUserInterceptor() {
    assertThat(CommandInterceptorOrdering.orderOf(new ApplicationValidationInterceptor()))
        .isEqualTo(CommandInterceptorOrdering.ORDER_USER_DEFAULT);
  }
}
