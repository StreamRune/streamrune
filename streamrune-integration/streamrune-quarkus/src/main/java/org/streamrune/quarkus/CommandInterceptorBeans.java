package org.streamrune.quarkus;

import io.quarkus.arc.InjectableBean;
import jakarta.enterprise.context.spi.CreationalContext;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.CommandInterceptor;

/**
 * Collects the {@link CommandInterceptor} beans of the deployment — the application's and the
 * framework's — for the command bus producer, without CDI ambiguity resolution. The startup
 * validators read the chain back from each built bus, so they check exactly what this collection
 * handed to the bus.
 *
 * <p><b>Why not {@code Instance<CommandInterceptor>} or {@code @All List<CommandInterceptor>}.</b>
 * Arc resolves both through the same ambiguity resolution it applies to a single-valued injection
 * point, and that resolution drops every {@code @DefaultBean} candidate as soon as one non-default
 * candidate of the collected type exists. The framework's interceptor producers are
 * {@code @DefaultBean} — so that an application bean of the same type replaces one of them, as
 * {@code @ConditionalOnMissingBean} does on Spring and {@code @Requires(missingBeans = …)} on
 * Micronaut — which made the first application {@code CommandInterceptor} bean (a maintenance gate,
 * a rate limiter) evict all of them: the bus ran without authorization, audit and circuit breaker,
 * and the identity validator, iterating the same stripped collection, let startup proceed.
 *
 * <p>{@link BeanManager#getBeans} applies typesafe resolution only (bean type, qualifiers, enabled
 * alternatives), so every interceptor bean is seen. The replace rule the other integrations get
 * from their conditions is applied here explicitly, per bean: a {@code @DefaultBean} interceptor is
 * left out exactly when a non-default bean exists whose implementation class is assignable to the
 * default's — an application {@code AuditCommandInterceptor} bean stands in for the framework's, an
 * application gate of its own type stands beside them — and the decision is logged. A bean Arc
 * suppresses at lookup time ({@code @LookupIfProperty}, {@code @LookupUnlessProperty}) is skipped,
 * as an {@code Instance} would skip it. A dependent-scoped producer that declines (returns {@code
 * null}) contributes nothing.
 *
 * <p><b>The command bus producer also declares an {@code @Any Instance<CommandInterceptor>}
 * injection point, which it never reads.</b> Quarkus removes unused beans at build time by default
 * ({@code quarkus.arc.remove-unused-beans=all}): a bean survives only if an injection point
 * resolves to it, an {@code Instance} or {@code @All} injection point matches it, it is named, it
 * declares an observer, or it is otherwise excluded. A {@link BeanManager} lookup at runtime is
 * invisible to that analysis. Without a matching injection point, every framework interceptor
 * producer — none of which anything else injects — and every application interceptor bean nothing
 * else injects would be removed from the application, and this collection would return an empty
 * chain: no authorization, no audit, a startup check that sees nothing. The unread {@code @Any
 * Instance} on the command bus producer matches every bean this class collects, so Arc keeps them
 * all as long as the bus is kept — and the startup validators inject every {@code
 * VirtualThreadCommandBus}, which keeps the bus even when nothing else injects it. The {@code
 * Instance} itself is never iterated, because that would apply the ambiguity resolution described
 * above.
 *
 * <p>The order of the returned list is unspecified; the command bus sorts it with {@link
 * org.streamrune.runtime.CommandInterceptorOrdering}, which reads an application interceptor's
 * {@code @Priority} from the interceptor's class. A {@code @Priority} on a {@code @Produces} method
 * orders the bean for CDI but does not position the interceptor in the chain. A replacement of a
 * framework interceptor reaches the bus through Arc's client proxy when it is normal-scoped; the
 * ordering looks through that generated subclass, so the replacement keeps the framework slot.
 */
final class CommandInterceptorBeans {

  private static final Logger LOG = LoggerFactory.getLogger(CommandInterceptorBeans.class);

  private CommandInterceptorBeans() {}

  /**
   * Returns one instance of every {@link CommandInterceptor} bean that takes part in the chain.
   *
   * @param beanManager the deployment's bean manager
   * @return the interceptors, never containing {@code null}
   */
  static List<CommandInterceptor> collect(BeanManager beanManager) {
    List<Bean<?>> application = new ArrayList<>();
    List<Bean<?>> defaults = new ArrayList<>();
    for (Bean<?> bean : beanManager.getBeans(CommandInterceptor.class, Any.Literal.INSTANCE)) {
      if (bean instanceof InjectableBean<?> injectable) {
        if (injectable.isSuppressed()) {
          continue;
        }
        if (injectable.isDefaultBean()) {
          defaults.add(bean);
          continue;
        }
      }
      application.add(bean);
    }

    List<CommandInterceptor> interceptors = new ArrayList<>();
    for (Bean<?> bean : application) {
      addReference(interceptors, beanManager, bean);
    }
    for (Bean<?> bean : defaults) {
      Class<?> type = implementationClass(bean);
      if (type != null && isReplaced(type, application)) {
        LOG.info(
            "StreamRune: the application supplies its own {} bean, so the framework does not"
                + " register one",
            type.getSimpleName());
        continue;
      }
      addReference(interceptors, beanManager, bean);
    }
    return interceptors;
  }

  /** Whether an application bean's instances are of {@code type} (or a subtype). */
  private static boolean isReplaced(Class<?> type, List<Bean<?>> application) {
    for (Bean<?> candidate : application) {
      Class<?> candidateType = implementationClass(candidate);
      if (candidateType != null && type.isAssignableFrom(candidateType)) {
        return true;
      }
    }
    return false;
  }

  private static void addReference(
      List<CommandInterceptor> out, BeanManager beanManager, Bean<?> bean) {
    CreationalContext<?> context = beanManager.createCreationalContext(bean);
    Object reference = beanManager.getReference(bean, CommandInterceptor.class, context);
    if (reference != null) {
      out.add((CommandInterceptor) reference);
    }
  }

  /** The class of the bean's instances: a producer's return type, else the bean class. */
  private static Class<?> implementationClass(Bean<?> bean) {
    if (bean instanceof InjectableBean<?> injectable
        && injectable.getImplementationClass() != null) {
      return injectable.getImplementationClass();
    }
    return bean.getBeanClass();
  }
}
