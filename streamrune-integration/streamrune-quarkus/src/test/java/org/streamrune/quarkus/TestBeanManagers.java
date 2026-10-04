package org.streamrune.quarkus;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.streamrune.core.CommandInterceptor;

/**
 * Mocked {@link BeanManager}s for the unit tests that hand the command bus producer in {@link
 * StreamRuneProducers} its {@link CommandInterceptor} beans without a container — the collection
 * {@link CommandInterceptorBeans} performs is driven through its two {@code BeanManager} calls.
 */
final class TestBeanManagers {

  private TestBeanManagers() {}

  /**
   * A bean manager whose {@link CommandInterceptor} beans are {@code interceptors}, each a plain
   * (non-default) bean whose contextual reference is the given instance. A {@code null} element
   * models a dependent-scoped optional producer that declined.
   */
  static BeanManager withInterceptors(CommandInterceptor... interceptors) {
    return withInterceptors(Arrays.asList(interceptors));
  }

  /** As {@link #withInterceptors(CommandInterceptor...)}, from a list that may contain nulls. */
  @SuppressWarnings("unchecked")
  static BeanManager withInterceptors(List<CommandInterceptor> interceptors) {
    BeanManager beanManager = mock(BeanManager.class);
    Set<Bean<?>> beans = new LinkedHashSet<>();
    for (CommandInterceptor interceptor : interceptors) {
      Bean<Object> bean = mock(Bean.class);
      Class<?> beanClass = interceptor == null ? Object.class : interceptor.getClass();
      doReturn(beanClass).when(bean).getBeanClass();
      when(beanManager.getReference(eq(bean), eq(CommandInterceptor.class), any()))
          .thenReturn(interceptor);
      beans.add(bean);
    }
    when(beanManager.getBeans(CommandInterceptor.class, Any.Literal.INSTANCE)).thenReturn(beans);
    return beanManager;
  }
}
