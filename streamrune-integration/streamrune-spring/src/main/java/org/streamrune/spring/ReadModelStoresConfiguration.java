package org.streamrune.spring;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.integration.ProjectionStoreRouting;
import org.streamrune.integration.ReadModelStores;

/**
 * The application's read-model stores in one bean, {@value #READ_MODEL_STORES}: the store beans the
 * application defines, and the default store — the bean {@value
 * ProjectionStoreRouting#DEFAULT_STORE_BEAN} {@link StreamRuneAutoConfiguration} registers on the
 * {@code DataSource} — unless one of them is named {@value ProjectionStoreRouting#DEFAULT_STORE}.
 * Imported by {@link StreamRuneAutoConfiguration} (whose {@code ProjectionRepository} bean reads
 * it) and by {@link ProjectionAutoConfig} (whose routes read it), so either one alone has it.
 *
 * <p>Replacing the default store is by name, so the condition cannot be a bean condition: a store's
 * name is known only once the store exists. The default store is therefore always defined (lazily)
 * under its framework bean name, and this bean leaves it out of the application's stores by that
 * name — enumerating the bean names, which creates nothing — then creates it only when no
 * application store took its name ({@link ProjectionStoreRouting#selectStores}). The application's
 * stores are created to read their names; a replaced default store is never created, so its
 * database is never touched. Code that collects every {@code ReadModelStore} bean itself (an
 * injected {@code List<ReadModelStore>}) would create the replaced default store too; this bean's
 * {@link ReadModelStores#stores()} is the selection.
 */
@Configuration(proxyBeanMethods = false)
class ReadModelStoresConfiguration {

  /** The bean name of the stores, {@value}. */
  static final String READ_MODEL_STORES = "streamRuneReadModelStores";

  /**
   * The application's read-model stores, selected and validated on first use.
   *
   * @param beanFactory the bean factory the stores are collected from
   * @return the stores
   */
  @Bean(READ_MODEL_STORES)
  ReadModelStores streamRuneReadModelStores(ListableBeanFactory beanFactory) {
    return new ReadModelStores(
        () -> defaultStore(beanFactory), () -> applicationStores(beanFactory));
  }

  /** The default store bean, created now; empty when it is not defined (no DataSource). */
  private static Optional<ReadModelStore> defaultStore(ListableBeanFactory beanFactory) {
    if (!beanFactory.containsBean(ProjectionStoreRouting.DEFAULT_STORE_BEAN)) {
      return Optional.empty();
    }
    return beanFactory.getBean(ProjectionStoreRouting.DEFAULT_STORE_BEAN)
            instanceof ReadModelStore store
        ? Optional.of(store)
        : Optional.empty();
  }

  /**
   * Every {@link ReadModelStore} bean but the default one, in registration order. A {@code @Bean}
   * that returned {@code null} is no store.
   */
  private static List<ReadModelStore> applicationStores(ListableBeanFactory beanFactory) {
    var stores = new ArrayList<ReadModelStore>();
    for (String name : beanFactory.getBeanNamesForType(ReadModelStore.class)) {
      if (!ProjectionStoreRouting.DEFAULT_STORE_BEAN.equals(name)
          && beanFactory.getBean(name) instanceof ReadModelStore store) {
        stores.add(store);
      }
    }
    return stores;
  }
}
