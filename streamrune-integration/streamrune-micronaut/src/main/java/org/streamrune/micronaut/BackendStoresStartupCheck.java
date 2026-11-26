package org.streamrune.micronaut;

import io.micronaut.context.BeanProvider;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import jakarta.inject.Singleton;
import javax.sql.DataSource;
import org.streamrune.core.backend.BackendStores;

/**
 * Opens the backend's stores when the application starts, whenever a {@code DataSource} exists.
 * Micronaut creates singletons on first use, so without this an application that runs no projection
 * or saga (commands only) opened them at its first command. Opening runs the backend's startup
 * checks: on SQLite the DataSource must be a SQLite database with the required connection setup,
 * and the instance guard of the file is taken, so another process on the same file fails the start.
 * On Postgres, opening touches no database.
 *
 * <p>Ordered after the configuration validators (900, 1000) and before the listeners that start the
 * runners and sweepers (1100): a refused start has started none of them. Micronaut creates every
 * startup listener before it calls the first, and creating {@code StreamRuneLifecycle} builds the
 * inbox retention sweeper over the framework's inbox, which opens the stores too; this check does
 * not depend on that.
 */
@Singleton
@Requires(beans = DataSource.class)
final class BackendStoresStartupCheck implements ApplicationEventListener<StartupEvent>, Ordered {

  private final BeanProvider<BackendStores> stores;

  BackendStoresStartupCheck(BeanProvider<BackendStores> stores) {
    this.stores = stores;
  }

  @Override
  public int getOrder() {
    return 1050;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    stores.get();
  }
}
