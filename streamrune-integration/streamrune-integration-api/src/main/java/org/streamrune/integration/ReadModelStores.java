package org.streamrune.integration;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.streamrune.core.Internal;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;

/**
 * The read-model stores of one application, as each framework integration collects them into one
 * bean: the application's own {@link ReadModelStore} beans, and the default store unless one of
 * them is named {@value ProjectionStoreRouting#DEFAULT_STORE} ({@link
 * ProjectionStoreRouting#selectStores}). The selection — and its validation — runs once, the first
 * time {@link #stores()} is asked for, so the bean itself is created for free and a store is
 * created only when something routes to the stores or needs the default repository.
 *
 * <p>Everything in an integration that reads the stores reads them here: the {@link
 * ProjectionRoutes} its runners and index declaration share, and the injectable {@code
 * ProjectionRepository} bean ({@link #defaultRepository()}).
 *
 * @hidden
 */
@Internal
public final class ReadModelStores {

  private final Supplier<Optional<ReadModelStore>> defaultStore;
  private final Supplier<List<ReadModelStore>> applicationStores;
  private List<ReadModelStore> selected;

  /**
   * The stores over two suppliers, neither called yet.
   *
   * @param defaultStore creates the default store, or supplies none (no {@code DataSource}); called
   *     only when no application store is named {@value ProjectionStoreRouting#DEFAULT_STORE}
   * @param applicationStores the application's own stores, without the default store; creating them
   *     is how their names become known
   * @throws NullPointerException if an argument is {@code null}
   */
  public ReadModelStores(
      Supplier<Optional<ReadModelStore>> defaultStore,
      Supplier<List<ReadModelStore>> applicationStores) {
    this.defaultStore = Objects.requireNonNull(defaultStore, "defaultStore is required");
    this.applicationStores =
        Objects.requireNonNull(applicationStores, "applicationStores is required");
  }

  /**
   * The selected stores: the default store (when kept) followed by the application's. Selected and
   * validated on the first call; a failure is thrown on every call, never remembered.
   *
   * @return the validated stores, possibly empty
   * @throws IllegalStateException on a duplicate name or write-target identity
   */
  public synchronized List<ReadModelStore> stores() {
    if (selected == null) {
      List<ReadModelStore> application =
          Objects.requireNonNull(applicationStores.get(), "applicationStores must supply a list");
      selected = ProjectionStoreRouting.selectStores(defaultStore, application);
    }
    return selected;
  }

  /**
   * The repository of the store named {@value ProjectionStoreRouting#DEFAULT_STORE} — the default
   * store's, or the application store's that replaced it: the integrations' injectable {@code
   * ProjectionRepository} bean.
   *
   * @return that store's repository
   * @throws IllegalStateException when the store set is invalid, or no store has that name
   */
  public ProjectionRepository defaultRepository() {
    return ProjectionStoreRouting.defaultRepository(stores(), ProjectionStoreRouting.DEFAULT_STORE);
  }
}
