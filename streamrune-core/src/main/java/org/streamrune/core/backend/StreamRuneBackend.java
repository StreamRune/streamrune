package org.streamrune.core.backend;

import javax.sql.DataSource;

/**
 * A storage technology StreamRune can run on, chosen by the {@code streamrune.backend} property.
 *
 * <p>A backend is stateless: {@link #verify(DataSource)} checks that a {@code DataSource} is this
 * backend's database, and {@link #open(BackendContext)} builds the {@link BackendStores} bundle
 * over it. An application opens one bundle per {@code DataSource} and closes it on shutdown.
 */
public interface StreamRuneBackend {

  /**
   * The value of {@code streamrune.backend} that selects this backend, also the name of the
   * backend's default read-model store ({@link BackendStores#defaultReadModelStore()}).
   *
   * @return the backend's name, for example {@code "postgres"} or {@code "sqlite"}; never blank
   */
  String name();

  /**
   * Fails fast when {@code dataSource} is not this backend's database: the product, its version, or
   * the connection setup this backend needs.
   *
   * @param dataSource the application's {@code DataSource}; not {@code null}
   * @throws IllegalStateException if the database is not one this backend runs on; the message
   *     names what the backend expects and what the database is
   */
  void verify(DataSource dataSource);

  /**
   * Builds the store bundle for one {@code DataSource}. Opening touches no table: every store is
   * built, and reaches the database, only when its accessor is first called.
   *
   * @param context the {@code DataSource} and the shared settings the stores are built with; not
   *     {@code null}
   * @return the bundle; the caller closes it when the application stops
   */
  BackendStores open(BackendContext context);
}
