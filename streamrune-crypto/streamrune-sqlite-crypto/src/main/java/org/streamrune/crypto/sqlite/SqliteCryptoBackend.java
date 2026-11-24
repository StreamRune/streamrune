package org.streamrune.crypto.sqlite;

import java.util.function.Function;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CryptoBackend;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.crypto.ForgottenSubjectStore;
import org.streamrune.sqlite.SqliteDatabase;

/**
 * The crypto half of the SQLite backend.
 *
 * <ul>
 *   <li>{@link #forgottenSubjects(DataSource)}: a {@link SqliteForgottenSubjectStore};
 *   <li>{@link #forgetSignal(DataSource)} and {@link #forgetSignal(DataSource, Supplier)}: an
 *       {@link InProcessCryptoForgetSignal}, which holds no listening connection, so the factory is
 *       never called;
 *   <li>{@link #keyStoreEngine(DataSource, CryptoBackend.KeyStoreEngineSettings)}: a {@link
 *       SqliteCryptoEngine} built with the settings' table name ({@code null} = {@value
 *       SqliteCryptoEngine#DEFAULT_TABLE_NAME}; SQLite has no table-name property, so a rejected
 *       name is reported by its value alone) and metrics.
 * </ul>
 *
 * <p><b>What each call holds.</b> Each call opens a {@link SqliteDatabase} on the data source for
 * the call only: it checks the connection settings and that no other process holds the file's
 * instance guard, builds the product over the data source and closes the handle before it returns.
 * The products hold nothing that needs releasing: the store and the engine borrow a connection per
 * operation, and the signal keeps only the file's path. The instance guard stays held, for the
 * whole run, by the handle of the backend's store half, which the application opens on the same
 * file. Each call builds a new instance and touches no table.
 */
public final class SqliteCryptoBackend implements CryptoBackend {

  /** Creates the backend; it holds no state. */
  public SqliteCryptoBackend() {}

  @Override
  public String name() {
    return "sqlite";
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException if the data source is not a usable SQLite database (see {@link
   *     SqliteDatabase#open})
   */
  @Override
  public ForgottenSubjectStore forgottenSubjects(DataSource dataSource) {
    return onDatabase(dataSource, SqliteForgottenSubjectStore::new);
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException if the data source is not a usable SQLite database (see {@link
   *     SqliteDatabase#open})
   */
  @Override
  public CryptoForgetSignal forgetSignal(DataSource dataSource) {
    return onDatabase(dataSource, InProcessCryptoForgetSignal::new);
  }

  /**
   * {@inheritDoc}
   *
   * <p>The signal holds no listening connection, so {@code listenDataSourceFactory} is never
   * called: this returns what {@link #forgetSignal(DataSource)} returns.
   *
   * @throws IllegalStateException if the data source is not a usable SQLite database (see {@link
   *     SqliteDatabase#open})
   */
  @Override
  public CryptoForgetSignal forgetSignal(
      DataSource dataSource, Supplier<? extends DataSource> listenDataSourceFactory) {
    requireDataSource(dataSource);
    if (listenDataSourceFactory == null) {
      throw new IllegalArgumentException("listenDataSourceFactory is required");
    }
    return forgetSignal(dataSource);
  }

  /**
   * {@inheritDoc}
   *
   * <p>A rejected table name is reported with its value; SQLite has no table-name property.
   *
   * @throws IllegalStateException if the data source is not a usable SQLite database (see {@link
   *     SqliteDatabase#open})
   */
  @Override
  public CryptoEngine keyStoreEngine(DataSource dataSource, KeyStoreEngineSettings settings) {
    requireDataSource(dataSource);
    if (settings == null) {
      throw new IllegalArgumentException("settings is required");
    }
    return onDatabase(
        dataSource,
        database ->
            SqliteCryptoEngine.builder()
                .database(database)
                .tableName(settings.tableName())
                .metrics(settings.metrics())
                .build());
  }

  /**
   * Builds a product over a handle opened for this call only: the open checks the data source and
   * the instance guard, and the handle is closed before the product is returned.
   */
  private static <T> T onDatabase(DataSource dataSource, Function<SqliteDatabase, T> build) {
    try (SqliteDatabase database = SqliteDatabase.open(requireDataSource(dataSource))) {
      return build.apply(database);
    }
  }

  private static DataSource requireDataSource(DataSource dataSource) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    return dataSource;
  }
}
