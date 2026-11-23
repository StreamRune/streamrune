package org.streamrune.crypto.postgres;

import java.util.function.Supplier;
import javax.sql.DataSource;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CryptoBackend;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.crypto.ForgottenSubjectStore;

/**
 * The crypto half of the PostgreSQL backend: it builds the Postgres crypto classes the Spring,
 * Quarkus and Micronaut integrations built before the backend existed, with the same arguments.
 *
 * <ul>
 *   <li>{@link #forgottenSubjects(DataSource)}: a {@link JdbcForgottenSubjectStore};
 *   <li>{@link #forgetSignal(DataSource)}: a {@link PostgresCryptoForgetSignal} that opens its own
 *       LISTEN pool from the DataSource's URL;
 *   <li>{@link #forgetSignal(DataSource, Supplier)}: a {@link PostgresCryptoForgetSignal} that
 *       listens on the pool the factory returns (Quarkus passes one for Agroal);
 *   <li>{@link #keyStoreEngine(DataSource, CryptoBackend.KeyStoreEngineSettings)}: a {@link
 *       PostgresCryptoEngine} built with the settings' table name ({@code null} = {@value
 *       PostgresCryptoEngine#DEFAULT_TABLE_NAME}) and metrics.
 * </ul>
 *
 * <p>Each call builds a new instance and touches no table. The store and the engine hold nothing
 * that needs releasing: they borrow a connection from the caller's DataSource per operation. The
 * forget signal is the caller's to close.
 */
public final class PostgresCryptoBackend implements CryptoBackend {

  /** Creates the backend; it holds no state. */
  public PostgresCryptoBackend() {}

  @Override
  public String name() {
    return "postgres";
  }

  @Override
  public ForgottenSubjectStore forgottenSubjects(DataSource dataSource) {
    return new JdbcForgottenSubjectStore(requireDataSource(dataSource));
  }

  @Override
  public CryptoForgetSignal forgetSignal(DataSource dataSource) {
    return new PostgresCryptoForgetSignal(requireDataSource(dataSource));
  }

  @Override
  public CryptoForgetSignal forgetSignal(
      DataSource dataSource, Supplier<? extends DataSource> listenDataSourceFactory) {
    requireDataSource(dataSource);
    if (listenDataSourceFactory == null) {
      throw new IllegalArgumentException("listenDataSourceFactory is required");
    }
    return new PostgresCryptoForgetSignal(dataSource, listenDataSourceFactory);
  }

  /**
   * {@inheritDoc}
   *
   * <p>A table name the engine refuses is reported with the message naming {@code
   * streamrune.crypto.postgres.table-name}.
   */
  @Override
  public CryptoEngine keyStoreEngine(DataSource dataSource, KeyStoreEngineSettings settings) {
    requireDataSource(dataSource);
    if (settings == null) {
      throw new IllegalArgumentException("settings is required");
    }
    return PostgresCryptoEngine.builder()
        .dataSource(dataSource)
        .tableName(settings.tableName())
        .metrics(settings.metrics())
        .build();
  }

  private static DataSource requireDataSource(DataSource dataSource) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    return dataSource;
  }
}
