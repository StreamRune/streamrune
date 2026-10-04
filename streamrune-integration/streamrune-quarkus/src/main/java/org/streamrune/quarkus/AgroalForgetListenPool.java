package org.streamrune.quarkus;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ConnectionValidator;
import io.agroal.api.configuration.supplier.AgroalConnectionFactoryConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import java.sql.SQLException;
import java.time.Duration;
import javax.sql.DataSource;

/**
 * Opens the LISTEN pool of a {@link org.streamrune.crypto.postgres.PostgresCryptoForgetSignal} for
 * the datasource quarkus-agroal injects.
 *
 * <p>Agroal keeps its JDBC URL and credentials in a configuration object, not behind the {@code
 * getJdbcUrl()}/{@code getUrl()} accessors the signal reads, so the signal cannot open its LISTEN
 * connection from an Agroal datasource on its own. This class copies the datasource's whole
 * connection-factory configuration — JDBC URL, driver, user and password or credentials provider,
 * JDBC properties, initial SQL — into a separate Agroal pool of one connection: the LISTEN
 * connection connects exactly as the application's connections do and never occupies one of the
 * application pool's slots.
 *
 * <p>The pool opens no connection when it is created, so a database that is unreachable at startup
 * delays the LISTEN instead of disabling it: the signal's listen loop keeps retrying. It validates
 * a connection when it is borrowed, so after the LISTEN connection breaks (a failover, an idle
 * session kill) the loop gets a fresh connection rather than the broken one back. It has no
 * transaction integration and no maximum lifetime, idle reaping or leak detection, none of which
 * apply to a connection held open for the life of the application.
 *
 * <p>Only reached once {@link StreamRuneCryptoProducers} has seen that the {@code DataSource}
 * implements {@code io.agroal.api.AgroalDataSource}, so an application without Agroal on its
 * classpath never loads this class.
 */
final class AgroalForgetListenPool {

  /** How long the listen loop waits for the pool's connection before backing off and retrying. */
  private static final Duration ACQUISITION_TIMEOUT = Duration.ofSeconds(5);

  private AgroalForgetListenPool() {}

  /**
   * Opens a pool of one connection with the connection settings of {@code dataSource}.
   *
   * @param dataSource an {@code AgroalDataSource} (or a client proxy of one)
   * @return the dedicated pool; the forget signal closes it when it is closed
   * @throws IllegalStateException if Agroal refuses to create the pool
   */
  static DataSource open(DataSource dataSource) {
    AgroalConnectionFactoryConfiguration connectionSettings =
        ((AgroalDataSource) dataSource)
            .getConfiguration()
            .connectionPoolConfiguration()
            .connectionFactoryConfiguration();
    var config =
        new AgroalDataSourceConfigurationSupplier()
            .connectionPoolConfiguration(
                pool ->
                    pool.initialSize(0)
                        .minSize(0)
                        .maxSize(1)
                        .acquisitionTimeout(ACQUISITION_TIMEOUT)
                        .validateOnBorrow(true)
                        .connectionValidator(ConnectionValidator.defaultValidator())
                        .connectionFactoryConfiguration(
                            new AgroalConnectionFactoryConfigurationSupplier(connectionSettings)));
    try {
      return AgroalDataSource.from(config.get());
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Cannot create the crypto-forget LISTEN pool from the Agroal datasource's connection"
              + " settings",
          e);
    }
  }
}
