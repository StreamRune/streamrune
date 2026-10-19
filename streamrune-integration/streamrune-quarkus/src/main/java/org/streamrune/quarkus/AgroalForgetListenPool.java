package org.streamrune.quarkus;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ConnectionValidator;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.AgroalSecurityProvider;
import java.security.Principal;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.streamrune.crypto.postgres.PostgresCryptoForgetSignal;

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
 * application pool's slots. Its sessions carry their own {@code application_name} — the
 * application's, if it sets one, followed by {@code -streamrune-crypto-forget-listen} — so that
 * {@code pg_stat_activity} tells them from the application's, whether the datasource connects
 * through the driver or is a {@code DataSource} class such as an XA datasource. The TCP keepalive
 * of its sockets is on, unless the datasource's JDBC properties or URL say otherwise. The
 * application's own configuration is never changed: the LISTEN pool gets its own URL and its own
 * copy of the JDBC properties.
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
    String jdbcUrl = connectionSettings.jdbcUrl();
    String listenName =
        PostgresCryptoForgetSignal.listenApplicationName(
            jdbcUrl, connectionSettings.jdbcProperties().getProperty("ApplicationName"));
    // The name goes into the URL, which the driver lets win over a property when Agroal connects
    // through the driver, and into the properties, which Agroal sets after the URL when its
    // connection provider is a DataSource class (an XA datasource): the application's own
    // ApplicationName property would otherwise name the LISTEN session after all.
    String listenUrl =
        jdbcUrl == null
            ? null
            : PostgresCryptoForgetSignal.withApplicationName(jdbcUrl, listenName);
    // A private copy: the application's own configuration shares its properties with anything built
    // from it, and must never change.
    Properties listenProperties = new Properties();
    for (String name : connectionSettings.jdbcProperties().stringPropertyNames()) {
      listenProperties.setProperty(name, connectionSettings.jdbcProperties().getProperty(name));
    }
    listenProperties.setProperty("ApplicationName", listenName);
    // The driver's own default is off; a silent middlebox then drops an idle LISTEN connection
    // without a word. The liveness probe finds that within seconds — the keepalive helps the
    // operating system and the middlebox itself keep the connection.
    if (!listenProperties.containsKey("tcpKeepAlive")
        && (jdbcUrl == null || !jdbcUrl.contains("tcpKeepAlive="))) {
      listenProperties.setProperty("tcpKeepAlive", "true");
    }
    AgroalConnectionFactoryConfiguration listenSettings =
        new ListenSettings(connectionSettings, listenUrl, listenProperties);
    Supplier<AgroalConnectionFactoryConfiguration> listenFactory = () -> listenSettings;
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
                        .connectionFactoryConfiguration(listenFactory));
    try {
      return AgroalDataSource.from(config.get());
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Cannot create the crypto-forget LISTEN pool from the Agroal datasource's connection"
              + " settings",
          e);
    }
  }

  /**
   * The application's connection-factory configuration with its own JDBC URL and JDBC properties.
   * Written out rather than built on Agroal's configuration supplier, which hands the application's
   * property map on as it is; a new method of the interface fails this class's compilation instead
   * of being dropped silently.
   */
  private record ListenSettings(
      AgroalConnectionFactoryConfiguration base, String jdbcUrl, Properties jdbcProperties)
      implements AgroalConnectionFactoryConfiguration {

    @Override
    public boolean autoCommit() {
      return base.autoCommit();
    }

    @Override
    public boolean readOnly() {
      return base.readOnly();
    }

    @Override
    public boolean trackJdbcResources() {
      return base.trackJdbcResources();
    }

    @Override
    public Duration loginTimeout() {
      return base.loginTimeout();
    }

    @Override
    public Duration networkTimeout() {
      return base.networkTimeout();
    }

    @Override
    public String initialSql() {
      return base.initialSql();
    }

    @Override
    public Class<?> connectionProviderClass() {
      return base.connectionProviderClass();
    }

    @Override
    public IsolationLevel jdbcTransactionIsolation() {
      return base.jdbcTransactionIsolation();
    }

    @Override
    public Collection<AgroalSecurityProvider> securityProviders() {
      return base.securityProviders();
    }

    @Override
    public Principal principal() {
      return base.principal();
    }

    @Override
    public Collection<Object> credentials() {
      return base.credentials();
    }

    @Override
    public boolean poolRecovery() {
      return base.poolRecovery();
    }

    @Override
    public Principal recoveryPrincipal() {
      return base.recoveryPrincipal();
    }

    @Override
    public Collection<Object> recoveryCredentials() {
      return base.recoveryCredentials();
    }

    @Override
    public Properties xaProperties() {
      return base.xaProperties();
    }
  }
}
