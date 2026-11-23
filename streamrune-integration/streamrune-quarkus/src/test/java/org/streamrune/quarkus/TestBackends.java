package org.streamrune.quarkus;

import java.time.Clock;
import javax.sql.DataSource;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.crypto.CryptoBackend;
import org.streamrune.crypto.postgres.PostgresCryptoBackend;
import org.streamrune.postgres.PostgresBackend;

/**
 * The backend the producers take, for tests that call a producer method directly: the Postgres
 * backend's stores opened the way {@link StreamRuneProducers#streamRuneBackendStores} opens them,
 * and its crypto backend.
 */
final class TestBackends {

  /** The crypto backend the crypto producers take on PostgreSQL. */
  static final CryptoBackend CRYPTO = new PostgresCryptoBackend();

  private TestBackends() {}

  /** The Postgres stores on {@code dataSource}, without a crypto engine. */
  static BackendStores stores(DataSource dataSource) {
    return stores(dataSource, null);
  }

  /** The Postgres stores on {@code dataSource}, with {@code cryptoEngine} (or none). */
  static BackendStores stores(DataSource dataSource, CryptoEngine cryptoEngine) {
    return new PostgresBackend()
        .open(
            new BackendContext(
                dataSource,
                cryptoEngine,
                Clock.systemUTC(),
                StreamRuneMetrics.NOOP,
                StreamRuneTracing.NOOP,
                true,
                true,
                true));
  }
}
