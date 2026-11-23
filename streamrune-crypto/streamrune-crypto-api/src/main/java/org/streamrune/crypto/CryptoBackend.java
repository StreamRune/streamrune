package org.streamrune.crypto;

import java.util.function.Supplier;
import javax.sql.DataSource;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;

/**
 * The crypto half of a storage backend: the parts of GDPR crypto-shredding that live in the
 * selected backend's database. Its store half is {@link
 * org.streamrune.core.backend.StreamRuneBackend}; this half lives in {@code streamrune-crypto-api}
 * because {@link ForgottenSubjectStore} and {@link CryptoForgetSignal} live here and core does not
 * depend on this module.
 *
 * <p>Every crypto engine takes what it keeps in the database from the selected backend's {@code
 * CryptoBackend}: the filesystem, HashiCorp Vault and AWS KMS engines take their {@linkplain
 * #forgottenSubjects(DataSource) forgotten-subject store} (Vault and KMS) and their {@linkplain
 * #forgetSignal(DataSource) forget signal} from it, and the backend's own key-store engine comes
 * from {@link #keyStoreEngine(DataSource, KeyStoreEngineSettings)}. So with a non-Postgres backend
 * selected, Vault and KMS keep their keys remotely and their forget records in that backend's
 * database.
 *
 * <p>An integration asks for a {@code CryptoBackend} only when a crypto engine is configured; an
 * application without crypto never creates one, and a backend module whose crypto half is a
 * separate artifact is then not needed on the class path.
 *
 * <p><b>Why {@link #keyStoreEngine} returns a built engine.</b> The three integrations configure
 * the backend's own key-store engine with three things only: the {@code DataSource}, the key-table
 * name ({@code streamrune.crypto.postgres.table-name}) and the metrics. They wrap the engine in a
 * {@link CachedCryptoEngine} themselves, with the cache settings and the {@link
 * #forgetSignal(DataSource) forget signal}, outside the engine's builder. A backend-neutral {@link
 * KeyStoreEngineSettings} record carries those settings, and the backend turns it into a {@link
 * CryptoEngine}. The integrations so name no backend type and need no cast. Returning the backend's
 * builder instead would need either a builder interface every backend builder implements, for three
 * setters, or a type parameter that makes the integrations name the backend's builder type.
 *
 * <p>Implementations are stateless factories: each call builds a new instance over the given {@code
 * DataSource}, and the caller owns and closes what it gets.
 */
public interface CryptoBackend {

  /**
   * The backend's name, equal to {@code StreamRuneBackend.name()} of the store half it belongs to,
   * for example {@code "postgres"}.
   *
   * @return the backend name; never blank
   */
  String name();

  /**
   * A durable forgotten-subject store in this backend's database, for the engines whose erasure is
   * not terminal by itself (Vault, AWS KMS). It tracks key-creation evidence and the first-forget
   * instant, as {@link ForgottenSubjectStore} describes.
   *
   * @param dataSource the application's {@code DataSource} on this backend's database
   * @return a new store over {@code dataSource}
   * @throws IllegalArgumentException if {@code dataSource} is {@code null}
   */
  ForgottenSubjectStore forgottenSubjects(DataSource dataSource);

  /**
   * The cross-instance forget signal this backend offers, for the {@link CachedCryptoEngine} of
   * every crypto engine. A backend whose listening side needs a dedicated connection opens it from
   * {@code dataSource} itself; when it cannot read how to, see {@link #forgetSignal(DataSource,
   * Supplier)}.
   *
   * @param dataSource the application's {@code DataSource} on this backend's database
   * @return a new signal over {@code dataSource}; the caller closes it
   * @throws IllegalArgumentException if {@code dataSource} is {@code null}
   */
  CryptoForgetSignal forgetSignal(DataSource dataSource);

  /**
   * As {@link #forgetSignal(DataSource)}, with the factory of the dedicated {@code DataSource} the
   * signal listens on, for a {@code DataSource} the backend cannot open that connection from, such
   * as a pool that keeps its URL in a configuration object (the Quarkus integration passes one for
   * Agroal). The factory runs at most once, when the signal first starts listening; the signal owns
   * what it returns. A backend whose signal holds no dedicated connection never calls it, and
   * returns what {@link #forgetSignal(DataSource)} returns.
   *
   * @param dataSource the application's {@code DataSource} on this backend's database
   * @param listenDataSourceFactory opens the dedicated pool the signal listens on
   * @return a new signal over {@code dataSource}; the caller closes it
   * @throws IllegalArgumentException if either argument is {@code null}
   */
  CryptoForgetSignal forgetSignal(
      DataSource dataSource, Supplier<? extends DataSource> listenDataSourceFactory);

  /**
   * This backend's own key-store engine: per-subject keys and tombstones kept in this backend's
   * database, built from {@code settings}. Enabled by {@code streamrune.crypto.<backend>.enabled}.
   * The engine is not cached; the integration wraps it in a {@link CachedCryptoEngine} when the
   * cache is enabled.
   *
   * @param dataSource the application's {@code DataSource} on this backend's database
   * @param settings the key-table name and metrics
   * @return a new engine over {@code dataSource}
   * @throws IllegalArgumentException if an argument is {@code null}, or the table name is not a
   *     valid identifier for this backend
   */
  CryptoEngine keyStoreEngine(DataSource dataSource, KeyStoreEngineSettings settings);

  /**
   * What {@link #keyStoreEngine(DataSource, KeyStoreEngineSettings)} builds the key-store engine
   * with.
   *
   * @param tableName the key table's name, or {@code null} for the backend's default key table; a
   *     blank name also means the default and is stored as {@code null}. Any other value is passed
   *     on unchanged: the backend validates it as an identifier of its database and applies that
   *     database's case folding
   * @param metrics counts discarded encryptions and retried decryptions; not {@code null}, {@link
   *     StreamRuneMetrics#NOOP} for none
   */
  record KeyStoreEngineSettings(String tableName, StreamRuneMetrics metrics) {

    /**
     * Validates the settings and maps a blank table name to {@code null}.
     *
     * @throws IllegalArgumentException if {@code metrics} is {@code null}
     */
    public KeyStoreEngineSettings {
      if (metrics == null) {
        throw new IllegalArgumentException(
            "metrics is required (pass StreamRuneMetrics.NOOP for none)");
      }
      if (tableName != null && tableName.isBlank()) {
        tableName = null;
      }
    }
  }
}
