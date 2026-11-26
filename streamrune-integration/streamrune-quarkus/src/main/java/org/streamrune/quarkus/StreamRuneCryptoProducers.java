package org.streamrune.quarkus;

import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.backend.StreamRuneBackend;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.CryptoBackend;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.crypto.aws.AwsKmsCryptoEngine;
import org.streamrune.crypto.filesystem.FileSystemCryptoEngine;
import org.streamrune.crypto.postgres.PostgresCryptoBackend;
import org.streamrune.crypto.vault.VaultCryptoEngine;
import org.streamrune.integration.BackendSelection;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;

/**
 * Quarkus CDI producers for crypto engine auto-configuration. Each backend is gated at build time
 * by the documented config key {@code streamrune.crypto.<backend>.enabled=true} — exactly one
 * backend should be enabled; enabling several creates an ambiguous {@link CryptoEngine} dependency.
 *
 * <p>Engines are singletons and fail fast at startup with an explicit message when a required
 * setting (Vault token, AWS region / KMS key id) is missing, instead of silently producing no
 * engine.
 *
 * <h2>Why one nested producer class per backend</h2>
 *
 * <p>This class itself declares no beans. Each backend's {@code @Produces} methods and the
 * {@code @Disposes} methods that tear their products down live together in a nested class carrying
 * that backend's {@code @IfBuildProperty} <b>on the class</b>, so a producer and its disposer can
 * only appear and disappear together.
 *
 * <p>They must, because a build-time condition on a producer <em>method</em> does not reach its
 * disposer. {@code @IfBuildProperty} is resolved by {@code BuildTimeEnabledProcessor}, which marks
 * a disabled declaration {@code @Vetoed} (class) or {@code @io.quarkus.arc.VetoedProducer} (method
 * or field); Arc's {@code BeanDeployment#findBeans} honours {@code @VetoedProducer} only where it
 * collects producer methods and producer fields, and the loop that collects {@code @Disposes}
 * methods tests for {@code @Disposes} alone. With the conditions on the methods, disabling a
 * backend therefore deleted its producer and kept its disposer, and Arc <b>failed augmentation</b>
 * — a build failure, not a startup failure — with "No producer method or field declared by the bean
 * class that is assignable to the disposed parameter of a disposer method". Every crypto backend is
 * off by default, so this broke the build of every Quarkus application that did not enable all of
 * them. Adding the same {@code @IfBuildProperty} to each disposer does <b>not</b> fix it: it marks
 * a method Arc never inspects for that marker. Only the class-level gate does, because a vetoed
 * class stops being a bean class before any of its methods are examined.
 *
 * <p>Spring and Micronaut have no equivalent exposure: Spring puts each backend in its own
 * {@code @ConditionalOnProperty @Configuration} class and infers {@code destroyMethod} on the
 * {@code @Bean} itself, and Micronaut writes {@code @Bean(preDestroy = "close")} on the very bean
 * definition that carries {@code @Requires}. Only CDI makes teardown a separate declaration that
 * can outlive what it tears down.
 */
public class StreamRuneCryptoProducers {

  private static final Logger log = LoggerFactory.getLogger(StreamRuneCryptoProducers.class);

  /** Creates the shared build logic holder. Not a bean — see the class javadoc. */
  public StreamRuneCryptoProducers() {}

  /**
   * Closes a produced {@link CryptoEngine} if it is {@link AutoCloseable}. The produced engine is a
   * {@link CachedCryptoEngine} (AutoCloseable) whenever the cache is enabled, and closing it stops
   * its {@link CryptoForgetSignal}'s {@code LISTEN} connection, dedicated LISTEN pool (+
   * housekeeping thread) and listener virtual thread; an uncached delegate that is not
   * AutoCloseable is left untouched. Shared by every backend's disposer so the four cannot drift.
   */
  static void closeEngine(CryptoEngine engine) {
    if (engine instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        log.warn("Failed to close the CryptoEngine on context shutdown", e);
      }
    }
  }

  /**
   * The crypto half of the storage backend: the {@link CryptoBackend} the crypto engines take their
   * forgotten-subject store, forget signal and key-store engine from. It follows the {@link
   * StreamRuneBackend} bean ({@code streamrune.backend}): {@link PostgresCryptoBackend} for {@code
   * postgres}, and for {@code sqlite} {@code org.streamrune.crypto.sqlite.SqliteCryptoBackend},
   * loaded by name ({@link BackendSelection#resolveCrypto}); without the module {@code
   * org.streamrune:streamrune-sqlite-crypto} the start fails naming it. The key-store engine of the
   * other backend ({@code streamrune.crypto.postgres.enabled} on {@code sqlite}, {@code
   * streamrune.crypto.sqlite.enabled} on {@code postgres}) fails the start here too.
   *
   * <p>Not gated: CDI has no build-time condition for "any of the four engines is enabled". Only
   * the engine producers inject it, so a Quarkus application (which removes unused beans, {@code
   * quarkus.arc.remove-unused-beans=all} by default) has the bean only when a crypto engine is
   * enabled; elsewhere it is an inert factory that nothing creates.
   */
  @ApplicationScoped
  public static class CryptoBackendProducer {

    /** Creates the producer. */
    public CryptoBackendProducer() {}

    /**
     * The crypto backend of {@code backend}.
     *
     * @param backend the storage backend
     * @return its crypto backend
     * @throws IllegalStateException if the backend has no crypto backend in this integration
     */
    @Produces
    @Singleton
    public CryptoBackend streamRuneCryptoBackend(
        StreamRuneBackend backend, StreamRuneQuarkusCryptoProperties props) {
      return cryptoBackendOf(backend, props);
    }
  }

  /**
   * The crypto backend of {@code backend}, selected by its name: {@link PostgresCryptoBackend} for
   * {@code postgres}, the SQLite crypto backend loaded by name for {@code sqlite}. Package-private
   * for direct testing.
   *
   * @throws IllegalStateException if the key-store engine of another backend is enabled, or the
   *     SQLite crypto backend cannot be loaded
   */
  static CryptoBackend cryptoBackendOf(
      StreamRuneBackend backend, StreamRuneQuarkusCryptoProperties props) {
    org.streamrune.integration.BackendStartup.requireKeyStoreEngineOfBackend(
        backend.name(), props.postgres().enabled(), props.sqlite().enabled());
    return BackendSelection.resolveCrypto(
        backend.name(), CryptoBackend.class, CryptoBackend::name, PostgresCryptoBackend::new);
  }

  /**
   * File-system backend: its {@link CryptoEngine} producer and that engine's disposer, gated
   * together on {@code streamrune.crypto.filesystem.enabled}.
   */
  @ApplicationScoped
  @IfBuildProperty(name = "streamrune.crypto.filesystem.enabled", stringValue = "true")
  public static class FileSystemCryptoProducer {

    private final StreamRuneCryptoProducers build = new StreamRuneCryptoProducers();

    /** Creates the producer. */
    public FileSystemCryptoProducer() {}

    /** Creates a file-system-backed {@link CryptoEngine}. */
    @Produces
    @Singleton
    public CryptoEngine fileSystemCryptoEngine(
        CryptoBackend cryptoBackend,
        Instance<DataSource> dataSource,
        StreamRuneQuarkusCryptoProperties props,
        Instance<StreamRuneMetrics> metrics) {
      return build.buildFileSystemCryptoEngine(
          cryptoBackend,
          dataSource.isResolvable() ? dataSource.get() : null,
          props,
          resolvedOrNoop(metrics));
    }

    /** Disposer for {@link #fileSystemCryptoEngine}. */
    public void disposeCryptoEngine(@Disposes CryptoEngine engine) {
      closeEngine(engine);
    }
  }

  /**
   * PostgreSQL backend: its {@link CryptoEngine} producer and that engine's disposer, gated
   * together on {@code streamrune.crypto.postgres.enabled}.
   */
  @ApplicationScoped
  @IfBuildProperty(name = "streamrune.crypto.postgres.enabled", stringValue = "true")
  public static class PostgresCryptoProducer {

    private final StreamRuneCryptoProducers build = new StreamRuneCryptoProducers();

    /** Creates the producer. */
    public PostgresCryptoProducer() {}

    /** Creates a PostgreSQL-backed {@link CryptoEngine}. */
    @Produces
    @Singleton
    public CryptoEngine postgresCryptoEngine(
        CryptoBackend cryptoBackend,
        DataSource dataSource,
        StreamRuneQuarkusCryptoProperties props,
        Instance<StreamRuneMetrics> metrics) {
      return build.buildPostgresCryptoEngine(
          cryptoBackend, dataSource, props, resolvedOrNoop(metrics));
    }

    /** Disposer for {@link #postgresCryptoEngine}. */
    public void disposeCryptoEngine(@Disposes CryptoEngine engine) {
      closeEngine(engine);
    }
  }

  /**
   * SQLite backend: its {@link CryptoEngine} producer and that engine's disposer, gated together on
   * {@code streamrune.crypto.sqlite.enabled}. The engine is the SQLite crypto backend's key store
   * ({@code org.streamrune:streamrune-sqlite-crypto}) in the application's SQLite database; the
   * integration names no SQLite type.
   */
  @ApplicationScoped
  @IfBuildProperty(name = "streamrune.crypto.sqlite.enabled", stringValue = "true")
  public static class SqliteCryptoProducer {

    private final StreamRuneCryptoProducers build = new StreamRuneCryptoProducers();

    /** Creates the producer. */
    public SqliteCryptoProducer() {}

    /** Creates a SQLite-backed {@link CryptoEngine}. */
    @Produces
    @Singleton
    public CryptoEngine sqliteCryptoEngine(
        CryptoBackend cryptoBackend,
        DataSource dataSource,
        StreamRuneQuarkusCryptoProperties props,
        Instance<StreamRuneMetrics> metrics) {
      return build.buildSqliteCryptoEngine(
          cryptoBackend, dataSource, props, resolvedOrNoop(metrics));
    }

    /** Disposer for {@link #sqliteCryptoEngine}. */
    public void disposeCryptoEngine(@Disposes CryptoEngine engine) {
      closeEngine(engine);
    }
  }

  /**
   * HashiCorp Vault backend: the container-managed {@link HttpClient} holder, the {@link
   * CryptoEngine} that uses it, and both disposers — gated together on {@code
   * streamrune.crypto.vault.enabled}.
   */
  @ApplicationScoped
  @IfBuildProperty(name = "streamrune.crypto.vault.enabled", stringValue = "true")
  public static class VaultCryptoProducer {

    private final StreamRuneCryptoProducers build = new StreamRuneCryptoProducers();

    /** Creates the producer. */
    public VaultCryptoProducer() {}

    /**
     * Produces the container-managed {@link HttpClient} the Vault engine uses, wrapped in a
     * StreamRune-owned holder type. Registering it as a bean — mirroring Spring's {@code
     * streamRuneVaultHttpClient} bean — lets Arc close it via {@link
     * #closeVaultHttpClient(StreamRuneVaultHttpClient)} on context shutdown (dev-mode live reload,
     * {@code @QuarkusTest} multi-context suites), instead of leaking a JDK HttpClient (selector
     * thread + connection pool) built inline in the producer and owned by nobody.
     *
     * <p>The holder exists because producing a bare {@code java.net.http.HttpClient} put the
     * framework into by-type resolution of a type applications routinely produce themselves: an app
     * with its own {@code @Produces HttpClient} made this producer's own consumer ambiguous and
     * failed the Quarkus <em>build</em> with {@code AmbiguousResolutionException} — the app's only
     * symptom being that setting {@code streamrune.crypto.vault.enabled=true} stopped its build.
     * Wrapping keeps the shutdown-close property while registering no bean of the ubiquitous type
     * at all. A {@code @DefaultBean} would be the wrong direction: it would silently route Vault
     * traffic, bearer token included, over an arbitrary application client.
     */
    @Produces
    @Singleton
    public StreamRuneVaultHttpClient vaultHttpClient() {
      return new StreamRuneVaultHttpClient(HttpClient.newHttpClient());
    }

    /** Closes the container-managed Vault {@link HttpClient} on shutdown. */
    public void closeVaultHttpClient(@Disposes StreamRuneVaultHttpClient httpClient) {
      httpClient.close();
    }

    /**
     * Creates a HashiCorp Vault-backed {@link CryptoEngine}. Fails fast when no token is
     * configured.
     */
    @Produces
    @Singleton
    public CryptoEngine vaultCryptoEngine(
        CryptoBackend cryptoBackend,
        StreamRuneVaultHttpClient httpClient,
        Instance<DataSource> dataSource,
        StreamRuneQuarkusCryptoProperties props) {
      return build.buildVaultCryptoEngine(
          cryptoBackend,
          httpClient.client(),
          dataSource.isResolvable() ? dataSource.get() : null,
          props);
    }

    /** Disposer for {@link #vaultCryptoEngine}. */
    public void disposeCryptoEngine(@Disposes CryptoEngine engine) {
      closeEngine(engine);
    }
  }

  /**
   * AWS-KMS backend: the container-managed {@link KmsClient} holder, the {@link CryptoEngine} that
   * uses it, and both disposers — gated together on {@code streamrune.crypto.aws.enabled}.
   */
  @ApplicationScoped
  @IfBuildProperty(name = "streamrune.crypto.aws.enabled", stringValue = "true")
  public static class AwsKmsCryptoProducer {

    private final StreamRuneCryptoProducers build = new StreamRuneCryptoProducers();

    /** Creates the producer. */
    public AwsKmsCryptoProducer() {}

    /**
     * Produces the container-managed {@link KmsClient} the AWS-KMS engine uses. Registering it as a
     * bean — mirroring Spring's {@code kmsClient} bean — lets Arc close it via {@link
     * #closeAwsKmsClient(StreamRuneKmsClient)} on context shutdown, instead of leaking the SDK
     * client's Apache connection pool + idle-connection-reaper thread built inline in the producer
     * and owned by nobody. The region is validated here (fail-fast) exactly as the engine build
     * path requires it.
     */
    @Produces
    @Singleton
    public StreamRuneKmsClient awsKmsClient(StreamRuneQuarkusCryptoProperties props) {
      String region =
          requirePresent(
              props.aws().region(),
              "streamrune.crypto.aws.region",
              "an AWS region (e.g. eu-central-1)");
      return new StreamRuneKmsClient(KmsClient.builder().region(Region.of(region)).build());
    }

    /** Closes the container-managed {@link KmsClient} on shutdown. */
    public void closeAwsKmsClient(@Disposes StreamRuneKmsClient kmsClient) {
      kmsClient.close();
    }

    /**
     * Creates an AWS KMS-backed {@link CryptoEngine}. Fails fast when no region or KMS key id is
     * configured.
     *
     * <p>When a {@link DataSource} is present, the crypto backend's durable forgotten-subject store
     * ({@link CryptoBackend#forgottenSubjects}) is auto-wired so AWS-KMS GDPR erasure survives
     * restarts out of the box; without one the engine fails closed rather than recording in-memory
     * tombstones that vanish on restart. {@code Instance<DataSource>} makes the dependency optional
     * (KMS does not otherwise need a database).
     */
    @Produces
    @Singleton
    public CryptoEngine awsKmsCryptoEngine(
        CryptoBackend cryptoBackend,
        StreamRuneKmsClient kmsClient,
        Instance<DataSource> dataSource,
        StreamRuneQuarkusCryptoProperties props,
        Instance<StreamRuneMetrics> metrics) {
      return build.buildAwsKmsCryptoEngine(
          cryptoBackend,
          kmsClient.client(),
          dataSource.isResolvable() ? dataSource.get() : null,
          props,
          resolvedOrNoop(metrics));
    }

    /** Disposer for {@link #awsKmsCryptoEngine}. */
    public void disposeCryptoEngine(@Disposes CryptoEngine engine) {
      closeEngine(engine);
    }
  }

  /**
   * Build logic for {@link FileSystemCryptoProducer#fileSystemCryptoEngine}, package-private so
   * unit tests can drive it with a {@code null}/mock {@link DataSource} without an Arc container.
   * The optional DataSource wires the cross-replica LISTEN/NOTIFY forget channel ({@link
   * #resolveForgetSignal}).
   */
  CryptoEngine buildFileSystemCryptoEngine(
      CryptoBackend cryptoBackend,
      DataSource dataSource,
      StreamRuneQuarkusCryptoProperties props,
      StreamRuneMetrics metrics) {
    var filesystem = props.filesystem();
    requireRuntimeEnabled(filesystem.enabled(), "streamrune.crypto.filesystem.enabled");
    CryptoEngine delegate;
    try {
      delegate =
          FileSystemCryptoEngine.builder()
              .keyDirectory(Path.of(filesystem.keyDirectory()))
              .metrics(metrics)
              .build();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to build FileSystemCryptoEngine. Check streamrune.crypto.filesystem.key-directory.",
          e);
    }
    return wrapWithCache(
        delegate,
        props.cache(),
        resolveForgetSignal(
            cryptoBackend, dataSource, props.cache().expireAfterWrite(), "filesystem"));
  }

  /**
   * Build logic for {@link PostgresCryptoProducer#postgresCryptoEngine}, package-private so unit
   * tests can drive it with a mock {@link DataSource} without an Arc container.
   *
   * <p>The key-store engine is the crypto backend's ({@link CryptoBackend#keyStoreEngine}). A
   * DataSource is present by construction, so the backend's forget channel is wired for prompt
   * cross-replica cache invalidation on deleteKey ({@link #forgetSignal}). The cached engine is
   * AutoCloseable and is disposed by {@link PostgresCryptoProducer#disposeCryptoEngine} on context
   * shutdown, which stops the listener and closes its dedicated LISTEN pool.
   */
  CryptoEngine buildPostgresCryptoEngine(
      CryptoBackend cryptoBackend,
      DataSource dataSource,
      StreamRuneQuarkusCryptoProperties props,
      StreamRuneMetrics metrics) {
    requireRuntimeEnabled(props.postgres().enabled(), "streamrune.crypto.postgres.enabled");
    CryptoEngine delegate;
    try {
      delegate =
          cryptoBackend.keyStoreEngine(
              dataSource,
              new CryptoBackend.KeyStoreEngineSettings(props.postgres().tableName(), metrics));
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to build the "
              + cryptoBackend.name()
              + " key-store engine. Check streamrune.crypto.postgres config.",
          e);
    }
    return wrapWithCache(delegate, props.cache(), forgetSignal(cryptoBackend, dataSource));
  }

  /**
   * Build logic for {@link SqliteCryptoProducer#sqliteCryptoEngine}, package-private for direct
   * testing: the crypto backend's key-store engine with the default key table (SQLite has no
   * table-name property), wrapped in the cache with the backend's (in-process) forget signal.
   */
  CryptoEngine buildSqliteCryptoEngine(
      CryptoBackend cryptoBackend,
      DataSource dataSource,
      StreamRuneQuarkusCryptoProperties props,
      StreamRuneMetrics metrics) {
    requireRuntimeEnabled(props.sqlite().enabled(), "streamrune.crypto.sqlite.enabled");
    CryptoEngine delegate;
    try {
      delegate =
          cryptoBackend.keyStoreEngine(
              dataSource, new CryptoBackend.KeyStoreEngineSettings(null, metrics));
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to build the "
              + cryptoBackend.name()
              + " key-store engine on the application's DataSource: "
              + e.getMessage(),
          e);
    }
    return wrapWithCache(delegate, props.cache(), forgetSignal(cryptoBackend, dataSource));
  }

  /**
   * StreamRune-owned holder for the Vault {@link HttpClient}. Public because Arc's generated bean
   * classes are loaded by a different classloader than the application classes in dev-mode/test
   * deployments, so a package-private bean type fails with {@code IllegalAccessError} at runtime
   * even though the package name matches.
   */
  public record StreamRuneVaultHttpClient(HttpClient client) implements AutoCloseable {
    @Override
    public void close() {
      client.close();
    }
  }

  /**
   * StreamRune-owned holder for the AWS {@link KmsClient}. Public for the same classloader reason
   * as {@link StreamRuneVaultHttpClient}.
   */
  public record StreamRuneKmsClient(KmsClient client) implements AutoCloseable {
    @Override
    public void close() {
      client.close();
    }
  }

  /**
   * Test-only overload that builds its own (un-managed) {@link HttpClient}. Retained so unit tests
   * can drive the build logic without a container-managed client; production always routes through
   * the container-managed {@link VaultCryptoProducer#vaultHttpClient()} bean so the client is
   * closed on shutdown.
   */
  CryptoEngine buildVaultCryptoEngine(
      CryptoBackend cryptoBackend, DataSource dataSource, StreamRuneQuarkusCryptoProperties props) {
    return buildVaultCryptoEngine(cryptoBackend, HttpClient.newHttpClient(), dataSource, props);
  }

  /**
   * Build logic for {@link VaultCryptoProducer#vaultCryptoEngine}, package-private so unit tests
   * can drive it with a {@code null}/mock {@link DataSource} without an Arc container. Not a
   * producer (no {@code @Produces}), so Arc ignores it.
   *
   * <p>When a {@link DataSource} is present the crypto backend's durable forgotten-subject store
   * ({@link CryptoBackend#forgottenSubjects}) is auto-wired so Vault erasure is terminal (a
   * forgotten subject cannot be re-encrypted); without one the engine fails closed rather than
   * shipping non-terminal erasure. {@code Instance<DataSource>} makes the dependency optional.
   */
  CryptoEngine buildVaultCryptoEngine(
      CryptoBackend cryptoBackend,
      HttpClient httpClient,
      DataSource dataSource,
      StreamRuneQuarkusCryptoProperties props) {
    var vault = props.vault();
    requireRuntimeEnabled(vault.enabled(), "streamrune.crypto.vault.enabled");
    String token =
        requirePresent(
            vault.token(), "streamrune.crypto.vault.token", "a Vault token (e.g. ${VAULT_TOKEN})");
    CryptoEngine delegate;
    try {
      var builder =
          VaultCryptoEngine.builder()
              .httpClient(httpClient)
              .vaultAddress(vault.address())
              .token(token)
              .engineMount(vault.engineMount());
      if (dataSource != null) {
        builder.forgottenSubjectStore(cryptoBackend.forgottenSubjects(dataSource));
      }
      // No DataSource → build() fails closed; rethrown below.
      delegate = builder.build();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to build VaultCryptoEngine. Check streamrune.crypto.vault config, and provide a"
              + " DataSource so a durable ForgottenSubjectStore can be wired for terminal GDPR"
              + " erasure.",
          e);
    }
    // The same DataSource wires the LISTEN/NOTIFY forget channel.
    return wrapWithCache(
        delegate,
        props.cache(),
        resolveForgetSignal(cryptoBackend, dataSource, props.cache().expireAfterWrite(), "vault"));
  }

  /**
   * Test-only overload that builds its own (un-managed) {@link KmsClient}. Retained so unit tests
   * can drive the build logic without a container-managed client; production always routes through
   * the container-managed {@link
   * AwsKmsCryptoProducer#awsKmsClient(StreamRuneQuarkusCryptoProperties)} bean so the client is
   * closed on shutdown. Validation runs before the client is built so an error-path (disabled /
   * missing region / missing key id) never allocates a client — preserving the original single-arg
   * build's fail-fast order.
   */
  CryptoEngine buildAwsKmsCryptoEngine(
      CryptoBackend cryptoBackend,
      DataSource dataSource,
      StreamRuneQuarkusCryptoProperties props,
      StreamRuneMetrics metrics) {
    var aws = props.aws();
    requireRuntimeEnabled(aws.enabled(), "streamrune.crypto.aws.enabled");
    String region =
        requirePresent(
            aws.region(), "streamrune.crypto.aws.region", "an AWS region (e.g. eu-central-1)");
    requirePresent(
        aws.kmsKeyId(), "streamrune.crypto.aws.kms-key-id", "the ARN or id of a KMS symmetric key");
    return buildAwsKmsCryptoEngine(
        cryptoBackend,
        KmsClient.builder().region(Region.of(region)).build(),
        dataSource,
        props,
        metrics);
  }

  /**
   * Build logic for {@link AwsKmsCryptoProducer#awsKmsCryptoEngine}, package-private so unit tests
   * can drive it with a {@code null}/mock {@link DataSource} without an Arc container (an {@code
   * Instance} is impractical to stub). Not a producer (no {@code @Produces}), so Arc ignores it.
   */
  CryptoEngine buildAwsKmsCryptoEngine(
      CryptoBackend cryptoBackend,
      KmsClient kmsClient,
      DataSource dataSource,
      StreamRuneQuarkusCryptoProperties props,
      StreamRuneMetrics metrics) {
    var aws = props.aws();
    requireRuntimeEnabled(aws.enabled(), "streamrune.crypto.aws.enabled");
    requirePresent(
        aws.region(), "streamrune.crypto.aws.region", "an AWS region (e.g. eu-central-1)");
    String kmsKeyId =
        requirePresent(
            aws.kmsKeyId(),
            "streamrune.crypto.aws.kms-key-id",
            "the ARN or id of a KMS symmetric key");
    var dataKeyCache = aws.dataKeyCache();
    int dataKeyCacheMaximumSize = dataKeyCache.maximumSize();
    if (dataKeyCacheMaximumSize < 0) {
      throw new IllegalStateException(
          "streamrune.crypto.aws.data-key-cache.maximum-size must be 0 or more (0 disables the"
              + " data-key cache), got "
              + dataKeyCacheMaximumSize);
    }
    Duration dataKeyCacheExpireAfterWrite = dataKeyCache.expireAfterWrite();
    if (dataKeyCacheExpireAfterWrite.isZero() || dataKeyCacheExpireAfterWrite.isNegative()) {
      throw new IllegalStateException(
          "streamrune.crypto.aws.data-key-cache.expire-after-write must be a positive duration"
              + " (e.g. PT5M), got "
              + dataKeyCacheExpireAfterWrite);
    }
    CryptoEngine delegate;
    try {
      var builder =
          AwsKmsCryptoEngine.builder()
              .kmsClient(kmsClient)
              .kmsKeyId(kmsKeyId)
              .metrics(metrics)
              // streamrune.crypto.cache.enabled=false is the no-cache posture: neither decrypted
              // plaintext nor plaintext data keys are kept in heap.
              .dataKeyCacheMaximumSize(props.cache().enabled() ? dataKeyCacheMaximumSize : 0)
              .dataKeyCacheExpireAfterWrite(dataKeyCacheExpireAfterWrite);
      if (dataSource != null) {
        builder.forgottenSubjectStore(cryptoBackend.forgottenSubjects(dataSource));
      }
      // No DataSource → build() fails closed; rethrown below with actionable guidance.
      delegate = builder.build();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to build AwsKmsCryptoEngine. Check streamrune.crypto.aws config, and provide a"
              + " DataSource so a durable ForgottenSubjectStore can be wired for GDPR erasure that"
              + " survives restarts.",
          e);
    }
    // The same DataSource that backs the durable tombstone store also wires the LISTEN/NOTIFY
    // forget channel for prompt cross-replica cache eviction.
    return wrapWithCache(
        delegate,
        props.cache(),
        resolveForgetSignal(
            cryptoBackend, dataSource, props.cache().expireAfterWrite(), "aws-kms"));
  }

  /**
   * The application's metrics bean, or {@link StreamRuneMetrics#NOOP} without one; the engines
   * count discarded encryptions and retried decryptions with it.
   */
  static StreamRuneMetrics resolvedOrNoop(Instance<StreamRuneMetrics> metrics) {
    StreamRuneMetrics resolved = metrics.isResolvable() ? metrics.get() : null;
    return resolved != null ? resolved : StreamRuneMetrics.NOOP;
  }

  /**
   * Guards against a build-time/runtime configuration mismatch: the producer only exists because
   * the key was {@code true} at build time, so flipping it to {@code false} at runtime (e.g. via an
   * environment variable) cannot remove the bean — fail fast instead of producing a broken null
   * engine.
   */
  private static void requireRuntimeEnabled(boolean enabled, String key) {
    if (!enabled) {
      throw new IllegalStateException(
          key
              + " was 'true' at build time but 'false' at runtime. Crypto engine selection is "
              + "fixed at build time; rebuild with the desired value instead of overriding it.");
    }
  }

  private static String requirePresent(
      java.util.Optional<String> value, String key, String description) {
    if (value.isEmpty() || value.get().isBlank()) {
      throw new IllegalStateException(
          "Missing required config '" + key + "': provide " + description + ".");
    }
    return value.get();
  }

  /**
   * Resolves the cross-replica forget channel for an engine that keeps its keys outside the
   * database — the crypto backend's prompt signal (on PostgreSQL LISTEN/NOTIFY, see {@link
   * #forgetSignal}) when a DataSource is present, else {@link CryptoForgetSignal#NOOP} with a
   * startup WARN naming the cache write-TTL ceiling as the only cross-replica erasure bound.
   * Package-private for direct testing.
   */
  static CryptoForgetSignal resolveForgetSignal(
      CryptoBackend cryptoBackend,
      DataSource dataSource,
      Duration configuredWriteTtl,
      String backend) {
    if (dataSource != null) {
      return forgetSignal(cryptoBackend, dataSource);
    }
    log.warn(
        "No DataSource available to wire the cross-replica crypto-forget channel for the {} crypto"
            + " backend; after a GDPR erasure, peer replicas will keep serving a forgotten subject's"
            + " cached plaintext for up to the cache write-TTL ceiling ({}) — the only cross-replica"
            + " erasure bound. Provide a DataSource on the selected backend's database so its forget"
            + " signal evicts promptly.",
        backend,
        configuredWriteTtl);
    return CryptoForgetSignal.NOOP;
  }

  /**
   * The crypto backend's forget channel over {@code dataSource} — on PostgreSQL LISTEN/NOTIFY:
   * publishing borrows a connection of {@code dataSource}; listening holds one connection of a
   * dedicated pool. The datasource quarkus-agroal injects keeps its JDBC URL and credentials in its
   * configuration rather than behind the accessors the channel reads, so for an Agroal datasource
   * the backend is handed a factory that opens the dedicated pool from that configuration ({@link
   * AgroalForgetListenPool}); any other {@code DataSource} (an application-produced {@code
   * HikariDataSource}, say) goes through the channel's own derivation. Package-private for direct
   * testing.
   */
  static CryptoForgetSignal forgetSignal(CryptoBackend cryptoBackend, DataSource dataSource) {
    if (AgroalTypes.isAgroal(dataSource.getClass())) {
      return cryptoBackend.forgetSignal(dataSource, () -> AgroalForgetListenPool.open(dataSource));
    }
    return cryptoBackend.forgetSignal(dataSource);
  }

  private CryptoEngine wrapWithCache(
      CryptoEngine delegate,
      StreamRuneQuarkusCryptoProperties.CacheConfig cache,
      CryptoForgetSignal forgetSignal) {
    if (!cache.enabled()) {
      return delegate;
    }
    var builder =
        CachedCryptoEngine.builder()
            .delegate(delegate)
            .maximumSize(cache.maximumSize())
            .forgetSignal(forgetSignal)
            .expireAfterWrite(cache.expireAfterWrite());
    cache.expireAfterAccess().ifPresent(builder::expireAfterAccess);
    return builder.build();
  }
}
