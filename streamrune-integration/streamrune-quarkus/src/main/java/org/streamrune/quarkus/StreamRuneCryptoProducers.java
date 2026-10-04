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
import org.streamrune.aws.AwsKmsCryptoEngine;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.crypto.postgres.JdbcForgottenSubjectStore;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.crypto.postgres.PostgresCryptoForgetSignal;
import org.streamrune.filesystem.FileSystemCryptoEngine;
import org.streamrune.vault.VaultCryptoEngine;
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
        Instance<DataSource> dataSource, StreamRuneQuarkusCryptoProperties props) {
      return build.buildFileSystemCryptoEngine(
          dataSource.isResolvable() ? dataSource.get() : null, props);
    }

    /** Disposer for {@link #fileSystemCryptoEngine}. */
    void disposeCryptoEngine(@Disposes CryptoEngine engine) {
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
        DataSource dataSource, StreamRuneQuarkusCryptoProperties props) {
      return build.buildPostgresCryptoEngine(dataSource, props);
    }

    /** Disposer for {@link #postgresCryptoEngine}. */
    void disposeCryptoEngine(@Disposes CryptoEngine engine) {
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
    void closeVaultHttpClient(@Disposes StreamRuneVaultHttpClient httpClient) {
      httpClient.close();
    }

    /**
     * Creates a HashiCorp Vault-backed {@link CryptoEngine}. Fails fast when no token is
     * configured.
     */
    @Produces
    @Singleton
    public CryptoEngine vaultCryptoEngine(
        StreamRuneVaultHttpClient httpClient,
        Instance<DataSource> dataSource,
        StreamRuneQuarkusCryptoProperties props) {
      return build.buildVaultCryptoEngine(
          httpClient.client(), dataSource.isResolvable() ? dataSource.get() : null, props);
    }

    /** Disposer for {@link #vaultCryptoEngine}. */
    void disposeCryptoEngine(@Disposes CryptoEngine engine) {
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
    void closeAwsKmsClient(@Disposes StreamRuneKmsClient kmsClient) {
      kmsClient.close();
    }

    /**
     * Creates an AWS KMS-backed {@link CryptoEngine}. Fails fast when no region or KMS key id is
     * configured.
     *
     * <p>When a {@link DataSource} is present, a durable {@link JdbcForgottenSubjectStore} is
     * auto-wired so AWS-KMS GDPR erasure survives restarts out of the box; without one the engine
     * fails closed rather than recording in-memory tombstones that vanish on restart. {@code
     * Instance<DataSource>} makes the dependency optional (KMS does not otherwise need a database).
     */
    @Produces
    @Singleton
    public CryptoEngine awsKmsCryptoEngine(
        StreamRuneKmsClient kmsClient,
        Instance<DataSource> dataSource,
        StreamRuneQuarkusCryptoProperties props) {
      return build.buildAwsKmsCryptoEngine(
          kmsClient.client(), dataSource.isResolvable() ? dataSource.get() : null, props);
    }

    /** Disposer for {@link #awsKmsCryptoEngine}. */
    void disposeCryptoEngine(@Disposes CryptoEngine engine) {
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
      DataSource dataSource, StreamRuneQuarkusCryptoProperties props) {
    var filesystem = props.filesystem();
    requireRuntimeEnabled(filesystem.enabled(), "streamrune.crypto.filesystem.enabled");
    CryptoEngine delegate;
    try {
      delegate =
          FileSystemCryptoEngine.builder().keyDirectory(Path.of(filesystem.keyDirectory())).build();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to build FileSystemCryptoEngine. Check streamrune.crypto.filesystem.key-directory.",
          e);
    }
    return wrapWithCache(
        delegate,
        props.cache(),
        resolveForgetSignal(dataSource, props.cache().expireAfterWriteRaw(), "filesystem"));
  }

  /**
   * Build logic for {@link PostgresCryptoProducer#postgresCryptoEngine}, package-private so unit
   * tests can drive it with a mock {@link DataSource} without an Arc container.
   *
   * <p>A Postgres DataSource is present by construction, so the LISTEN/NOTIFY forget channel is
   * wired for prompt cross-replica cache invalidation on deleteKey ({@link #postgresForgetSignal}).
   * The cached engine is AutoCloseable and is disposed by {@link
   * PostgresCryptoProducer#disposeCryptoEngine} on context shutdown, which stops the listener and
   * closes its dedicated LISTEN pool.
   */
  CryptoEngine buildPostgresCryptoEngine(
      DataSource dataSource, StreamRuneQuarkusCryptoProperties props) {
    requireRuntimeEnabled(props.postgres().enabled(), "streamrune.crypto.postgres.enabled");
    CryptoEngine delegate;
    try {
      delegate =
          PostgresCryptoEngine.builder()
              .dataSource(dataSource)
              .tableName(props.postgres().tableName())
              .build();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to build PostgresCryptoEngine. Check streamrune.crypto.postgres config.", e);
    }
    return wrapWithCache(delegate, props.cache(), postgresForgetSignal(dataSource));
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
      DataSource dataSource, StreamRuneQuarkusCryptoProperties props) {
    return buildVaultCryptoEngine(HttpClient.newHttpClient(), dataSource, props);
  }

  /**
   * Build logic for {@link VaultCryptoProducer#vaultCryptoEngine}, package-private so unit tests
   * can drive it with a {@code null}/mock {@link DataSource} without an Arc container. Not a
   * producer (no {@code @Produces}), so Arc ignores it.
   *
   * <p>When a {@link DataSource} is present a durable {@link JdbcForgottenSubjectStore} is
   * auto-wired so Vault erasure is terminal (a forgotten subject cannot be re-encrypted); without
   * one the engine fails closed rather than shipping non-terminal erasure. {@code
   * Instance<DataSource>} makes the dependency optional.
   */
  CryptoEngine buildVaultCryptoEngine(
      HttpClient httpClient, DataSource dataSource, StreamRuneQuarkusCryptoProperties props) {
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
        builder.forgottenSubjectStore(new JdbcForgottenSubjectStore(dataSource));
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
        resolveForgetSignal(dataSource, props.cache().expireAfterWriteRaw(), "vault"));
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
      DataSource dataSource, StreamRuneQuarkusCryptoProperties props) {
    var aws = props.aws();
    requireRuntimeEnabled(aws.enabled(), "streamrune.crypto.aws.enabled");
    String region =
        requirePresent(
            aws.region(), "streamrune.crypto.aws.region", "an AWS region (e.g. eu-central-1)");
    requirePresent(
        aws.kmsKeyId(), "streamrune.crypto.aws.kms-key-id", "the ARN or id of a KMS symmetric key");
    return buildAwsKmsCryptoEngine(
        KmsClient.builder().region(Region.of(region)).build(), dataSource, props);
  }

  /**
   * Build logic for {@link AwsKmsCryptoProducer#awsKmsCryptoEngine}, package-private so unit tests
   * can drive it with a {@code null}/mock {@link DataSource} without an Arc container (an {@code
   * Instance} is impractical to stub). Not a producer (no {@code @Produces}), so Arc ignores it.
   */
  CryptoEngine buildAwsKmsCryptoEngine(
      KmsClient kmsClient, DataSource dataSource, StreamRuneQuarkusCryptoProperties props) {
    var aws = props.aws();
    requireRuntimeEnabled(aws.enabled(), "streamrune.crypto.aws.enabled");
    requirePresent(
        aws.region(), "streamrune.crypto.aws.region", "an AWS region (e.g. eu-central-1)");
    String kmsKeyId =
        requirePresent(
            aws.kmsKeyId(),
            "streamrune.crypto.aws.kms-key-id",
            "the ARN or id of a KMS symmetric key");
    CryptoEngine delegate;
    try {
      var builder = AwsKmsCryptoEngine.builder().kmsClient(kmsClient).kmsKeyId(kmsKeyId);
      if (dataSource != null) {
        builder.forgottenSubjectStore(new JdbcForgottenSubjectStore(dataSource));
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
        resolveForgetSignal(dataSource, props.cache().expireAfterWriteRaw(), "aws-kms"));
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
   * Resolves the cross-replica forget channel for a non-Postgres backend — the prompt {@link
   * PostgresCryptoForgetSignal} (LISTEN/NOTIFY, see {@link #postgresForgetSignal}) when a
   * DataSource is present, else {@link CryptoForgetSignal#NOOP} with a startup WARN naming the
   * cache write-TTL ceiling as the only cross-replica erasure bound. Package-private for direct
   * testing.
   */
  static CryptoForgetSignal resolveForgetSignal(
      DataSource dataSource, java.util.Optional<String> configuredWriteTtlRaw, String backend) {
    if (dataSource != null) {
      return postgresForgetSignal(dataSource);
    }
    String writeTtl =
        configuredWriteTtlRaw
            .filter(v -> !v.isBlank())
            .orElse(CachedCryptoEngine.DEFAULT_EXPIRE_AFTER_WRITE.toString());
    log.warn(
        "No DataSource available to wire the cross-replica crypto-forget channel for the {} crypto"
            + " backend; after a GDPR erasure, peer replicas will keep serving a forgotten subject's"
            + " cached plaintext for up to the cache write-TTL ceiling ({}) — the only cross-replica"
            + " erasure bound. Provide a Postgres DataSource for prompt (LISTEN/NOTIFY) eviction.",
        backend,
        writeTtl);
    return CryptoForgetSignal.NOOP;
  }

  /**
   * The LISTEN/NOTIFY forget channel over {@code dataSource}. Publishing borrows a connection of
   * {@code dataSource}; listening holds one connection of a dedicated pool. The datasource
   * quarkus-agroal injects keeps its JDBC URL and credentials in its configuration rather than
   * behind the accessors the channel reads, so for an Agroal datasource the dedicated pool is
   * opened from that configuration ({@link AgroalForgetListenPool}); any other {@code DataSource}
   * (an application-produced {@code HikariDataSource}, say) goes through the channel's own
   * derivation. Package-private for direct testing.
   */
  static CryptoForgetSignal postgresForgetSignal(DataSource dataSource) {
    if (isAgroal(dataSource.getClass())) {
      return new PostgresCryptoForgetSignal(
          dataSource, () -> AgroalForgetListenPool.open(dataSource));
    }
    return new PostgresCryptoForgetSignal(dataSource);
  }

  /**
   * Whether {@code type} implements {@code io.agroal.api.AgroalDataSource}, decided by name so that
   * an application without Agroal on its classpath never loads an Agroal type (Agroal is a
   * compile-only dependency of this module).
   */
  private static boolean isAgroal(Class<?> type) {
    if (type == null) {
      return false;
    }
    if (type.getName().equals("io.agroal.api.AgroalDataSource")) {
      return true;
    }
    for (Class<?> implemented : type.getInterfaces()) {
      if (isAgroal(implemented)) {
        return true;
      }
    }
    return isAgroal(type.getSuperclass());
  }

  private CryptoEngine wrapWithCache(
      CryptoEngine delegate,
      StreamRuneQuarkusCryptoProperties.CacheConfig cache,
      CryptoForgetSignal forgetSignal) {
    if (!cache.enabled()) return delegate;
    var builder =
        CachedCryptoEngine.builder()
            .delegate(delegate)
            .maximumSize(cache.maximumSize())
            .forgetSignal(forgetSignal);
    expiry("streamrune.crypto.cache.expire-after-write", cache.expireAfterWriteRaw())
        .ifPresent(builder::expireAfterWrite);
    expiry("streamrune.crypto.cache.expire-after-access", cache.expireAfterAccessRaw())
        .ifPresent(builder::expireAfterAccess);
    return builder.build();
  }

  /**
   * Parses a cache expiry value with the strict ISO-8601 {@link Duration} format. Quarkus' own
   * duration shorthand ({@code 5m}, {@code 1h}) is deliberately not supported here: these values
   * are mapped as raw strings so behavior is identical inside Quarkus and in plain SmallRye Config
   * — fail fast naming the offending key instead of surfacing a bare {@link
   * java.time.format.DateTimeParseException}.
   */
  private static java.util.Optional<Duration> expiry(String key, java.util.Optional<String> raw) {
    return raw.filter(v -> !v.isBlank())
        .map(
            value -> {
              try {
                return Duration.parse(value);
              } catch (java.time.format.DateTimeParseException e) {
                throw new IllegalStateException(
                    "Invalid value '"
                        + value
                        + "' for '"
                        + key
                        + "': expected an ISO-8601 duration (e.g. PT5M, PT1H).",
                    e);
              }
            });
  }
}
