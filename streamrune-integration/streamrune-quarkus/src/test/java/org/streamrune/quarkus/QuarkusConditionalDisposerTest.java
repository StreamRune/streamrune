package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;
import javax.sql.DataSource;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexReader;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoEngine;

/**
 * A conditional producer whose teardown is not conditional breaks the Quarkus <em>build</em>.
 *
 * <h2>The mechanism</h2>
 *
 * <p>{@code @IfBuildProperty} is resolved by {@code
 * io.quarkus.arc.deployment.BuildTimeEnabledProcessor}, which converts a condition that evaluated
 * {@code false} into {@code @Vetoed} (on a class) or {@code @io.quarkus.arc.VetoedProducer} (on a
 * method or field). Arc's {@code BeanDeployment#findBeans} then tests {@code @VetoedProducer} in
 * exactly two places — where it collects {@code @Produces} methods, and where it collects
 * {@code @Produces} fields. The adjacent loop that collects {@code @Disposes} methods tests for
 * {@code @Disposes} and nothing else.
 *
 * <p>So a gated producer disappears while the disposer that disposes its product stays, and Arc
 * fails the deployment with a {@code DefinitionException}: <em>"No producer method or field
 * declared by the bean class that is assignable to the disposed parameter of a disposer
 * method"</em>. Augmentation, not startup — so the failure is a build failure for every application
 * that leaves the backend disabled, which is the default.
 *
 * <p><strong>Putting the same {@code @IfBuildProperty} on the disposer does not fix it.</strong>
 * That adds {@code @VetoedProducer} to a method Arc never inspects for it; the disposer is still
 * collected and still orphaned. The condition has to move to the declaring <em>class</em>, because
 * a vetoed class is dropped before any of its methods are looked at — which is what {@link
 * StreamRuneCryptoProducers} now does, one nested producer class per backend, so a producer and its
 * disposer can only ever appear and disappear together.
 *
 * @see RealArcTestContainer#boot(List, Map) for how the augmentation step is reproduced here
 */
class QuarkusConditionalDisposerTest {

  /** Every crypto backend key, so a test can state a configuration exhaustively. */
  private static final String FILESYSTEM = "streamrune.crypto.filesystem.enabled";

  private static final String POSTGRES = "streamrune.crypto.postgres.enabled";
  private static final String VAULT = "streamrune.crypto.vault.enabled";
  private static final String AWS = "streamrune.crypto.aws.enabled";

  /** The producer classes a real Quarkus deployment indexes from this library's Jandex index. */
  private static List<Class<?>> cryptoDeployment() {
    var classes = new ArrayList<Class<?>>();
    classes.add(StreamRuneCryptoProducers.class);
    classes.addAll(List.of(StreamRuneCryptoProducers.class.getDeclaredClasses()));
    classes.add(CryptoInfrastructure.class);
    return classes;
  }

  private static void bootWith(Map<String, String> buildTimeProperties) {
    try (var arc = RealArcTestContainer.boot(cryptoDeployment(), buildTimeProperties)) {
      assertTrue(arc.container().isRunning());
    } catch (Exception e) {
      throw new AssertionError(
          "Arc augmentation failed for build-time properties " + buildTimeProperties, e);
    }
  }

  /**
   * The default configuration: no crypto backend enabled at all. Every {@code CryptoEngine}
   * producer is gated, so before the fix {@code disposeCryptoEngine} had no producer left to
   * dispose and the deployment could not be built — an application that never asked for crypto
   * could not build the framework in.
   */
  @Test
  void bootsWithNoCryptoBackendEnabled() {
    bootWith(Map.of());
  }

  /**
   * The e-commerce demo's configuration, and the one the downstream report was filed from: Postgres
   * crypto on, Vault and AWS-KMS off. {@code closeVaultHttpClient} and {@code closeAwsKmsClient}
   * were then orphaned even though the engine disposer had a producer.
   */
  @Test
  void bootsWithPostgresCryptoOnlyLikeTheDemo() {
    bootWith(Map.of(POSTGRES, "true"));
  }

  @Test
  void bootsWithFileSystemCryptoOnly() {
    bootWith(Map.of(FILESYSTEM, "true"));
  }

  /** The enabled direction: gating must not make the backend's own wiring unsatisfiable. */
  @Test
  void bootsWithVaultCryptoOnlyAndKeepsItsManagedClient() {
    try (var arc = RealArcTestContainer.boot(cryptoDeployment(), Map.of(VAULT, "true"))) {
      assertTrue(
          arc.container()
              .instance(StreamRuneCryptoProducers.StreamRuneVaultHttpClient.class)
              .isAvailable(),
          "enabling Vault must still register the container-managed client bean");
    } catch (Exception e) {
      throw new AssertionError("Arc augmentation failed with Vault enabled", e);
    }
  }

  @Test
  void bootsWithAwsKmsCryptoOnlyAndKeepsItsManagedClient() {
    try (var arc =
        RealArcTestContainer.boot(
            cryptoDeployment(),
            Map.of(AWS, "true", "streamrune.crypto.aws.region", "eu-central-1"))) {
      assertTrue(
          arc.container()
              .instance(StreamRuneCryptoProducers.StreamRuneKmsClient.class)
              .isAvailable(),
          "enabling AWS-KMS must still register the container-managed client bean");
    } catch (Exception e) {
      throw new AssertionError("Arc augmentation failed with AWS-KMS enabled", e);
    }
  }

  /** Two backends at once is documented as unsupported, but it must not fail at augmentation. */
  @Test
  void bootsWithSeveralBackendsEnabled() {
    assertDoesNotThrow(
        () ->
            bootWith(
                Map.of(
                    POSTGRES,
                    "true",
                    VAULT,
                    "true",
                    AWS,
                    "true",
                    "streamrune.crypto.aws.region",
                    "eu-central-1")));
  }

  // ---- the shape, not the site ---------------------------------------------------------------

  /**
   * The generalization. For every {@code @Disposes} method in this module, the class that declares
   * it must declare at least one {@code @Produces} method of the disposed type that carries no
   * <em>method-level</em> build-time condition — i.e. one that cannot vanish while the disposer
   * stays. A condition is allowed, but only on the declaring class, where it takes the disposer
   * with it.
   *
   * <p>Stated over the module rather than over the three known sites, because the defect is a
   * shape: {@code @Disposes} is the only teardown idiom in the three integrations that is a
   * separate declaration from the producer it tears down (Spring infers {@code destroyMethod} on
   * the {@code @Bean} itself, Micronaut uses {@code @Bean(preDestroy = "close")} on the bean
   * definition itself) — so it is the only one that can fall out of sync with its producer's
   * condition.
   */
  @Test
  void everyDisposerHasAProducerThatCannotVanishWithoutIt() {
    var violations = new ArrayList<String>();
    for (Class<?> declaring : producerClasses()) {
      for (Method disposer : declaring.getDeclaredMethods()) {
        Class<?> disposed = disposedTypeOf(disposer);
        if (disposed == null) {
          continue;
        }
        boolean hasUnconditionalProducer =
            java.util.Arrays.stream(declaring.getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(Produces.class))
                .filter(m -> disposed.isAssignableFrom(m.getReturnType()))
                .anyMatch(m -> m.getAnnotation(IfBuildProperty.class) == null);
        if (!hasUnconditionalProducer) {
          violations.add(
              declaring.getName()
                  + "#"
                  + disposer.getName()
                  + " disposes "
                  + disposed.getSimpleName()
                  + " but every producer of that type in the same class is gated by a"
                  + " METHOD-level @IfBuildProperty. Arc ignores @VetoedProducer when it collects"
                  + " disposers, so the disposer outlives its producer and augmentation fails with"
                  + " a DefinitionException. Move the condition to the declaring class.");
        }
      }
    }
    if (!violations.isEmpty()) {
      fail(String.join("\n", violations));
    }
  }

  /**
   * The fix only reaches a consumer if the nested producer classes are in the shipped Jandex index
   * — this module is a plain producers library with no {@code -deployment} module, so {@code
   * META-INF/jandex.idx} is the only thing that makes its beans visible to a Quarkus application.
   * Moving producers into nested classes is exactly the kind of change that could have silently
   * dropped them from the index, and the symptom would be no crypto engine at all rather than a
   * build failure.
   */
  @Test
  void theShippedJandexIndexCarriesEveryBackendProducerClass() throws Exception {
    String jarPath = System.getProperty("streamrune.quarkus.jar");
    assertNotNull(jarPath, "the Gradle test task must pass -Dstreamrune.quarkus.jar");
    try (JarFile jar = new JarFile(jarPath)) {
      Index index;
      try (InputStream in = jar.getInputStream(jar.getJarEntry("META-INF/jandex.idx"))) {
        index = new IndexReader(in).read();
      }
      for (Class<?> producerClass : StreamRuneCryptoProducers.class.getDeclaredClasses()) {
        if (producerClass.getAnnotation(IfBuildProperty.class) == null) {
          continue;
        }
        var name = DotName.createSimple(producerClass.getName());
        assertNotNull(
            index.getClassByName(name),
            name + " must be in the shipped jandex index or Quarkus never sees these producers");
      }
    }
  }

  /** Every class in this module that declares a CDI producer method. */
  private static List<Class<?>> producerClasses() {
    var classes = new ArrayList<Class<?>>();
    for (Class<?> top : List.of(StreamRuneProducers.class, StreamRuneCryptoProducers.class)) {
      classes.add(top);
      classes.addAll(List.of(top.getDeclaredClasses()));
    }
    return classes;
  }

  private static Class<?> disposedTypeOf(Method method) {
    for (Parameter parameter : method.getParameters()) {
      if (parameter.isAnnotationPresent(Disposes.class)) {
        return parameter.getType();
      }
    }
    return null;
  }

  /**
   * The two beans a real Quarkus application supplies that raw Arc cannot: the Agroal {@code
   * DataSource} and the bound {@code @ConfigMapping} properties. Mirrors {@code
   * CryptoClientBeanCollisionTest.CryptoInfrastructure}.
   */
  @ApplicationScoped
  public static class CryptoInfrastructure {

    @Produces
    @Singleton
    public DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusCryptoProperties properties() {
      SmallRyeConfig config =
          new SmallRyeConfigBuilder()
              .withMapping(StreamRuneQuarkusCryptoProperties.class)
              .withConverter(
                  Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
              .withDefaultValues(Map.of("streamrune.crypto.aws.region", "eu-central-1"))
              .build();
      return config.getConfigMapping(StreamRuneQuarkusCryptoProperties.class);
    }
  }

  /** Keeps the unused-import checker honest about the type this suite is really about. */
  @SuppressWarnings("unused")
  private static final Class<?> DISPOSED_TYPE_UNDER_TEST = CryptoEngine.class;
}
