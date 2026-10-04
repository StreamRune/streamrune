package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.aot.hint.predicate.RuntimeHintsPredicates.reflection;
import static org.springframework.aot.hint.predicate.RuntimeHintsPredicates.resource;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.ReflectionHints;
import org.springframework.aot.hint.RuntimeHints;
import org.streamrune.spring.nativehintsfixture.SampleCommand;
import org.streamrune.spring.nativehintsfixture.SampleEvent;
import org.streamrune.spring.nativehintsfixture.SampleMoney;
import org.streamrune.spring.nativehintsfixture.SamplePort;
import org.streamrune.spring.nativehintsfixture.SampleStatus;
import org.streamrune.test.NativeReflectionTypes;
import org.streamrune.test.ShippedMigrationScripts;

/** Tests for {@link StreamRuneRuntimeHints}. */
class StreamRuneRuntimeHintsTest {

  private RuntimeHints hints;

  @BeforeEach
  void setUp() {
    hints = new RuntimeHints();
    new StreamRuneRuntimeHints().registerHints(hints, getClass().getClassLoader());
  }

  static java.util.List<Class<?>> allTypes() {
    return StreamRuneRuntimeHints.NATIVE_IMAGE_TYPES;
  }

  @ParameterizedTest(name = "{0} is registered for reflection")
  @MethodSource("allTypes")
  void everyTypeIsRegisteredForReflection(Class<?> type) {
    assertThat(reflection().onType(type)).accepts(hints);
  }

  /**
   * Completeness parity check DERIVED FROM CODE: every {@code @JsonValue} value type in {@code
   * org.streamrune.core.types} plus every framework record/enum reachable through the registered
   * DTOs' components must actually be registered for native-image reflection. Unlike a hard-coded
   * {@code .contains(...)} allow-list, this fails the moment a new value type or nested component
   * is left unregistered — which is exactly how {@code SubjectId}/{@code ProjectionName}/etc.
   * shipped silently broken in native images.
   */
  @Test
  void everyCodeDerivedSerializableTypeIsRegisteredForReflection() {
    Set<Class<?>> registeredRoots = new LinkedHashSet<>(StreamRuneRuntimeHints.NATIVE_IMAGE_TYPES);
    Set<Class<?>> expected = NativeReflectionTypes.expectedNativeReflectionTypes(registeredRoots);

    List<Class<?>> missing =
        expected.stream()
            .filter(type -> !reflection().onType(type).test(hints))
            .sorted(Comparator.comparing(Class::getName))
            .toList();

    assertThat(missing)
        .as(
            "framework types reachable/derived from code but NOT registered for native-image"
                + " reflection")
        .isEmpty();
  }

  /**
   * {@link org.streamrune.core.projection.Projection#writesThroughRepository()} resolves {@code
   * process(List, ProjectionRepository)} with {@code getClass().getMethod(...)}; the method is
   * declared by {@code Projection} (the default) or by {@code BaseProjection} (final). A native
   * image finds it only on a type registered for reflection — the Quarkus and Micronaut binaries
   * failed to build their transactional projection runner without it, and the Spring starter wires
   * the auto-configured {@code JdbcProjectionRepository} as the transactional processor, which is
   * what makes the runner ask.
   */
  @Test
  void registersTheProjectionTypesWhoseProcessMethodIsResolvedReflectively() {
    for (Class<?> type :
        List.of(
            org.streamrune.core.projection.Projection.class,
            org.streamrune.core.projection.BaseProjection.class)) {
      assertThat(
              reflection().onType(type).withMemberCategory(MemberCategory.INVOKE_DECLARED_METHODS))
          .as("%s is registered with its declared methods", type.getSimpleName())
          .accepts(hints);
    }
  }

  /**
   * Schema auto-initialization reads every shipped migration script by name, and a native image
   * holds only the resources registered at build time; Flyway reads its own {@code version.txt}
   * when it starts. The expected scripts are read from the class path, so a script added to a
   * series is checked without being listed here.
   */
  @Test
  void registersEveryShippedMigrationScriptAndFlywaysVersionFileAsResources() {
    ClassLoader classLoader = getClass().getClassLoader();
    List<String> expected =
        new java.util.ArrayList<>(ShippedMigrationScripts.onClassPath(classLoader));
    expected.add("org/flywaydb/core/internal/version.txt");

    for (String name : expected) {
      assertThat(classLoader.getResource(name)).as("%s is on the class path", name).isNotNull();
      assertThat(resource().forResource(name)).as("%s is a native resource", name).accepts(hints);
    }
    assertThat(expected)
        .contains(
            "db/streamrune-migration/V001__streamrune_baseline.sql",
            "db/crypto-migration/V001__crypto_baseline.sql");
  }

  @Test
  void registerSerializableTypesRegistersAccessorMethodsForRecordComponents() {
    var appHints = new RuntimeHints();
    StreamRuneRuntimeHints.registerSerializableTypes(appHints, List.of(SampleMoney.class));
    // INVOKE_DECLARED_METHODS is the category that makes Class.getRecordComponents() work natively.
    assertThat(
            reflection()
                .onType(SampleMoney.class)
                .withMemberCategory(MemberCategory.INVOKE_DECLARED_METHODS))
        .accepts(appHints);
  }

  @Test
  void registerDomainPackagesScansNestedRecordsValueObjectsAndEnums() {
    var appHints = new RuntimeHints();
    StreamRuneRuntimeHints.registerDomainPackages(
        appHints, getClass().getClassLoader(), "org.streamrune.spring.nativehintsfixture");
    assertThat(reflection().onType(SampleEvent.Created.class)).accepts(appHints);
    assertThat(reflection().onType(SampleEvent.Renamed.class)).accepts(appHints);
    assertThat(reflection().onType(SampleMoney.class)).accepts(appHints);
    assertThat(reflection().onType(SampleStatus.class)).accepts(appHints);
  }

  /**
   * A GraalVM native image answers {@code getPermittedSubclasses()} only for a sealed type that is
   * itself registered for reflection; for any other it reports the type sealed with NO permitted
   * subclasses, and the framework's sealed walks (dead-letter command registration, the
   * authorization and {@code @Encrypted} startup checks) refuse it. Registering the subclasses is
   * not enough, so the scan registers every sealed interface and abstract class too — each nested
   * sealed level included — but no other abstract type.
   */
  @Test
  void registerDomainPackagesRegistersEverySealedTypeSoTheImageCanListItsPermittedSubclasses() {
    var appHints = new RuntimeHints();
    StreamRuneRuntimeHints.registerDomainPackages(
        appHints, getClass().getClassLoader(), "org.streamrune.spring.nativehintsfixture");

    assertThat(reflection().onType(SampleEvent.class)).accepts(appHints);
    assertThat(reflection().onType(SampleCommand.class)).accepts(appHints);
    assertThat(reflection().onType(SampleCommand.Refund.class)).accepts(appHints);
    assertThat(reflection().onType(SampleCommand.Refund.Full.class)).accepts(appHints);
    assertThat(reflection().onType(SamplePort.class)).rejects(appHints);
  }

  /**
   * Sonar S1181: only a class that cannot be LOADED is skipped. The catch used to be {@code
   * Throwable}, so a failure while registering a loaded type was swallowed as well and the type
   * silently went missing from the native reflection hints, to fail only at native runtime.
   */
  @Test
  void registerDomainPackagesPropagatesAFailureToRegisterALoadedType() {
    var failure = new IllegalStateException("simulated registration failure");
    var appHints =
        new RuntimeHints() {
          private final ReflectionHints failing =
              new ReflectionHints() {
                @Override
                public ReflectionHints registerType(Class<?> type, MemberCategory... categories) {
                  throw failure;
                }
              };

          @Override
          public ReflectionHints reflection() {
            return failing;
          }
        };

    assertThatThrownBy(
            () ->
                StreamRuneRuntimeHints.registerDomainPackages(
                    appHints,
                    getClass().getClassLoader(),
                    "org.streamrune.spring.nativehintsfixture"))
        .isSameAs(failure);
  }

  @Test
  void registerDomainPackagesSkipsTypesThatCannotBeLoaded() {
    var appHints = new RuntimeHints();
    // Parent = bootstrap, so the fixture classes are not found: ClassNotFoundException.
    var notFound = new ClassLoader(null) {};
    // A linkage failure while defining the class.
    var linkageFailure =
        new ClassLoader(null) {
          @Override
          protected Class<?> findClass(String name) {
            throw new NoClassDefFoundError(name);
          }
        };

    StreamRuneRuntimeHints.registerDomainPackages(
        appHints, notFound, "org.streamrune.spring.nativehintsfixture");
    StreamRuneRuntimeHints.registerDomainPackages(
        appHints, linkageFailure, "org.streamrune.spring.nativehintsfixture");

    assertThat(reflection().onType(SampleMoney.class)).rejects(appHints);
  }

  @Test
  void registerDomainPackagesToleratesNoPackagesAndUnknownPackages() {
    var appHints = new RuntimeHints();
    StreamRuneRuntimeHints.registerDomainPackages(appHints, getClass().getClassLoader());
    StreamRuneRuntimeHints.registerDomainPackages(
        appHints, getClass().getClassLoader(), "com.example.does.not.exist");
    assertThat(reflection().onType(SampleMoney.class)).rejects(appHints);
  }
}
