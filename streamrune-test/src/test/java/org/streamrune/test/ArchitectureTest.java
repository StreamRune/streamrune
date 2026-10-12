package org.streamrune.test;

import static com.tngtech.archunit.core.importer.ImportOption.DoNotIncludeTests;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.AccessTarget;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;

/**
 * ArchUnit rules for streamrune-test module.
 *
 * <p>These rules enforce test utilities conventions and the project-wide test ecosystem standards:
 *
 * <ul>
 *   <li>JUnit 5 as the sole test framework (no Spock, TestNG, JUnit 4, etc.)
 *   <li>AssertJ for fluent assertions
 *   <li>Testcontainers for integration tests that need a real database
 *   <li>Java 25 features (records for test DTOs, switch expressions) in test code
 * </ul>
 *
 * <p>Two scanning scopes are used:
 *
 * <ul>
 *   <li>{@code packages = "org.streamrune.test"} with {@link DoNotIncludeTests} — the production
 *       classes of streamrune-test itself (no framework deps rule).
 *   <li>{@code packages = "org.streamrune"} with no import options — all classes in the project,
 *       including test classes from every module, for the project-wide ecosystem rules.
 * </ul>
 */
@AnalyzeClasses(packages = "org.streamrune.test", importOptions = DoNotIncludeTests.class)
class ArchitectureTest {

  // ========== Production Framework Isolation (streamrune-test only) ==========

  @ArchTest
  static final ArchRule no_production_framework_dependencies =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.test..")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(
              "org.springframework..", "org.jboss..", "io.micronaut..", "io.quarkus..");

  // ========== Saga Provenance (streamrune-test only) ==========

  /**
   * Metadata that names a saga origin is built in two places of this module: the in-memory command
   * bus, which copies the bound dispatch cause as the production bus does, and {@link
   * SagaEnvelopes}, the helper for hand-built saga-caused envelopes. Every other test double builds
   * origin-less metadata through the shorter constructors. A method reference to the ten-argument
   * constructor ({@code EventMetadata::new}) is matched like a call of it.
   */
  @ArchTest
  static final ArchRule onlyTheBusAndSagaEnvelopesBuildMetadataWithAnOrigin =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.test..")
          .and()
          .doNotBelongToAnyOf(InMemoryCommandBus.class, SagaEnvelopes.class)
          .should()
          .accessTargetWhere(
              DescribedPredicate.describe(
                  "target is the ten-argument EventMetadata constructor, called or referenced",
                  ArchitectureTest::targetsTheMetadataConstructorWithAnOrigin));

  private static final List<String> METADATA_WITH_ORIGIN_PARAMETERS =
      Stream.of(
              EventId.class,
              CommandId.class,
              TraceId.class,
              SpanId.class,
              CorrelationId.class,
              CausationId.class,
              UserId.class,
              Instant.class,
              Map.class,
              SagaOrigin.class)
          .map(Class::getName)
          .toList();

  private static boolean targetsTheMetadataConstructorWithAnOrigin(JavaAccess<?> access) {
    return access.getTarget() instanceof AccessTarget.CodeUnitAccessTarget target
        && target.getOwner().isEquivalentTo(EventMetadata.class)
        && target.getName().equals("<init>")
        && target.getRawParameterTypes().stream()
            .map(JavaClass::getName)
            .toList()
            .equals(METADATA_WITH_ORIGIN_PARAMETERS);
  }
}
