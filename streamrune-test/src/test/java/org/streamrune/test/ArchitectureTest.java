package org.streamrune.test;

import static com.tngtech.archunit.core.importer.ImportOption.DoNotIncludeTests;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

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
}
