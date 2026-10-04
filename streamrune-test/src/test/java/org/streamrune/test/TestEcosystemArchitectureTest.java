package org.streamrune.test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * Project-wide test ecosystem ArchUnit rules.
 *
 * <p>These rules apply to ALL test classes across the entire project (every module). The scan
 * covers the full {@code org.streamrune} package tree without excluding test classes, which is
 * necessary to verify cross-module conventions.
 *
 * <ul>
 *   <li>JUnit 5 as the sole test framework (no Spock, TestNG, JUnit 4)
 *   <li>AssertJ for fluent assertions
 *   <li>Testcontainers for integration tests
 *   <li>Records (Java 25) for immutable test DTOs
 * </ul>
 */
@AnalyzeClasses(packages = "org.streamrune")
class TestEcosystemArchitectureTest {

  private static final String ALL_PACKAGES = "org.streamrune..";

  // --- Test framework: JUnit 5 only, no Spock or TestNG ---

  @ArchTest
  static final ArchRule no_spock_framework =
      noClasses()
          .that()
          .resideInAnyPackage(ALL_PACKAGES)
          .and()
          .haveNameMatching(".*Test|.*Tests")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage("spock..", "groovy..");

  @ArchTest
  static final ArchRule no_testng_framework =
      noClasses()
          .that()
          .resideInAnyPackage(ALL_PACKAGES)
          .and()
          .haveNameMatching(".*Test|.*Tests")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage("org.testng..");

  // ArchUnit's own test runner does not use JUnit 5 — exclude it. A *ContractTest (e.g.
  // InMemorySagaStoreContractTest) is a thin factory-only subclass of a shared abstract JUnit 5
  // suite (e.g. SagaStoreContract): every @Test method is inherited, so the subclass's OWN
  // bytecode has no direct org.junit.jupiter reference — ArchUnit's dependency check is per-class,
  // not transitive through the superclass, so it would otherwise flag a class that is exercising
  // JUnit 5 exactly as intended. Excluded by name pattern rather than allowlisting the concrete
  // class, since every future *ContractTest subclass repeats this shape.
  @ArchTest
  static final ArchRule junit5_is_present =
      classes()
          .that()
          .resideInAnyPackage(ALL_PACKAGES)
          .and()
          .haveNameMatching(".*Test|.*Tests")
          .and()
          .haveNameNotMatching(".*ArchitectureTest|.*ContractTest")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage("org.junit.jupiter..");

  // --- Integration tests should use Testcontainers ---

  @ArchTest
  static final ArchRule integration_tests_use_testcontainers =
      classes()
          .that()
          .resideInAnyPackage(ALL_PACKAGES)
          .and()
          .haveNameMatching(".*IntegrationTest|.*IntegrationTests|.*IT|.*ITs")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage("org.testcontainers..")
          .allowEmptyShould(true);

  // --- AssertJ for assertions (JUnit 4 Assert is forbidden) ---

  @ArchTest
  static final ArchRule no_junit4_assertions =
      noClasses()
          .that()
          .resideInAnyPackage(ALL_PACKAGES)
          .and()
          .haveNameMatching(".*Test|.*Tests")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage("junit.framework..", "org.junit.Assert");

  // --- Test DTOs should be records (Java 25 feature) ---
  // Excludes *Fixture classes — fixtures are intentionally mutable test helpers.

  @ArchTest
  static final ArchRule test_dtos_should_be_records =
      classes()
          .that()
          .resideInAnyPackage(ALL_PACKAGES)
          .and()
          .haveNameMatching(".*(Dto|DTO|Request|Response)$")
          .should()
          .beRecords();
}
