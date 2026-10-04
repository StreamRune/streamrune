package org.streamrune.crypto;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * ArchUnit rules for streamrune-crypto module.
 *
 * <p>These rules enforce clean architecture for cryptographic operations.
 */
@AnalyzeClasses(
    packages = "org.streamrune.crypto",
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  // ========== Dependency Rules ==========

  @ArchTest
  static final ArchRule no_framework_dependencies =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.crypto..")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(
              "org.springframework..",
              "org.jboss..",
              "io.micronaut..",
              "io.quarkus..",
              "jakarta.inject..",
              "javax.inject..");

  @ArchTest
  static final ArchRule only_allowed_dependencies =
      classes()
          .that()
          .resideInAPackage("org.streamrune.crypto..")
          .should()
          .onlyDependOnClassesThat()
          .resideInAnyPackage(
              "java..",
              "javax..",
              "com.fasterxml.jackson..",
              "com.github.benmanes.caffeine..",
              "org.slf4j..",
              "org.streamrune.core..",
              "org.streamrune.crypto..");

  // ========== Naming Conventions ==========

  @ArchTest
  static final ArchRule exception_classes_end_with_exception =
      classes()
          .that()
          .resideInAPackage("org.streamrune.crypto..")
          .and()
          .haveNameMatching(".*Exception$")
          .should()
          .beAssignableTo(Exception.class)
          .allowEmptyShould(true);
}
