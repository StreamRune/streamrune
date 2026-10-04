package org.streamrune.runtime;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * ArchUnit rules for streamrune-runtime module.
 *
 * <p>These rules enforce clean architecture and prevent framework-specific code.
 */
@AnalyzeClasses(
    packages = "org.streamrune.runtime",
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  // ========== Dependency Rules ==========

  @ArchTest
  static final ArchRule no_framework_dependencies =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.runtime..")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(
              "org.springframework..",
              "org.jboss..",
              "io.micronaut..",
              "jakarta.inject..",
              "javax.inject..");

  @ArchTest
  static final ArchRule only_core_and_crypto_dependencies =
      classes()
          .that()
          .resideInAPackage("org.streamrune.runtime..")
          .should()
          .onlyDependOnClassesThat()
          .resideInAnyPackage(
              "java..",
              "javax..",
              "jakarta.validation..",
              "org.streamrune.core..",
              "org.streamrune.crypto..",
              "org.streamrune.runtime..",
              "org.slf4j..",
              "com.fasterxml.jackson..",
              "com.cronutils..",
              "com.github.benmanes.caffeine..",
              "io.opentelemetry..");

  // ========== Package Structure Rules ==========

  @ArchTest
  static final ArchRule streamRune_in_runtime_package =
      classes()
          .that()
          .haveNameMatching(".*\\.StreamRune$")
          .should()
          .resideInAPackage("org.streamrune.runtime");
}
