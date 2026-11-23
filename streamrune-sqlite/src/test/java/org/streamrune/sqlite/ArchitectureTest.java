package org.streamrune.sqlite;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * ArchUnit rules for streamrune-sqlite module.
 *
 * <p>These rules keep the module free of framework dependencies and enforce naming conventions.
 */
@AnalyzeClasses(
    packages = "org.streamrune.sqlite",
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  // ========== Dependency Rules ==========

  @ArchTest
  static final ArchRule no_framework_dependencies =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.sqlite..")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage("org.springframework..", "org.jboss..", "io.micronaut..");

  @ArchTest
  static final ArchRule only_allowed_dependencies =
      classes()
          .that()
          .resideInAPackage("org.streamrune.sqlite..")
          .should()
          .onlyDependOnClassesThat()
          .resideInAnyPackage(
              "java..",
              "javax..",
              "com.fasterxml.jackson..",
              "org.flywaydb..",
              "org.sqlite..",
              "org.slf4j..",
              "org.streamrune.core..",
              "org.streamrune.runtime..",
              "org.streamrune.crypto..",
              "org.streamrune.sqlite..");

  // ========== Naming Conventions ==========

  @ArchTest
  static final ArchRule store_implementations_end_with_store =
      classes()
          .that()
          .implement("org.streamrune.core.EventStore")
          .should()
          .haveNameMatching(".*Store$");
}
