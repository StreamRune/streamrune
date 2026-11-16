package org.streamrune.postgres;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * ArchUnit rules for streamrune-postgres module.
 *
 * <p>These rules enforce clean architecture and database conventions.
 */
@AnalyzeClasses(
    packages = "org.streamrune.postgres",
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  // ========== Dependency Rules ==========

  @ArchTest
  static final ArchRule no_framework_dependencies =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.postgres..")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage("org.springframework..", "org.jboss..", "io.micronaut..");

  @ArchTest
  static final ArchRule only_allowed_dependencies =
      classes()
          .that()
          .resideInAPackage("org.streamrune.postgres..")
          .should()
          .onlyDependOnClassesThat()
          .resideInAnyPackage(
              "java..",
              "javax..",
              "org.postgresql..",
              "com.fasterxml.jackson..",
              "com.zaxxer.hikari..",
              "org.flywaydb..",
              "org.slf4j..",
              "org.streamrune.core..",
              "org.streamrune.runtime..",
              "org.streamrune.crypto..",
              "org.streamrune.postgres..");

  // ========== Naming Conventions ==========

  @ArchTest
  static final ArchRule store_implementations_end_with_store =
      classes()
          .that()
          .implement("org.streamrune.core.EventStore")
          .should()
          .haveNameMatching(".*Store$");

  @ArchTest
  static final ArchRule locker_implementations_end_with_locker =
      classes()
          .that()
          .implement("org.streamrune.core.AggregateLocker")
          .should()
          .haveNameMatching(".*Locker$");
}
