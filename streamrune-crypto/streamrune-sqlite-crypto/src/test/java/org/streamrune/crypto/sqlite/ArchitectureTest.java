package org.streamrune.crypto.sqlite;

import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.simpleNameEndingWith;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * ArchUnit rules for the streamrune-sqlite-crypto module: no framework dependencies, and only the
 * crypto API, the SQLite module, the driver and the JDK beside core.
 */
@AnalyzeClasses(
    packages = "org.streamrune.crypto.sqlite",
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  @ArchTest
  static final ArchRule no_framework_dependencies =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.crypto.sqlite..")
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
          .resideInAPackage("org.streamrune.crypto.sqlite..")
          .should()
          .onlyDependOnClassesThat()
          .resideInAnyPackage(
              "java..",
              "javax..",
              "org.sqlite..",
              "org.slf4j..",
              "org.streamrune.core..",
              "org.streamrune.crypto..",
              "org.streamrune.sqlite..");

  /** The module builds on the SQLite handle and shared helpers, never on a store of it. */
  @ArchTest
  static final ArchRule no_dependency_on_a_store_of_the_sqlite_module =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.crypto.sqlite..")
          .should()
          .dependOnClassesThat(
              resideInAPackage("org.streamrune.sqlite..").and(simpleNameEndingWith("Store")));
}
