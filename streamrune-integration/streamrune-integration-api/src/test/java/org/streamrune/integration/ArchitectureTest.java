package org.streamrune.integration;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * ArchUnit rules for streamrune-integration-api module.
 *
 * <p>These rules enforce integration API patterns.
 */
@AnalyzeClasses(
    packages = "org.streamrune.integration",
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  // ========== Dependency Rules ==========

  @ArchTest
  static final ArchRule no_framework_dependencies =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.integration..")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(
              "org.springframework..", "org.jboss..", "io.micronaut..", "io.quarkus..");

  @ArchTest
  static final ArchRule only_core_and_java_dependencies =
      classes()
          .that()
          .resideInAPackage("org.streamrune.integration..")
          .should()
          .onlyDependOnClassesThat()
          .resideInAnyPackage(
              "java..",
              "javax..",
              "org.streamrune.core..",
              "org.streamrune.integration..",
              "io.micrometer..",
              // Logging goes through SLF4J everywhere in the framework.
              "org.slf4j..");
}
