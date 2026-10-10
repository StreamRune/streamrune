package org.streamrune.quarkus;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAnyPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * ArchUnit rules for streamrune-quarkus module.
 *
 * <p>These rules enforce Quarkus integration patterns.
 */
@AnalyzeClasses(
    packages = "org.streamrune.quarkus",
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  // ========== Dependency Rules ==========

  // org.jboss.resteasy.reactive itself (not its sub-packages) is the API package of Quarkus REST:
  // the endpoint annotations an application writes, such as @RestStreamElementType. The rest of
  // org.jboss stays out.
  @ArchTest
  static final ArchRule no_other_framework_dependencies =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.quarkus..")
          .should()
          .dependOnClassesThat(
              resideInAnyPackage("org.springframework..", "io.micronaut..", "org.jboss..")
                  .and(not(resideInAPackage("org.jboss.resteasy.reactive"))));

  // ========== No Business Logic ==========

  @ArchTest
  static final ArchRule no_domain_logic =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.quarkus..")
          .should()
          .haveNameMatching(".*(Decider|Command|Event|Projection)$");
}
