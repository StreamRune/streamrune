package org.streamrune.e2e;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import java.util.Set;
import org.streamrune.core.ScopedValueReads;

/**
 * No class of the framework but {@link ScopedValueReads} reads a {@link ScopedValue} — the rule of
 * {@code streamrune-test}'s {@code ScopedValueReadArchitectureTest}, checked here on the compiled
 * classes of this module's test classpath, which reach the PostgreSQL store and the Kafka and
 * RabbitMQ outbox publishers.
 */
@AnalyzeClasses(packages = "org.streamrune", importOptions = ImportOption.DoNotIncludeTests.class)
class ScopedValueReadArchitectureTest {

  private static final Set<String> READS = Set.of("get", "isBound", "orElse", "orElseThrow");

  @ArchTest
  static final ArchRule onlyTheReadHelperReadsAScopedValue =
      noClasses()
          .that()
          .doNotHaveFullyQualifiedName(ScopedValueReads.class.getName())
          .should()
          .accessTargetWhere(
              DescribedPredicate.describe(
                  "a read of a ScopedValue (get, isBound, orElse, orElseThrow)",
                  (JavaAccess<?> access) ->
                      access.getTarget().getOwner().isEquivalentTo(ScopedValue.class)
                          && READS.contains(access.getTarget().getName())));
}
