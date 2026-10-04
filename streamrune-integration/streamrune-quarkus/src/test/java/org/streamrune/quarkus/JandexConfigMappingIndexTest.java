package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexReader;
import org.junit.jupiter.api.Test;

/**
 * Guards the one fact that lets a real published-jar Quarkus consumer resolve {@link
 * StreamRuneQuarkusProperties}: this module is a plain CDI-producers library with no Quarkus {@code
 * -deployment} module / {@code BuildStep}, so the ONLY thing that auto-registers the
 * {@code @ConfigMapping} is that the built jar ships {@code META-INF/jandex.idx} (produced by the
 * bespoke {@code jandexIndex} Gradle task) which Quarkus's {@code ConfigBuildStep} scans.
 *
 * <p>If that jandex task ever regresses — index missing, stripped from the jar,
 * format-incompatible, or the interface not indexed — every published Quarkus consumer boots into a
 * hard {@code SRCFG00027: Could not find a mapping for ...StreamRuneQuarkusProperties}, yet the
 * framework build would otherwise stay GREEN because nothing else exercises this path. This test
 * reads the assembled jar's index directly and goes RED in that case.
 *
 * <p>The Gradle {@code test} task supplies the jar path via {@code -Dstreamrune.quarkus.jar} and
 * depends on the {@code jar} task, so the artifact exists when this runs.
 */
class JandexConfigMappingIndexTest {

  private static final DotName CONFIG_MAPPING =
      DotName.createSimple("io.smallrye.config.ConfigMapping");
  private static final DotName PROPERTIES =
      DotName.createSimple("org.streamrune.quarkus.StreamRuneQuarkusProperties");

  @Test
  void builtJarShipsJandexIndexRegisteringTheConfigMapping() throws Exception {
    String jarPath = System.getProperty("streamrune.quarkus.jar");
    assertThat(jarPath)
        .as("the Gradle test task must pass -Dstreamrune.quarkus.jar pointing at the built jar")
        .isNotNull();

    try (JarFile jar = new JarFile(jarPath)) {
      JarEntry indexEntry = jar.getJarEntry("META-INF/jandex.idx");
      assertThat(indexEntry)
          .as("published jar must ship META-INF/jandex.idx so Quarkus can index this library")
          .isNotNull();

      Index index;
      try (InputStream in = jar.getInputStream(indexEntry)) {
        index = new IndexReader(in).read();
      }

      ClassInfo properties = index.getClassByName(PROPERTIES);
      assertThat(properties)
          .as("jandex index must contain %s so Quarkus's ConfigBuildStep can find it", PROPERTIES)
          .isNotNull();

      assertThat(properties.hasDeclaredAnnotation(CONFIG_MAPPING))
          .as(
              "%s must carry @ConfigMapping in the index — the exact fact Quarkus's ConfigBuildStep"
                  + " relies on to auto-register the mapping",
              PROPERTIES)
          .isTrue();
    }
  }
}
