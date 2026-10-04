package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.streamrune.spring.scanfixture.ScanFixtureComponent;

/**
 * Proves that {@link StreamRuneComponent} is a working Spring stereotype: classes annotated with it
 * are registered as beans by a regular component scan of the application's own packages, without
 * any StreamRune-specific scan configuration.
 */
class StreamRuneComponentScanTest {

  @Test
  void streamRuneComponentClassesAreRegisteredByDefaultComponentScan() {
    try (var ctx = new AnnotationConfigApplicationContext()) {
      ctx.scan("org.streamrune.spring.scanfixture");
      ctx.refresh();
      assertThat(ctx.getBean(ScanFixtureComponent.class)).isNotNull();
    }
  }
}
