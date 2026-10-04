package org.streamrune.core.projection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

class ProjectionEpochRegressionExceptionTest {

  private static final String FORGED = "orders\n2026-10-04 WARN forged\r\u0085\u202e";

  @Test
  void message_namesTheProjectionSanitized() {
    var ex = new ProjectionEpochRegressionException(new ProjectionName(FORGED), 1, 57);

    assertTrue(ex.getMessage().contains(LogSanitizer.sanitizeForLog(FORGED)), ex.getMessage());
    assertFalse(ex.getMessage().chars().anyMatch(Character::isISOControl), ex.getMessage());
    assertFalse(ex.getMessage().contains("\u202e"), ex.getMessage());
  }

  @Test
  void message_staysReadableForAnOrdinaryName() {
    var ex = new ProjectionEpochRegressionException(new ProjectionName("orders"), 1, 57);

    assertTrue(ex.getMessage().startsWith("Projection 'orders' was handed leadership epoch 1"));
  }

  @Test
  void message_rendersANullNameAsNull() {
    var ex = new ProjectionEpochRegressionException(null, 1, 57);

    assertTrue(ex.getMessage().startsWith("Projection 'null' was handed"), ex.getMessage());
  }
}
