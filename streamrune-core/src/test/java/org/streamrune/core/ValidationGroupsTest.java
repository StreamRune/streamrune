package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;
import org.junit.jupiter.api.Test;

class ValidationGroupsTest {

  interface CreateOrder {}

  interface UpdateOrder {}

  @ValidationGroups({CreateOrder.class, UpdateOrder.class})
  record AnnotatedCommand() {}

  record UnannotatedCommand() {}

  @Test
  void shouldHaveRuntimeRetention() {
    var retention = ValidationGroups.class.getAnnotation(java.lang.annotation.Retention.class);
    assertNotNull(retention);
    assertEquals(RetentionPolicy.RUNTIME, retention.value());
  }

  @Test
  void shouldTargetType() {
    var target = ValidationGroups.class.getAnnotation(java.lang.annotation.Target.class);
    assertNotNull(target);
    assertArrayEquals(new ElementType[] {ElementType.TYPE}, target.value());
  }

  @Test
  void shouldContainSpecifiedGroups() {
    var annotation = AnnotatedCommand.class.getAnnotation(ValidationGroups.class);
    assertNotNull(annotation);
    assertArrayEquals(new Class<?>[] {CreateOrder.class, UpdateOrder.class}, annotation.value());
  }

  @Test
  void shouldBeAbsentWhenNotAnnotated() {
    var annotation = UnannotatedCommand.class.getAnnotation(ValidationGroups.class);
    assertNull(annotation);
  }
}
