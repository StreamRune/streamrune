package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.streamrune.test.UnregisteredSealedTypes;

class SealedHierarchyTest {

  sealed interface Shape permits Circle, Polygon {}

  record Circle() implements Shape {}

  sealed interface Polygon extends Shape permits Square {}

  record Square() implements Polygon {}

  interface Open {}

  @Test
  void aSealedTypeReturnsItsDirectPermittedSubclasses() {
    assertEquals(
        Set.of(Circle.class, Polygon.class),
        Set.copyOf(SealedHierarchy.permittedSubclasses(Shape.class, "a test walk")));
    assertEquals(
        List.of(Square.class), SealedHierarchy.permittedSubclasses(Polygon.class, "a test walk"));
  }

  @Test
  void aTypeThatIsNotSealedHasNoPermittedSubclasses() {
    assertTrue(SealedHierarchy.permittedSubclasses(Circle.class, "a test walk").isEmpty());
    assertTrue(SealedHierarchy.permittedSubclasses(Open.class, "a test walk").isEmpty());
    assertTrue(SealedHierarchy.permittedSubclasses(String.class, "a test walk").isEmpty());
    assertTrue(SealedHierarchy.permittedSubclasses(int.class, "a test walk").isEmpty());
    assertTrue(SealedHierarchy.permittedSubclasses(Shape[].class, "a test walk").isEmpty());
  }

  @Test
  void aSealedTypeWithNoReadablePermittedSubclassIsRefusedNotTreatedAsALeaf() {
    Class<?> unreadable = UnregisteredSealedTypes.sealedInterface(MethodHandles.lookup());
    // The shape a native image reports for an unregistered sealed type: sealed, nothing to walk.
    assertTrue(unreadable.isSealed());
    assertEquals(0, unreadable.getPermittedSubclasses().length);

    IllegalStateException refused =
        assertThrows(
            IllegalStateException.class,
            () -> SealedHierarchy.permittedSubclasses(unreadable, "the walk under test"));
    String message = refused.getMessage();
    assertTrue(message.contains(unreadable.getName()), message);
    assertTrue(message.contains("the walk under test"), message);
    assertTrue(message.contains("native image"), message);
    assertTrue(message.contains("registered for reflection"), message);
    // How to register it: a plain type entry in the image's reachability metadata. On Quarkus,
    // @RegisterForReflection on the sealed type is not enough (measured: Quarkus 3.32 emits a
    // legacy reflect-config entry without permitted subclasses, and the binary still refused).
    assertTrue(message.contains("reachability-metadata.json"), message);
    assertTrue(message.contains("@RegisterForReflection"), message);
    // On Micronaut, @TypeHint / @ReflectiveAccess on the sealed type are not enough either
    // (measured: Micronaut 4.10 registers the type through RuntimeReflection.register(Class),
    // which leaves the permitted subclasses unregistered, and the demo's binary still refused).
    assertTrue(message.contains("@TypeHint"), message);
    assertTrue(message.contains("@ReflectiveAccess"), message);
  }

  @Test
  void nullArgumentsAreRejected() {
    assertThrows(
        IllegalArgumentException.class, () -> SealedHierarchy.permittedSubclasses(null, "walk"));
    assertThrows(
        IllegalArgumentException.class,
        () -> SealedHierarchy.permittedSubclasses(Shape.class, null));
  }
}
