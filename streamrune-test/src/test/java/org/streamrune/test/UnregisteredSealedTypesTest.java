package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.invoke.MethodHandles;
import org.junit.jupiter.api.Test;

class UnregisteredSealedTypesTest {

  @Test
  void definesASealedInterfaceThatReportsNoPermittedSubclasses() {
    Class<?> type = UnregisteredSealedTypes.sealedInterface(MethodHandles.lookup());

    assertTrue(type.isInterface());
    assertTrue(type.isSealed());
    assertEquals(0, type.getPermittedSubclasses().length);
    assertEquals(UnregisteredSealedTypesTest.class.getPackageName(), type.getPackageName());
  }

  @Test
  void eachCallDefinesADistinctType() {
    assertNotSame(
        UnregisteredSealedTypes.sealedInterface(MethodHandles.lookup()),
        UnregisteredSealedTypes.sealedInterface(MethodHandles.lookup()));
  }

  @Test
  void aLookupWithoutPackageAccessIsRejected() {
    MethodHandles.Lookup publicOnly = MethodHandles.publicLookup();

    assertThrows(
        IllegalArgumentException.class, () -> UnregisteredSealedTypes.sealedInterface(publicOnly));
  }
}
