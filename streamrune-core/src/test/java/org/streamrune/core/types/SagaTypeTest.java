package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.classfile.ClassFile;
import java.lang.constant.ClassDesc;
import java.lang.invoke.MethodHandles;
import org.junit.jupiter.api.Test;

class SagaTypeTest {

  @Test
  void constructsWithValue() {
    assertEquals("OrderFulfillmentSaga", new SagaType("OrderFulfillmentSaga").value());
  }

  @Test
  void ofIsAlias() {
    assertEquals(SagaType.of("OrderFulfillmentSaga"), new SagaType("OrderFulfillmentSaga"));
  }

  @Test
  void rejectsNull() {
    assertThrows(IllegalArgumentException.class, () -> new SagaType(null));
  }

  @Test
  void rejectsBlank() {
    assertThrows(IllegalArgumentException.class, () -> new SagaType(""));
    assertThrows(IllegalArgumentException.class, () -> new SagaType("   "));
  }

  @Test
  void fromClassUsesTheFullyQualifiedName() {
    assertEquals(
        SagaType.of("org.streamrune.core.types.SagaTypeTest"),
        SagaType.fromClass(SagaTypeTest.class));
  }

  // Two saga-state classes with the SAME simple name in different scopes. Distinct
  // holders give two nested classes the same innermost simple name, reproducing the cross-package
  // case.
  static final class Fulfillment {
    record OrderSagaState() {}
  }

  static final class Refund {
    record OrderSagaState() {}
  }

  @Test
  void fromClass_sameSimpleNameDifferentScopes_deriveDistinctTypes() {
    assertEquals("OrderSagaState", Fulfillment.OrderSagaState.class.getSimpleName());
    assertEquals("OrderSagaState", Refund.OrderSagaState.class.getSimpleName());
    assertNotEquals(
        SagaType.fromClass(Fulfillment.OrderSagaState.class),
        SagaType.fromClass(Refund.OrderSagaState.class));
  }

  @Test
  void fromClass_nestedClass_rendersWithDollar() {
    assertEquals(
        "org.streamrune.core.types.SagaTypeTest$Fulfillment$OrderSagaState",
        SagaType.fromClass(Fulfillment.OrderSagaState.class).value());
  }

  @Test
  void fromClass_rejectsANameTheSagaTypeColumnCannotHold() throws Exception {
    // saga_type is VARCHAR(255): a longer derived name is refused at derivation, not at the first
    // INSERT. A hidden class defined in this package carries the long name.
    byte[] bytes =
        ClassFile.of()
            .build(ClassDesc.of("org.streamrune.core.types." + "L".repeat(240)), cb -> {});
    Class<?> longNamed = MethodHandles.lookup().defineHiddenClass(bytes, false).lookupClass();
    assertTrue(longNamed.getName().length() > 255);
    var thrown = assertThrows(IllegalArgumentException.class, () -> SagaType.fromClass(longNamed));
    assertTrue(thrown.getMessage().contains("at most 255"));
  }

  @Test
  void toStringReturnsValue() {
    assertEquals("PaymentSaga", SagaType.of("PaymentSaga").toString());
  }

  @Test
  void serializesAsPlainStringAndRoundTrips() throws Exception {
    var mapper = new ObjectMapper();
    var type = SagaType.of("OrderFulfillmentSaga");
    assertEquals("OrderFulfillmentSaga", type.toString());
    assertEquals("\"OrderFulfillmentSaga\"", mapper.writeValueAsString(type));
    assertEquals(type, mapper.readValue("\"OrderFulfillmentSaga\"", SagaType.class));
  }
}
