package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.Page;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.Versioned;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.SubscriptionName;

/**
 * Tests for {@link NativeReflectionTypes} — the shared native-hint derivation used by the parity
 * tests.
 */
class NativeReflectionTypesTest {

  @Test
  void jsonValueScanFindsEveryValueTypeIncludingTheOnesThatShippedUnregistered() {
    Set<Class<?>> valueTypes = NativeReflectionTypes.jsonValueTypesInCorePackage();

    // Every record in org.streamrune.core.types serialises as one JSON value; there are 18 of them.
    assertEquals(18, valueTypes.size(), () -> "value types: " + valueTypes);
    assertTrue(
        valueTypes.containsAll(
            List.of(
                SubjectId.class,
                ProjectionName.class,
                SubscriptionName.class,
                SagaType.class,
                IdempotencyKey.class,
                AggregateType.class,
                StreamId.class)));
  }

  @Test
  void reachabilityFollowsNestedRecordsAndGenericTypeArguments() {
    // Sort -> List<Order> -> Order(String, SortDirection): the enum is reached through a generic
    // type argument, which a raw-component walk would miss.
    Set<Class<?>> reachable = NativeReflectionTypes.reachableSerializableTypes(List.of(Sort.class));

    assertTrue(reachable.contains(Sort.class));
    assertTrue(reachable.contains(Sort.Order.class));
    assertTrue(reachable.contains(SortDirection.class));
  }

  @Test
  void reachabilityFollowsNestedEnums() {
    Set<Class<?>> reachable =
        NativeReflectionTypes.reachableSerializableTypes(List.of(SubscriptionHealth.class));

    assertTrue(reachable.contains(SubscriptionLifecycleState.class));
    assertTrue(
        reachable.contains(SubscriptionHealth.Status.class), () -> "reachable: " + reachable);
  }

  @Test
  void reachabilitySkipsTypeVariablesAndNonFrameworkTypes() {
    // Page<T>(List<T> content, ...): the raw List and the type variable T are not framework types.
    Set<Class<?>> reachable = NativeReflectionTypes.reachableSerializableTypes(List.of(Page.class));

    assertEquals(Set.of(Page.class), reachable);
  }

  @Test
  void reachabilityExcludesNonFrameworkRoots() {
    assertTrue(NativeReflectionTypes.reachableSerializableTypes(List.of(String.class)).isEmpty());
  }

  @Test
  void isFrameworkSerializableAcceptsRecordsAndEnumsButNotInterfacesOrForeignTypes() {
    assertTrue(NativeReflectionTypes.isFrameworkSerializable(SubjectId.class)); // record
    assertTrue(NativeReflectionTypes.isFrameworkSerializable(SortDirection.class)); // enum
    assertFalse(
        NativeReflectionTypes.isFrameworkSerializable(Command.class)); // framework interface
    assertFalse(NativeReflectionTypes.isFrameworkSerializable(String.class)); // foreign
  }

  @Test
  void hasJsonValueDistinguishesValueTypesFromOtherRecordsAndEnums() {
    assertTrue(NativeReflectionTypes.hasJsonValue(SubjectId.class)); // @JsonValue record
    assertTrue(
        NativeReflectionTypes.hasJsonValue(StreamId.class)); // @JsonValue on a derived method
    assertFalse(NativeReflectionTypes.hasJsonValue(Sort.class)); // record, no @JsonValue
    assertFalse(NativeReflectionTypes.hasJsonValue(SortDirection.class)); // not a record
  }

  @Test
  void classesInExtractsRawParameterizedAndSkipsTypeVariables() {
    // Raw class component: SubjectId(String value).
    Type rawStringComponent = componentType(SubjectId.class, "value");
    assertEquals(List.of(String.class), NativeReflectionTypes.classesIn(rawStringComponent));

    // Parameterized component: Sort(List<Order> orders).
    Type parameterized = componentType(Sort.class, "orders");
    assertEquals(
        List.of(List.class, Sort.Order.class), NativeReflectionTypes.classesIn(parameterized));

    // Type-variable component: Versioned<T>(T data, ...).
    Type typeVariable = componentType(Versioned.class, "data");
    assertTrue(NativeReflectionTypes.classesIn(typeVariable).isEmpty());
  }

  @Test
  void expectedSetIsTheUnionOfValueTypesAndReachableTypes() {
    Set<Class<?>> expected =
        NativeReflectionTypes.expectedNativeReflectionTypes(List.of(SubscriptionHealth.class));

    // Value types come from the package scan.
    assertTrue(expected.contains(SubjectId.class));
    // Nested enums come from reachability.
    assertTrue(expected.contains(SubscriptionLifecycleState.class));
    assertTrue(expected.contains(SubscriptionHealth.Status.class));
  }

  @Test
  void expectedSetContainsBothStreamIdAndAggregateType() {
    var expected = NativeReflectionTypes.expectedNativeReflectionTypes(List.of());
    assertTrue(expected.contains(StreamId.class));
    assertTrue(expected.contains(AggregateType.class));
  }

  private static Type componentType(Class<?> record, String componentName) {
    for (RecordComponent component : record.getRecordComponents()) {
      if (component.getName().equals(componentName)) {
        return component.getGenericType();
      }
    }
    throw new AssertionError("no component " + componentName + " on " + record);
  }
}
