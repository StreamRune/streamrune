package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * A type-safe wrapper for saga type names.
 *
 * <p>SagaType provides stronger typing compared to raw Strings, making the codebase more type-safe
 * and reducing the risk of typos and invalid type references. Serializes as a plain JSON string,
 * like the other identifier value types.
 *
 * @param value the saga type name — the saga state's fully-qualified class name when derived by
 *     {@link #fromClass(Class)}, which is how the framework's runners derive it
 */
public record SagaType(@JsonValue String value) {

  /** Creates a new SagaType with the given value. */
  @JsonCreator
  public SagaType {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("SagaType value must not be blank");
    }
  }

  /**
   * Creates a SagaType from a String value.
   *
   * @param value the saga type name
   * @return new SagaType
   */
  public static SagaType of(String value) {
    return new SagaType(value);
  }

  /**
   * The longest saga type name the PostgreSQL schema stores: {@code saga_state.saga_type} and
   * {@code saga_dead_letters.saga_type} are {@code VARCHAR(255)}.
   */
  private static final int MAX_DERIVED_LENGTH = 255;

  /**
   * Creates a SagaType from a Class using its <b>fully-qualified</b> name ({@link
   * Class#getName()}). Every framework runner ({@code SagaRunner}, {@code SagaTimeoutRunner},
   * {@code SagaCompensationRetrySweeper}, {@code SagaDeadLetterReplayer}) derives its saga type
   * this way from the saga state class, and the stores persist that value in {@code saga_type}. The
   * same value is the {@code saga.type} tag of every saga metric and names the saga drivers' health
   * components ({@code saga-compensation-retry:<value>}, {@code saga-timeout:<value>}).
   *
   * <p>The fully-qualified name is collision-free across packages: two saga-state classes with the
   * same simple name (e.g. {@code com.shop.fulfillment.OrderSagaState} and {@code
   * com.shop.refund.OrderSagaState}) derive distinct types. Under a colliding {@link
   * org.streamrune.core.saga.SagaId SagaId} the saga store's type-scoping stops one type from
   * reading, CAS-writing or deleting the other's row; the store is keyed by saga id alone, so the
   * second create fails loudly with {@link org.streamrune.core.saga.SagaTypeCollisionException}.
   * Distinct types also keep their metric series and health components apart. A nested class
   * renders with {@code $} (e.g. {@code com.shop.Sagas$OrderState}). Renaming or moving a
   * saga-state class changes its saga type: rows persisted under the old name are not found under
   * the new one.
   *
   * @param clazz the class to derive the saga type name from
   * @return a SagaType with the fully-qualified class name
   * @throws IllegalArgumentException if the fully-qualified name is longer than 255 characters (it
   *     would not fit the {@code saga_type} column)
   */
  public static SagaType fromClass(Class<?> clazz) {
    String name = clazz.getName();
    if (name.length() > MAX_DERIVED_LENGTH) {
      throw new IllegalArgumentException(
          "saga state class name is "
              + name.length()
              + " characters long; the saga_type column holds at most "
              + MAX_DERIVED_LENGTH
              + ": "
              + name);
    }
    return new SagaType(name);
  }

  @Override
  public String toString() {
    return value;
  }
}
