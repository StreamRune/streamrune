package org.streamrune.core.projection;

import java.util.Objects;

/**
 * The filter of {@link ProjectionRepository#findAllBy}: the read models whose top-level field
 * {@code field} holds the text {@code value}.
 *
 * <p>The comparison is on text. A repository compares the field as its stored JSON renders it as
 * text — on PostgreSQL {@code data->>'field'}, so a string compares by its characters, the number
 * {@code 42} as {@code "42"}, {@code true} as {@code "true"} — and equal means equal character by
 * character, with no case folding and no number parsing ({@code "042"} does not match {@code 42}).
 * A read model whose field is missing or JSON {@code null} matches no filter. Filter on a scalar
 * field: an object or array field compares as JSON text, which the stores do not format alike. An
 * {@code @Encrypted} field is stored as ciphertext and matches no plaintext value.
 *
 * <p>{@code field} names a top-level field of the read model as the repository's JSON mapper writes
 * it. A repository refuses, with an {@link IllegalArgumentException} at the call, a field outside
 * {@code [A-Za-z0-9_]+} (nested paths included) and the names of its own columns {@code id}, {@code
 * version} and {@code updated_at}; to read one row by id, use {@link
 * ProjectionRepository#findById}.
 *
 * @param field the top-level read-model field; not {@code null}, not blank
 * @param value the text the field must hold; not {@code null}
 */
public record FieldEquals(String field, String value) {

  /**
   * Checks the arguments.
   *
   * @throws NullPointerException if {@code field} or {@code value} is {@code null}
   * @throws IllegalArgumentException if {@code field} is blank
   */
  public FieldEquals {
    Objects.requireNonNull(field, "field");
    if (field.isBlank()) {
      throw new IllegalArgumentException("field must not be blank");
    }
    Objects.requireNonNull(value, "value");
  }

  /**
   * The read models whose top-level {@code field} holds the text {@code value}.
   *
   * @param field the top-level read-model field
   * @param value the text the field must hold
   * @return the filter
   */
  public static FieldEquals of(String field, String value) {
    return new FieldEquals(field, value);
  }
}
