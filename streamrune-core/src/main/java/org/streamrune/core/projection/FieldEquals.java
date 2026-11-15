package org.streamrune.core.projection;

import java.util.Set;
import org.streamrune.core.types.LogSanitizer;

/**
 * The filter of {@link ProjectionRepository#findAllBy}: the read models whose top-level field
 * {@code field} holds the text {@code value}.
 *
 * <p>The comparison is on text. A repository compares the field as its stored JSON renders it as
 * text — on PostgreSQL {@code data->>'field'} — and equal means equal character by character, with
 * no case folding and no number parsing:
 *
 * <ul>
 *   <li>a string compares by its characters, a boolean as {@code "true"} or {@code "false"};
 *   <li>a whole number as its digits ({@code 42} as {@code "42"}; {@code "042"} does not match);
 *   <li>a decimal number as PostgreSQL prints {@code numeric}: no exponent, the scale as written
 *       ({@code BigDecimal} 19.90 as {@code "19.90"}, the {@code double} 1.2345678E7 as {@code
 *       "12345678"}, 1.0E-4 as {@code "0.00010"}, -0.0 as {@code "0.0"}, a {@code Duration} of 90 s
 *       as {@code "90.000000000"});
 *   <li>a missing or JSON-{@code null} field matches no value.
 * </ul>
 *
 * <p>Filter on a scalar field: an object or array field compares as JSON text, which the stores do
 * not format alike.
 *
 * <p>{@code field} names a top-level field of the read model as the repository's JSON mapper writes
 * it. Refused with an {@link IllegalArgumentException}: a blank field and the names of the
 * repository's own columns {@code id}, {@code version} and {@code updated_at} (here, at
 * construction); a field outside {@code [A-Za-z0-9_]+} (nested paths included), and an {@code
 * Encrypted} record component of the read-model type or of a record it embeds with {@code
 * JsonUnwrapped}, whose stored text is ciphertext (by the repository, at the read; a read-model
 * type that is not a record, such as {@code Map}, is not checked). To read one row by id, use
 * {@link ProjectionRepository#findById}.
 *
 * @param field the top-level read-model field; not {@code null}, not blank, not a column name
 * @param value the text the field must hold; not {@code null}
 */
public record FieldEquals(String field, String value) {

  /** The repository's own columns, which a read-model field of the same name is hidden by. */
  private static final Set<String> COLUMNS = Set.of("id", "version", "updated_at");

  /**
   * Checks the arguments.
   *
   * @throws IllegalArgumentException if {@code field} is {@code null}, blank or a column name, or
   *     {@code value} is {@code null}
   */
  public FieldEquals {
    if (field == null || field.isBlank()) {
      throw new IllegalArgumentException("field cannot be null or blank");
    }
    if (COLUMNS.contains(field)) {
      throw new IllegalArgumentException(
          "A filter names a read-model field, not the repository column '"
              + field
              + "': read one row by id with findById; version and updated_at cannot be filtered");
    }
    if (value == null) {
      throw new IllegalArgumentException(
          "value cannot be null (filter field " + LogSanitizer.sanitizeForLog(field) + ")");
    }
  }

  /**
   * The read models whose top-level {@code field} holds the text {@code value}.
   *
   * @param field the top-level read-model field
   * @param value the text the field must hold
   * @return the filter
   * @throws IllegalArgumentException as the constructor
   */
  public static FieldEquals of(String field, String value) {
    return new FieldEquals(field, value);
  }
}
