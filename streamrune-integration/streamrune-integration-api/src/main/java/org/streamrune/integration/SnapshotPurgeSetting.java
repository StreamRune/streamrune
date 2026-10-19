package org.streamrune.integration;

import java.util.List;
import java.util.Locale;
import org.streamrune.core.gdpr.SnapshotPurger;
import org.streamrune.core.gdpr.SubjectDataPurger;

/**
 * The value of {@code streamrune.gdpr.snapshot-purge}: whether the {@code ForgetSubjectService} an
 * integration builds must run a {@link SnapshotPurger}.
 *
 * <ul>
 *   <li>{@link #REQUIRED} (the default) — a {@link SnapshotPurger} bean must be registered beside
 *       the read-model purgers; for {@code PostgresEventStore}, a {@code
 *       PostgresSnapshotStorePurger} bean
 *   <li>{@link #NONE} — the application has never stored a snapshot; the service is built with
 *       {@code withoutSnapshotPurge()}
 * </ul>
 *
 * <p>The integration cannot register the snapshot purger itself: which streams' snapshots hold a
 * subject's data is a decision of the application's domain (the purger takes it as a {@code
 * subjectId -> streams} mapping). It checks the purger beans against this setting with {@link
 * #withoutSnapshotPurge(List)} before building the service, so the refusal names the bean and the
 * property to set rather than the builder method.
 */
public enum SnapshotPurgeSetting {
  REQUIRED,
  NONE;

  /** The property name, {@value}. */
  public static final String PROPERTY = "streamrune.gdpr.snapshot-purge";

  /**
   * Parses a configured value, case-insensitively and ignoring surrounding whitespace.
   *
   * @param value the configured text; {@code null} and blank mean {@link #REQUIRED}
   * @throws IllegalArgumentException if the value is neither {@code required} nor {@code none}; the
   *     message names the property and the value
   */
  public static SnapshotPurgeSetting parse(String value) {
    if (value == null || value.isBlank()) {
      return REQUIRED;
    }
    return switch (value.strip().toLowerCase(Locale.ROOT)) {
      case "required" -> REQUIRED;
      case "none" -> NONE;
      default ->
          throw new IllegalArgumentException(
              PROPERTY + " must be required or none (case-insensitive); got: '" + value + "'");
    };
  }

  /**
   * Checks the application's purger beans against this setting and answers whether the service must
   * be built with {@code withoutSnapshotPurge()}.
   *
   * @param purgers every {@link SubjectDataPurger} bean the service will run
   * @return {@code true} under {@link #NONE}, {@code false} under {@link #REQUIRED}
   * @throws IllegalStateException under {@link #REQUIRED} when no purger is a {@link
   *     SnapshotPurger}, and under {@link #NONE} when one is; the message says what to change
   */
  public boolean withoutSnapshotPurge(List<? extends SubjectDataPurger> purgers) {
    List<String> snapshotPurgers =
        purgers.stream()
            .filter(SnapshotPurger.class::isInstance)
            .map(purger -> purger.getClass().getName())
            .toList();
    if (this == REQUIRED && snapshotPurgers.isEmpty()) {
      throw new IllegalStateException(
          "The ForgetSubjectService StreamRune configures has no snapshot purger. An aggregate"
              + " snapshot stores the aggregate's state as it was when the snapshot was taken,"
              + " every field not marked @Encrypted in the clear, so after a forget it still holds"
              + " whatever that state kept of the subject's data. Declare a SnapshotPurger bean —"
              + " for PostgresEventStore: new PostgresSnapshotStorePurger(dataSource, subjectId ->"
              + " the streams whose snapshots hold the subject's data); for another store, a"
              + " purger of your own that implements SnapshotPurger. A wrapper or proxy around a"
              + " snapshot purger must implement SnapshotPurger itself. The ForgetSubjectService"
              + " runs it with the other SubjectDataPurger beans — or set "
              + PROPERTY
              + "=none if this application has never stored a snapshot.");
    }
    if (this == NONE && !snapshotPurgers.isEmpty()) {
      throw new IllegalStateException(
          PROPERTY
              + "=none says this application has never stored a snapshot, but a snapshot purger"
              + " bean is registered ("
              + String.join(", ", snapshotPurgers)
              + "): keep the bean and remove "
              + PROPERTY
              + "=none, or remove the bean if the application really stores none.");
    }
    return this == NONE;
  }
}
