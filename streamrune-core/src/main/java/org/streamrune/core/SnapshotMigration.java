package org.streamrune.core;

/**
 * Upgrades a snapshot state from one schema version to another.
 *
 * <p>Core defines only the migration step itself — the {@link EventStore} interface has no
 * registration method, so how (and whether) migrations are registered and applied is event-store
 * implementation-specific. An implementation that supports migrations must document its
 * registration point (typically its factory or builder); code written against the core abstractions
 * alone must not assume migrations run.
 *
 * <p><b>Relationship to {@link EventStore#load(org.streamrune.core.types.StreamId, int)}.</b> That
 * method is the baseline snapshot-evolution strategy: on a snapshot schema-version mismatch the
 * snapshot is discarded and all events are replayed. Migrations are an optional optimization an
 * implementation may layer on top. Where both are in play, the implementation must first try to
 * migrate the stored snapshot up to the expected version and fall back to discarding it (full
 * replay) when no complete migration chain exists — applying migrations never changes the result,
 * only the cost of reaching it.
 *
 * <p><b>Chain contract</b> for implementations that apply migrations:
 *
 * <ul>
 *   <li>Each step must satisfy {@code toVersion() > fromVersion()}; steps are applied in ascending
 *       {@code fromVersion()} order.
 *   <li>Two registered migrations with the same {@code fromVersion()} are ambiguous and must be
 *       rejected at registration time.
 *   <li>A gapped chain (e.g. 1&rarr;2 registered, snapshot at version 1, expected version 3, no
 *       2&rarr;3 step) must not be applied partially: the snapshot is unmigratable and must be
 *       discarded, exactly like a plain version mismatch.
 * </ul>
 *
 * <p>Example — adding an {@code age} field with a default of 0 when upgrading from version 1 to 2:
 *
 * <pre>{@code
 * class AddAgeFieldMigration implements SnapshotMigration {
 *   public int fromVersion() { return 1; }
 *   public int toVersion()   { return 2; }
 *   public AggregateState migrate(AggregateState state) {
 *     UserState v1 = (UserState) state;
 *     return new UserStateV2(v1.name(), 0);
 *   }
 * }
 * }</pre>
 */
public interface SnapshotMigration {

  /** The snapshot schema version this migration reads from. */
  int fromVersion();

  /** The snapshot schema version this migration produces. */
  int toVersion();

  /**
   * Upgrades {@code state} from {@link #fromVersion()} to {@link #toVersion()}.
   *
   * @param state the snapshot state at {@link #fromVersion()}
   * @return the migrated state at {@link #toVersion()}, never {@code null}
   */
  AggregateState migrate(AggregateState state);

  /**
   * The concrete class {@link #migrate} is expected to return, or {@code null} (the default) if
   * this step declares none.
   *
   * <p>When declared, {@link SnapshotMigrationChain#migrate} verifies the step's return value is an
   * instance of this class and fails fast — the same "programming error in this step, not a chain
   * configuration gap" treatment as a step returning {@code null} — if it is not. Undeclared, no
   * check runs.
   *
   * <p>This exists to close a specific silent-corruption path: a step that returns its INPUT
   * unchanged (or any other wrong-shaped object) instead of the upgraded state passes {@link
   * SnapshotMigrationChain#migrate}'s other check — {@code migrate} returned non-{@code null} — and
   * the chain reports success. The caller then persists that wrong-shaped object tagged with the
   * step's {@code toVersion()}; every later load takes the "compatible" fast path (no mismatch, so
   * no migration ever runs again) and rehydrates from the wrong-shaped state forever. Declaring
   * {@code toType()} turns that silent, permanent corruption into an immediate, loud failure naming
   * the offending step instead.
   *
   * @return the class {@link #migrate}'s result must be an instance of, or {@code null} to skip the
   *     check
   */
  default Class<? extends AggregateState> toType() {
    return null;
  }
}
