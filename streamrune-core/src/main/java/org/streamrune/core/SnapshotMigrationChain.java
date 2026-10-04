package org.streamrune.core;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * An immutable, validated collection of {@link SnapshotMigration} steps that an {@link EventStore}
 * implementation applies on load to upgrade a stored snapshot from its schema version to the
 * version the caller expects — the store-agnostic realization of the chain contract documented on
 * {@link SnapshotMigration}.
 *
 * <p>Validated once at {@link #of construction}:
 *
 * <ul>
 *   <li>every step satisfies {@code toVersion() > fromVersion()};
 *   <li>no two steps share a {@code fromVersion()} (an ambiguous chain).
 * </ul>
 *
 * <p>{@link #migrate} walks the steps in ascending {@code fromVersion()} order from the stored
 * version up to the expected version. If a complete chain exists it returns the migrated state; if
 * <b>any</b> step is missing before the expected version is reached, or a step would overshoot the
 * expected version, it returns {@link Optional#empty()} — the snapshot is unmigratable and the
 * caller must discard it and replay from events. A partial chain is never applied.
 */
public final class SnapshotMigrationChain {

  private static final SnapshotMigrationChain EMPTY = new SnapshotMigrationChain(Map.of());

  /** Keyed by {@code fromVersion}; TreeMap only for deterministic error messages / iteration. */
  private final Map<Integer, SnapshotMigration> byFromVersion;

  private SnapshotMigrationChain(Map<Integer, SnapshotMigration> byFromVersion) {
    this.byFromVersion = byFromVersion;
  }

  /**
   * Builds a validated chain from {@code migrations}. A {@code null} or empty list yields the empty
   * chain (which reports {@link #isEmpty()} and migrates nothing). Validation is performed here,
   * once, so a store constructs its chain at startup and fails fast on a misconfigured migration
   * set rather than on the first load.
   *
   * @param migrations the migration steps, in any order (may be {@code null} or empty)
   * @return an immutable, validated chain
   * @throws IllegalArgumentException if a step is {@code null}, has {@code toVersion() <=
   *     fromVersion()}, or two steps share the same {@code fromVersion()}
   */
  public static SnapshotMigrationChain of(List<SnapshotMigration> migrations) {
    if (migrations == null || migrations.isEmpty()) {
      return EMPTY;
    }
    Map<Integer, SnapshotMigration> map = new TreeMap<>();
    for (SnapshotMigration m : migrations) {
      if (m == null) {
        throw new IllegalArgumentException("snapshot migration list contains a null element");
      }
      int from = m.fromVersion();
      int to = m.toVersion();
      if (to <= from) {
        throw new IllegalArgumentException(
            "snapshot migration "
                + m.getClass().getName()
                + " must have toVersion ("
                + to
                + ") > fromVersion ("
                + from
                + ")");
      }
      SnapshotMigration existing = map.putIfAbsent(from, m);
      if (existing != null) {
        throw new IllegalArgumentException(
            "duplicate snapshot migration fromVersion "
                + from
                + ": "
                + existing.getClass().getName()
                + " and "
                + m.getClass().getName()
                + " both migrate from that version — a chain must have at most one step per"
                + " fromVersion");
      }
    }
    return new SnapshotMigrationChain(Map.copyOf(map));
  }

  /** Whether this chain holds no migration steps (nothing can ever be migrated). */
  public boolean isEmpty() {
    return byFromVersion.isEmpty();
  }

  /**
   * Reports whether a complete migration chain exists from {@code fromVersion} up to exactly {@code
   * expectedVersion}, <b>without applying any step or touching the state</b>. Same reachability
   * logic as {@link #migrate} (ascending walk, any missing step or overshoot fails), but pure — it
   * lets a store decide, while a JDBC connection is still held and before deserializing the
   * snapshot payload, whether the snapshot is migratable (so it may narrow the event replay) or
   * must be discarded (so it must replay from the beginning).
   *
   * @param fromVersion the stored snapshot schema version
   * @param expectedVersion the schema version the caller requires
   * @return {@code true} iff a complete chain reaches exactly {@code expectedVersion}
   */
  public boolean canReach(int fromVersion, int expectedVersion) {
    if (fromVersion == expectedVersion) {
      return true;
    }
    if (fromVersion > expectedVersion) {
      return false;
    }
    int version = fromVersion;
    while (version < expectedVersion) {
      SnapshotMigration step = byFromVersion.get(version);
      if (step == null || step.toVersion() > expectedVersion) {
        return false;
      }
      version = step.toVersion();
    }
    return true;
  }

  /**
   * Attempts to migrate {@code state} from {@code fromVersion} up to {@code expectedVersion} by
   * applying the registered steps in ascending order.
   *
   * <ul>
   *   <li>{@code fromVersion == expectedVersion}: returns {@code state} unchanged.
   *   <li>A complete chain reaching exactly {@code expectedVersion} exists: returns the fully
   *       migrated state.
   *   <li>Any step is missing before {@code expectedVersion} is reached, or a step's {@code
   *       toVersion} would overshoot {@code expectedVersion}: returns {@link Optional#empty()} —
   *       the snapshot is unmigratable and the caller must discard it (full replay). No partial
   *       result is ever returned.
   * </ul>
   *
   * @param state the stored snapshot state at {@code fromVersion} (already deserialized against the
   *     {@code fromVersion} class)
   * @param fromVersion the stored snapshot schema version
   * @param expectedVersion the schema version the caller requires
   * @return the migrated state, or empty if no complete chain reaches {@code expectedVersion}
   * @throws NullPointerException if a registered migration step returns {@code null} (a programming
   *     error in the step) — the message names the offending {@code fromVersion->toVersion} step so
   *     the fault surfaces here rather than as a downstream NPE
   * @throws IllegalStateException if a step declares {@link SnapshotMigration#toType()} and its
   *     result is not an instance of that class — e.g. a step that returns its input unchanged
   *     instead of the upgraded state. Same "programming error in this step" treatment as the
   *     null-return case above, and for the same reason: silently accepting the wrong-shaped object
   *     would let the caller persist it tagged as the target version, and because the stored
   *     version would then match the expected one, no migration would ever run again — the
   *     corruption would be silent and permanent.
   */
  public Optional<AggregateState> migrate(
      AggregateState state, int fromVersion, int expectedVersion) {
    if (fromVersion == expectedVersion) {
      return Optional.of(state);
    }
    // A backwards or empty gap is not migratable; only forward chains are supported.
    if (fromVersion > expectedVersion) {
      return Optional.empty();
    }
    AggregateState current = state;
    int version = fromVersion;
    while (version < expectedVersion) {
      SnapshotMigration step = byFromVersion.get(version);
      if (step == null) {
        // Missing step before reaching the expected version -> gapped chain -> unmigratable.
        return Optional.empty();
      }
      int next = step.toVersion();
      if (next > expectedVersion) {
        // A step overshoots the expected version -> cannot land exactly on it -> unmigratable.
        return Optional.empty();
      }
      int from = version; // effectively-final snapshot for the diagnostic messages below
      current =
          Objects.requireNonNull(
              step.migrate(current),
              () ->
                  "snapshot migration "
                      + step.getClass().getName()
                      + " ("
                      + from
                      + "->"
                      + next
                      + ") returned null");
      Class<? extends AggregateState> declaredToType = step.toType();
      if (declaredToType != null && !declaredToType.isInstance(current)) {
        throw new IllegalStateException(
            "snapshot migration "
                + step.getClass().getName()
                + " ("
                + from
                + "->"
                + next
                + ") declared toType() "
                + declaredToType.getName()
                + " but returned an instance of "
                + current.getClass().getName()
                + " — a migration step must return the upgraded state, never its unchanged input"
                + " or another wrong-shaped object, or the caller would persist it mis-tagged as"
                + " the target version and never migrate it again");
      }
      version = next;
    }
    return Optional.of(current);
  }
}
