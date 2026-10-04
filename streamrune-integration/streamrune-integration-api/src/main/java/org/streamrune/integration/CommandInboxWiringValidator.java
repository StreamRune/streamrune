package org.streamrune.integration;

import org.streamrune.core.CommandInbox;

/**
 * Validates the composition invariant shared by all three auto-configurations: <b>if the command
 * bus resolved a {@link CommandInbox}, the framework-built event store must have been built with
 * the same inbox implementation.</b>
 *
 * <p><b>Why it matters.</b> {@code CommandBus.execute(command, key)} promises effectively-once
 * execution, and that promise is only kept when the key claim and the event append commit in ONE
 * transaction. The bus's {@code commandInbox.find(key)} answers duplicates (before the aggregate
 * lock, under it, and after a keyed rejection) from the rows the store claims; the authoritative
 * claim happens inside {@code EventStore.appendWithKey}, which the PostgreSQL store performs only
 * when the store itself was built with an inbox. The two sides are wired from different injection
 * points and different types — the bus takes the {@code CommandInbox} interface, the event-store
 * factory takes the concrete {@code PostgresCommandInbox} (a concrete return type, needed so the
 * store can claim the key on its own connection) — so an application that supplies its own {@code
 * CommandInbox} suppresses (or outranks) the framework's default and silently breaks the pairing:
 *
 * <ul>
 *   <li>the bus reports {@code hasCommandInbox() == true}, so {@code execute(command, key)} sails
 *       past the pre-check into {@code appendWithKey}, which throws {@code
 *       UnsupportedOperationException("This PostgresEventStore was built without a command inbox")}
 *       — <b>every</b> keyed execution fails at runtime, including the saga executor's
 *       idempotency-keyed dispatch, dead-lettering or compensating perfectly healthy commands; or
 *   <li>where the framework default survives concrete-typed resolution, the store keeps claiming
 *       keys in the framework's own {@code command_inbox} table while the bus pre-checks the user's
 *       inbox — the override is silently ignored on the authoritative dedup path and the user's
 *       store never receives a row.
 * </ul>
 *
 * <p>The bean javadoc in all three integrations advertises the override ("Applications that supply
 * their own CommandInbox bean will suppress this default"), so the trap is reachable by following
 * the documentation. Nothing detected it before the first keyed command in production.
 *
 * <p><b>Scope: the framework-built event-store factory only.</b> An application that supplies BOTH
 * a custom {@code CommandInbox} and its own {@code EventStoreFactory}/{@code EventStore} is a
 * legitimate, fully-custom composition: its store may well honour the custom inbox atomically, and
 * the framework cannot introspect that. In that configuration the framework factory is not created
 * at all, so this validator never runs. The contract for such a factory is exactly the one above —
 * {@code appendWithKey} must claim the key in the same transaction as the append.
 *
 * <p>Comparison is by implementation CLASS, not identity: the Quarkus default inbox producer is
 * dependent-scoped, so the bus and the store legitimately hold two equivalent instances of the same
 * stateless {@code PostgresCommandInbox}.
 */
public final class CommandInboxWiringValidator {

  private CommandInboxWiringValidator() {}

  /**
   * Fails fast when the command bus has an inbox that the framework-built event store does not
   * share.
   *
   * @param busCommandInbox the {@link CommandInbox} the command bus resolved, or {@code null} when
   *     it has none (nothing then promises keyed atomicity, so there is nothing to break)
   * @param eventStoreCommandInbox the inbox the framework's event-store factory was built with, or
   *     {@code null} when it was built without one
   * @throws IllegalStateException when the bus has an inbox the event store was not built with
   */
  public static void validateFrameworkEventStoreWiring(
      CommandInbox busCommandInbox, CommandInbox eventStoreCommandInbox) {
    if (busCommandInbox == null) {
      return;
    }
    if (eventStoreCommandInbox != null
        && eventStoreCommandInbox.getClass().equals(busCommandInbox.getClass())) {
      return;
    }
    throw new IllegalStateException(
        "StreamRune command-inbox wiring: the command bus resolved a CommandInbox ("
            + busCommandInbox.getClass().getName()
            + ") that the framework's PostgreSQL event store was "
            + (eventStoreCommandInbox == null
                ? "built WITHOUT"
                : "NOT built with (it got " + eventStoreCommandInbox.getClass().getName() + ")")
            + ". The event-store factory resolves the concrete PostgresCommandInbox, so a custom"
            + " CommandInbox bean is used for the bus's inbox lookups but NOT for the authoritative,"
            + " same-transaction key claim inside EventStore.appendWithKey. Every keyed execution —"
            + " including the saga executor's idempotency-keyed command dispatch — would therefore"
            + " lose the effectively-once guarantee execute(command, key) documents, failing with"
            + " UnsupportedOperationException or silently deduplicating in the wrong table. Either"
            + " (a) supply a PostgresCommandInbox bean of your own so the bus and the store"
            + " share one inbox implementation, or (b) also supply your own EventStoreFactory /"
            + " EventStore whose appendWithKey claims the idempotency key in the same transaction as"
            + " the append — the framework then steps aside entirely and this check does not run.");
  }
}
