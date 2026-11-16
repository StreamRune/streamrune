package org.streamrune.integration;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.streamrune.core.Internal;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.LogSanitizer;

/**
 * How the three framework integrations route one projection registration to the {@link
 * CheckpointTarget} its runner reads and commits through: a {@link ReadModelStore}'s, or the event
 * store's subscription checkpoint. A pure function of what the registration declares, what its
 * projection exposes ({@link Projection#writeTarget()}), the stores defined and the leadership —
 * resolved once per registration at startup, so every failure is a startup error.
 *
 * <p>The rules, in order; the first that applies decides:
 *
 * <ol>
 *   <li><b>Explicit declaration.</b> A non-empty {@link Registration#readModelStore()} names the
 *       store. An unknown name is an error, and so is a {@code writeTarget()} that is not that
 *       store's repository: the declaration and the code disagree.
 *   <li><b>{@code writeTarget()} matches a store</b> — its {@link
 *       ProjectionRepository#writeTargetIdentity()} equals the store repository's — routes there.
 *   <li><b>{@link ProjectionDeliveryMode#TRANSACTIONAL_LOCAL} / {@link
 *       ProjectionDeliveryMode#EXTERNAL_EFFECT}, no match:</b> a projection exposing no {@code
 *       writeTarget()} goes to the one store whose processor is {@link
 *       AtomicBatchProcessor#transactional()}, accepted as <em>declared</em> write-through. A
 *       {@code writeTarget()} that belongs to no store, no transactional store, or several are an
 *       error.
 *   <li><b>{@link ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT}, no match</b> (no {@code
 *       writeTarget()}, or one that belongs to no store): the subscription checkpoint.
 * </ol>
 *
 * <p>Then the routed target's processor must carry what the registration needs: {@link
 * AtomicBatchProcessor#transactional()} for a transactional mode, and {@link
 * AtomicBatchProcessor#fencesCheckpoint()} under real leadership (anything but {@link
 * SubscriptionLeadership#NOOP}), whatever the mode.
 *
 * <p>Every refusal is an {@link IllegalStateException} that names the projection, its mode, the
 * stores with their capabilities, and the fix: declare {@code readModelStore = "<name>"}, or expose
 * {@code writeTarget()}.
 *
 * <p>The INLINE rule lives here too: an INLINE projection runs post-commit on the command thread
 * with no checkpoint and no transaction of its own, so it can only keep the {@link
 * ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT} promise.
 *
 * @hidden
 */
@Internal
public final class ProjectionStoreRouting {

  private ProjectionStoreRouting() {}

  /**
   * One discovered registration, as routing needs it.
   *
   * @param projectionName the name it is registered under (sanitized into messages)
   * @param mode the delivery mode its {@code @ProjectionConfig} declares
   * @param readModelStore the store its {@code @ProjectionConfig} names, or {@code ""} for none
   * @param projection the registered projection, decorators included: they forward {@link
   *     Projection#writeTarget()}
   */
  public record Registration(
      String projectionName,
      ProjectionDeliveryMode mode,
      String readModelStore,
      Projection projection) {

    /**
     * Validates the registration.
     *
     * @throws NullPointerException if any component is {@code null}
     */
    public Registration {
      Objects.requireNonNull(projectionName, "projectionName is required");
      Objects.requireNonNull(mode, "mode is required");
      Objects.requireNonNull(readModelStore, "readModelStore is required (\"\" for none)");
      Objects.requireNonNull(projection, "projection is required");
    }
  }

  /** The one INFO sentence the integrations log per INLINE registration. */
  public static final String INLINE_INFO =
      "Projection '{}' delivery mode AT_LEAST_ONCE_IDEMPOTENT inline — post-commit on the command"
          + " thread, no checkpoint; a failure is logged and the batch is not retried";

  /** The fix every routing refusal offers, as a clause. */
  private static final String FIX =
      "declare readModelStore = \"<name>\" in its @ProjectionConfig naming the store it writes to,"
          + " or expose writeTarget() returning that store's repository";

  /**
   * Refuses a store set routing could not decide over: two stores with the same {@link
   * ReadModelStore#name()} (explicit routing would be ambiguous), or two whose repositories have
   * equal {@link ProjectionRepository#writeTargetIdentity()} (rule 2 would match both). Replacing a
   * store is by name and explicit: define one store under that name, never a second.
   *
   * @param stores every store defined
   * @throws NullPointerException if {@code stores} or an element is {@code null}
   * @throws IllegalStateException on a duplicate name or write-target identity, naming both stores
   */
  public static void validateStores(List<ReadModelStore> stores) {
    Objects.requireNonNull(stores, "stores is required");
    for (int i = 0; i < stores.size(); i++) {
      ReadModelStore first = Objects.requireNonNull(stores.get(i), "stores must not hold null");
      for (int j = i + 1; j < stores.size(); j++) {
        ReadModelStore second = Objects.requireNonNull(stores.get(j), "stores must not hold null");
        if (first.name().equals(second.name())) {
          throw new IllegalStateException(
              "Two read-model stores are named '"
                  + LogSanitizer.sanitizeForLog(first.name())
                  + "': "
                  + describe(first)
                  + " and "
                  + describe(second)
                  + ". Store names are unique: rename one, or — to replace a store — define only"
                  + " the one you want under that name.");
        }
        if (sameStore(first.repository(), second.repository())) {
          throw new IllegalStateException(
              "Read-model stores '"
                  + LogSanitizer.sanitizeForLog(first.name())
                  + "' and '"
                  + LogSanitizer.sanitizeForLog(second.name())
                  + "' write to the same place (their repositories' writeTargetIdentity() are"
                  + " equal): "
                  + describe(first)
                  + " and "
                  + describe(second)
                  + ". A projection's writeTarget() could not choose between them. Define one store"
                  + " per place.");
        }
      }
    }
  }

  /**
   * The repository of the store named {@code name} — the integrations' injectable {@code
   * ProjectionRepository} bean, which query handlers and {@code BaseProjection} constructors read
   * and write: the default read-model store's.
   *
   * @param stores every store defined (0, 1 or more)
   * @param name the default store's name
   * @return that store's {@link ReadModelStore#repository()}
   * @throws NullPointerException if an argument or an element of {@code stores} is {@code null}
   * @throws IllegalStateException when no store has that name (the application's stores replaced
   *     the default one without taking its name), naming the stores and the fix
   */
  public static ProjectionRepository defaultRepository(List<ReadModelStore> stores, String name) {
    Objects.requireNonNull(stores, "stores is required");
    Objects.requireNonNull(name, "name is required");
    return stores.stream()
        .map(store -> Objects.requireNonNull(store, "stores must not hold null"))
        .filter(store -> name.equals(store.name()))
        .findFirst()
        .map(ReadModelStore::repository)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "No read-model store is named '"
                        + LogSanitizer.sanitizeForLog(name)
                        + "' ("
                        + (stores.isEmpty()
                            ? "no read-model store is defined"
                            : "stores: "
                                + stores.stream()
                                    .map(store -> "'" + safeName(store) + "'")
                                    .collect(Collectors.joining(", ")))
                        + "), so there is no default store to take the ProjectionRepository bean"
                        + " from. Define a ProjectionRepository bean, or name one store '"
                        + LogSanitizer.sanitizeForLog(name)
                        + "'."));
  }

  /**
   * Routes one registration, then checks the routed target's processor against the registration's
   * mode and the leadership. Validates {@code stores} first ({@link #validateStores}), so rule 2
   * never sees two matches.
   *
   * @param registration the registration to route
   * @param stores every store defined (0, 1 or more)
   * @param subscriptionCheckpoint the event store's subscription checkpoint pair, rule 4's target
   * @param leadership the configured leadership, or {@link SubscriptionLeadership#NOOP} when none
   *     exists
   * @return the pair the registration's runner reads its position from and commits through
   * @throws NullPointerException if an argument is {@code null}
   * @throws IllegalStateException if the store set is invalid, no rule routes the registration, or
   *     the routed processor lacks a capability the registration needs
   */
  public static CheckpointTarget route(
      Registration registration,
      List<ReadModelStore> stores,
      CheckpointTarget subscriptionCheckpoint,
      SubscriptionLeadership leadership) {
    Objects.requireNonNull(registration, "registration is required");
    Objects.requireNonNull(stores, "stores is required");
    Objects.requireNonNull(subscriptionCheckpoint, "subscriptionCheckpoint is required");
    Objects.requireNonNull(leadership, "leadership is required");
    validateStores(stores);

    Routed routed = select(registration, stores, subscriptionCheckpoint);
    AtomicBatchProcessor processor = routed.target().processor();
    if (registration.mode().writesInCheckpointTransaction() && !processor.transactional()) {
      throw new IllegalStateException(
          head(registration)
              + " is routed to '"
              + LogSanitizer.sanitizeForLog(routed.target().name())
              + "' ("
              + routed.how()
              + "), whose processor is not transactional(): the read-model writes and the"
              + " checkpoint would not commit together; "
              + storesClause(stores)
              + ". Route it to a transactional store — "
              + FIX
              + " — or declare the projection AT_LEAST_ONCE_IDEMPOTENT.");
    }
    if (leadership != SubscriptionLeadership.NOOP && !processor.fencesCheckpoint()) {
      throw new IllegalStateException(
          head(registration)
              + " is routed to '"
              + LogSanitizer.sanitizeForLog(routed.target().name())
              + "' ("
              + routed.how()
              + "), whose processor does not fence its checkpoint (fencesCheckpoint() is false),"
              + " but single-active-consumer leadership ("
              + leadership.getClass().getName()
              + ") is on: a superseded leader resuming after a pause could re-apply its in-flight"
              + " batch or move the checkpoint with nothing to reject it; "
              + storesClause(stores)
              + ". Route it to a store whose processor fences its checkpoint — "
              + FIX
              + " — or, if exactly one instance runs this projection, disable leadership"
              + " (streamrune.subscription.single-active-consumer.enabled=false).");
    }
    return routed.target();
  }

  /**
   * Refuses an INLINE registration that declares anything but {@link
   * ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT}: INLINE runs post-commit on the command thread
   * with no checkpoint and no transaction of its own, so it cannot keep a transactional promise.
   *
   * @param projectionName the INLINE projection's name (sanitized into the message)
   * @param mode the mode its {@code @ProjectionConfig} declares
   * @throws IllegalStateException when {@code mode} is not {@code AT_LEAST_ONCE_IDEMPOTENT}
   */
  public static void requireInlineMode(String projectionName, ProjectionDeliveryMode mode) {
    if (mode == ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT) {
      return;
    }
    throw new IllegalStateException(
        "projection '"
            + LogSanitizer.sanitizeForLog(projectionName)
            + "' is INLINE and declares "
            + mode
            + ": INLINE runs post-commit on the command thread with no checkpoint and no transaction"
            + " of its own. Declare AT_LEAST_ONCE_IDEMPOTENT, or run it CONTINUOUS or SCHEDULED.");
  }

  /** A routed target and the rule that chose it, for the capability refusals. */
  private record Routed(CheckpointTarget target, String how) {}

  /** Rules 1–4 of the class javadoc, in order. */
  private static Routed select(
      Registration registration,
      List<ReadModelStore> stores,
      CheckpointTarget subscriptionCheckpoint) {
    Optional<ProjectionRepository> writeTarget = registration.projection().writeTarget();
    Optional<ReadModelStore> owner = writeTarget.flatMap(target -> ownerOf(target, stores));

    // Rule 1: explicit declaration.
    if (!registration.readModelStore().isEmpty()) {
      ReadModelStore declared =
          stores.stream()
              .filter(s -> s.name().equals(registration.readModelStore()))
              .findFirst()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          head(registration)
                              + " declares "
                              + declaration(registration)
                              + ", but no read-model store has that name; "
                              + storesClause(stores)
                              + ". Name one of them: "
                              + FIX
                              + "."));
      if (writeTarget.isPresent() && !sameStore(declared.repository(), writeTarget.get())) {
        throw new IllegalStateException(
            head(registration)
                + " declares "
                + declaration(registration)
                + ", but its writeTarget() "
                + owner
                    .map(o -> "is the repository of store '" + safeName(o) + "'")
                    .orElse("belongs to no read-model store")
                + ": the declaration and the code disagree; "
                + storesClause(stores)
                + ". Make them agree: "
                + FIX
                + ".");
      }
      return new Routed(declared.checkpointTarget(), "by its readModelStore declaration");
    }

    // Rule 2: writeTarget() matches a store.
    if (owner.isPresent()) {
      return new Routed(owner.get().checkpointTarget(), "by its writeTarget()");
    }

    // Rule 3: a transactional mode with no match.
    if (registration.mode().writesInCheckpointTransaction()) {
      if (writeTarget.isPresent()) {
        throw new IllegalStateException(
            head(registration)
                + ": its writeTarget() belongs to no read-model store, so its read-model writes"
                + " cannot commit with its checkpoint; "
                + storesClause(stores)
                + ". Define a ReadModelStore over that repository, or "
                + FIX
                + ".");
      }
      List<ReadModelStore> transactional =
          stores.stream().filter(s -> s.processor().transactional()).toList();
      if (transactional.size() == 1) {
        return new Routed(
            transactional.getFirst().checkpointTarget(), "as the only transactional store");
      }
      if (transactional.isEmpty()) {
        throw new IllegalStateException(
            head(registration)
                + " exposes no writeTarget() and no read-model store is transactional; "
                + storesClause(stores)
                + ". Define a transactional ReadModelStore and "
                + FIX
                + " — or declare the projection AT_LEAST_ONCE_IDEMPOTENT.");
      }
      throw new IllegalStateException(
          head(registration)
              + " exposes no writeTarget() and several read-model stores are transactional ("
              + transactional.stream()
                  .map(s -> "'" + safeName(s) + "'")
                  .collect(Collectors.joining(", "))
              + "), so routing cannot choose; "
              + storesClause(stores)
              + ". Choose one: "
              + FIX
              + ".");
    }

    // Rule 4: at-least-once with no match.
    return new Routed(subscriptionCheckpoint, "as the event store's subscription checkpoint");
  }

  /**
   * The store whose repository {@code target} writes to. The comparison is the one {@link
   * AtomicBatchProcessor#writesTo} makes: {@link ProjectionRepository#writeTargetIdentity()} with
   * {@code equals}, so a bean proxy or a transaction-scoped view on either side is transparent.
   */
  private static Optional<ReadModelStore> ownerOf(
      ProjectionRepository target, List<ReadModelStore> stores) {
    return stores.stream().filter(s -> sameStore(s.repository(), target)).findFirst();
  }

  private static boolean sameStore(ProjectionRepository a, ProjectionRepository b) {
    return a.writeTargetIdentity().equals(b.writeTargetIdentity());
  }

  private static String head(Registration registration) {
    return "projection '"
        + LogSanitizer.sanitizeForLog(registration.projectionName())
        + "' ("
        + registration.mode()
        + ")";
  }

  private static String declaration(Registration registration) {
    return "readModelStore = \""
        + LogSanitizer.sanitizeForLog(registration.readModelStore())
        + "\"";
  }

  private static String safeName(ReadModelStore store) {
    return LogSanitizer.sanitizeForLog(store.name());
  }

  private static String describe(ReadModelStore store) {
    return "'"
        + safeName(store)
        + "' ("
        + store.getClass().getName()
        + " over "
        + store.repository().getClass().getName()
        + ")";
  }

  /** The candidate stores with the capabilities routing checks, or that there are none. */
  private static String storesClause(List<ReadModelStore> stores) {
    if (stores.isEmpty()) {
      return "no read-model store is defined";
    }
    return "read-model stores: "
        + stores.stream()
            .map(
                s ->
                    "'"
                        + safeName(s)
                        + "' ("
                        + (s.processor().transactional() ? "transactional" : "not transactional")
                        + ", "
                        + (s.processor().fencesCheckpoint()
                            ? "fences its checkpoint"
                            : "does not fence its checkpoint")
                        + ")")
            .collect(Collectors.joining(", "));
  }
}
