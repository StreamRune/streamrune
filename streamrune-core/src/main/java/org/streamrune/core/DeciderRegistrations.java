package org.streamrune.core;

import java.util.Collection;
import java.util.Objects;
import org.streamrune.core.types.AggregateType;

/**
 * The registration rule every command bus applies when a decider is registered: a command type
 * routes to exactly one decider, registrations whose command roots overlap (one assignable to the
 * other) declare the same {@link AggregateType}, and one decider instance belongs to one aggregate
 * type. Several unrelated command roots may share a type — that is how an application states that
 * they are one aggregate.
 */
public final class DeciderRegistrations {

  /**
   * What the rule needs to know about one registration.
   *
   * @param aggregateType the aggregate type the registration declares
   * @param commandType the registered command root
   * @param decider the decider instance
   */
  public record Registration(
      AggregateType aggregateType, Class<? extends Command> commandType, Decider<?, ?, ?> decider) {
    public Registration {
      Objects.requireNonNull(aggregateType, "aggregateType is required");
      Objects.requireNonNull(commandType, "commandType is required");
      Objects.requireNonNull(decider, "decider is required");
    }
  }

  private DeciderRegistrations() {}

  /**
   * Refuses {@code candidate} if it conflicts with a registration already accepted.
   *
   * @param existing the registrations accepted so far, in registration order
   * @param candidate the registration being added
   * @throws IllegalArgumentException if the candidate's type is outside {@link
   *     AggregateType#SYNTAX}, its command type is already registered, it overlaps a registered
   *     root under a different type, or its decider instance is registered under a different type
   */
  public static void requireCompatible(Collection<Registration> existing, Registration candidate) {
    Objects.requireNonNull(existing, "existing is required");
    Objects.requireNonNull(candidate, "candidate is required");
    AggregateType.of(candidate.aggregateType().value());
    for (Registration r : existing) {
      if (r.commandType().equals(candidate.commandType())) {
        throw new IllegalArgumentException(
            "command type "
                + candidate.commandType().getName()
                + " is already registered (aggregate type '"
                + r.aggregateType().value()
                + "'); a command type routes to exactly one decider");
      }
    }
    for (Registration r : existing) {
      boolean overlap =
          r.commandType().isAssignableFrom(candidate.commandType())
              || candidate.commandType().isAssignableFrom(r.commandType());
      if (overlap && !r.aggregateType().equals(candidate.aggregateType())) {
        throw new IllegalArgumentException(
            "command types "
                + r.commandType().getName()
                + " and "
                + candidate.commandType().getName()
                + " overlap (one is assignable to the other) but declare aggregate types '"
                + r.aggregateType().value()
                + "' and '"
                + candidate.aggregateType().value()
                + "'; commands of both route to the same streams, so they must declare the same"
                + " aggregate type");
      }
    }
    for (Registration r : existing) {
      if (r.decider() == candidate.decider()
          && !r.aggregateType().equals(candidate.aggregateType())) {
        throw new IllegalArgumentException(
            "this decider instance is already registered under aggregate type '"
                + r.aggregateType().value()
                + "' (command type "
                + r.commandType().getName()
                + "); registering it under '"
                + candidate.aggregateType().value()
                + "' for "
                + candidate.commandType().getName()
                + " would split one aggregate across two stream namespaces");
      }
    }
  }
}
