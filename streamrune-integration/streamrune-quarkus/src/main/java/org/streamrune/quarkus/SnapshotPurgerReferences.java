package org.streamrune.quarkus;

import io.quarkus.arc.ClientProxy;
import jakarta.enterprise.context.ContextNotActiveException;
import java.util.List;
import org.streamrune.core.gdpr.SnapshotPurger;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.SubjectId;

/**
 * Lets the forget service see the {@link SnapshotPurger} behind an Arc client proxy.
 *
 * <p>{@code @All List<SubjectDataPurger>} hands over contextual references. For a normal-scoped
 * bean (an {@code @ApplicationScoped} producer, say) that reference is a client proxy implementing
 * only the bean's declared types, so a producer declared as {@link SubjectDataPurger} that returns
 * a {@code PostgresSnapshotStorePurger} reaches the forget without the {@link SnapshotPurger}
 * marker, and the forget would be refused although the application declared its snapshot purger. A
 * bean's types cannot tell either: a producer's types are its declared return type. The marker is
 * therefore read on the instance behind the proxy; when that instance is a {@link SnapshotPurger},
 * the proxy is handed on behind {@link ProxiedSnapshotPurger}, which carries the marker and still
 * calls through the proxy, so the bean's scope is kept.
 *
 * <p>A proxy whose context is not active when the service is built (a {@code @RequestScoped} bean
 * at startup) cannot be looked through and is handed on unchanged, so it does not count as a
 * snapshot purger.
 */
final class SnapshotPurgerReferences {

  private SnapshotPurgerReferences() {}

  /**
   * The purgers with every client proxy of a snapshot purger exposed as a {@link SnapshotPurger},
   * in the same order; every other element, {@code null} included, is unchanged.
   */
  static List<SubjectDataPurger> exposeSnapshotPurgers(List<SubjectDataPurger> purgers) {
    return purgers.stream().map(SnapshotPurgerReferences::expose).toList();
  }

  private static SubjectDataPurger expose(SubjectDataPurger purger) {
    if (purger instanceof SnapshotPurger || !(purger instanceof ClientProxy proxy)) {
      return purger;
    }
    Object instance;
    try {
      instance = proxy.arc_contextualInstance();
    } catch (ContextNotActiveException notActive) {
      return purger;
    }
    return instance instanceof SnapshotPurger ? new ProxiedSnapshotPurger(purger) : purger;
  }

  /** A client proxy of a snapshot purger, carrying the marker the proxy itself lacks. */
  record ProxiedSnapshotPurger(SubjectDataPurger proxy) implements SnapshotPurger {
    @Override
    public String name() {
      return proxy.name();
    }

    @Override
    public void purge(SubjectId subjectId) {
      proxy.purge(subjectId);
    }
  }
}
