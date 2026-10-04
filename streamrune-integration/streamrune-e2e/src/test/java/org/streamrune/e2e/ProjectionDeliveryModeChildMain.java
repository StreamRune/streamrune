package org.streamrune.e2e;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.postgres.PostgresEventStore;
import org.streamrune.postgres.PostgresOffsetStore;
import org.streamrune.runtime.PollingProjectionRunner;

/**
 * The process that crashes, started by {@link ProjectionDeliveryModeProcessKillIT}: it runs ONE
 * batch of a counting projection under the requested delivery mode and halts the JVM ({@code
 * Runtime.getRuntime().halt(137)} — no shutdown hooks, no {@code finally}, no client-side rollback)
 * at the requested point.
 *
 * <p>Arguments: {@code jdbcUrl user password mode crashPoint projectionName}, where {@code mode} is
 * {@code TRANSACTIONAL_LOCAL} or {@code AT_LEAST_ONCE_IDEMPOTENT} and {@code crashPoint} is {@code
 * AFTER_WRITE} (the read-model write is made, the checkpoint is not advanced) or {@code
 * AFTER_COMMIT} (both are durable). Exit status {@code 137} means the crash point was reached.
 */
public final class ProjectionDeliveryModeChildMain {

  /** Exit status of a child that reached its crash point. */
  static final int HALTED = 137;

  /** The crash point that never fires: what the parent's rerun uses. */
  static final String NO_CRASH = "NONE";

  private ProjectionDeliveryModeChildMain() {}

  /**
   * The read model: one row, {@code "row"}, holding how many event applications it has seen. A
   * re-applied batch is counted again, so the value shows whether a rerun double-applied.
   *
   * @param id the row id
   * @param applied how many event applications the row has seen
   */
  public record Count(String id, int applied) {
    /**
     * Jackson creator, so the JDBC repository can read the row back.
     *
     * @param id the row id
     * @param applied how many event applications the row has seen
     */
    @JsonCreator
    public Count(@JsonProperty("id") String id, @JsonProperty("applied") int applied) {
      this.id = id;
      this.applied = applied;
    }
  }

  /**
   * Runs one batch and halts at the crash point.
   *
   * @param args {@code jdbcUrl user password mode crashPoint projectionName}
   */
  public static void main(String[] args) {
    var ds = new PGSimpleDataSource();
    ds.setUrl(args[0]);
    ds.setUser(args[1]);
    ds.setPassword(args[2]);
    var mode = ProjectionDeliveryMode.valueOf(args[3]);
    var crash = args[4];
    var name = ProjectionName.of(args[5]);

    var store =
        PostgresEventStore.builder().dataSource(ds).typeRegistry(E2ETestBase.TYPE_REGISTRY).build();
    var repo = new JdbcProjectionRepository(ds);
    OffsetStore offsets = new PostgresOffsetStore(ds);

    AtomicBatchProcessor processor =
        mode == ProjectionDeliveryMode.TRANSACTIONAL_LOCAL
            ? repo
            : AtomicBatchProcessor.nonAtomicAtLeastOnce();
    if (mode == ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT && crash.equals("AFTER_COMMIT")) {
      var delegate = offsets;
      offsets =
          new OffsetStore() {
            @Override
            public GlobalOffset getLastOffset(ProjectionName n) {
              return delegate.getLastOffset(n);
            }

            @Override
            public void saveOffset(ProjectionName n, GlobalOffset o) {
              delegate.saveOffset(n, o);
              Runtime.getRuntime().halt(HALTED); // both durable; nothing to redo
            }
          };
    }
    new PollingProjectionRunner(store, offsets, 100, 100, processor)
        .run(name, projection(mode, repo, name, crash), mode);
    System.exit(0); // not reached when a crash point is armed
  }

  /**
   * The child's two projection shapes without a crash point. The parent's rerun uses this, so both
   * sides run one definition.
   *
   * @param mode the delivery mode the projection is registered under
   * @param repo the JDBC repository: the processor under {@code TRANSACTIONAL_LOCAL}, the
   *     projection's own store under {@code AT_LEAST_ONCE_IDEMPOTENT}
   * @param name the projection name
   * @return the counting projection for {@code mode}
   */
  static Projection rerunProjection(
      ProjectionDeliveryMode mode, JdbcProjectionRepository repo, ProjectionName name) {
    return projection(mode, repo, name, NO_CRASH);
  }

  private static Projection projection(
      ProjectionDeliveryMode mode,
      JdbcProjectionRepository repo,
      ProjectionName name,
      String crash) {
    if (mode == ProjectionDeliveryMode.TRANSACTIONAL_LOCAL) {
      return new Projection() {
        @Override
        public void process(List<EventEnvelope> batch) {
          throw new IllegalStateException("driven through the repository overload");
        }

        @Override
        public void process(List<EventEnvelope> batch, ProjectionRepository tx) {
          int applied =
              tx.findById(name, "row", Count.class).map(Count::applied).orElse(0) + batch.size();
          tx.save(name, "row", new Count("row", applied));
          if (crash.equals("AFTER_WRITE")) {
            // Written on the transaction's connection, never committed.
            Runtime.getRuntime().halt(HALTED);
          }
          tx.afterCommit(
              () -> {
                if (crash.equals("AFTER_COMMIT")) {
                  Runtime.getRuntime().halt(HALTED); // read model and checkpoint both committed
                }
              });
        }
      };
    }
    return batch -> {
      int applied =
          repo.findById(name, "row", Count.class).map(Count::applied).orElse(0) + batch.size();
      repo.save(name, "row", new Count("row", applied)); // autocommit, own connection
      if (crash.equals("AFTER_WRITE")) {
        Runtime.getRuntime().halt(HALTED); // written and durable, checkpoint not advanced
      }
    };
  }
}
