package org.streamrune.core.projection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.LockMode;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Versioned;
import org.streamrune.core.types.ProjectionName;

/**
 * {@link Projection#writesThroughRepository()} is a declaration: {@code false} unless the
 * projection says otherwise, and {@code true} for {@link BaseProjection}. It is never inferred from
 * the concrete class, because a class generated around the bean (an interceptor subclass, a proxy)
 * declares every method of the bean and would make the inference answer {@code true} for a
 * projection that writes nowhere through the handed repository.
 */
class ProjectionWritesThroughRepositoryTest {

  /** A lambda only ever implements the 1-arg overload. */
  @Test
  void lambdaProjection_reportsFalse() {
    Projection lambda = batch -> {};
    assertFalse(lambda.writesThroughRepository());
  }

  /** A class that only implements process(List) inherits the default 2-arg overload too. */
  @Test
  void plainClassOverridingOnlyOneArgProcess_reportsFalse() {
    class PlainProjection implements Projection {
      @Override
      public void process(List<EventEnvelope> batch) {}
    }
    assertFalse(new PlainProjection().writesThroughRepository());
  }

  /**
   * Overriding the 2-arg process does not by itself make the projection a write-through one: the
   * class must also declare it. This is what keeps a generated subclass, which overrides the 2-arg
   * process of a projection that implements only the 1-arg one, from reading as write-through.
   */
  @Test
  void classOverridingTwoArgProcessWithoutDeclaring_reportsFalse() {
    class PlainProjection implements Projection {
      @Override
      public void process(List<EventEnvelope> batch) {}
    }
    class GeneratedSubclassLikeProjection extends PlainProjection {
      @Override
      public void process(List<EventEnvelope> batch, ProjectionRepository repository) {
        super.process(batch, repository);
      }
    }
    assertFalse(new GeneratedSubclassLikeProjection().writesThroughRepository());
  }

  /** A class that overrides the 2-arg process and declares it reports true. */
  @Test
  void classOverridingTwoArgProcessAndDeclaringIt_reportsTrue() {
    class TransactionalProjection implements Projection {
      @Override
      public void process(List<EventEnvelope> batch) {
        process(batch, null);
      }

      @Override
      public void process(List<EventEnvelope> batch, ProjectionRepository repository) {}

      @Override
      public boolean writesThroughRepository() {
        return true;
      }
    }
    assertTrue(new TransactionalProjection().writesThroughRepository());
  }

  /**
   * BaseProjection.process(List, ProjectionRepository) is {@code final} and routes through the
   * supplied repository — every subclass must report true without having to override
   * writesThroughRepository() itself.
   */
  @Test
  void baseProjectionSubclass_reportsTrue() {
    class RepoBackedProjection extends BaseProjection {
      RepoBackedProjection(ProjectionRepository repository) {
        super(repository, "repo-backed");
      }

      @Override
      public void process(List<EventEnvelope> batch) {}
    }

    assertTrue(new RepoBackedProjection(noopRepository()).writesThroughRepository());
  }

  @Test
  void writeTarget_defaultIsEmpty_andWriteTargetIdentityDefaultIsThis() {
    Projection lambda = batch -> {};
    assertTrue(lambda.writeTarget().isEmpty());
    ProjectionRepository repo = noopRepository();
    assertSame(repo, repo.writeTargetIdentity());
  }

  /**
   * Never actually invoked by these tests — writesThroughRepository(), writeTarget() and
   * writeTargetIdentity() call no read or write method — but ProjectionRepository's data methods
   * are abstract, so a compiling anonymous implementation needs a body for every one of them.
   */
  private static ProjectionRepository noopRepository() {
    return new ProjectionRepository() {
      @Override
      public <T> void save(ProjectionName projectionName, String id, T readModel) {
        throw new UnsupportedOperationException("not exercised by this test");
      }

      @Override
      public <T> Optional<T> findById(ProjectionName projectionName, String id, Class<T> type) {
        throw new UnsupportedOperationException("not exercised by this test");
      }

      @Override
      public <T> List<T> findAll(ProjectionName projectionName, Class<T> type) {
        throw new UnsupportedOperationException("not exercised by this test");
      }

      @Override
      public void delete(ProjectionName projectionName, String id) {
        throw new UnsupportedOperationException("not exercised by this test");
      }

      @Override
      public <T> Page<T> findAll(
          ProjectionName projectionName, Class<T> type, PageRequest pageRequest) {
        throw new UnsupportedOperationException("not exercised by this test");
      }

      @Override
      public <T> Optional<Versioned<T>> findById(
          ProjectionName projectionName, String id, Class<T> type, LockMode lockMode) {
        throw new UnsupportedOperationException("not exercised by this test");
      }

      @Override
      public <T> void save(
          ProjectionName projectionName, String id, T readModel, long expectedVersion) {
        throw new UnsupportedOperationException("not exercised by this test");
      }
    };
  }
}
