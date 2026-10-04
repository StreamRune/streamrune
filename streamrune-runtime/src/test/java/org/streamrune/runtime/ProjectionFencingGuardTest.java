package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * The epoch fence must fail CLOSED. A processor that cannot honour {@code fencingEpoch} paired with
 * a real (multi-replica) {@link SubscriptionLeadership} is a configuration in which every guard the
 * leadership protocol advertises — the epoch fence, the overlap guard and the atomic checkpoint —
 * is inert, and nothing in the framework said so: the takeover stamp was a silent no-op default,
 * the sequential processor dropped the epoch on the floor, and every builder accepted the pairing.
 */
class ProjectionFencingGuardTest {

  private static final ProjectionName NAME = ProjectionName.of("orders");

  /** A leadership that issues real, non-zero fencing epochs — i.e. a multi-replica deployment. */
  private static final class EpochLeadership implements SubscriptionLeadership {
    private final long epoch;

    EpochLeadership(long epoch) {
      this.epoch = epoch;
    }

    @Override
    public Optional<Lease> tryAcquire(String consumerName) {
      return Optional.of(new Lease(epoch));
    }

    @Override
    public Optional<Lease> current(String consumerName) {
      return Optional.of(new Lease(epoch));
    }

    @Override
    public void resign(String consumerName) {}

    @Override
    public void close() {}
  }

  /** Stands in for a transactional processor: it declares that it honours the fencing epoch. */
  private static final class FencingProcessor implements AtomicBatchProcessor {
    @Override
    public boolean supportsFencing() {
      return true;
    }

    @Override
    public void executeAtomically(
        ProjectionName projectionName,
        List<EventEnvelope> batch,
        GlobalOffset newOffset,
        long fencingEpoch,
        ProjectionUpdater projectionUpdater,
        OffsetStore offsetStore) {
      projectionUpdater.update(null);
      offsetStore.saveOffset(projectionName, newOffset);
    }

    @Override
    public void stampFencingEpoch(ProjectionName projectionName, long fencingEpoch) {}
  }

  private static EventStore eventStore() {
    return SimpleTestEventStore.of(List.of());
  }

  private static Projection noop() {
    return batch -> {};
  }

  // ── the SPI itself must not silently pretend to fence ───────────────────────────────────────

  @Test
  void sequentialProcessor_doesNotClaimFencingSupport() {
    assertThat(AtomicBatchProcessor.nonAtomicAtLeastOnce().supportsFencing())
        .as("the sequential processor is non-transactional and cannot fence")
        .isFalse();
  }

  @Test
  void takeoverStampOnANonFencingProcessor_failsLoudlyInsteadOfSilentlySucceeding() {
    // The stamp is what arms the fence at takeover. A no-op default made a processor that cannot
    // fence indistinguishable from one that durably stamped, so the runner proceeded believing the
    // fence was armed.
    assertThatThrownBy(
            () -> AtomicBatchProcessor.nonAtomicAtLeastOnce().stampFencingEpoch(NAME, 6L))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void sequentialProcessor_refusesToCommitUnderANonZeroFencingEpoch() {
    // The damage site: the read-model write happens INSIDE executeAtomically. A processor that
    // ignores the epoch applies a superseded leader's batch before anything can reject it.
    var applied = new AtomicBoolean(false);
    var saved = new AtomicBoolean(false);
    OffsetStore offsets =
        new OffsetStore() {
          @Override
          public GlobalOffset getLastOffset(ProjectionName n) {
            return GlobalOffset.initial();
          }

          @Override
          public void saveOffset(ProjectionName n, GlobalOffset o) {
            saved.set(true);
          }
        };

    assertThatThrownBy(
            () ->
                AtomicBatchProcessor.nonAtomicAtLeastOnce()
                    .executeAtomically(
                        NAME,
                        List.of(),
                        GlobalOffset.of(150),
                        5L,
                        repository -> applied.set(true),
                        offsets))
        .isInstanceOf(IllegalStateException.class);

    assertThat(applied)
        .as("the read model must not be written under an unhonourable epoch")
        .isFalse();
    assertThat(saved).as("the checkpoint must not advance either").isFalse();
  }

  @Test
  void sequentialProcessor_stillCommitsWhenUnfenced() {
    // Epoch 0 is the single-node / SubscriptionLeadership.NOOP contract: nothing to fence.
    var applied = new AtomicBoolean(false);
    var saved = new AtomicBoolean(false);
    OffsetStore offsets =
        new OffsetStore() {
          @Override
          public GlobalOffset getLastOffset(ProjectionName n) {
            return GlobalOffset.initial();
          }

          @Override
          public void saveOffset(ProjectionName n, GlobalOffset o) {
            saved.set(true);
          }
        };

    AtomicBatchProcessor.nonAtomicAtLeastOnce()
        .executeAtomically(
            NAME, List.of(), GlobalOffset.of(150), 0L, repository -> applied.set(true), offsets);

    assertThat(applied).isTrue();
    assertThat(saved).isTrue();
  }

  // ── no runner may boot an unfenceable multi-replica projection ──────────────────────────────

  @Test
  void continuousRunnerBuilder_rejectsRealLeadershipWithANonFencingProcessor() {
    assertThatThrownBy(
            () ->
                ContinuousProjectionRunner.builder()
                    .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                    .eventStore(eventStore())
                    .offsetStore(new InMemoryOffsetStore())
                    .leadership(new EpochLeadership(5L))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot honour the leadership fencing epoch")
        .hasMessageContaining("Define an AtomicBatchProcessor bean")
        .hasMessageNotContaining("whenever a DataSource is present");
  }

  @Test
  void multiProjectionRunnerBuilder_rejectsRealLeadershipWithANonFencingProcessor() {
    assertThatThrownBy(
            () ->
                MultiProjectionRunner.builder()
                    .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                    .eventStore(eventStore())
                    .offsetStore(new InMemoryOffsetStore())
                    .leadership(new EpochLeadership(5L))
                    .register("orders", noop(), AT_LEAST_ONCE_IDEMPOTENT)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot honour the leadership fencing epoch")
        .hasMessageContaining("Define an AtomicBatchProcessor bean")
        .hasMessageNotContaining("whenever a DataSource is present");
  }

  @Test
  void scheduledRunnerBuilder_rejectsRealLeadershipWithANonFencingProcessor() {
    assertThatThrownBy(
            () ->
                ScheduledProjectionRunner.builder()
                    .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                    .eventStore(eventStore())
                    .offsetStore(new InMemoryOffsetStore())
                    .leadership(new EpochLeadership(5L))
                    .register("orders", noop(), "0 0 * * * *", AT_LEAST_ONCE_IDEMPOTENT)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot honour the leadership fencing epoch")
        .hasMessageContaining("Define an AtomicBatchProcessor bean")
        .hasMessageNotContaining("whenever a DataSource is present");
  }

  // ── the two configurations that ARE sound must keep building ────────────────────────────────

  @Test
  void noopLeadershipWithTheSequentialProcessor_stillBuilds() {
    assertThatCode(
            () -> {
              ContinuousProjectionRunner.builder()
                  .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                  .eventStore(eventStore())
                  .offsetStore(new InMemoryOffsetStore())
                  .build();
              MultiProjectionRunner.builder()
                  .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                  .eventStore(eventStore())
                  .offsetStore(new InMemoryOffsetStore())
                  .leadership(SubscriptionLeadership.NOOP)
                  .register("orders", noop(), AT_LEAST_ONCE_IDEMPOTENT)
                  .build()
                  .close();
              ScheduledProjectionRunner.builder()
                  .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                  .eventStore(eventStore())
                  .offsetStore(new InMemoryOffsetStore())
                  .register("orders", noop(), "0 0 * * * *", AT_LEAST_ONCE_IDEMPOTENT)
                  .build();
            })
        .doesNotThrowAnyException();
  }

  @Test
  void realLeadershipWithAFencingProcessor_stillBuilds() {
    assertThatCode(
            () -> {
              ContinuousProjectionRunner.builder()
                  .eventStore(eventStore())
                  .offsetStore(new InMemoryOffsetStore())
                  .leadership(new EpochLeadership(5L))
                  .atomicProcessor(new FencingProcessor())
                  .build();
              MultiProjectionRunner.builder()
                  .eventStore(eventStore())
                  .offsetStore(new InMemoryOffsetStore())
                  .leadership(new EpochLeadership(5L))
                  .atomicProcessor(new FencingProcessor())
                  // noop() is a plain lambda, registered AT_LEAST_ONCE_IDEMPOTENT: this test is
                  // about the LEADERSHIP/fencing pairing, not the delivery mode.
                  .register("orders", noop(), AT_LEAST_ONCE_IDEMPOTENT)
                  .build()
                  .close();
              ScheduledProjectionRunner.builder()
                  .eventStore(eventStore())
                  .offsetStore(new InMemoryOffsetStore())
                  .leadership(new EpochLeadership(5L))
                  .atomicProcessor(new FencingProcessor())
                  .register("orders", noop(), "0 0 * * * *", AT_LEAST_ONCE_IDEMPOTENT)
                  .build();
            })
        .doesNotThrowAnyException();
  }
}
