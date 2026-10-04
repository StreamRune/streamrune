package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * What {@link AtomicBatchProcessor#supportsFencing()} {@code == true} promises: (i) the repository
 * handed to the updater is non-null; (ii) an updater exception commits nothing; (iii) a write
 * through the handed repository and the checkpoint become visible together, never one without the
 * other; (iv) a stale epoch is rejected before the updater runs; (v) {@code writesTo(this)} is
 * true, and stays true through a {@link Proxy} that forwards to the real object. Run by {@code
 * InMemoryProjectionRepositoryContractTest} (streamrune-test) and {@code
 * JdbcProjectionRepositoryContractTest} (streamrune-postgres): the test-time half of "startup
 * verifies the shared transaction" — the static {@code ProjectionDeliveryPolicy} trusts the flag,
 * this suite is what makes the flag honest.
 */
public abstract class AtomicBatchProcessorContract {

  private static final StreamId CONTRACT_STREAM =
      StreamId.of(AggregateType.of("contract"), AggregateId.of("contract-stream"));

  private static final AtomicInteger NAMES = new AtomicInteger();

  /** For a subclass that supplies the store under test through the abstract hooks. */
  protected AtomicBatchProcessorContract() {}

  /**
   * The processor under test. It must also be the {@link ProjectionRepository} it writes through.
   *
   * @return the processor under test
   */
  protected abstract AtomicBatchProcessor processor();

  /**
   * A repository whose {@code writeTargetIdentity()} names a DIFFERENT store than the processor.
   *
   * @return a repository of another store
   */
  protected abstract ProjectionRepository repositoryOfAnotherStore();

  /**
   * An {@link OffsetStore} over the SAME checkpoint the processor advances.
   *
   * @return the offset store that shares the processor's checkpoint
   */
  protected abstract OffsetStore offsetStore();

  /**
   * The checkpoint as a second reader sees it (another connection for JDBC).
   *
   * @param name the projection
   * @return the committed checkpoint
   */
  protected abstract GlobalOffset committedOffset(ProjectionName name);

  /**
   * A read-model row as a second reader sees it (another connection for JDBC), or empty.
   *
   * @param name the projection
   * @param id the row id
   * @return the committed row, or empty
   */
  protected abstract Optional<ContractView> readFromOutside(ProjectionName name, String id);

  /**
   * The processor under test seen as the repository it writes through.
   *
   * @return {@link #processor()} as a {@link ProjectionRepository}
   */
  protected ProjectionRepository repository() {
    return (ProjectionRepository) processor();
  }

  /**
   * A projection name no other test in this JVM has used.
   *
   * @return a fresh projection name
   */
  protected ProjectionName freshName() {
    return ProjectionName.of("contract_" + NAMES.incrementAndGet());
  }

  /**
   * The read model the contract writes.
   *
   * @param id the row id
   * @param value the payload
   */
  public record ContractView(String id, String value) {
    /**
     * Jackson creator, so a serializing store can read the row back.
     *
     * @param id the row id
     * @param value the payload
     */
    @JsonCreator
    public ContractView(@JsonProperty("id") String id, @JsonProperty("value") String value) {
      this.id = id;
      this.value = value;
    }
  }

  /**
   * The event the contract's batches carry.
   *
   * @param payload the payload
   */
  public record ContractEvent(String payload) implements DomainEvent {}

  /**
   * One envelope per global offset in {@code [fromOffset, toOffset]}.
   *
   * @param fromOffset the first global offset, inclusive
   * @param toOffset the last global offset, inclusive
   * @return the batch
   */
  protected static List<EventEnvelope> batch(long fromOffset, long toOffset) {
    var events = new ArrayList<EventEnvelope>();
    for (long o = fromOffset; o <= toOffset; o++) {
      events.add(
          new EventEnvelope(
              GlobalOffset.of(o),
              CONTRACT_STREAM,
              new Version(o),
              new EventType("ContractEvent"),
              new ContractEvent("p" + o),
              new EventMetadata(
                  IdGenerator.generateEventId(),
                  IdGenerator.generateCommandId(),
                  null,
                  null,
                  CorrelationId.of("contract"),
                  null,
                  null,
                  Instant.now())));
    }
    return events;
  }

  /** (i) The updater always receives a repository. */
  @Test
  public void handedRepositoryIsNonNull() {
    var name = freshName();
    var handed = new AtomicReference<ProjectionRepository>();
    processor()
        .executeAtomically(name, batch(1, 1), GlobalOffset.of(1), 0L, handed::set, offsetStore());
    assertNotNull(handed.get(), "(i) a transactional processor always hands a repository");
  }

  /** (ii) An updater exception rolls back the write and the checkpoint; the rerun applies once. */
  @Test
  public void updaterExceptionCommitsNothing_andTheRerunAppliesOnce() {
    var name = freshName();
    var attempts = new AtomicInteger();
    assertThrows(
        IllegalStateException.class,
        () ->
            processor()
                .executeAtomically(
                    name,
                    batch(1, 2),
                    GlobalOffset.of(2),
                    0L,
                    repo -> {
                      attempts.incrementAndGet();
                      repo.save(name, "a", new ContractView("a", "first"));
                      throw new IllegalStateException("boom after the write");
                    },
                    offsetStore()));
    assertTrue(readFromOutside(name, "a").isEmpty(), "(ii) the write was rolled back");
    assertEquals(GlobalOffset.initial(), committedOffset(name), "(ii) the checkpoint did not move");

    processor()
        .executeAtomically(
            name,
            batch(1, 2),
            GlobalOffset.of(2),
            0L,
            repo -> {
              attempts.incrementAndGet();
              repo.save(name, "a", new ContractView("a", "second"));
            },
            offsetStore());
    assertEquals(2, attempts.get());
    assertEquals(Optional.of(new ContractView("a", "second")), readFromOutside(name, "a"));
    assertEquals(GlobalOffset.of(2), committedOffset(name));
  }

  /** (iii) The write and the checkpoint move are invisible until both commit. */
  @Test
  public void writeAndCheckpointBecomeVisibleTogether_neverOneWithoutTheOther() {
    var name = freshName();
    processor()
        .executeAtomically(
            name,
            batch(1, 3),
            GlobalOffset.of(3),
            0L,
            repo -> {
              repo.save(name, "a", new ContractView("a", "v"));
              // (iii) inside the transaction a second reader sees NEITHER the row NOR the move.
              assertTrue(readFromOutside(name, "a").isEmpty(), "row invisible before commit");
              assertEquals(GlobalOffset.initial(), committedOffset(name), "checkpoint unmoved");
            },
            offsetStore());
    assertEquals(Optional.of(new ContractView("a", "v")), readFromOutside(name, "a"));
    assertEquals(GlobalOffset.of(3), committedOffset(name));
    assertEquals(GlobalOffset.of(3), offsetStore().getLastOffset(name), "same checkpoint");
  }

  /** (iv) A commit from a superseded epoch fails before the updater writes anything. */
  @Test
  public void staleEpochIsRejectedBeforeTheUpdaterRuns() {
    var name = freshName();
    processor().stampFencingEpoch(name, 2L);
    var updaterRan = new AtomicBoolean();
    assertThrows(
        ProjectionCommitFencedException.class,
        () ->
            processor()
                .executeAtomically(
                    name,
                    batch(1, 1),
                    GlobalOffset.of(1),
                    1L,
                    repo -> {
                      updaterRan.set(true);
                      repo.save(name, "a", new ContractView("a", "stale"));
                    },
                    offsetStore()));
    assertFalse(updaterRan.get(), "(iv) the fence runs before the updater");
    assertTrue(readFromOutside(name, "a").isEmpty());
    assertEquals(GlobalOffset.initial(), committedOffset(name));
  }

  /** A batch that starts at or before the checkpoint is rejected and not applied twice. */
  @Test
  public void overlappingBatchIsRejected_andTheReadModelIsNotDoubleApplied() {
    var name = freshName();
    processor()
        .executeAtomically(
            name,
            batch(1, 2),
            GlobalOffset.of(2),
            0L,
            repo -> repo.save(name, "a", new ContractView("a", "once")),
            offsetStore());
    assertThrows(
        ProjectionCommitFencedException.class,
        () ->
            processor()
                .executeAtomically(
                    name,
                    batch(2, 3),
                    GlobalOffset.of(3),
                    0L,
                    repo -> repo.save(name, "a", new ContractView("a", "twice")),
                    offsetStore()));
    assertEquals(Optional.of(new ContractView("a", "once")), readFromOutside(name, "a"));
    assertEquals(GlobalOffset.of(2), committedOffset(name));
  }

  /** An after-commit action sees the committed write, and never runs after a rollback. */
  @Test
  public void afterCommitRunsAfterTheCommit_andIsDroppedOnRollback() {
    var name = freshName();
    var ran = new AtomicReference<String>();
    processor()
        .executeAtomically(
            name,
            batch(1, 1),
            GlobalOffset.of(1),
            0L,
            repo -> {
              repo.save(name, "a", new ContractView("a", "v"));
              repo.afterCommit(
                  () ->
                      ran.set(
                          readFromOutside(name, "a").isPresent() ? "after-commit" : "too-early"));
            },
            offsetStore());
    assertEquals("after-commit", ran.get());

    var dropped = new AtomicBoolean();
    assertThrows(
        IllegalStateException.class,
        () ->
            processor()
                .executeAtomically(
                    name,
                    batch(2, 2),
                    GlobalOffset.of(2),
                    0L,
                    repo -> {
                      repo.afterCommit(() -> dropped.set(true));
                      throw new IllegalStateException("rollback");
                    },
                    offsetStore()));
    assertFalse(dropped.get(), "an after-commit action of a rolled-back transaction never runs");
  }

  /** (v) The processor writes to its own store and not to another one. */
  @Test
  public void writesToItself_andNotToAnotherStore() {
    assertTrue(processor().writesTo(repository()), "(v)");
    assertFalse(processor().writesTo(repositoryOfAnotherStore()));
  }

  /** (v) A forwarding proxy on either side keeps {@code writesTo} true. */
  @Test
  public void writesToStaysTrueThroughAForwardingProxyOnEitherSide() {
    Object real = processor();
    InvocationHandler forward = (proxy, method, args) -> method.invoke(real, args);
    var proxied =
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {ProjectionRepository.class, AtomicBatchProcessor.class},
            forward);
    var proxiedProcessor = (AtomicBatchProcessor) proxied;
    var proxiedRepository = (ProjectionRepository) proxied;
    assertTrue(processor().writesTo(proxiedRepository), "real processor, proxied repository");
    assertTrue(proxiedProcessor.writesTo(repository()), "proxied processor, real repository");
    assertTrue(proxiedProcessor.writesTo(proxiedRepository), "both proxied");
    assertFalse(proxiedProcessor.writesTo(repositoryOfAnotherStore()));
  }
}
