package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.postgres.SagaFenceNodes.TYPE;
import static org.streamrune.postgres.SagaFenceNodes.onAnotherThread;
import static org.streamrune.postgres.SagaFenceNodes.outcomeOf;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.postgres.SagaFenceNodes.Node;
import org.streamrune.postgres.SagaFenceNodes.Opened;
import org.streamrune.postgres.SagaFenceNodes.ProcessDeath;
import org.streamrune.postgres.SagaFenceNodes.Refund;
import org.streamrune.postgres.SagaFenceNodes.Reserve;
import org.streamrune.postgres.SagaFenceNodes.Ship;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Every point at which a process can die around a fenced forward command, and the rerun that
 * converges from there, on PostgreSQL with two processes.
 *
 * <p>Each case runs an uninterrupted reference schedule on one order and the interrupted schedule
 * on another, over the same database. A process that dies is rebuilt — every in-memory object of
 * it, over the same database — and the event in flight is delivered again, as its subscription
 * restarting from its checkpoint delivers it. The rerun ends where the reference ends: the same
 * stored events in the same order and the same final status, each compensation command stored once,
 * and no forward command stored after a compensation command.
 */
@Testcontainers
@Timeout(120)
class SagaForwardFenceCrashPointIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("saga_fence_crash");

  /** The first statement of an append after its fence read: the offset reservation. */
  private static final String OFFSET_RESERVATION = "UPDATE global_offset_sequence";

  /** The statement whose transaction is a command's append. */
  private static final String EVENT_INSERT = "INSERT INTO event_stream";

  static PGSimpleDataSource direct;

  private SagaFenceNodes nodes;

  /** The start event of each order a case appended, by order id. */
  private final Map<String, EventEnvelope> starts = new HashMap<>();

  @BeforeAll
  static void schema() {
    direct = dataSource("crash-test");
    new PostgresEventStoreFactory(direct, SagaFenceNodes.REGISTRY).create();
  }

  @BeforeEach
  void fresh() throws Exception {
    nodes = new SagaFenceNodes(direct);
    nodes.clean();
  }

  @AfterEach
  void closeAll() {
    nodes.closeAll();
  }

  /**
   * The fence refuses the step's second command and the process dies before the step routes the
   * refusal. The row carries the stamp of the other node's claim, and the step's first command was
   * stored before it; the redelivery meets the open episode and resumes it.
   */
  @Test
  void theRefusalIsTheLastThingTheProcessDoes() throws Exception {
    Node a = nodes.node("node-a", dataSource("node-a"));
    Node b = nodes.node("node-b", dataSource("node-b"));
    a.saga.startAlsoShips = true;
    // B's compensation stops after its first command: the episode stays open for A to resume.
    b.refund.failure = () -> new IllegalStateException("connection reset");

    EventEnvelope reference = nodes.append(new Opened("ref"));
    a.bus.runOnceBefore(Ship.class, () -> nodes.timeOut(b));
    a.deliver(reference);

    EventEnvelope opened = nodes.append(new Opened("1"));
    a.bus.runOnceBefore(Ship.class, () -> nodes.timeOut(b));
    a.bus.runOnceWhenRefused(
        () -> {
          throw new ProcessDeath();
        });
    assertThatThrownBy(() -> a.deliver(opened)).isInstanceOf(ProcessDeath.class);
    assertThat(nodes.status("1")).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(nodes.storedOrder("1")).containsExactly("Reserved", "Released");

    nodes.node("node-a", dataSource("node-a")).deliver(opened);

    assertConvergedOnTheReference("1", "ref");
    assertThat(nodes.storedOrder("1")).containsExactly("Reserved", "Released", "Refunded");
    assertThat(nodes.status("1")).isEqualTo(SagaStatus.COMPENSATED);
  }

  /**
   * The append's backend dies after its fence read, before its commit: nothing of the command is
   * stored, its inbox key included. The redelivery dispatches the command under the same key and
   * reads the fence again — the saga still in its forward phase, and once more with the other
   * node's claim landing between the redelivery's read of the row and its append.
   */
  @Test
  void theAppendDiesAfterItsFenceRead() throws Exception {
    Node reference = nodes.node("node-a", dataSource("node-a"));
    reference.deliver(nodes.append(new Opened("ref")));

    PausingDataSource pausing = new PausingDataSource(dataSource("node-a"));
    Node a = nodes.node("node-a", pausing);
    EventEnvelope opened = nodes.append(new Opened("1"));
    dieAfterTheFenceRead(pausing, a, opened);
    assertThat(nodes.storedOrder("1")).as("nothing of the command").isEmpty();
    assertThat(nodes.inboxKeys("1")).as("its inbox key neither").isEmpty();

    nodes.node("node-a", dataSource("node-a")).deliver(opened);
    assertConvergedOnTheReference("1", "ref");
    assertThat(nodes.storedOrder("1")).containsExactly("Reserved");
    assertThat(nodes.status("1")).isEqualTo(SagaStatus.RUNNING);

    // With the other node's claim between the redelivery's read of the row and its append.
    Node b = nodes.node("node-b", dataSource("node-b"));
    Node withAClaim = nodes.node("node-a", dataSource("node-a"));
    withAClaim.bus.runOnceBefore(Reserve.class, () -> nodes.timeOut(b));
    withAClaim.deliver(nodes.append(new Opened("ref-claimed")));

    PausingDataSource pausingAgain = new PausingDataSource(dataSource("node-a"));
    Node again = nodes.node("node-a", pausingAgain);
    EventEnvelope claimed = nodes.append(new Opened("2"));
    dieAfterTheFenceRead(pausingAgain, again, claimed);
    assertThat(nodes.storedOrder("2")).isEmpty();
    assertThat(nodes.inboxKeys("2")).isEmpty();

    Node rerun = nodes.node("node-a", dataSource("node-a"));
    rerun.bus.runOnceBefore(Reserve.class, () -> nodes.timeOut(b));
    rerun.deliver(claimed);

    assertThat(rerun.fencedSamples)
        .as("the rerun reads the fence again")
        .containsExactly(TYPE.value());
    assertConvergedOnTheReference("2", "ref-claimed");
    assertThat(nodes.storedOrder("2")).containsExactly("Released", "Refunded");
    assertThat(nodes.status("2")).isEqualTo(SagaStatus.COMPENSATED);
  }

  /**
   * The append commits, and the caller is told it failed — the connection lost while the commit's
   * reply was on the wire. The command is stored, before any later stamp. The redelivery is
   * answered from the command inbox; or, when the other node claimed in between, it meets the
   * stamped row and resumes the episode.
   */
  @Test
  void theCommitLandsAndTheCallerDoesNotLearnIt() throws Exception {
    Node reference = nodes.node("node-a", dataSource("node-a"));
    reference.deliver(nodes.append(new Opened("ref")));
    Node b = nodes.node("node-b", dataSource("node-b"));

    PausingDataSource losing = new PausingDataSource(dataSource("node-a"));
    Node a = nodes.node("node-a", losing);
    EventEnvelope opened = nodes.append(new Opened("1"));
    losing.failCommitAfterDelegating(EVENT_INSERT);
    assertThatThrownBy(() -> a.deliver(opened)).as("the delivery fails: retry later").isNotNull();
    assertThat(nodes.storedOrder("1")).as("the command is stored").containsExactly("Reserved");

    Node rerun = nodes.node("node-a", dataSource("node-a"));
    rerun.deliver(opened);
    assertThat(nodes.inboxKeys("1")).as("answered from the inbox: stored once").hasSize(1);
    assertConvergedOnTheReference("1", "ref");
    assertThat(nodes.status("1")).isEqualTo(SagaStatus.RUNNING);

    // The other node claims between the lost reply and the redelivery.
    nodes.timeOut(b); // compensates the reference sagas "ref" and "1"
    assertThat(nodes.storedOrder("ref")).containsExactly("Reserved", "Released", "Refunded");

    PausingDataSource losingAgain = new PausingDataSource(dataSource("node-a"));
    Node again = nodes.node("node-a", losingAgain);
    EventEnvelope claimed = nodes.append(new Opened("2"));
    losingAgain.failCommitAfterDelegating(EVENT_INSERT);
    assertThatThrownBy(() -> again.deliver(claimed)).isNotNull();
    Node claimer = nodes.node("node-b", dataSource("node-b"));
    claimer.refund.failure = () -> new IllegalStateException("connection reset");
    nodes.timeOut(claimer);
    assertThat(nodes.status("2")).isEqualTo(SagaStatus.COMPENSATING);

    nodes.node("node-a", dataSource("node-a")).deliver(claimed);

    assertConvergedOnTheReference("2", "ref");
    assertThat(nodes.storedOrder("2")).containsExactly("Reserved", "Released", "Refunded");
    assertThat(nodes.status("2")).isEqualTo(SagaStatus.COMPENSATED);
  }

  /**
   * The other node's claim waits on the saga's fence lock behind a forward append, and the claimer
   * dies there: no claim is written. The append commits, and the next poll claims.
   */
  @Test
  void aClaimWaitingOnTheFenceDies() throws Exception {
    Node referenceA = nodes.node("node-a", dataSource("node-a"));
    referenceA.deliver(nodes.append(new Opened("ref")));
    nodes.timeOut(nodes.node("node-b", dataSource("node-b")));

    PausingDataSource pausing = new PausingDataSource(dataSource("node-a"));
    Node a = nodes.node("node-a", pausing);
    Node b = nodes.node("node-b", dataSource("node-b"));
    EventEnvelope opened = nodes.append(new Opened("1"));
    PausingDataSource.Pause pause = pausing.pauseBefore(OFFSET_RESERVATION);
    CompletableFuture<Void> delivery = onAnotherThread(() -> a.deliver(opened));
    pause.awaitPaused();
    int appendPid = nodes.idleInTransaction("node-a");

    CompletableFuture<Void> claim = onAnotherThread(() -> nodes.timeOut(b));
    int claimPid = nodes.awaitBlockedBy(appendPid, "node-b");
    assertThat(nodes.waitOf(claimPid).event()).as("the saga's fence lock").isEqualTo("advisory");
    nodes.terminate(claimPid);

    assertThat(outcomeOf(claim)).as("the poll logs the failed claim and ends").isNull();
    assertThat(nodes.row("1").orElseThrow().episodeVersion()).as("no claim").isNull();

    pause.release();
    assertThat(outcomeOf(delivery)).as("the append commits").isNull();
    assertThat(nodes.status("1")).isEqualTo(SagaStatus.RUNNING);

    nodes.timeOut(nodes.node("node-b", dataSource("node-b")));

    assertConvergedOnTheReference("1", "ref");
    assertThat(nodes.storedOrder("1")).containsExactly("Reserved", "Released", "Refunded");
    assertThat(nodes.status("1")).isEqualTo(SagaStatus.COMPENSATED);
  }

  /**
   * The claim commits and the claimer dies before its first compensation command: the row is
   * stamped and nothing is dispatched. Each resume path completes the episode: the re-pick of a
   * rebuilt timeout runner here, the redelivery of the start event and the compensation-retry
   * sweeper in the two cases below.
   */
  @Test
  void theClaimCommitsAndTheClaimerDies() throws Exception {
    Node a = claimerDiesAfterItsClaim();

    nodes.timeOut(nodes.node("node-b", dataSource("node-b")));

    assertConvergedOnTheReference("1", "ref");
    assertThat(nodes.status("1")).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(a.fencedSamples).isEmpty();
  }

  /** The redelivery of the start event completes an episode whose claimer died after its claim. */
  @Test
  void theClaimCommitsAndTheClaimerDies_theRedeliveryResumesIt() throws Exception {
    claimerDiesAfterItsClaim();

    nodes.node("node-a", dataSource("node-a")).deliver(startOf("1"));

    assertConvergedOnTheReference("1", "ref");
    assertThat(nodes.status("1")).isEqualTo(SagaStatus.COMPENSATED);
  }

  /** The compensation-retry sweeper completes an episode whose claimer died after its claim. */
  @Test
  void theClaimCommitsAndTheClaimerDies_theSweeperResumesIt() throws Exception {
    claimerDiesAfterItsClaim();

    nodes.sweep(nodes.node("node-a", dataSource("node-a")));

    assertConvergedOnTheReference("1", "ref");
    assertThat(nodes.status("1")).isEqualTo(SagaStatus.COMPENSATED);
  }

  /**
   * A step refused at its second command resumes the episode the other node opened, and its process
   * dies inside that compensation, before compensation command 1. The row is stamped and part of
   * the compensation is stored; a rebuilt timeout runner's re-pick, or the sweeper, completes it.
   */
  @Test
  void theFencedStepDiesInsideItsCompensation() throws Exception {
    fencedReference();
    fencedStepDiesInsideItsCompensation("1");

    nodes.timeOut(nodes.node("node-b", dataSource("node-b")));

    assertConvergedOnTheReference("1", "ref");
    assertThat(nodes.status("1")).isEqualTo(SagaStatus.COMPENSATED);
  }

  @Test
  void theFencedStepDiesInsideItsCompensation_theSweeperCompletesIt() throws Exception {
    fencedReference();
    fencedStepDiesInsideItsCompensation("1");

    nodes.sweep(nodes.node("node-a", dataSource("node-a")));

    assertConvergedOnTheReference("1", "ref");
    assertThat(nodes.status("1")).isEqualTo(SagaStatus.COMPENSATED);
  }

  /** The reference: A's second command is refused after B's claim, and B compensates. */
  private void fencedReference() {
    Node a = nodes.node("node-a", dataSource("node-a"));
    Node b = nodes.node("node-b", dataSource("node-b"));
    a.saga.startAlsoShips = true;
    a.bus.runOnceBefore(Ship.class, () -> nodes.timeOut(b));
    a.deliver(nodes.append(new Opened("ref")));
    assertThat(nodes.storedOrder("ref")).containsExactly("Reserved", "Released", "Refunded");
  }

  /**
   * B claims between A's two commands and its compensation stops after its first command; A's
   * second command is refused, A resumes the episode and dies right before compensation command 1.
   */
  private void fencedStepDiesInsideItsCompensation(String order) {
    Node dying = nodes.node("node-a", dataSource("node-a"));
    Node claimer = nodes.node("node-b", dataSource("node-b"));
    dying.saga.startAlsoShips = true;
    claimer.refund.failure = () -> new IllegalStateException("connection reset");
    dying.bus.runOnceBefore(
        Ship.class,
        () -> {
          nodes.timeOut(claimer);
          dying.bus.runOnceBefore(
              Refund.class,
              () -> {
                throw new ProcessDeath();
              });
        });
    EventEnvelope opened = nodes.append(new Opened(order));
    assertThatThrownBy(() -> dying.deliver(opened)).isInstanceOf(ProcessDeath.class);
    assertThat(dying.fencedSamples).containsExactly(TYPE.value());
    assertThat(nodes.status(order)).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(nodes.storedOrder(order)).containsExactly("Reserved", "Released");
  }

  /**
   * The reference saga "ref": stored by A, claimed and compensated by B. Then saga "1": stored by
   * A, claimed by B, whose process dies right after the claim committed. Returns A.
   */
  private Node claimerDiesAfterItsClaim() {
    Node a = nodes.node("node-a", dataSource("node-a"));
    a.deliver(nodes.append(new Opened("ref")));
    nodes.timeOut(nodes.node("node-b", dataSource("node-b")));

    EventEnvelope opened = nodes.append(new Opened("1"));
    starts.put("1", opened);
    a.deliver(opened);
    Node b = nodes.node("node-b", dataSource("node-b"));
    b.afterTheNextClaim.set(
        () -> {
          throw new ProcessDeath();
        });
    assertThatThrownBy(() -> nodes.timeOut(b)).isInstanceOf(ProcessDeath.class);
    assertThat(nodes.status("1")).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(nodes.storedOrder("1")).as("nothing dispatched").containsExactly("Reserved");
    return a;
  }

  private EventEnvelope startOf(String order) {
    return starts.get(order);
  }

  // ---------------------------------------------------------------------------------------------

  /**
   * {@code a}'s append of the start event's command stops after its fence read; its backend dies.
   */
  private void dieAfterTheFenceRead(PausingDataSource pausing, Node a, EventEnvelope opened)
      throws Exception {
    PausingDataSource.Pause pause = pausing.pauseBefore(OFFSET_RESERVATION);
    CompletableFuture<Void> delivery = onAnotherThread(() -> a.deliver(opened));
    pause.awaitPaused();
    int appendPid = nodes.idleInTransaction("node-a");
    nodes.terminate(appendPid);
    nodes.awaitGone(appendPid);
    pause.release();
    assertThat(outcomeOf(delivery)).as("the delivery fails: retry later").isNotNull();
  }

  /** The order's saga ended where the reference's did, and the fence held for it. */
  private void assertConvergedOnTheReference(String order, String reference) throws Exception {
    assertThat(nodes.storedOrder(order)).isEqualTo(nodes.storedOrder(reference));
    assertThat(nodes.status(order)).isEqualTo(nodes.status(reference));
    assertThat(compensationKeyCount(order)).isEqualTo(compensationKeyCount(reference));
    nodes.assertTheFenceHeld(order);
  }

  private long compensationKeyCount(String order) throws Exception {
    return nodes.inboxKeys(order).stream().filter(key -> key.contains(":episode:")).count();
  }

  private static PGSimpleDataSource dataSource(String applicationName) {
    PGSimpleDataSource ds = new PGSimpleDataSource();
    ds.setUrl(PG.getJdbcUrl());
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());
    ds.setApplicationName(applicationName);
    return ds;
  }
}
