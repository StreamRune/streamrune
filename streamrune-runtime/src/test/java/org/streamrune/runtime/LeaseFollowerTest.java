package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * A follower says "no subscription anchored" at INFO only when its second consecutive poll finds
 * the lease unanchored (one poll interval of its own after the first), so a subscription that
 * starts a moment after the follower never makes it write the line. Past the grace it warns.
 */
class LeaseFollowerTest {

  private static final String NAME = "saga:orders";
  private static final Duration POLL = Duration.ofSeconds(1);

  private final RecordingLeadership leadership = new RecordingLeadership();
  private final AtomicLong now = new AtomicLong(1_000_000_000L);
  private final Logger followerLog = (Logger) LoggerFactory.getLogger("test.follower");
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
  private Level levelBefore;

  @BeforeEach
  void captureTheLog() {
    levelBefore = followerLog.getLevel();
    followerLog.setLevel(Level.INFO);
    appender.start();
    followerLog.addAppender(appender);
  }

  @AfterEach
  void releaseTheLog() {
    followerLog.detachAppender(appender);
    followerLog.setLevel(levelBefore);
  }

  @Test
  void theFirstUnanchoredPollSaysNothing() {
    LeaseFollower follower = follower();
    follower.started();

    assertThat(follower.mayAct()).isFalse();

    assertThat(lines(Level.INFO)).isEmpty();
    assertThat(lines(Level.WARN)).isEmpty();
  }

  @Test
  void anAnchorThatOpensBeforeTheSecondPoll_costsNoLine() {
    leadership.grant(NAME);
    SubscriptionLease lease = SubscriptionLease.of(leadership, NAME, now::get);
    LeaseFollower follower = follower(lease);
    follower.started();
    assertThat(follower.mayAct()).isFalse();

    SubscriptionLease.Anchor anchor = lease.anchor();
    now.addAndGet(POLL.toNanos());
    assertThat(follower.mayAct()).isFalse(); // an anchor, not leading yet: a standby
    assertThat(anchor.lead(Duration.ZERO)).isTrue();
    now.addAndGet(POLL.toNanos());
    assertThat(follower.mayAct()).isTrue();

    assertThat(lines(Level.INFO)).isEmpty();
    assertThat(lines(Level.WARN)).isEmpty();
    anchor.close();
  }

  @Test
  void aFollowerStillUnanchoredAtItsSecondPoll_logsTheInfoOnce_thenWarnsPastTheGrace() {
    LeaseFollower follower = follower();
    follower.started();

    assertThat(follower.mayAct()).isFalse();
    now.addAndGet(POLL.toNanos());
    assertThat(follower.mayAct()).isFalse();
    now.addAndGet(POLL.toNanos());
    assertThat(follower.mayAct()).isFalse();
    now.addAndGet(POLL.toNanos());
    assertThat(follower.mayAct()).isFalse();

    assertThat(lines(Level.INFO))
        .singleElement()
        .satisfies(
            line ->
                assertThat(line)
                    .contains("no subscription of this saga type is anchored")
                    .contains("orders"));
    assertThat(lines(Level.WARN)).isEmpty();
    assertThat(follower.unanchoredPastGrace()).isFalse();

    now.addAndGet(LeaseFollower.DEFAULT_ANCHOR_GRACE.toNanos());
    assertThat(follower.mayAct()).isFalse();
    assertThat(follower.mayAct()).isFalse();

    assertThat(lines(Level.INFO)).hasSize(1);
    assertThat(lines(Level.WARN))
        .singleElement()
        .satisfies(line -> assertThat(line).contains("has anchored its lease").contains(NAME));
    assertThat(follower.unanchoredPastGrace()).isTrue();
  }

  @Test
  void aNewStartCountsFromItsFirstPollAgain() {
    LeaseFollower follower = follower();
    follower.started();
    follower.mayAct();
    follower.mayAct();
    assertThat(lines(Level.INFO)).hasSize(1);

    follower.started();
    follower.mayAct();

    assertThat(lines(Level.INFO)).hasSize(1);
    follower.mayAct();
    assertThat(lines(Level.INFO)).hasSize(2);
  }

  private LeaseFollower follower() {
    return follower(SubscriptionLease.of(leadership, NAME, now::get));
  }

  private LeaseFollower follower(SubscriptionLease lease) {
    return new LeaseFollower(
        lease,
        followerLog,
        "timeout runner",
        "orders",
        "deadlines are claimed",
        LeaseFollower.DEFAULT_ANCHOR_GRACE,
        now::get);
  }

  private List<String> lines(Level level) {
    return appender.list.stream()
        .filter(e -> e.getLevel() == level)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }
}
