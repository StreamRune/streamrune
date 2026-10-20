package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * An anchor writes one INFO line per change of this process's role under the lease — taking it,
 * losing it — naming the lease, and nothing while the role stays as it was.
 */
class SubscriptionLeaseTransitionLogTest {

  private static final String NAME = "saga:orders";
  private static final Duration INTERVAL = Duration.ofMillis(10);

  private final RecordingLeadership leadership = new RecordingLeadership();
  private final Logger log = (Logger) LoggerFactory.getLogger(SubscriptionLease.class);
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
  private Level levelBefore;

  @BeforeEach
  void captureTheLeasesLog() {
    levelBefore = log.getLevel();
    log.setLevel(Level.INFO);
    appender.start();
    log.addAppender(appender);
  }

  @AfterEach
  void releaseTheLeasesLog() {
    log.detachAppender(appender);
    log.setLevel(levelBefore);
  }

  @Test
  void takingTheLeaseIsOneInfoLine_steadyPollsAddNone() {
    leadership.grant(NAME);
    SubscriptionLease.Anchor anchor = SubscriptionLease.of(leadership, NAME).anchor();

    for (int i = 0; i < 10; i++) {
      assertThat(anchor.lead(INTERVAL)).isTrue();
      assertThat(anchor.stillLeads()).isTrue();
    }

    assertThat(infoLines())
        .singleElement()
        .satisfies(line -> assertThat(line).contains(NAME).contains("now leads"));
  }

  @Test
  void aStandbyThatNeverTakesTheLease_logsNothing() {
    SubscriptionLease.Anchor anchor = SubscriptionLease.of(leadership, NAME).anchor();

    for (int i = 0; i < 10; i++) {
      assertThat(anchor.lead(Duration.ZERO)).isFalse();
      assertThat(anchor.stillLeads()).isFalse();
    }

    assertThat(infoLines()).isEmpty();
  }

  @Test
  void aCheckThatFindsTheLeaseGone_logsOnce_whichEverCheckFindsIt() {
    leadership.grant(NAME);
    SubscriptionLease.Anchor anchor = SubscriptionLease.of(leadership, NAME).anchor();
    assertThat(anchor.lead(INTERVAL)).isTrue();
    appender.list.clear();

    leadership.revoke(NAME);
    assertThat(anchor.stillLeads()).isFalse();
    assertThat(anchor.stillLeads()).isFalse();
    assertThat(anchor.lead(Duration.ofHours(1))).isFalse();

    assertThat(infoLines())
        .singleElement()
        .satisfies(line -> assertThat(line).contains(NAME).contains("has lost it"));
  }

  @Test
  void aLeaseFoundGoneByLead_logsOnce() {
    leadership.grant(NAME);
    SubscriptionLease.Anchor anchor = SubscriptionLease.of(leadership, NAME).anchor();
    assertThat(anchor.lead(INTERVAL)).isTrue();
    appender.list.clear();

    leadership.revoke(NAME);
    assertThat(anchor.lead(Duration.ofHours(1))).isFalse();
    assertThat(anchor.lead(Duration.ofHours(1))).isFalse();
    assertThat(anchor.stillLeads()).isFalse();

    assertThat(infoLines())
        .singleElement()
        .satisfies(line -> assertThat(line).contains(NAME).contains("has lost it"));
  }

  @Test
  void aLeaseTakenAgainAfterALoss_isAnotherLine() {
    leadership.grant(NAME);
    SubscriptionLease.Anchor anchor = SubscriptionLease.of(leadership, NAME).anchor();
    assertThat(anchor.lead(Duration.ZERO)).isTrue();
    leadership.revoke(NAME);
    assertThat(anchor.stillLeads()).isFalse();

    leadership.grant(NAME);
    assertThat(anchor.lead(Duration.ZERO)).isTrue();

    assertThat(infoLines())
        .hasSize(3)
        .satisfiesExactly(
            line -> assertThat(line).contains("now leads"),
            line -> assertThat(line).contains("has lost it"),
            line -> assertThat(line).contains("now leads"));
  }

  @Test
  void closingTheAnchorIsNotALoss() {
    leadership.grant(NAME);
    SubscriptionLease.Anchor anchor = SubscriptionLease.of(leadership, NAME).anchor();
    assertThat(anchor.lead(INTERVAL)).isTrue();
    appender.list.clear();

    anchor.close();
    assertThat(anchor.stillLeads()).isFalse();
    assertThat(anchor.lead(INTERVAL)).isFalse();

    assertThat(infoLines()).isEmpty();
  }

  private List<String> infoLines() {
    return appender.list.stream()
        .filter(e -> e.getLevel() == Level.INFO)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }
}
