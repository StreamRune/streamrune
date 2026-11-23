package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.ConnectionFailureClassifier;
import org.streamrune.core.EventStoreException;

class SagaPageStopRuleTest {

  private static final SagaPageStopRule RULE =
      new SagaPageStopRule(ConnectionFailureClassifier.GENERIC);

  private static PageResult result(
      int failed, Throwable failure, Throwable stoppedBy, boolean interrupted) {
    return new PageResult(
        5,
        5,
        failed,
        0,
        failure,
        failure == null || interrupted ? -1 : 0,
        stoppedBy,
        interrupted,
        0L,
        0L);
  }

  @Test
  void aCircuitBreakerRefusalDoesNotStopThePage() {
    assertThat(RULE.stopsPage(new CircuitBreakerOpenException("payments"), 1)).isFalse();
  }

  @Test
  void aStatementTimeoutOfOneSagaDoesNotStopThePage() {
    assertThat(
            RULE.stopsPage(
                new EventStoreException("append", new SQLException("timeout", "57014")), 1))
        .isFalse();
  }

  @Test
  void aLockTimeoutAndASerializationFailureDoNotStopThePage() {
    for (String state : new String[] {"55P03", "40001"}) {
      for (int n = 1; n <= 2; n++) {
        assertThat(RULE.stopsPage(new SQLException("x", state), n)).isFalse();
      }
    }
  }

  @Test
  void aLostConnectionStopsThePage_atAnyDepth() {
    assertThat(RULE.stopsPage(new SQLException("reset", "08006"), 1)).isTrue();
    assertThat(
            RULE.stopsPage(
                new EventStoreException(
                    "append", new RuntimeException(new SQLTransientConnectionException("pool"))),
                1))
        .isTrue();
  }

  @Test
  void theThirdFailureStopsThePage_theSecondDoesNot() {
    assertThat(RULE.stopsPage(new IllegalStateException(), 2)).isFalse();
    assertThat(RULE.stopsPage(new IllegalStateException(), 3)).isTrue();
  }

  @Test
  void theRuleAsksTheClassifierItWasGiven() {
    SagaPageStopRule rule = new SagaPageStopRule(f -> f instanceof java.net.ConnectException);
    assertThat(rule.stopsPage(new java.net.ConnectException(), 1)).isTrue();
    assertThat(rule.stopsPage(new SQLException("x", "08006"), 1)).isFalse();
  }

  @Test
  void aPageWhoseOtherSagasRanToTheirEndIsTaggedNone() {
    for (int failed = 1; failed <= 2; failed++) {
      assertThat(RULE.stopTag(result(failed, new IllegalStateException(), null, false)))
          .contains("none");
    }
  }

  @Test
  void aPageStoppedOnAConnectionClassFailureIsTaggedConnection() {
    SQLException lost = new SQLException("x", "08006");
    assertThat(RULE.stopTag(result(1, lost, lost, false))).contains("connection");
  }

  @Test
  void aPageWithThreeFailuresIsTaggedThirdFailure() {
    IllegalStateException third = new IllegalStateException();
    assertThat(RULE.stopTag(result(3, third, third, false))).contains("third_failure");
  }

  @Test
  void anErrorRecordsNoTag() {
    assertThat(RULE.stopTag(result(1, new AssertionError(), null, false)))
        .isEqualTo(Optional.empty());
  }

  @Test
  void aCallerInterruptRecordsNoTag() {
    assertThat(RULE.stopTag(result(0, new IllegalStateException(), null, true)))
        .isEqualTo(Optional.empty());
  }

  @Test
  void aPageWithoutAFailureRecordsNoTag() {
    assertThat(RULE.stopTag(result(0, null, null, false))).isEqualTo(Optional.empty());
  }

  @Test
  void aPageStoppedOnAConnectionClassFailureIsTaggedConnection_alsoWhenTheItemsInFlightFailedToo() {
    SQLException lost = new SQLException("x", "08006");
    assertThat(RULE.stopTag(result(4, lost, lost, false))).contains("connection");
  }

  @Test
  void aPageStoppedOnItsThirdFailureKeepsThatTag_whenAnItemInFlightThenLosesItsConnection() {
    IllegalStateException first = new IllegalStateException();
    IllegalStateException third = new IllegalStateException();
    first.addSuppressed(third);
    first.addSuppressed(new SQLException("x", "08006"));
    assertThat(RULE.stopTag(result(4, first, third, false))).contains("third_failure");
  }

  @Test
  void anErrorRecordsNoTag_alsoWhenItsCauseIsConnectionClass() {
    AssertionError error = new AssertionError("x", new SQLException("x", "08006"));
    assertThat(RULE.stopTag(result(1, error, error, false))).isEmpty();
  }

  @Test
  void aCallerInterruptRecordsNoTag_alsoWhenThePageStoppedOnAConnectionClassFailure() {
    SQLException lost = new SQLException("x", "08006");
    assertThat(RULE.stopTag(result(1, new IllegalStateException(), lost, true))).isEmpty();
  }

  @Test
  void theTagAsksTheClassifierItWasGiven() {
    SagaPageStopRule rule = new SagaPageStopRule(f -> f instanceof java.net.ConnectException);
    java.net.ConnectException refused = new java.net.ConnectException();
    assertThat(rule.stopTag(result(1, refused, refused, false))).contains("connection");
  }

  @Test
  void aFailureAfterTheThirdStopsThePageToo() {
    assertThat(RULE.stopsPage(new IllegalStateException(), 4)).isTrue();
  }

  @Test
  void aRuleWithoutAClassifierIsRefused() {
    assertThatNullPointerException()
        .isThrownBy(() -> new SagaPageStopRule(null))
        .withMessage("classifier is required");
  }

  @Test
  void aPageOfOneSagaThatLosesItsConnectionIsTaggedConnection_thoughTheRuleWasNeverAsked() {
    SQLException lost = new SQLException("x", "08006");
    PageResult oneSaga = new PageResult(3, 1, 1, 2, lost, 0, null, false, 0L, 0L);
    assertThat(RULE.stopTag(oneSaga)).contains("connection");
  }

  @Test
  void aPageStoppedOnItsThirdFailureKeepsThatTag_alsoWhenTheThrownFailureIsTheLateLostConnection() {
    SQLException lateAtTheLowestPosition = new SQLException("x", "08006");
    IllegalStateException third = new IllegalStateException();
    lateAtTheLowestPosition.addSuppressed(third);
    assertThat(RULE.stopTag(result(4, lateAtTheLowestPosition, third, false)))
        .contains("third_failure");
  }

  @Test
  void aPageStoppedByAThrowingRuleOrObserverWithFewFailuresIsTaggedNone() {
    assertThat(
            RULE.stopTag(
                result(2, new IllegalStateException(), new IllegalArgumentException(), false)))
        .contains("none");
  }

  @Test
  void theFailureThePageStoppedOnDecides_notTheThrownOne() {
    SQLException lost = new SQLException("x", "08006");
    assertThat(RULE.stopTag(result(2, new IllegalStateException(), lost, false)))
        .contains("connection");
  }
}
