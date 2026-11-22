package org.streamrune.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.core.ConnectionFailureClassifier.GENERIC;

import java.net.ConnectException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ConnectionFailureClassifierTest {

  @ParameterizedTest
  @ValueSource(strings = {"08000", "08001", "08003", "08004", "08006", "08007", "08P01"})
  void sqlStateClass08IsAConnectionFailure(String state) {
    assertThat(GENERIC.isConnectionFailure(new SQLException("x", state))).isTrue();
  }

  @Test
  void theTwoConnectionExceptionTypesAreConnectionFailures_withAnyState() {
    assertThat(GENERIC.isConnectionFailure(new SQLTransientConnectionException("pool timeout")))
        .isTrue();
    assertThat(
            GENERIC.isConnectionFailure(new SQLNonTransientConnectionException("refused", "99999")))
        .isTrue();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "57014", "55P03", "40001", "40P01", "23505", "01000", "22008", "42P08", "57P01", "53300"
      })
  void otherSqlStatesAreNot_theBackendCodesIncluded(String state) {
    assertThat(GENERIC.isConnectionFailure(new SQLException("x", state))).isFalse();
  }

  @Test
  void anSqlExceptionWithoutAStateIsNot() {
    assertThat(GENERIC.isConnectionFailure(new SQLException("x"))).isFalse();
  }

  @Test
  void aFailureWithoutJdbcEvidenceIsNot() {
    assertThat(GENERIC.isConnectionFailure(new IllegalStateException("x"))).isFalse();
    assertThat(GENERIC.isConnectionFailure(null)).isFalse();
  }

  @Test
  void theEvidenceIsFoundAnywhereInTheCauseChain() {
    var failure = new RuntimeException(new RuntimeException(new SQLException("x", "08006")));

    assertThat(GENERIC.isConnectionFailure(failure)).isTrue();
  }

  private static Throwable wrapped(int times) {
    Throwable t = new SQLException("x", "08006");
    for (int i = 0; i < times; i++) {
      t = new RuntimeException(t);
    }
    return t;
  }

  @Test
  void evidenceAtTheLastInspectedDepthIsFound() {
    assertThat(GENERIC.isConnectionFailure(wrapped(49))).isTrue();
  }

  @Test
  void evidenceOneBeyondTheWalkIsNotFound() {
    assertThat(GENERIC.isConnectionFailure(wrapped(50))).isFalse();
  }

  @Test
  void aCauseCycleEndsTheWalk() {
    var a = new RuntimeException("a");
    var b = new RuntimeException("b", a);
    a.initCause(b);

    assertThat(GENERIC.isConnectionFailure(a)).isFalse();
  }

  @Test
  void genericAndSqlStatesAddsExactlyTheNamedStates() {
    var classifier = ConnectionFailureClassifier.genericAndSqlStates("57P01");

    assertThat(classifier.isConnectionFailure(new SQLException("x", "57P01"))).isTrue();
    assertThat(classifier.isConnectionFailure(new SQLException("x", "57P02"))).isFalse();
    assertThat(classifier.isConnectionFailure(new SQLException("x", "08006"))).isTrue();
    assertThat(classifier.isConnectionFailure(new RuntimeException(new SQLException("x", "57P01"))))
        .isTrue();
  }

  @Test
  void orRefusesANullClassifierWhenItIsCalled() {
    org.assertj.core.api.Assertions.assertThatNullPointerException()
        .isThrownBy(() -> GENERIC.or(null));
  }

  @Test
  void orCombinesTwoClassifiers() {
    var combined = GENERIC.or(f -> f instanceof ConnectException);

    assertThat(combined.isConnectionFailure(new ConnectException("refused"))).isTrue();
    assertThat(combined.isConnectionFailure(new SQLException("x", "08006"))).isTrue();
    assertThat(combined.isConnectionFailure(new IllegalStateException("x"))).isFalse();
  }
}
