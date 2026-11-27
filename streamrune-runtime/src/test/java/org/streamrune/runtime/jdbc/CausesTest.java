package org.streamrune.runtime.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class CausesTest {

  @Test
  void findReturnsTheFirstMatch() {
    var inner = new SQLException("inner");
    var outer = new SQLException("outer", inner);
    var top = new IllegalStateException("top", outer);

    assertThat((Throwable) Causes.find(top, SQLException.class)).isSameAs(outer);
    assertThat(Causes.find(top, IllegalStateException.class))
        .as("the throwable itself is the first link")
        .isSameAs(top);
  }

  @Test
  void findReturnsNullWithoutAMatch() {
    var top = new IllegalStateException("top", new RuntimeException("cause"));

    assertThat((Throwable) Causes.find(top, SQLException.class)).isNull();
    assertThat((Throwable) Causes.find(null, SQLException.class)).isNull();
    assertThat(Causes.anyMatch(null, t -> true)).isFalse();
  }

  @Test
  @Timeout(5)
  void aCyclicChainTerminates() {
    var a = new RuntimeException("a");
    var b = new RuntimeException("b", a);
    a.initCause(b);

    assertThat((Throwable) Causes.find(a, SQLException.class)).isNull();
    assertThat(Causes.anyMatch(a, t -> t instanceof SQLException)).isFalse();
  }

  @Test
  void inspectsExactlyFiftyLinks() {
    assertThat(Causes.MAX_DEPTH).isEqualTo(50);

    Throwable matchAtFifty = chainEndingIn(new IOException("link 50"), 49);
    assertThat(Causes.find(matchAtFifty, IOException.class)).isNotNull();
    assertThat(Causes.anyMatch(matchAtFifty, t -> t instanceof IOException)).isTrue();

    Throwable matchAtFiftyOne = chainEndingIn(new IOException("link 51"), 50);
    assertThat(Causes.find(matchAtFiftyOne, IOException.class)).isNull();
    assertThat(Causes.anyMatch(matchAtFiftyOne, t -> t instanceof IOException)).isFalse();
  }

  @Test
  void anyMatchUsesThePredicate() {
    var top = new RuntimeException("top", new SQLException("busy", "40001"));

    assertThat(
            Causes.anyMatch(
                top, t -> t instanceof SQLException sql && "40001".equals(sql.getSQLState())))
        .isTrue();
    assertThat(
            Causes.anyMatch(
                top, t -> t instanceof SQLException sql && "23505".equals(sql.getSQLState())))
        .isFalse();
  }

  /** {@code wrappers} plain links in front of {@code last}, which is link {@code wrappers + 1}. */
  private static Throwable chainEndingIn(Throwable last, int wrappers) {
    Throwable t = last;
    for (int i = 0; i < wrappers; i++) {
      t = new RuntimeException("wrapper " + i, t);
    }
    return t;
  }
}
