package org.streamrune.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.gdpr.SnapshotPurger;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.SubjectId;

/** {@code streamrune.gdpr.snapshot-purge}, checked against the purger beans. */
class SnapshotPurgeSettingTest {

  private static final SubjectDataPurger READ_MODEL =
      new SubjectDataPurger() {
        @Override
        public String name() {
          return "customers_view";
        }

        @Override
        public void purge(SubjectId subjectId) {}
      };

  static final class Snapshots implements SnapshotPurger {
    @Override
    public String name() {
      return "snapshot_store";
    }

    @Override
    public void purge(SubjectId subjectId) {}
  }

  @Test
  void absentOrBlankMeansRequired() {
    assertThat(SnapshotPurgeSetting.parse(null)).isEqualTo(SnapshotPurgeSetting.REQUIRED);
    assertThat(SnapshotPurgeSetting.parse(" ")).isEqualTo(SnapshotPurgeSetting.REQUIRED);
  }

  @Test
  void bothValuesParseCaseInsensitively() {
    assertThat(SnapshotPurgeSetting.parse(" Required ")).isEqualTo(SnapshotPurgeSetting.REQUIRED);
    assertThat(SnapshotPurgeSetting.parse("NONE")).isEqualTo(SnapshotPurgeSetting.NONE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"off", "false", "maybe"})
  void anUnknownValueIsRefusedNamingTheProperty(String value) {
    assertThatThrownBy(() -> SnapshotPurgeSetting.parse(value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "streamrune.gdpr.snapshot-purge must be required or none (case-insensitive); got: '"
                + value
                + "'");
  }

  @Test
  void requiredWithASnapshotPurgerBuildsWithThePurger() {
    assertThat(
            SnapshotPurgeSetting.REQUIRED.withoutSnapshotPurge(
                List.of(READ_MODEL, new Snapshots())))
        .isFalse();
  }

  @Test
  void requiredWithoutASnapshotPurgerIsRefusedWithTheBeanAndThePropertyToSet() {
    assertThatThrownBy(
            () -> SnapshotPurgeSetting.REQUIRED.withoutSnapshotPurge(List.of(READ_MODEL)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "The ForgetSubjectService StreamRune configures has no snapshot purger. An aggregate"
                + " snapshot stores the aggregate's state as it was when the snapshot was taken,"
                + " every field not marked @Encrypted in the clear, so after a forget it still"
                + " holds whatever that state kept of the subject's data. Declare a SnapshotPurger"
                + " bean — for PostgresEventStore: new PostgresSnapshotStorePurger(dataSource,"
                + " subjectId -> the streams whose snapshots hold the subject's data); for another"
                + " store, a purger of your own that implements SnapshotPurger. A wrapper or proxy"
                + " around a snapshot purger must implement SnapshotPurger itself. The"
                + " ForgetSubjectService runs it with the other SubjectDataPurger beans — or set"
                + " streamrune.gdpr.snapshot-purge=none if this application has never stored a"
                + " snapshot.");
  }

  @Test
  void noneWithoutASnapshotPurgerBuildsWithoutSnapshotPurge() {
    assertThat(SnapshotPurgeSetting.NONE.withoutSnapshotPurge(List.of(READ_MODEL))).isTrue();
    assertThat(SnapshotPurgeSetting.NONE.withoutSnapshotPurge(List.of())).isTrue();
  }

  @Test
  void noneWithASnapshotPurgerIsContradictory() {
    assertThatThrownBy(
            () -> SnapshotPurgeSetting.NONE.withoutSnapshotPurge(List.of(new Snapshots())))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "streamrune.gdpr.snapshot-purge=none says this application has never stored a"
                + " snapshot, but a snapshot purger bean is registered ("
                + Snapshots.class.getName()
                + "): keep the bean and remove streamrune.gdpr.snapshot-purge=none, or remove the"
                + " bean if the application really stores none.");
  }
}
