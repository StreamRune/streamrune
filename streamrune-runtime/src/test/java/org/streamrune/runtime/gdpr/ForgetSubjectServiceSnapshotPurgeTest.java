package org.streamrune.runtime.gdpr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.gdpr.SnapshotPurger;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;

/**
 * A snapshot keeps the aggregate's state in the clear wherever a field is not {@code @Encrypted},
 * so a forget that does not delete it leaves the subject's data readable. The builder refuses a
 * service that neither registers a {@link SnapshotPurger} nor states {@code
 * withoutSnapshotPurge()}.
 */
class ForgetSubjectServiceSnapshotPurgeTest {

  private final CryptoEngine crypto = mock(CryptoEngine.class);

  @Test
  void aServiceWithNoSnapshotPurgerIsRefusedWithWhatToAdd() {
    var builder =
        ForgetSubjectService.builder().cryptoEngine(crypto).purgers(List.of(new ReadModel()));

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "ForgetSubjectService has no snapshot purger. An aggregate snapshot stores the"
                + " aggregate's state as it was when the snapshot was taken, every field not"
                + " marked @Encrypted in the clear, so after a forget it still holds whatever"
                + " that state kept of the subject's data. Add one to purgers(...) — for"
                + " PostgresEventStore: new PostgresSnapshotStorePurger(dataSource, subjectId ->"
                + " the streams whose snapshots hold the subject's data); for another store, a"
                + " purger of your own that implements SnapshotPurger, as must any wrapper around"
                + " one — or call withoutSnapshotPurge() if this application has never stored a"
                + " snapshot.");
    verifyNoInteractions(crypto);
  }

  @Test
  void aServiceWithNoPurgersAtAllIsRefusedToo() {
    assertThatThrownBy(() -> ForgetSubjectService.builder().cryptoEngine(crypto).build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("ForgetSubjectService has no snapshot purger.");
  }

  @Test
  void statingWithoutSnapshotPurgeBuildsTheService() {
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .purgers(List.of(new ReadModel()))
            .withoutSnapshotPurge()
            .build();

    assertThat(service).isNotNull();
  }

  @Test
  void aRegisteredSnapshotPurgerBuildsTheServiceAndRunsOnEveryForget() {
    var snapshots = new Snapshots();
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .purgers(List.of(new ReadModel(), snapshots))
            .build();

    var result = service.forget(SubjectId.of("subject-1"), UserId.of("dpo"));

    assertThat(snapshots.purged).containsExactly(SubjectId.of("subject-1"));
    assertThat(result.purgeOutcomes())
        .extracting(ForgetResult.PurgeOutcome::purgerName)
        .containsExactly("customers_view", "snapshots");
    assertThat(result.fullyErased()).isTrue();
  }

  @Test
  void aSnapshotPurgerTogetherWithWithoutSnapshotPurgeIsContradictory() {
    var builder =
        ForgetSubjectService.builder()
            .cryptoEngine(crypto)
            .purgers(List.of(new Snapshots()))
            .withoutSnapshotPurge();

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "ForgetSubjectService was given a snapshot purger ("
                + Snapshots.class.getName()
                + ") and withoutSnapshotPurge(), which says the application has never stored a"
                + " snapshot: keep the purger and drop withoutSnapshotPurge(), or drop the purger"
                + " if the application really stores none.");
  }

  /** A read-model purger; not a snapshot purger. */
  static final class ReadModel implements SubjectDataPurger {
    @Override
    public String name() {
      return "customers_view";
    }

    @Override
    public void purge(SubjectId subjectId) {}
  }

  /** A snapshot purger that records what it purged. */
  static final class Snapshots implements SnapshotPurger {
    final List<SubjectId> purged = new ArrayList<>();

    @Override
    public String name() {
      return "snapshots";
    }

    @Override
    public void purge(SubjectId subjectId) {
      purged.add(subjectId);
    }
  }
}
