package org.streamrune.core.projection;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * {@link OffsetStore#reset(ProjectionName)}'s default implementation must fail loudly ({@link
 * UnsupportedOperationException}) rather than silently delegating to {@link
 * OffsetStore#saveOffset}, which a guarded store would reject as a no-op regression.
 */
class OffsetStoreTest {

  /** An implementation that overrides neither {@code reset} nor anything else unusual. */
  private static final class MinimalOffsetStore implements OffsetStore {
    private final Map<String, GlobalOffset> offsets = new HashMap<>();

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      return offsets.getOrDefault(projectionName.value(), GlobalOffset.initial());
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      offsets.put(projectionName.value(), offset);
    }
  }

  /**
   * Models a guarded store (mirrors PostgresOffsetStore's monotonic guard): saveOffset silently
   * no-ops a backward/sideways move. If reset() delegated to saveOffset(initial()), this store
   * would never actually rewind — the exact silent-no-op bug the real reset() avoids.
   */
  private static final class GuardedOffsetStore implements OffsetStore {
    private final Map<String, GlobalOffset> offsets = new HashMap<>();

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      return offsets.getOrDefault(projectionName.value(), GlobalOffset.initial());
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      GlobalOffset current = getLastOffset(projectionName);
      if (offset.value() > current.value()) {
        offsets.put(projectionName.value(), offset);
      }
      // else: silent no-op, exactly like the monotonic guard's documented contract.
    }
  }

  @Test
  void defaultResetThrowsForImplementationsThatDoNotOverrideIt() {
    var store = new MinimalOffsetStore();
    var name = ProjectionName.of("test-projection");

    var ex = assertThrows(UnsupportedOperationException.class, () -> store.reset(name));
    assertTrue(ex.getMessage().contains("reset"), ex.getMessage());
  }

  @Test
  void defaultResetWouldHaveSilentlyNoOppedOnAGuardedStoreBeforeTheFix() {
    // Documents *why* the default must throw rather than delegate to saveOffset: a guarded store's
    // saveOffset silently rejects the rewind-to-initial as a backward move.
    var store = new GuardedOffsetStore();
    var name = ProjectionName.of("test-projection");
    store.saveOffset(name, GlobalOffset.of(100));

    // The guard itself is silent (no exception) — proven directly here.
    store.saveOffset(name, GlobalOffset.initial());
    assertEquals(GlobalOffset.of(100), store.getLastOffset(name), "guard rejects the regression");

    // Since GuardedOffsetStore does not override reset(), the interface default now throws instead
    // of silently calling the no-op saveOffset path above.
    assertThrows(UnsupportedOperationException.class, () -> store.reset(name));
  }
}
