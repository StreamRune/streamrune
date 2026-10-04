package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.UserId;

class CachingUserRoleResolverTest {

  private UserRoleResolver delegate;
  private CachingUserRoleResolver resolver;

  private static final UserId USER_1 = UserId.of("user-1");
  private static final UserId USER_2 = UserId.of("user-2");
  private static final UserAuthority ADMIN =
      new UserAuthority(Set.of("ADMIN"), Set.of("read", "write"));
  private static final UserAuthority VIEWER = new UserAuthority(Set.of("VIEWER"), Set.of("read"));

  @BeforeEach
  void setUp() {
    delegate = mock(UserRoleResolver.class);
    resolver = new CachingUserRoleResolver(delegate);
  }

  @Test
  void cacheMissCallsDelegate() {
    when(delegate.resolve(USER_1)).thenReturn(ADMIN);

    UserAuthority result = resolver.resolve(USER_1);

    assertThat(result).isEqualTo(ADMIN);
    verify(delegate, times(1)).resolve(USER_1);
  }

  @Test
  void cacheHitSkipsDelegate() {
    when(delegate.resolve(USER_1)).thenReturn(ADMIN);

    resolver.resolve(USER_1);
    UserAuthority second = resolver.resolve(USER_1);

    assertThat(second).isEqualTo(ADMIN);
    verify(delegate, times(1)).resolve(USER_1);
  }

  @Test
  void differentUsersGetSeparateEntries() {
    when(delegate.resolve(USER_1)).thenReturn(ADMIN);
    when(delegate.resolve(USER_2)).thenReturn(VIEWER);

    assertThat(resolver.resolve(USER_1)).isEqualTo(ADMIN);
    assertThat(resolver.resolve(USER_2)).isEqualTo(VIEWER);
    verify(delegate, times(1)).resolve(USER_1);
    verify(delegate, times(1)).resolve(USER_2);
  }

  @Test
  void invalidateEvictsSpecificUser() {
    when(delegate.resolve(USER_1)).thenReturn(ADMIN);

    resolver.resolve(USER_1);
    resolver.invalidate(USER_1);

    when(delegate.resolve(USER_1)).thenReturn(VIEWER);
    UserAuthority afterInvalidate = resolver.resolve(USER_1);

    assertThat(afterInvalidate).isEqualTo(VIEWER);
    verify(delegate, times(2)).resolve(USER_1);
  }

  @Test
  void invalidateAllClearsCache() {
    when(delegate.resolve(USER_1)).thenReturn(ADMIN);
    when(delegate.resolve(USER_2)).thenReturn(VIEWER);

    resolver.resolve(USER_1);
    resolver.resolve(USER_2);
    resolver.invalidateAll();

    resolver.resolve(USER_1);
    resolver.resolve(USER_2);

    verify(delegate, times(2)).resolve(USER_1);
    verify(delegate, times(2)).resolve(USER_2);
  }

  @Test
  void nullUserIdPassesThroughWithoutCaching() {
    when(delegate.resolve(null)).thenReturn(UserAuthority.EMPTY);

    resolver.resolve(null);
    resolver.resolve(null);

    verify(delegate, times(2)).resolve(null);
  }

  @Test
  void customTtlAndMaxSize() {
    resolver = new CachingUserRoleResolver(delegate, Duration.ofSeconds(1), 100);
    when(delegate.resolve(USER_1)).thenReturn(ADMIN);

    resolver.resolve(USER_1);
    UserAuthority cached = resolver.resolve(USER_1);

    assertThat(cached).isEqualTo(ADMIN);
    verify(delegate, times(1)).resolve(USER_1);
  }

  @Test
  void concurrentAccessIsSafe() throws InterruptedException {
    AtomicInteger callCount = new AtomicInteger();
    UserRoleResolver slowDelegate =
        userId -> {
          callCount.incrementAndGet();
          try {
            Thread.sleep(10);
          } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
          }
          return ADMIN;
        };
    resolver = new CachingUserRoleResolver(slowDelegate);

    int threads = 10;
    CountDownLatch latch = new CountDownLatch(threads);
    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int i = 0; i < threads; i++) {
        executor.submit(
            () -> {
              resolver.resolve(USER_1);
              latch.countDown();
            });
      }
      latch.await();
    }

    // Caffeine coalesces concurrent loads — should be far fewer than 10
    assertThat(callCount.get()).isLessThanOrEqualTo(3);
  }
}
