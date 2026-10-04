package org.streamrune.runtime.dlqfixture;

import org.streamrune.core.Command;
import org.streamrune.core.crypto.Encrypted;

/**
 * Fixtures for {@code DeadLetterRetryRunnerTest}: command records whose components the dead-letter
 * redaction scan cannot read with a plain {@code Method.invoke}.
 *
 * <p>They live OUTSIDE {@code org.streamrune.runtime} on purpose. {@code DeadLetterRetryRunner}
 * reads record components reflectively, and a package-private record in its own package would be
 * readable through ordinary package access; only a record in another package reproduces the {@code
 * IllegalAccessException} an application's non-public command hits. Jackson deserializes all of
 * these without complaint (it overrides access modifiers), and the framework does not require a
 * command to be public.
 *
 * <p>The {@code Unreadable*} records go one step further: an explicit accessor that throws, so the
 * component cannot be read even once access is granted. The payloads for those are written from
 * public twins of the same JSON shape in the test, since serializing them would call the very
 * accessor that throws.
 */
public final class NonPublicCommands {

  /** {@link HiddenPiiCmd}: package-private, one {@code @Encrypted} field. */
  public static final Class<? extends Command> HIDDEN_PII_CMD = HiddenPiiCmd.class;

  /**
   * {@link HiddenNestedPiiCmd}: package-private, its {@code @Encrypted} field on a nested record.
   */
  public static final Class<? extends Command> HIDDEN_NESTED_PII_CMD = HiddenNestedPiiCmd.class;

  /** {@link UnreadablePiiCmd}: the {@code @Encrypted} component's accessor throws. */
  public static final Class<? extends Command> UNREADABLE_PII_CMD = UnreadablePiiCmd.class;

  /** {@link UnreadableNestedPiiCmd}: the accessor of the record on the path to one throws. */
  public static final Class<? extends Command> UNREADABLE_NESTED_PII_CMD =
      UnreadableNestedPiiCmd.class;

  /**
   * {@link UnreadableAtomicPiiCmd}: the accessor of the {@code AtomicReference} holding a record
   * with an {@code @Encrypted} field throws.
   */
  public static final Class<? extends Command> UNREADABLE_ATOMIC_PII_CMD =
      UnreadableAtomicPiiCmd.class;

  /** {@link UnreadableLabelCmd}: only a plain String component (no PII path) is unreadable. */
  public static final Class<? extends Command> UNREADABLE_LABEL_CMD = UnreadableLabelCmd.class;

  /**
   * {@link UnreadableMalformedPathCmd}: the unreadable component's type declares a malformed
   * {@code @Encrypted} field, so the type walk that would clear it throws instead of answering.
   */
  public static final Class<? extends Command> UNREADABLE_MALFORMED_PATH_CMD =
      UnreadableMalformedPathCmd.class;

  private NonPublicCommands() {}

  record HiddenPiiCmd(String customerId, @Encrypted(subjectId = "customerId") String email)
      implements Command {}

  record HiddenBilling(String subjectId, @Encrypted(subjectId = "subjectId") String cardHolder) {}

  record HiddenNestedPiiCmd(String customerId, HiddenBilling billing) implements Command {}

  record UnreadablePiiCmd(String customerId, @Encrypted(subjectId = "customerId") String email)
      implements Command {
    @Override
    public String email() {
      throw new IllegalStateException("email accessor unavailable");
    }
  }

  record UnreadableNestedPiiCmd(String customerId, HiddenBilling billing) implements Command {
    @Override
    public HiddenBilling billing() {
      throw new IllegalStateException("billing accessor unavailable");
    }
  }

  record UnreadableAtomicPiiCmd(
      String customerId, java.util.concurrent.atomic.AtomicReference<HiddenBilling> billing)
      implements Command {
    @Override
    public java.util.concurrent.atomic.AtomicReference<HiddenBilling> billing() {
      throw new IllegalStateException("billing accessor unavailable");
    }
  }

  /** Names a subject component that does not exist: CryptoShreddingModule would reject it. */
  record MalformedPii(String subjectId, @Encrypted(subjectId = "missing") String secret) {}

  record UnreadableMalformedPathCmd(String customerId, MalformedPii pii) implements Command {
    @Override
    public MalformedPii pii() {
      throw new IllegalStateException("pii accessor unavailable");
    }
  }

  record UnreadableLabelCmd(
      String customerId, @Encrypted(subjectId = "customerId") String email, String label)
      implements Command {
    @Override
    public String label() {
      throw new IllegalStateException("label accessor unavailable");
    }
  }
}
