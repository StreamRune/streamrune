package org.streamrune.core.backend;

import java.io.Serial;

/**
 * An {@link EventStoreOptions} value the backend cannot honour, for example a statement timeout
 * shorter than the backend can apply. {@link #option()} names the component, so an integration can
 * report the error under its own configuration property.
 *
 * <p>A store that is not the backend's own type is not an option value the backend cannot honour:
 * {@link BackendStores#eventStoreFactory(EventStoreOptions)} refuses it with a plain {@link
 * IllegalArgumentException}.
 */
public final class BackendOptionException extends IllegalArgumentException {

  @Serial private static final long serialVersionUID = 1L;

  /** The {@link EventStoreOptions} component the backend refused. */
  private final String option;

  /**
   * An option the backend refused.
   *
   * @param option the {@link EventStoreOptions} component name, for example {@code
   *     "statementTimeout"}; not blank
   * @param message why the backend cannot honour the value
   * @throws IllegalArgumentException if {@code option} is {@code null} or blank
   */
  public BackendOptionException(String option, String message) {
    this(option, message, null);
  }

  /**
   * An option the backend refused, with the exception that reported it.
   *
   * @param option the {@link EventStoreOptions} component name, for example {@code
   *     "statementTimeout"}; not blank
   * @param message why the backend cannot honour the value
   * @param cause the exception that reported the refusal; {@code null} for none
   * @throws IllegalArgumentException if {@code option} is {@code null} or blank
   */
  public BackendOptionException(String option, String message, Throwable cause) {
    super(message, cause);
    this.option = requireOption(option);
  }

  private static String requireOption(String option) {
    if (option == null || option.isBlank()) {
      throw new IllegalArgumentException("option is required and must not be blank");
    }
    return option;
  }

  /**
   * The {@link EventStoreOptions} component the backend refused.
   *
   * @return the component name, for example {@code "statementTimeout"}; never blank
   */
  public String option() {
    return option;
  }
}
