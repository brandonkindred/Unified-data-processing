package com.unifieddataprocessing.pubsub.schema;

import java.util.List;
import java.util.Objects;

/**
 * Outcome of a {@link SchemaValidator} call: a {@code valid} flag plus a defensive copy of any
 * error messages.
 *
 * <p>Successful validation carries an empty {@code errors} list; failures carry at least one
 * human-readable message. The list is copied in the compact constructor so callers cannot mutate
 * state after construction.
 */
public record ValidationResult(boolean valid, List<String> errors) {

  public ValidationResult {
    Objects.requireNonNull(errors, "errors");
    errors = List.copyOf(errors);
  }

  /** Convenience factory for a passing result with no errors. */
  public static ValidationResult ok() {
    return new ValidationResult(true, List.of());
  }

  /** Convenience factory for a failing result carrying a single error message. */
  public static ValidationResult fail(String error) {
    Objects.requireNonNull(error, "error");
    return new ValidationResult(false, List.of(error));
  }

  /** Convenience factory for a failing result carrying multiple error messages. */
  public static ValidationResult fail(List<String> errors) {
    return new ValidationResult(false, errors);
  }
}
