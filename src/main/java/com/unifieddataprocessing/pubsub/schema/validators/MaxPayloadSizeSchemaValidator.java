package com.unifieddataprocessing.pubsub.schema.validators;

import com.unifieddataprocessing.pubsub.schema.Schema;
import com.unifieddataprocessing.pubsub.schema.SchemaValidator;
import com.unifieddataprocessing.pubsub.schema.ValidationResult;
import java.util.Objects;

/**
 * {@link SchemaValidator} that rejects payloads strictly larger than a configured byte budget.
 *
 * <p>Independent of {@link Schema#definition()} — useful as a safety net on every topic when a
 * real per-type validator is not yet wired up, or as one branch of a {@link
 * CompositeSchemaValidator} dispatch table.
 */
public final class MaxPayloadSizeSchemaValidator implements SchemaValidator {

  private final int maxBytes;

  /** Constructs a validator that rejects any payload larger than {@code maxBytes} bytes. */
  public MaxPayloadSizeSchemaValidator(int maxBytes) {
    if (maxBytes < 0) {
      throw new IllegalArgumentException("maxBytes must be >= 0, got " + maxBytes);
    }
    this.maxBytes = maxBytes;
  }

  @Override
  public ValidationResult validate(Schema schema, byte[] payload) {
    Objects.requireNonNull(payload, "payload");
    if (payload.length > maxBytes) {
      return ValidationResult.fail(
          "payload size " + payload.length + " exceeds max " + maxBytes + " bytes");
    }
    return ValidationResult.ok();
  }
}
